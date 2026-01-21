#!/usr/bin/env python3
"""
One-time Migration Script: Populate Album Names

Finds songs in songs_master with missing album_name and populates them from:
1. StreamingHistory (using spotify_track_uri to match)
2. Spotify API (as fallback)

This is a one-time migration script to backfill album_name for existing songs.
New songs added by process_new_content.py will have album_name set automatically.

Usage:
    python -m src.migrations.populate_album_names
"""

import time
import logging
from typing import Dict, List

from src.utils.database import MongoDBConnection
from src.utils.spotify_api import SpotifyClient
from src.utils.config import (
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
    STREAMING_FIELDS,
    SPOTIFY_BATCH_SIZE,
    REQUEST_DELAY,
)

logger = logging.getLogger(__name__)


def get_songs_missing_album(db_connection: MongoDBConnection) -> List[Dict]:
    """Get all songs from songs_master that are missing album_name."""
    collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

    songs = list(collection.find({
        "$or": [
            {"album_name": None},
            {"album_name": ""},
            {"album_name": {"$exists": False}}
        ]
    }, {
        "_id": 1,
        "song_name": 1,
        "artist_name": 1,
        "spotify_track_uri": 1
    }))

    logger.info(f"Found {len(songs)} songs missing album_name")
    return songs


def build_streaming_album_map(db_connection: MongoDBConnection) -> Dict[str, str]:
    """
    Build a mapping of spotify_track_uri -> album_name from StreamingHistory.

    Uses the new field name: master_metadata_album_album_name
    """
    collection = db_connection.get_collection(STREAMING_COLLECTION)

    album_map = {}

    # Get all unique track URIs with their album names
    pipeline = [
        {"$match": {
            "spotify_track_uri": {"$exists": True, "$ne": None, "$ne": ""},
            STREAMING_FIELDS['album_name']: {"$exists": True, "$ne": None, "$ne": ""}
        }},
        {"$group": {
            "_id": "$spotify_track_uri",
            "album_name": {"$first": f"${STREAMING_FIELDS['album_name']}"}
        }}
    ]

    results = collection.aggregate(pipeline)

    for doc in results:
        album_map[doc["_id"]] = doc["album_name"]

    logger.info(f"Built album mapping from StreamingHistory with {len(album_map)} entries")
    return album_map


def update_songs_from_streaming(
    db_connection: MongoDBConnection,
    songs: List[Dict],
    album_map: Dict[str, str]
) -> tuple:
    """
    Update songs with album_name from StreamingHistory mapping.

    Returns:
        Tuple of (updated_count, remaining_songs)
    """
    collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

    updated_count = 0
    remaining_songs = []

    for song in songs:
        track_uri = song.get("spotify_track_uri", "")

        if track_uri in album_map:
            album_name = album_map[track_uri]

            result = collection.update_one(
                {"_id": song["_id"]},
                {"$set": {"album_name": album_name}}
            )

            if result.modified_count > 0:
                updated_count += 1
                logger.debug(f"Updated '{song['song_name']}' -> album: {album_name}")
        else:
            remaining_songs.append(song)

    logger.info(f"Updated {updated_count} songs from StreamingHistory")
    logger.info(f"Remaining songs needing Spotify API: {len(remaining_songs)}")

    return updated_count, remaining_songs


def update_songs_from_spotify_api(
    db_connection: MongoDBConnection,
    songs: List[Dict]
) -> int:
    """
    Update remaining songs with album_name from Spotify API.

    Returns:
        Number of songs updated.
    """
    if not songs:
        return 0

    collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

    # Initialize Spotify client
    spotify = SpotifyClient(use_user_auth=False)
    if not spotify.authenticate():
        logger.error("Failed to authenticate with Spotify API")
        return 0

    updated_count = 0

    # Process in batches
    for i in range(0, len(songs), SPOTIFY_BATCH_SIZE):
        batch = songs[i:i + SPOTIFY_BATCH_SIZE]
        track_uris = [s["spotify_track_uri"] for s in batch if s.get("spotify_track_uri")]

        if not track_uris:
            continue

        logger.info(f"Fetching batch {i // SPOTIFY_BATCH_SIZE + 1}: {len(track_uris)} tracks")

        # Get track details from Spotify
        batch_results = spotify.get_batch_tracks(track_uris)
        time.sleep(REQUEST_DELAY)

        for song in batch:
            track_uri = song.get("spotify_track_uri", "")
            result = batch_results.get(track_uri, {})

            if result.get("status") == "success" and result.get("album_name"):
                album_name = result["album_name"]

                update_result = collection.update_one(
                    {"_id": song["_id"]},
                    {"$set": {"album_name": album_name}}
                )

                if update_result.modified_count > 0:
                    updated_count += 1
                    logger.debug(f"Updated '{song['song_name']}' -> album: {album_name}")
            else:
                logger.warning(
                    f"Could not get album for '{song['song_name']}' by {song['artist_name']}"
                )

    logger.info(f"Updated {updated_count} songs from Spotify API")
    return updated_count


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("ALBUM NAME MIGRATION SCRIPT")
    logger.info("=" * 60)
    logger.info("Populating missing album_name fields in songs_master")
    logger.info("=" * 60)

    try:
        with MongoDBConnection() as db_conn:
            # Step 1: Get songs missing album_name
            songs_missing_album = get_songs_missing_album(db_conn)

            if not songs_missing_album:
                logger.info("All songs already have album_name set!")
                return

            # Step 2: Build album mapping from StreamingHistory
            album_map = build_streaming_album_map(db_conn)

            # Step 3: Update songs from StreamingHistory
            from_streaming, remaining = update_songs_from_streaming(
                db_conn, songs_missing_album, album_map
            )

            # Step 4: Update remaining songs from Spotify API
            from_api = update_songs_from_spotify_api(db_conn, remaining)

            # Summary
            total_missing = len(songs_missing_album)
            total_updated = from_streaming + from_api
            not_found = total_missing - total_updated

            logger.info("=" * 60)
            logger.info("MIGRATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Songs missing album_name: {total_missing}")
            logger.info(f"Updated from StreamingHistory: {from_streaming}")
            logger.info(f"Updated from Spotify API: {from_api}")
            logger.info(f"Total updated: {total_updated}")
            logger.info(f"Not found: {not_found}")

            if not_found > 0:
                logger.warning(
                    f"{not_found} songs could not be updated. "
                    "These may need manual review."
                )

    except Exception as e:
        logger.error(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
