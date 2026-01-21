#!/usr/bin/env python3
"""
Lyrics Fetcher & Language Detection Script for MongoDB - REFACTORED

Processes songs with enrichment_status: 'pending', fetches lyrics using Genius API,
and re-runs language detection with lyrics for better accuracy.

Key improvements:
- Uses enrichment_status field instead of has_lyrics: null
- Uses shared utilities (MongoDBConnection, LanguageDetector)
- Re-runs language detection WITH lyrics for better accuracy
- Sets enrichment_status: 'processed' or 'failed' on completion

Collections:
- songs_master: Target for lyrics and language updates
- artists_master: Target for artist language updates
"""

import os
import time
import logging
from typing import Optional, Tuple
import lyricsgenius
from dotenv import load_dotenv

from src.utils.database import MongoDBConnection
from src.utils.language_detection import LanguageDetector, SoundtrackClassifier
from src.utils.config import (
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    MIN_LYRICS_LENGTH,
    ENRICHMENT_STATUS_PENDING,
    ENRICHMENT_STATUS_PROCESSED,
    ENRICHMENT_STATUS_FAILED,
)

load_dotenv()
logger = logging.getLogger(__name__)

# Genius API token
GENIUS_TOKEN = os.getenv('GENIUS_TOKEN')


def initialize_genius_api():
    """Initialize Genius API client."""
    if not GENIUS_TOKEN:
        logger.error("GENIUS_TOKEN not found in environment variables")
        return None

    try:
        genius = lyricsgenius.Genius(GENIUS_TOKEN)
        genius.verbose = False
        genius.remove_section_headers = False
        logger.info("Successfully connected to Genius API")
        return genius
    except Exception as e:
        logger.error(f"Error initializing Genius API: {e}")
        return None


def get_pending_songs(db_connection: MongoDBConnection):
    """
    Get songs pending enrichment.

    Queries by enrichment_status: 'pending' or missing enrichment_status
    for backwards compatibility with existing data.
    """
    try:
        collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

        # Query for pending songs OR songs without enrichment_status (legacy)
        songs = list(collection.find({
            "$or": [
                {"enrichment_status": ENRICHMENT_STATUS_PENDING},
                {"enrichment_status": {"$exists": False}},
                {"enrichment_status": None}
            ]
        }))

        logger.info(f"Found {len(songs)} songs pending enrichment")
        return songs
    except Exception as e:
        logger.error(f"Error getting pending songs: {e}")
        return []


def fetch_lyrics(genius, song_name: str, artist_name: str) -> Tuple[Optional[str], bool]:
    """
    Fetch lyrics for a single song.

    Returns:
        Tuple of (lyrics_text, has_lyrics_boolean).
    """
    try:
        logger.debug(f"Searching for lyrics: '{song_name}' by '{artist_name}'")

        song = genius.search_song(song_name, artist_name)

        if song and song.lyrics:
            lyrics = song.lyrics.strip()

            if len(lyrics) >= MIN_LYRICS_LENGTH:
                logger.debug(f"Found lyrics ({len(lyrics)} characters)")
                return lyrics, True
            else:
                logger.debug(f"Lyrics too short ({len(lyrics)} chars, min {MIN_LYRICS_LENGTH})")
                return None, False
        else:
            logger.debug("No lyrics found")
            return None, False

    except Exception as e:
        logger.debug(f"Error fetching lyrics: {e}")
        return None, False


def update_song_enrichment(
    db_connection: MongoDBConnection,
    song_id,
    lyrics: Optional[str],
    has_lyrics: bool,
    language: str,
    detection_method: str,
    is_soundtrack: bool,
    success: bool
) -> bool:
    """
    Update song record with enrichment results.

    Sets enrichment_status to 'processed' or 'failed' based on success.
    """
    try:
        collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

        update_data = {
            "has_lyrics": has_lyrics,
            "language": language,
            "detection_method": detection_method,
            "is_soundtrack": is_soundtrack,
            "enrichment_status": ENRICHMENT_STATUS_PROCESSED if success else ENRICHMENT_STATUS_FAILED,
        }

        if lyrics:
            update_data["lyrics"] = lyrics

        result = collection.update_one(
            {"_id": song_id},
            {"$set": update_data}
        )

        return result.modified_count > 0

    except Exception as e:
        logger.error(f"Error updating song in database: {e}")
        return False


def update_artist_language(
    db_connection: MongoDBConnection,
    artist_name: str,
    language: str,
    detection_method: str
) -> bool:
    """
    Update language for an artist if not already set.

    Only updates artists with null/Unknown/missing language.
    """
    try:
        collection = db_connection.get_collection(ARTISTS_MASTER_COLLECTION)

        result = collection.update_one(
            {
                "artist_name": artist_name,
                "$or": [
                    {"language": None},
                    {"language": "Unknown"},
                    {"language": {"$exists": False}}
                ]
            },
            {"$set": {
                "language": language,
                "detection_method": detection_method
            }}
        )

        if result.modified_count > 0:
            logger.info(f"  Updated artist language: {artist_name} -> {language}")

        return result.modified_count > 0

    except Exception as e:
        logger.error(f"Error updating artist language: {e}")
        return False


def fix_soundtrack_language_issues(db_connection: MongoDBConnection):
    """Fix existing soundtrack songs/artists that don't have language: 'Soundtrack'."""
    try:
        songs_collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)
        artists_collection = db_connection.get_collection(ARTISTS_MASTER_COLLECTION)

        logger.info("Fixing soundtrack language issues...")

        # Fix songs with is_soundtrack: true but language != "Soundtrack"
        songs_fixed = 0
        soundtrack_songs = songs_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        })

        for song in soundtrack_songs:
            result = songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )

            if result.modified_count > 0:
                songs_fixed += 1
                old_lang = song.get('language', 'None')
                logger.info(f"  SONG: '{song['song_name']}' by {song['artist_name']}: {old_lang} -> Soundtrack")

        # Fix artists with is_soundtrack: true but language != "Soundtrack"
        artists_fixed = 0
        soundtrack_artists = artists_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        })

        for artist in soundtrack_artists:
            result = artists_collection.update_one(
                {"_id": artist["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )

            if result.modified_count > 0:
                artists_fixed += 1
                old_lang = artist.get('language', 'None')
                logger.info(f"  ARTIST: {artist['artist_name']}: {old_lang} -> Soundtrack")

        if songs_fixed == 0 and artists_fixed == 0:
            logger.info("  No soundtrack language issues found")
        else:
            logger.info(f"Fixed {songs_fixed} songs and {artists_fixed} artists with soundtrack issues")

    except Exception as e:
        logger.error(f"Error fixing soundtrack language issues: {e}")


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("LYRICS ENRICHMENT SCRIPT")
    logger.info("=" * 60)
    logger.info(f"Processing: Songs with enrichment_status: '{ENRICHMENT_STATUS_PENDING}'")
    logger.info("=" * 60)

    # Initialize Genius API
    genius = initialize_genius_api()
    if not genius:
        return

    try:
        with MongoDBConnection() as db_conn:
            # Get pending songs
            pending_songs = get_pending_songs(db_conn)

            if not pending_songs:
                logger.info("No pending songs found! All songs have been enriched.")
                # Still run soundtrack fix in case there are issues
                fix_soundtrack_language_issues(db_conn)
                return

            logger.info(f"Processing {len(pending_songs)} songs...")

            processed_count = 0
            lyrics_found_count = 0
            soundtrack_count = 0
            updated_artists = set()

            for i, song in enumerate(pending_songs, 1):
                song_id = song['_id']
                song_name = song.get('song_name', '')
                artist_name = song.get('artist_name', '')
                album_name = song.get('album_name', '')

                # Get existing is_soundtrack or re-classify
                is_soundtrack = song.get('is_soundtrack', False)
                if not is_soundtrack:
                    is_soundtrack = SoundtrackClassifier.classify_song(
                        song_name, artist_name, album_name, ""
                    )

                logger.info(f"[{i}/{len(pending_songs)}] Processing: '{song_name}' by {artist_name}")

                if is_soundtrack:
                    soundtrack_count += 1
                    logger.info("  Classified as soundtrack")

                # Fetch lyrics (skip if soundtrack)
                lyrics = None
                has_lyrics = False

                if not is_soundtrack:
                    lyrics, has_lyrics = fetch_lyrics(genius, song_name, artist_name)
                    if has_lyrics:
                        lyrics_found_count += 1
                    time.sleep(0.1)  # Rate limiting
                else:
                    logger.info("  Skipping lyrics fetch for soundtrack")

                # Re-run language detection WITH lyrics for better accuracy
                # This may refine the initial detection from process_new_content
                language, detection_method = LanguageDetector.detect_language(
                    song_name=song_name,
                    artist_name=artist_name,
                    lyrics=lyrics,
                    is_soundtrack=is_soundtrack,
                    genres="",
                    album_name=album_name
                )

                logger.info(f"  Language: {language} (method: {detection_method})")

                # Update database
                success = update_song_enrichment(
                    db_conn, song_id, lyrics, has_lyrics,
                    language, detection_method, is_soundtrack,
                    success=True  # Mark as processed even if no lyrics found
                )

                if success:
                    processed_count += 1
                    logger.info(f"  Updated in database (enrichment_status: {ENRICHMENT_STATUS_PROCESSED})")

                    # Update artist language if needed
                    if language not in ['Unknown', 'Soundtrack'] and artist_name not in updated_artists:
                        update_artist_language(db_conn, artist_name, language, detection_method)
                        updated_artists.add(artist_name)
                else:
                    logger.error(f"  Failed to update database")

            # Fix any soundtrack language issues
            fix_soundtrack_language_issues(db_conn)

            # Summary
            logger.info("=" * 60)
            logger.info("ENRICHMENT COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Songs processed: {processed_count}/{len(pending_songs)}")
            logger.info(f"Lyrics found: {lyrics_found_count}")
            logger.info(f"Soundtracks identified: {soundtrack_count}")
            logger.info(f"Artist languages updated: {len(updated_artists)}")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
