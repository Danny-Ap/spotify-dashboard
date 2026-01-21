#!/usr/bin/env python3
"""
New Content Processor

Processes the 50 most recent songs from StreamingHistory collection,
identifies new songs/artists not in master collections, fetches Spotify details,
performs language detection, and adds them to songs_master and artists_master.

Key improvement: Language detection happens IMMEDIATELY when adding new songs,
not in a separate enrichment step. This prevents language: null issues.

Collections:
- StreamingHistory: Source of recent listening data
- songs_master: Target for unique songs
- artists_master: Target for unique artists
"""

import time
import logging
from typing import List, Dict, Tuple, Any

from src.utils.database import MongoDBConnection
from src.utils.spotify_api import SpotifyClient
from src.utils.language_detection import LanguageDetector, SoundtrackClassifier
from src.utils.config import (
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    STREAMING_FIELDS,
    ENRICHMENT_STATUS_PENDING,
    REQUEST_DELAY,
)

logger = logging.getLogger(__name__)


class ContentProcessor:
    """Processes new songs and artists from streaming history."""

    def __init__(self, db_connection: MongoDBConnection):
        """
        Initialize processor with database connection.

        Args:
            db_connection: Active MongoDB connection.
        """
        self.db = db_connection
        self.spotify = SpotifyClient(use_user_auth=False)  # Client credentials for public API

        if not self.spotify.authenticate():
            raise RuntimeError("Failed to authenticate with Spotify API")

    def get_recent_streaming_records(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get the most recent records from StreamingHistory collection.

        Args:
            limit: Maximum number of records to retrieve.

        Returns:
            List of streaming records sorted by timestamp descending.
        """
        try:
            collection = self.db.get_collection(STREAMING_COLLECTION)

            # Get most recent records sorted by timestamp descending
            records = list(collection.find().sort("ts_utc", -1).limit(limit))

            logger.info(f"Retrieved {len(records)} recent streaming records")
            return records

        except Exception as e:
            logger.error(f"Error getting streaming records: {e}")
            return []

    def get_existing_master_data(self) -> Tuple[Dict, Dict]:
        """
        Get existing songs and artists from master collections.

        Returns:
            Tuple of (existing_songs dict, existing_artists dict).
            Keys are lowercased for case-insensitive comparison.
        """
        try:
            songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)
            artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)

            # Get all existing songs (using song_name + artist_name as key)
            existing_songs = {}
            for song in songs_collection.find({}, {"song_name": 1, "artist_name": 1}):
                key = (song["song_name"].lower().strip(), song["artist_name"].lower().strip())
                existing_songs[key] = True

            # Get all existing artists (using artist_name as key)
            existing_artists = {}
            for artist in artists_collection.find({}, {"artist_name": 1}):
                key = artist["artist_name"].lower().strip()
                existing_artists[key] = True

            logger.info(
                f"Found {len(existing_songs)} existing songs and "
                f"{len(existing_artists)} existing artists in master collections"
            )
            return existing_songs, existing_artists

        except Exception as e:
            logger.error(f"Error getting existing master data: {e}")
            return {}, {}

    def identify_new_content(
        self,
        streaming_records: List[Dict[str, Any]],
        existing_songs: Dict,
        existing_artists: Dict
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Identify new songs and artists from streaming records.

        Uses the new field names from StreamingHistory:
        - master_metadata_track_name
        - master_metadata_album_artist_name
        - master_metadata_album_album_name

        Args:
            streaming_records: Recent streaming records.
            existing_songs: Dict of existing songs (keys are lowercased).
            existing_artists: Dict of existing artists (keys are lowercased).

        Returns:
            Tuple of (new_songs list, new_artists list).
        """
        new_songs = {}  # Use dict to avoid duplicates
        new_artists = {}

        for record in streaming_records:
            # Use new field names
            track_name = record.get(STREAMING_FIELDS['track_name'], "")
            artist_name = record.get(STREAMING_FIELDS['artist_name'], "")
            album_name = record.get(STREAMING_FIELDS['album_name'], "")
            track_uri = record.get("spotify_track_uri", "")

            if not track_name or not artist_name or not track_uri:
                continue

            # Check for new songs
            song_key = (track_name.lower().strip(), artist_name.lower().strip())
            if song_key not in existing_songs and song_key not in new_songs:
                new_songs[song_key] = {
                    "song_name": track_name,
                    "artist_name": artist_name,
                    "album_name": album_name,  # Include album from streaming history
                    "spotify_track_uri": track_uri
                }

            # Check for new artists
            artist_key = artist_name.lower().strip()
            if artist_key not in existing_artists and artist_key not in new_artists:
                new_artists[artist_key] = {
                    "artist_name": artist_name,
                    "spotify_track_uri": track_uri
                }

        new_songs_list = list(new_songs.values())
        new_artists_list = list(new_artists.values())

        logger.info(f"Identified {len(new_songs_list)} new songs and {len(new_artists_list)} new artists")

        # Log new items found
        if new_songs_list:
            logger.info("NEW SONGS FOUND:")
            for i, song in enumerate(new_songs_list, 1):
                logger.info(f"  {i}. '{song['song_name']}' by {song['artist_name']}")

        if new_artists_list:
            logger.info("NEW ARTISTS FOUND:")
            for i, artist in enumerate(new_artists_list, 1):
                logger.info(f"  {i}. {artist['artist_name']}")

        return new_songs_list, new_artists_list

    def process_new_songs(self, new_songs: List[Dict]) -> int:
        """
        Process new songs with Spotify API, language detection, and insert into songs_master.

        Key improvement: Language detection happens HERE, not in separate enrichment step.

        Args:
            new_songs: List of new songs to process.

        Returns:
            Number of songs successfully inserted.
        """
        if not new_songs:
            logger.info("No new songs to process")
            return 0

        logger.info(f"Processing {len(new_songs)} new songs...")

        # Extract track IDs and fetch Spotify details
        track_ids = [song['spotify_track_uri'] for song in new_songs]
        batch_results = self.spotify.get_batch_tracks(track_ids)
        time.sleep(REQUEST_DELAY)

        songs_to_insert = []

        for i, song in enumerate(new_songs):
            track_uri = song['spotify_track_uri']
            spotify_result = batch_results.get(track_uri, {})

            # Get album name (prefer Spotify API, fallback to streaming history)
            album_name = None
            if spotify_result.get('status') == 'success':
                album_name = spotify_result.get('album_name')
            if not album_name:
                album_name = song.get('album_name')

            # Classify as soundtrack
            is_soundtrack = SoundtrackClassifier.classify_song(
                song['song_name'],
                song['artist_name'],
                album_name or "",
                ""  # Genres not available yet
            )

            # IMMEDIATE LANGUAGE DETECTION - This is the key improvement!
            # Detect language now instead of waiting for lyrics enrichment
            language, detection_method = LanguageDetector.detect_language(
                song_name=song['song_name'],
                artist_name=song['artist_name'],
                lyrics=None,  # No lyrics yet - will be refined in enrichment
                is_soundtrack=is_soundtrack,
                genres="",  # Genres not available for songs
                album_name=album_name or ""
            )

            if spotify_result.get('status') == 'success':
                song_record = {
                    "song_name": song['song_name'],
                    "artist_name": song['artist_name'],
                    "album_name": album_name,
                    "spotify_track_uri": song['spotify_track_uri'],
                    "duration_ms": spotify_result.get('duration_ms'),
                    "duration_s": spotify_result.get('duration_s'),
                    "release_date": spotify_result.get('release_date'),
                    "release_date_year": spotify_result.get('release_date_year'),
                    "popularity": spotify_result.get('popularity'),
                    "is_soundtrack": is_soundtrack,
                    "has_lyrics": None,  # Will be set by lyrics enrichment
                    "lyrics": None,
                    "language": language,  # Set NOW, not later!
                    "detection_method": detection_method,
                    "enrichment_status": ENRICHMENT_STATUS_PENDING,  # Track enrichment state
                }
                logger.info(
                    f"  {i+1}/{len(new_songs)}: '{song['song_name']}' by {song['artist_name']} "
                    f"-> {language} ({detection_method})"
                )
            else:
                # Handle API failure - still add with language detection
                song_record = {
                    "song_name": song['song_name'],
                    "artist_name": song['artist_name'],
                    "album_name": album_name,
                    "spotify_track_uri": song['spotify_track_uri'],
                    "duration_ms": None,
                    "duration_s": None,
                    "release_date": None,
                    "release_date_year": None,
                    "popularity": None,
                    "is_soundtrack": is_soundtrack,
                    "has_lyrics": None,
                    "lyrics": None,
                    "language": language,  # Still set language!
                    "detection_method": detection_method,
                    "enrichment_status": ENRICHMENT_STATUS_PENDING,
                }
                error_msg = spotify_result.get('error_message', 'Unknown error')
                logger.warning(
                    f"  {i+1}/{len(new_songs)}: '{song['song_name']}' by {song['artist_name']} "
                    f"- API error: {error_msg}, language: {language}"
                )

            songs_to_insert.append(song_record)

        # Insert new songs
        if songs_to_insert:
            songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)
            result = songs_collection.insert_many(songs_to_insert)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} new songs into {SONGS_MASTER_COLLECTION}")
            return inserted_count

        return 0

    def process_new_artists(self, new_artists: List[Dict]) -> int:
        """
        Process new artists with Spotify API, language detection, and insert into artists_master.

        Args:
            new_artists: List of new artists to process.

        Returns:
            Number of artists successfully inserted.
        """
        if not new_artists:
            logger.info("No new artists to process")
            return 0

        logger.info(f"Processing {len(new_artists)} new artists...")

        # First, get artist IDs from track data
        track_ids = [artist['spotify_track_uri'] for artist in new_artists]
        track_results = self.spotify.get_batch_tracks(track_ids)
        time.sleep(REQUEST_DELAY)

        # Map artist names to their IDs
        artist_id_mapping = {}
        for artist in new_artists:
            track_uri = artist['spotify_track_uri']
            track_result = track_results.get(track_uri, {})

            if track_result.get('status') == 'success':
                # Find the matching artist from the track's artists
                for artist_info in track_result.get('artists', []):
                    if artist_info['name'].lower().strip() == artist['artist_name'].lower().strip():
                        artist_id_mapping[artist['artist_name']] = {
                            'id': artist_info['id'],
                            'uri': artist_info['uri']
                        }
                        break

        # Get artist details from Spotify
        artist_ids = [info['id'] for info in artist_id_mapping.values()]
        artist_results = {}
        if artist_ids:
            artist_results = self.spotify.get_batch_artists(artist_ids)
            time.sleep(REQUEST_DELAY)

        artists_to_insert = []

        for i, artist in enumerate(new_artists):
            artist_name = artist['artist_name']
            genres = ""
            artist_uri = ""
            followers = None
            popularity = None

            if artist_name in artist_id_mapping:
                artist_id = artist_id_mapping[artist_name]['id']
                artist_uri = artist_id_mapping[artist_name]['uri']
                spotify_result = artist_results.get(artist_id, {})

                if spotify_result.get('status') == 'success':
                    genres = spotify_result.get('genres', "")
                    followers = spotify_result.get('followers')
                    popularity = spotify_result.get('popularity')

            # Classify as soundtrack based on genres
            is_soundtrack = SoundtrackClassifier.classify_artist(artist_name, genres)

            # Detect language for artist
            language, detection_method = LanguageDetector.detect_language(
                song_name="",  # No song name for artist detection
                artist_name=artist_name,
                lyrics=None,
                is_soundtrack=is_soundtrack,
                genres=genres
            )

            artist_record = {
                "artist_name": artist_name,
                "artist_uri": artist_uri,
                "genres": genres,
                "followers": followers,
                "popularity": popularity,
                "is_soundtrack": is_soundtrack,
                "language": language,
                "detection_method": detection_method,
            }

            if artist_uri:
                logger.info(f"  {i+1}/{len(new_artists)}: {artist_name} -> {language} ({detection_method})")
            else:
                logger.warning(
                    f"  {i+1}/{len(new_artists)}: {artist_name} - Could not get artist ID, "
                    f"language: {language}"
                )

            artists_to_insert.append(artist_record)

        # Insert new artists
        if artists_to_insert:
            artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)
            result = artists_collection.insert_many(artists_to_insert)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} new artists into {ARTISTS_MASTER_COLLECTION}")
            return inserted_count

        return 0


def main():
    """Main execution function."""
    logger.info("Starting New Content Processor")

    try:
        with MongoDBConnection() as db_conn:
            processor = ContentProcessor(db_conn)

            # Get recent streaming records (last 50)
            streaming_records = processor.get_recent_streaming_records(limit=50)
            if not streaming_records:
                logger.info("No streaming records found")
                return

            # Get existing songs and artists from master collections
            existing_songs, existing_artists = processor.get_existing_master_data()

            # Identify new songs and artists
            new_songs, new_artists = processor.identify_new_content(
                streaming_records, existing_songs, existing_artists
            )

            if not new_songs and not new_artists:
                logger.info("No new songs or artists found in the last 50 streaming records!")
                logger.info("All songs and artists are already in the master collections.")
                return

            # Process new songs (with immediate language detection)
            songs_inserted = processor.process_new_songs(new_songs)

            # Process new artists (with immediate language detection)
            artists_inserted = processor.process_new_artists(new_artists)

            # Summary
            logger.info("=" * 60)
            logger.info("NEW CONTENT PROCESSING SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Streaming records checked: {len(streaming_records)}")
            logger.info(f"New songs found: {len(new_songs)}")
            logger.info(f"New artists found: {len(new_artists)}")
            logger.info(f"Songs inserted: {songs_inserted}")
            logger.info(f"Artists inserted: {artists_inserted}")
            logger.info("Language detection: Completed immediately (no null values)")

            if new_songs:
                logger.info("Next: Run lyrics enrichment to refine language detection with lyrics")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise


if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    main()
