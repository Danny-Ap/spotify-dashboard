#!/usr/bin/env python3
"""
Spotify Recent Tracks Fetcher

Fetches recently played tracks from Spotify API and inserts new tracks into MongoDB.
Returns the count of new tracks added for pipeline decision making.

Collections:
- StreamingHistory: Main streaming data with schema matching CSV format
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.utils.database import MongoDBConnection
from src.utils.spotify_api import SpotifyClient
from src.utils.config import (
    STREAMING_COLLECTION,
    STREAMING_FIELDS,
)

logger = logging.getLogger(__name__)


class RecentTracksFetcher:
    """Fetches and processes recently played tracks from Spotify API."""

    def __init__(self, db_connection: MongoDBConnection):
        """
        Initialize fetcher with database connection.

        Args:
            db_connection: Active MongoDB connection.
        """
        self.db = db_connection
        self.spotify = SpotifyClient(use_user_auth=True)

    def fetch_tracks(self, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch recently played tracks from Spotify API.

        Args:
            limit: Maximum number of tracks to fetch (max 50).

        Returns:
            List of track items from Spotify API, or None on failure.
        """
        if not self.spotify.authenticate():
            logger.error("Failed to authenticate with Spotify API")
            return None

        tracks = self.spotify.get_recently_played(limit=limit)

        if tracks is None:
            # Try refreshing token
            logger.warning("Failed to get tracks, attempting token refresh...")
            if self.spotify.refresh_access_token():
                tracks = self.spotify.get_recently_played(limit=limit)

        return tracks

    def convert_to_streaming_format(self, tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert Spotify API data to StreamingHistory schema format.

        Uses the new field names matching the CSV schema:
        - master_metadata_track_name
        - master_metadata_album_artist_name
        - master_metadata_album_album_name
        - duration_seconds, duration_minutes, duration_hours
        - Date, Year, Month, Day

        Args:
            tracks: Raw track data from Spotify API.

        Returns:
            List of converted track records ready for MongoDB insertion.
        """
        converted_tracks = []

        for track_item in tracks:
            try:
                track = track_item['track']
                played_at = track_item['played_at']

                # Parse timestamp and make it timezone-naive (UTC)
                ts = datetime.fromisoformat(played_at.replace('Z', '+00:00'))
                ts_utc = ts.replace(tzinfo=None)

                # Get track duration (full track duration since recently-played = completed plays)
                duration_ms = track['duration_ms']

                # Calculate duration in different units
                duration_seconds = round(duration_ms / 1000.0, 3)
                duration_minutes = round(duration_ms / (1000.0 * 60.0), 6)
                duration_hours = round(duration_ms / (1000.0 * 60.0 * 60.0), 8)

                # Create date fields matching CSV format
                date_str = ts_utc.strftime('%d/%m/%Y')  # DD/MM/YYYY format
                year = ts_utc.year
                month = ts_utc.strftime('%B')  # Full month name
                day_of_week = ts_utc.strftime('%A')  # Full day name

                # ISO timestamp string
                ts_iso = ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                # Get artist name (primary artist)
                artist_name = track['artists'][0]['name'] if track['artists'] else None

                # Build streaming record with new field names
                streaming_record = {
                    # Timestamps
                    'ts_utc': ts_utc,
                    STREAMING_FIELDS['ts']: ts_iso,
                    STREAMING_FIELDS['date']: date_str,
                    STREAMING_FIELDS['year']: year,
                    STREAMING_FIELDS['month']: month,
                    STREAMING_FIELDS['day']: day_of_week,

                    # Duration fields
                    STREAMING_FIELDS['ms_played']: duration_ms,
                    STREAMING_FIELDS['duration_seconds']: duration_seconds,
                    STREAMING_FIELDS['duration_minutes']: duration_minutes,
                    STREAMING_FIELDS['duration_hours']: duration_hours,

                    # Track metadata (new field names)
                    STREAMING_FIELDS['track_name']: track['name'],
                    STREAMING_FIELDS['artist_name']: artist_name,
                    STREAMING_FIELDS['album_name']: track['album']['name'],
                    STREAMING_FIELDS['spotify_track_uri']: track['uri'],

                    # Playback context - defaults for API data
                    STREAMING_FIELDS['conn_country']: None,
                    STREAMING_FIELDS['platform']: None,
                    STREAMING_FIELDS['shuffle']: None,
                    STREAMING_FIELDS['skipped']: False,  # Not skipped since played fully
                    STREAMING_FIELDS['offline']: False,  # Online since from API
                    STREAMING_FIELDS['offline_timestamp']: None,
                    STREAMING_FIELDS['incognito_mode']: False,
                    STREAMING_FIELDS['reason_start']: None,
                    STREAMING_FIELDS['reason_end']: 'trackdone',
                    STREAMING_FIELDS['ip_addr']: None,

                    # Source tracking
                    'data_source': 'recently_played_api',
                }

                converted_tracks.append(streaming_record)

                logger.debug(
                    f"Converted: {track['name']} - {artist_name} | "
                    f"{duration_minutes:.2f}min"
                )

            except Exception as e:
                logger.error(f"Error converting track: {e}")
                continue

        logger.info(f"Successfully converted {len(converted_tracks)} tracks")
        return converted_tracks

    def get_latest_timestamp(self) -> Optional[datetime]:
        """
        Get the latest timestamp from the StreamingHistory collection.

        Returns:
            Latest timestamp as datetime, or None if collection is empty.
        """
        try:
            collection = self.db.get_collection(STREAMING_COLLECTION)

            # Find the document with the latest timestamp
            latest_doc = collection.find_one(sort=[("ts_utc", -1)])

            if latest_doc and 'ts_utc' in latest_doc:
                latest_ts = latest_doc['ts_utc']

                # Ensure the timestamp is timezone-naive
                if hasattr(latest_ts, 'tzinfo') and latest_ts.tzinfo is not None:
                    latest_ts = latest_ts.replace(tzinfo=None)

                logger.info(f"Latest timestamp in database: {latest_ts}")
                return latest_ts
            else:
                logger.info("No documents found in database")
                return None

        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            return None

    def filter_new_tracks(
        self,
        tracks: List[Dict[str, Any]],
        latest_db_timestamp: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """
        Filter tracks to only include those newer than the latest database timestamp.

        Args:
            tracks: List of converted track records.
            latest_db_timestamp: Latest timestamp currently in database.

        Returns:
            List of new tracks not yet in database.
        """
        if not latest_db_timestamp:
            logger.info("No existing data in database, all tracks are new")
            return tracks

        new_tracks = []
        for track in tracks:
            track_ts = track['ts_utc']

            # Ensure both timestamps are timezone-naive for comparison
            if hasattr(track_ts, 'tzinfo') and track_ts.tzinfo is not None:
                track_ts = track_ts.replace(tzinfo=None)

            if track_ts > latest_db_timestamp:
                new_tracks.append(track)

        logger.info(f"Found {len(new_tracks)} new tracks (out of {len(tracks)} total)")
        return new_tracks

    def insert_tracks(self, tracks: List[Dict[str, Any]]) -> int:
        """
        Insert new tracks into StreamingHistory collection.

        Args:
            tracks: List of track records to insert.

        Returns:
            Number of tracks successfully inserted.
        """
        if not tracks:
            logger.info("No new tracks to insert")
            return 0

        try:
            collection = self.db.get_collection(STREAMING_COLLECTION)

            # Sort tracks by timestamp (oldest first) for insertion
            sorted_tracks = sorted(tracks, key=lambda x: x['ts_utc'])

            logger.info(f"Inserting {len(sorted_tracks)} new tracks...")
            result = collection.insert_many(sorted_tracks)

            inserted_count = len(result.inserted_ids)
            logger.info(f"Successfully inserted {inserted_count} tracks")

            # Log inserted tracks
            for track in sorted_tracks:
                duration_min = track[STREAMING_FIELDS['duration_minutes']]
                timestamp_str = track['ts_utc'].strftime('%Y-%m-%d %H:%M:%S')
                track_name = track[STREAMING_FIELDS['track_name']]
                artist_name = track[STREAMING_FIELDS['artist_name']]
                logger.info(
                    f"  + {timestamp_str} | {track_name} - {artist_name} | "
                    f"{duration_min:.2f}min"
                )

            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting tracks: {e}")
            return 0


def main() -> int:
    """
    Main execution function.

    Returns:
        Number of new tracks added (0 means no new data, pipeline should stop).
    """
    logger.info("Starting Spotify Recent Tracks Fetcher")

    try:
        with MongoDBConnection() as db_conn:
            fetcher = RecentTracksFetcher(db_conn)

            # Fetch recently played tracks
            tracks = fetcher.fetch_tracks(limit=50)

            if not tracks:
                logger.error("Failed to retrieve recently played tracks")
                return 0

            logger.info(f"Processing {len(tracks)} tracks...")

            # Convert to streaming history format
            streaming_tracks = fetcher.convert_to_streaming_format(tracks)

            if not streaming_tracks:
                logger.error("No tracks successfully converted")
                return 0

            # Get latest timestamp from database
            latest_db_timestamp = fetcher.get_latest_timestamp()

            # Filter new tracks
            new_tracks = fetcher.filter_new_tracks(streaming_tracks, latest_db_timestamp)

            # Insert new tracks
            inserted_count = fetcher.insert_tracks(new_tracks)

            # Summary
            logger.info("=" * 60)
            logger.info("FETCH RECENT TRACKS SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Fetched from Spotify API: {len(tracks)} tracks")
            logger.info(f"Converted successfully: {len(streaming_tracks)} tracks")
            logger.info(f"New tracks found: {len(new_tracks)} tracks")
            logger.info(f"Successfully inserted: {inserted_count} tracks")

            if latest_db_timestamp:
                logger.info(
                    f"Latest timestamp in DB: "
                    f"{latest_db_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                )

            if new_tracks:
                newest_track = max(new_tracks, key=lambda x: x['ts_utc'])
                track_name = newest_track[STREAMING_FIELDS['track_name']]
                artist_name = newest_track[STREAMING_FIELDS['artist_name']]
                logger.info(
                    f"Newest track added: "
                    f"{newest_track['ts_utc'].strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"{track_name} - {artist_name}"
                )

                # Total listening time for new tracks
                total_minutes = sum(
                    track[STREAMING_FIELDS['duration_minutes']]
                    for track in new_tracks
                )
                logger.info(
                    f"Total listening time (new tracks): "
                    f"{total_minutes:.1f} minutes ({total_minutes/60:.1f} hours)"
                )

            logger.info(f"Returning {inserted_count} new tracks for pipeline decision")
            return inserted_count

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        return 0


if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    result = main()
    print(f"Script completed. New tracks added: {result}")
