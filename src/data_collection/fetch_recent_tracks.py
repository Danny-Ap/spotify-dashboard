#!/usr/bin/env python3
"""
Spotify Recent Tracks Fetcher
Fetches recently played tracks from Spotify API and inserts new tracks into MongoDB.
Returns the count of new tracks added for pipeline decision making.

Collections:
- StreamingHistory: Main streaming data with updated schema
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DATABASE_NAME = "Spotify"
STREAMING_COLLECTION = "StreamingHistory"

# Spotify API Configuration
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_ACCESS_TOKEN = os.getenv('SPOTIFY_ACCESS_TOKEN')
SPOTIFY_REFRESH_TOKEN = os.getenv('SPOTIFY_REFRESH_TOKEN')
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')

# Spotify API Endpoints
SPOTIFY_RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"

logger = logging.getLogger(__name__)

class SpotifyTracksFetcher:
    def __init__(self):
        self.access_token = SPOTIFY_ACCESS_TOKEN
        self.refresh_token = SPOTIFY_REFRESH_TOKEN
        
    def refresh_access_token(self) -> Optional[str]:
        """Refresh the Spotify access token using the refresh token."""
        if not self.refresh_token or not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            logger.error("Missing refresh token or client credentials")
            return None
        
        try:
            auth_header = requests.auth.HTTPBasicAuth(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
            
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            
            logger.info("Refreshing Spotify access token...")
            response = requests.post(SPOTIFY_TOKEN_URL, auth=auth_header, data=data, headers=headers)
            
            if response.status_code == 200:
                token_data = response.json()
                new_access_token = token_data['access_token']
                logger.info("✅ Access token refreshed successfully")
                return new_access_token
            else:
                logger.error(f"Failed to refresh token: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error refreshing access token: {e}")
            return None

    def get_recently_played_tracks(self, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """Get recently played tracks from Spotify API."""
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            params = {'limit': min(limit, 50)}  # Spotify API max is 50
            
            logger.info(f"Fetching {limit} recently played tracks...")
            response = requests.get(SPOTIFY_RECENTLY_PLAYED_URL, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                tracks = data.get('items', [])
                logger.info(f"✅ Retrieved {len(tracks)} recently played tracks")
                return tracks
            elif response.status_code == 401:
                logger.warning("Access token expired, attempting to refresh...")
                return None
            else:
                logger.error(f"Failed to get recently played tracks: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching recently played tracks: {e}")
            return None

    def convert_to_streaming_format(self, tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert Spotify API data to StreamingHistory schema format."""
        converted_tracks = []
        
        for track_item in tracks:
            try:
                track = track_item['track']
                played_at = track_item['played_at']
                
                # Parse timestamp and make it timezone-naive (UTC)
                ts = datetime.fromisoformat(played_at.replace('Z', '+00:00'))
                ts_utc = ts.replace(tzinfo=None)  # Remove timezone info, keeping UTC time
                
                # Get track duration (since it was played fully)
                duration_ms = track['duration_ms']
                
                # Create date fields
                date_str = ts_utc.strftime('%Y-%m-%d')
                year = ts_utc.year
                month = ts_utc.strftime('%B')
                day_of_week = ts_utc.strftime('%A')
                
                # Convert to streaming history format
                streaming_record = {
                    # Timestamp fields
                    'ts_utc': ts_utc,
                    
                    # Track metadata
                    'track_name': track['name'],
                    'artist_name': track['artists'][0]['name'] if track['artists'] else None,
                    'album_name': track['album']['name'],
                    'spotify_track_uri': track['uri'],
                    
                    # Duration fields - full track duration since it was played completely
                    'ms_played': duration_ms,
                    's_played': round(duration_ms / 1000.0, 3),
                    'min_played': round(duration_ms / (1000.0 * 60.0), 6),
                    'h_played': round(duration_ms / (1000.0 * 60.0 * 60.0), 8),
                    
                    # Date fields
                    'date': date_str,
                    'year': year,
                    'month': month,
                    'day_of_week': day_of_week,
                    
                    # Source and completion status
                    'data_source': 'recently_played_api',
                    'is_complete_play': True,  # Recently played endpoint only returns completed tracks
                    
                    # Fields not available from recently played API
                    'conn_country': None,
                    'ip_addr': None,
                    'platform': None,
                    'reason_start': None,
                    'reason_end': None,
                    'shuffle': None,
                    'skipped': False,  # Not skipped since it was played fully
                    'offline': False,  # Assume online since we're getting from API
                    'offline_timestamp': None,
                    'incognito_mode': None,
                    
                    # Language will be set later by language detection pipeline
                    'language': None,
                    
                    # Metadata fields
                    'created_at': datetime.utcnow(),
                    'last_updated': datetime.utcnow(),
                    'processing_version': '2.0'
                }
                
                converted_tracks.append(streaming_record)
                
                # Log the converted track
                duration_min = streaming_record['min_played']
                logger.debug(f"✅ Converted: {track['name']} - {track['artists'][0]['name'] if track['artists'] else 'Unknown'} | {duration_min:.2f}min")
                
            except Exception as e:
                logger.error(f"Error converting track: {e}")
                continue
        
        logger.info(f"✅ Successfully converted {len(converted_tracks)} tracks to streaming format")
        return converted_tracks

class MongoDBManager:
    def __init__(self):
        self.client = None
        self.db = None
        
    def connect(self):
        """Connect to MongoDB Atlas."""
        if not MONGODB_CONNECTION_STRING:
            logger.error("MongoDB connection string not found in environment variables")
            return False
        
        try:
            logger.info("Connecting to MongoDB Atlas...")
            self.client = MongoClient(MONGODB_CONNECTION_STRING)
            
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[DATABASE_NAME]
            logger.info("✅ Connected to MongoDB Atlas")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get the latest timestamp from the StreamingHistory collection."""
        try:
            collection = self.db[STREAMING_COLLECTION]
            
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

    def filter_new_tracks(self, tracks: List[Dict[str, Any]], latest_db_timestamp: Optional[datetime]) -> List[Dict[str, Any]]:
        """Filter tracks to only include those newer than the latest database timestamp."""
        if not latest_db_timestamp:
            logger.info("No existing data in database, all tracks are new")
            return tracks
        
        new_tracks = []
        for track in tracks:
            track_ts = track['ts_utc']
            
            # Ensure both timestamps are timezone-naive for comparison
            if hasattr(track_ts, 'tzinfo') and track_ts.tzinfo is not None:
                track_ts = track_ts.replace(tzinfo=None)
            
            if hasattr(latest_db_timestamp, 'tzinfo') and latest_db_timestamp.tzinfo is not None:
                latest_db_timestamp = latest_db_timestamp.replace(tzinfo=None)
            
            # Compare timestamps
            if track_ts > latest_db_timestamp:
                new_tracks.append(track)
        
        logger.info(f"Found {len(new_tracks)} new tracks to insert (out of {len(tracks)} total)")
        return new_tracks

    def insert_tracks(self, tracks: List[Dict[str, Any]]) -> int:
        """Insert new tracks into StreamingHistory collection."""
        if not tracks:
            logger.info("No new tracks to insert")
            return 0
        
        try:
            collection = self.db[STREAMING_COLLECTION]
            
            # Sort tracks by timestamp (oldest first) for insertion
            sorted_tracks = sorted(tracks, key=lambda x: x['ts_utc'])
            
            logger.info(f"Inserting {len(sorted_tracks)} new tracks...")
            result = collection.insert_many(sorted_tracks)
            
            inserted_count = len(result.inserted_ids)
            logger.info(f"✅ Successfully inserted {inserted_count} tracks")
            
            # Log inserted tracks
            for track in sorted_tracks:
                duration_min = track['min_played']
                timestamp_str = track['ts_utc'].strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"  + {timestamp_str} | {track['track_name']} - {track['artist_name']} | {duration_min:.2f}min")
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting tracks: {e}")
            return 0

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main() -> int:
    """
    Main execution function.
    Returns: Number of new tracks added (0 means no new data, pipeline should stop)
    """
    logger.info("🎵 Starting Spotify Recent Tracks Fetcher")
    
    # Initialize components
    fetcher = SpotifyTracksFetcher()
    db_manager = MongoDBManager()
    
    try:
        # Connect to database
        if not db_manager.connect():
            return 0
        
        # Fetch recently played tracks
        tracks = fetcher.get_recently_played_tracks(limit=50)
        
        # If token expired, try to refresh
        if tracks is None and fetcher.refresh_token:
            new_token = fetcher.refresh_access_token()
            if new_token:
                fetcher.access_token = new_token
                tracks = fetcher.get_recently_played_tracks(limit=50)
        
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
        latest_db_timestamp = db_manager.get_latest_timestamp()
        
        # Filter new tracks
        new_tracks = db_manager.filter_new_tracks(streaming_tracks, latest_db_timestamp)
        
        # Insert new tracks
        inserted_count = db_manager.insert_tracks(new_tracks)
        
        # Summary
        logger.info("=" * 60)
        logger.info("FETCH RECENT TRACKS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Fetched from Spotify API: {len(tracks)} tracks")
        logger.info(f"Converted successfully: {len(streaming_tracks)} tracks")
        logger.info(f"New tracks found: {len(new_tracks)} tracks")
        logger.info(f"Successfully inserted: {inserted_count} tracks")
        
        if latest_db_timestamp:
            logger.info(f"Latest timestamp in DB: {latest_db_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if new_tracks:
            newest_track = max(new_tracks, key=lambda x: x['ts_utc'])
            logger.info(f"Newest track added: {newest_track['ts_utc'].strftime('%Y-%m-%d %H:%M:%S')} | {newest_track['track_name']} - {newest_track['artist_name']}")
            
            # Total listening time for new tracks
            total_minutes = sum(track['min_played'] for track in new_tracks)
            logger.info(f"Total listening time (new tracks): {total_minutes:.1f} minutes ({total_minutes/60:.1f} hours)")
        
        logger.info(f"🎉 Returning {inserted_count} new tracks for pipeline decision")
        return inserted_count
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        return 0
        
    finally:
        db_manager.close()

if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    result = main()
    print(f"Script completed. New tracks added: {result}")