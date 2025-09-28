#!/usr/bin/env python3
"""
New Content Processor
Processes the 50 most recent songs from StreamingHistory collection,
identifies new songs/artists not in master collections, fetches Spotify details,
and adds them to songs_master and artists_master collections.

Collections:
- StreamingHistory: Source of recent listening data
- songs_master: Target for unique songs
- artists_master: Target for unique artists
"""

import os
import json
import time
import base64
import re
import logging
from typing import List, Dict, Set, Tuple, Any, Optional
from datetime import datetime
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DATABASE_NAME = "Spotify"
STREAMING_COLLECTION = "StreamingHistory"
SONGS_MASTER_COLLECTION = "songs_master"
ARTISTS_MASTER_COLLECTION = "artists_master"

# MongoDB Configuration
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')

# Spotify API Configuration
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REQUEST_DELAY = 0.2
BATCH_SIZE = 50

logger = logging.getLogger(__name__)

class SpotifyAPI:
    def __init__(self):
        self.token = None
        self.get_token()
    
    def get_token(self):
        """Get Spotify API access token"""
        if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            logger.error("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in environment")
            raise Exception("Missing Spotify credentials")
        
        auth_string = SPOTIFY_CLIENT_ID + ":" + SPOTIFY_CLIENT_SECRET
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        try:
            result = requests.post(url, headers=headers, data=data)
            result.raise_for_status()
            json_result = json.loads(result.content)
            self.token = json_result["access_token"]
            logger.info("✅ Successfully obtained Spotify API token")
        except Exception as e:
            logger.error(f"Error getting Spotify token: {e}")
            raise

    def get_auth_header(self):
        """Get authorization header for API requests"""
        return {"Authorization": "Bearer " + self.token}

    def get_batch_track_details(self, track_ids: List[str]) -> Dict[str, Dict]:
        """Get track details for up to 50 tracks in a single API call"""
        if len(track_ids) > BATCH_SIZE:
            raise ValueError(f"Cannot process more than {BATCH_SIZE} tracks in a single batch")
        
        # Remove spotify:track: prefix if present
        clean_track_ids = [tid.replace('spotify:track:', '') for tid in track_ids]
        
        ids_string = ",".join(clean_track_ids)
        url = f"https://api.spotify.com/v1/tracks?ids={ids_string}"
        headers = self.get_auth_header()
        
        try:
            result = requests.get(url, headers=headers)
            result.raise_for_status()
            json_result = json.loads(result.content)
            
            batch_results = {}
            tracks = json_result.get("tracks", [])
            
            for i, track in enumerate(tracks):
                original_track_id = track_ids[i]  # Use original ID as key
                if track is None:
                    batch_results[original_track_id] = {
                        "duration_ms": None,
                        "duration_s": None,
                        "release_date": None,
                        "release_date_year": None,
                        "popularity": None,
                        "album_name": None,
                        "artists": [],
                        "status": "error",
                        "error_message": "Track not found or not available"
                    }
                else:
                    duration_ms = track.get("duration_ms")
                    duration_s = round(duration_ms / 1000, 2) if duration_ms else None
                    
                    album = track.get("album", {})
                    release_date = album.get("release_date")
                    album_name = album.get("name")
                    release_date_year = self.extract_year_from_release_date(release_date) if release_date else None
                    
                    popularity = track.get("popularity")
                    
                    # Extract all artists from the track
                    artists = []
                    for artist in track.get("artists", []):
                        artists.append({
                            "id": artist["id"],
                            "name": artist["name"],
                            "uri": artist["uri"]
                        })
                    
                    batch_results[original_track_id] = {
                        "duration_ms": duration_ms,
                        "duration_s": duration_s,
                        "release_date": release_date,
                        "release_date_year": release_date_year,
                        "popularity": popularity,
                        "album_name": album_name,
                        "artists": artists,
                        "status": "success",
                        "error_message": None
                    }
            
            return batch_results
            
        except Exception as e:
            logger.error(f"Error getting track details: {e}")
            batch_results = {}
            for track_id in track_ids:
                batch_results[track_id] = {
                    "duration_ms": None,
                    "duration_s": None,
                    "release_date": None,
                    "release_date_year": None,
                    "popularity": None,
                    "album_name": None,
                    "artists": [],
                    "status": "error",
                    "error_message": str(e)
                }
            return batch_results

    def get_batch_artist_details(self, artist_ids: List[str]) -> Dict[str, Dict]:
        """Get artist details for up to 50 artists in a single API call"""
        if len(artist_ids) > BATCH_SIZE:
            raise ValueError(f"Cannot process more than {BATCH_SIZE} artists in a single batch")
        
        ids_string = ",".join(artist_ids)
        url = f"https://api.spotify.com/v1/artists?ids={ids_string}"
        headers = self.get_auth_header()
        
        try:
            result = requests.get(url, headers=headers)
            result.raise_for_status()
            json_result = json.loads(result.content)
            
            batch_results = {}
            artists = json_result.get("artists", [])
            
            for i, artist in enumerate(artists):
                artist_id = artist_ids[i]
                if artist is None:
                    batch_results[artist_id] = {
                        "name": None,
                        "uri": None,
                        "genres": "",
                        "followers": None,
                        "popularity": None,
                        "status": "error",
                        "error_message": "Artist not found or not available"
                    }
                else:
                    name = artist.get("name")
                    uri = artist.get("uri")
                    genres = ", ".join(artist.get("genres", []))
                    followers = artist.get("followers", {}).get("total")
                    popularity = artist.get("popularity")
                    
                    batch_results[artist_id] = {
                        "name": name,
                        "uri": uri,
                        "genres": genres,
                        "followers": followers,
                        "popularity": popularity,
                        "status": "success",
                        "error_message": None
                    }
            
            return batch_results
            
        except Exception as e:
            logger.error(f"Error getting artist details: {e}")
            batch_results = {}
            for artist_id in artist_ids:
                batch_results[artist_id] = {
                    "name": None,
                    "uri": None,
                    "genres": "",
                    "followers": None,
                    "popularity": None,
                    "status": "error",
                    "error_message": str(e)
                }
            return batch_results

    def extract_year_from_release_date(self, release_date: str) -> int:
        """Extract year from release date string"""
        if not release_date:
            return None
        
        match = re.match(r'^(\d{4})', release_date)
        if match:
            return int(match.group(1))
        return None

class ContentProcessor:
    def __init__(self, db):
        self.db = db
        self.spotify_api = SpotifyAPI()

    def get_recent_streaming_records(self, limit=50):
        """Get the most recent 50 records from StreamingHistory collection"""
        try:
            collection = self.db[STREAMING_COLLECTION]
            
            # Get most recent records sorted by timestamp descending
            records = list(collection.find().sort("ts_utc", -1).limit(limit))
            
            logger.info(f"✅ Retrieved {len(records)} recent streaming records")
            return records
            
        except Exception as e:
            logger.error(f"Error getting streaming records: {e}")
            return []

    def get_existing_master_data(self):
        """Get existing songs and artists from master collections"""
        try:
            songs_collection = self.db[SONGS_MASTER_COLLECTION]
            artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
            
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
            
            logger.info(f"✅ Found {len(existing_songs)} existing songs and {len(existing_artists)} existing artists in master collections")
            return existing_songs, existing_artists
            
        except Exception as e:
            logger.error(f"Error getting existing master data: {e}")
            return {}, {}

    def identify_new_content(self, streaming_records, existing_songs, existing_artists):
        """Identify new songs and artists from streaming records"""
        new_songs = {}  # Use dict to avoid duplicates
        new_artists = {}  # Use dict to avoid duplicates
        
        for record in streaming_records:
            track_name = record.get("track_name", "")
            artist_name = record.get("artist_name", "")
            track_uri = record.get("spotify_track_uri", "")
            
            if not track_name or not artist_name or not track_uri:
                continue
            
            # Check for new songs
            song_key = (track_name.lower().strip(), artist_name.lower().strip())
            if song_key not in existing_songs and song_key not in new_songs:
                new_songs[song_key] = {
                    "song_name": track_name,
                    "artist_name": artist_name,
                    "spotify_track_uri": track_uri
                }
            
            # Check for new artists
            artist_key = artist_name.lower().strip()
            if artist_key not in existing_artists and artist_key not in new_artists:
                new_artists[artist_key] = {
                    "artist_name": artist_name,
                    "spotify_track_uri": track_uri  # We'll need this to get artist ID
                }
        
        new_songs_list = list(new_songs.values())
        new_artists_list = list(new_artists.values())
        
        logger.info(f"🔍 Identified {len(new_songs_list)} new songs and {len(new_artists_list)} new artists")
        
        # Log new items found
        if new_songs_list:
            logger.info("🎵 NEW SONGS FOUND:")
            for i, song in enumerate(new_songs_list, 1):
                logger.info(f"  {i}. '{song['song_name']}' by {song['artist_name']}")
        
        if new_artists_list:
            logger.info("🎤 NEW ARTISTS FOUND:")
            for i, artist in enumerate(new_artists_list, 1):
                logger.info(f"  {i}. {artist['artist_name']}")
        
        return new_songs_list, new_artists_list

    def process_new_songs(self, new_songs):
        """Process new songs with Spotify API and insert into songs_master collection"""
        if not new_songs:
            logger.info("No new songs to process")
            return 0
        
        logger.info(f"🎵 Processing {len(new_songs)} new songs...")
        
        # Extract track IDs
        track_ids = [song['spotify_track_uri'] for song in new_songs]
        
        # Get Spotify details for all tracks
        batch_results = self.spotify_api.get_batch_track_details(track_ids)
        time.sleep(REQUEST_DELAY)
        
        # Process each song
        songs_to_insert = []
        
        for i, song in enumerate(new_songs):
            track_uri = song['spotify_track_uri']
            spotify_result = batch_results[track_uri]
            
            if spotify_result['status'] == 'success':
                song_record = {
                    "song_name": song['song_name'],
                    "artist_name": song['artist_name'],
                    "spotify_track_uri": song['spotify_track_uri'],
                    "duration_ms": spotify_result['duration_ms'],
                    "duration_s": spotify_result['duration_s'],
                    "release_date": spotify_result['release_date'],
                    "release_date_year": spotify_result['release_date_year'],
                    "popularity": spotify_result['popularity'],
                    "album_name": spotify_result['album_name'],
                    "is_soundtrack": False,  # Default, will be classified later
                    "has_lyrics": None,      # Will be set by lyrics fetcher
                    "language": None,        # Will be set by language detector
                    "detection_method": None # Will be set by language detector
                }
                logger.info(f"  ✅ {i+1}/{len(new_songs)}: '{song['song_name']}' by {song['artist_name']}")
            else:
                # Handle API failure - add with minimal data
                song_record = {
                    "song_name": song['song_name'],
                    "artist_name": song['artist_name'],
                    "spotify_track_uri": song['spotify_track_uri'],
                    "duration_ms": None,
                    "duration_s": None,
                    "release_date": None,
                    "release_date_year": None,
                    "popularity": None,
                    "album_name": None,
                    "is_soundtrack": False,
                    "has_lyrics": None,
                    "language": None,
                    "detection_method": None
                }
                logger.warning(f"  ⚠️ {i+1}/{len(new_songs)}: '{song['song_name']}' by {song['artist_name']} - {spotify_result['error_message']}")
            
            songs_to_insert.append(song_record)
        
        # Insert new songs
        if songs_to_insert:
            songs_collection = self.db[SONGS_MASTER_COLLECTION]
            result = songs_collection.insert_many(songs_to_insert)
            inserted_count = len(result.inserted_ids)
            logger.info(f"✅ Inserted {inserted_count} new songs into {SONGS_MASTER_COLLECTION}")
            return inserted_count
        
        return 0

    def process_new_artists(self, new_artists):
        """Process new artists with Spotify API and insert into artists_master collection"""
        if not new_artists:
            logger.info("No new artists to process")
            return 0
        
        logger.info(f"🎤 Processing {len(new_artists)} new artists...")
        
        # We need to get artist IDs from track data first
        track_ids = [artist['spotify_track_uri'] for artist in new_artists]
        track_results = self.spotify_api.get_batch_track_details(track_ids)
        time.sleep(REQUEST_DELAY)
        
        # Map artist names to their IDs
        artist_id_mapping = {}
        for i, artist in enumerate(new_artists):
            track_uri = artist['spotify_track_uri']
            track_result = track_results[track_uri]
            
            if track_result['status'] == 'success':
                # Find the matching artist from the track's artists
                for artist_info in track_result['artists']:
                    if artist_info['name'].lower().strip() == artist['artist_name'].lower().strip():
                        artist_id_mapping[artist['artist_name']] = {
                            'id': artist_info['id'],
                            'uri': artist_info['uri']
                        }
                        break
        
        # Get artist details from Spotify
        artist_ids = [info['id'] for info in artist_id_mapping.values()]
        if artist_ids:
            artist_results = self.spotify_api.get_batch_artist_details(artist_ids)
            time.sleep(REQUEST_DELAY)
        else:
            artist_results = {}
        
        # Process each artist
        artists_to_insert = []
        
        for i, artist in enumerate(new_artists):
            artist_name = artist['artist_name']
            
            if artist_name in artist_id_mapping:
                artist_id = artist_id_mapping[artist_name]['id']
                artist_uri = artist_id_mapping[artist_name]['uri']
                spotify_result = artist_results.get(artist_id, {})
                
                if spotify_result.get('status') == 'success':
                    artist_record = {
                        "artist_name": artist_name,
                        "artist_uri": artist_uri,
                        "genres": spotify_result['genres'],
                        "followers": spotify_result['followers'],
                        "popularity": spotify_result['popularity'],
                        "language": None,        # Will be set by language detector
                        "detection_method": None, # Will be set by language detector
                        "is_soundtrack": False   # Default, will be classified later
                    }
                    logger.info(f"  ✅ {i+1}/{len(new_artists)}: {artist_name}")
                else:
                    artist_record = {
                        "artist_name": artist_name,
                        "artist_uri": artist_uri,
                        "genres": "",
                        "followers": None,
                        "popularity": None,
                        "language": None,
                        "detection_method": None,
                        "is_soundtrack": False
                    }
                    error_msg = spotify_result.get('error_message', 'Unknown error')
                    logger.warning(f"  ⚠️ {i+1}/{len(new_artists)}: {artist_name} - {error_msg}")
            else:
                # Couldn't get artist ID from track
                artist_record = {
                    "artist_name": artist_name,
                    "artist_uri": "",
                    "genres": "",
                    "followers": None,
                    "popularity": None,
                    "language": None,
                    "detection_method": None,
                    "is_soundtrack": False
                }
                logger.warning(f"  ⚠️ {i+1}/{len(new_artists)}: {artist_name} - Could not get artist ID from track")
            
            artists_to_insert.append(artist_record)
        
        # Insert new artists
        if artists_to_insert:
            artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
            result = artists_collection.insert_many(artists_to_insert)
            inserted_count = len(result.inserted_ids)
            logger.info(f"✅ Inserted {inserted_count} new artists into {ARTISTS_MASTER_COLLECTION}")
            return inserted_count
        
        return 0

def connect_to_mongodb():
    """Connect to MongoDB Atlas"""
    if not MONGODB_CONNECTION_STRING:
        logger.error("MongoDB connection string not found in environment variables")
        return None
    
    try:
        logger.info("Connecting to MongoDB Atlas...")
        client = MongoClient(MONGODB_CONNECTION_STRING)
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB Atlas")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def main():
    """Main execution function"""
    logger.info("🔍 Starting New Content Processor")
    
    # Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        db = client[DATABASE_NAME]
        processor = ContentProcessor(db)
        
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
            logger.info("🎉 No new songs or artists found in the last 50 streaming records!")
            logger.info("All songs and artists are already in the master collections.")
            return
        
        # Process new songs
        songs_inserted = processor.process_new_songs(new_songs)
        
        # Process new artists
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
        
        if new_songs or new_artists:
            logger.info("🔜 Next Steps:")
            logger.info("1. Run lyrics enrichment to get lyrics for new songs")
            logger.info("2. Run language detection to classify languages")
        
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
        
    finally:
        client.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    main()