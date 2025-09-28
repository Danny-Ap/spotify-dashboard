#!/usr/bin/env python3
"""
Data Validation & Consistency Checker
Validates data integrity across StreamingHistory, songs_master, and artists_master collections.
Checks for inconsistencies, missing data, and cross-collection relationship errors.

Auto-fixes: is_soundtrack: true → language: "Soundtrack"
Reports: All other issues to log output

Collections:
- StreamingHistory: Main listening data
- songs_master: Unique songs with metadata  
- artists_master: Unique artists with metadata
"""

import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Set, Any
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DATABASE_NAME = "Spotify"
STREAMING_COLLECTION = "StreamingHistory"
SONGS_MASTER_COLLECTION = "songs_master"
ARTISTS_MASTER_COLLECTION = "artists_master"
BATCH_SIZE = 1000

# MongoDB Configuration
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')

# Expected detection methods
VALID_DETECTION_METHODS = {
    "lyrics", "title", "artist_name", "character_detection", 
    "soundtrack", "majority_songs", "unknown"
}

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, db):
        self.db = db
        self.errors = []
        self.fixes_applied = 0
        
    def log_error(self, category: str, error_type: str, details: Dict[str, Any]):
        """Log an error to the errors list"""
        self.errors.append({
            "category": category,
            "error_type": error_type,
            "details": details,
            "timestamp": datetime.now().isoformat()
        })
        
        # Also log to standard logger
        logger.warning(f"{category} - {error_type}: {details}")
    
    def detect_hebrew_chars(self, text: str) -> bool:
        """Check if text contains Hebrew characters"""
        if not text:
            return False
        hebrew_pattern = re.compile(r'[\u0590-\u05FF\uFB1D-\uFB4F]')
        return bool(hebrew_pattern.search(text))
    
    def detect_japanese_chars(self, text: str) -> bool:
        """Check if text contains Japanese characters"""
        if not text:
            return False
        japanese_pattern = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]')
        return bool(japanese_pattern.search(text))
    
    def validate_soundtrack_consistency(self):
        """Validate and fix soundtrack language consistency"""
        logger.info("🎬 Validating soundtrack consistency...")
        
        songs_collection = self.db[SONGS_MASTER_COLLECTION]
        artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
        
        # Check songs with is_soundtrack: true but language != "Soundtrack"
        songs_to_fix = list(songs_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        }))
        
        for song in songs_to_fix:
            # Fix in database
            result = songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )
            
            if result.modified_count > 0:
                self.fixes_applied += 1
                logger.info(f"  🔧 FIXED SONG: '{song['song_name']}' by {song['artist_name']}: {song.get('language')} → Soundtrack")
        
        # Check artists with is_soundtrack: true but language != "Soundtrack"
        artists_to_fix = list(artists_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        }))
        
        for artist in artists_to_fix:
            # Fix in database
            result = artists_collection.update_one(
                {"_id": artist["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )
            
            if result.modified_count > 0:
                self.fixes_applied += 1
                logger.info(f"  🔧 FIXED ARTIST: {artist['artist_name']}: {artist.get('language')} → Soundtrack")
        
        logger.info(f"✅ Fixed {len(songs_to_fix)} songs and {len(artists_to_fix)} artists with soundtrack issues")
    
    def validate_required_fields(self):
        """Validate that required fields are not null/empty"""
        logger.info("📋 Validating required fields...")
        
        collections_to_check = [
            (STREAMING_COLLECTION, ["track_name", "artist_name", "ts_utc"]),
            (SONGS_MASTER_COLLECTION, ["song_name", "artist_name", "spotify_track_uri"]),
            (ARTISTS_MASTER_COLLECTION, ["artist_name"])
        ]
        
        for collection_name, required_fields in collections_to_check:
            collection = self.db[collection_name]
            
            for field in required_fields:
                # Check for null values
                null_count = collection.count_documents({field: None})
                if null_count > 0:
                    self.log_error("missing_required_fields", f"Required field '{field}' is null", {
                        "collection": collection_name,
                        "field": field,
                        "count": null_count
                    })
                
                # Check for empty strings
                empty_count = collection.count_documents({field: ""})
                if empty_count > 0:
                    self.log_error("missing_required_fields", f"Required field '{field}' is empty", {
                        "collection": collection_name,
                        "field": field,
                        "count": empty_count
                    })
    
    def validate_boolean_fields(self):
        """Validate that is_soundtrack fields are proper booleans"""
        logger.info("🔢 Validating boolean fields...")
        
        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]
        
        for collection_name in collections_to_check:
            collection = self.db[collection_name]
            
            # Check for non-boolean is_soundtrack values
            invalid_count = collection.count_documents({
                "is_soundtrack": {"$exists": True, "$not": {"$type": "bool"}}
            })
            
            if invalid_count > 0:
                self.log_error("invalid_data_types", "is_soundtrack field is not boolean", {
                    "collection": collection_name,
                    "field": "is_soundtrack",
                    "count": invalid_count
                })
    
    def validate_language_fields(self):
        """Validate that language fields contain proper values"""
        logger.info("🌍 Validating language fields...")
        
        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]
        
        for collection_name in collections_to_check:
            collection = self.db[collection_name]
            
            # Check for null language values
            null_language_count = collection.count_documents({"language": None})
            if null_language_count > 0:
                self.log_error("missing_language", "Language field is null", {
                    "collection": collection_name,
                    "count": null_language_count
                })
    
    def validate_detection_methods(self):
        """Validate detection_method field values"""
        logger.info("🔍 Validating detection methods...")
        
        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]
        
        for collection_name in collections_to_check:
            collection = self.db[collection_name]
            
            # Check for invalid detection methods
            invalid_methods = list(collection.find({
                "detection_method": {
                    "$exists": True,
                    "$nin": list(VALID_DETECTION_METHODS) + [None]
                }
            }, {"detection_method": 1}).limit(10))
            
            if invalid_methods:
                unique_invalid = set(doc.get("detection_method") for doc in invalid_methods)
                self.log_error("invalid_detection_method", "Invalid detection methods found", {
                    "collection": collection_name,
                    "invalid_methods": list(unique_invalid),
                    "valid_methods": list(VALID_DETECTION_METHODS),
                    "count": len(invalid_methods)
                })
    
    def validate_character_detection_consistency(self):
        """Validate Hebrew/Japanese character detection consistency"""
        logger.info("🔤 Validating character detection consistency...")
        
        songs_collection = self.db[SONGS_MASTER_COLLECTION]
        
        # Process in batches
        total_songs = songs_collection.count_documents({})
        inconsistencies = 0
        
        for skip in range(0, min(total_songs, 5000), BATCH_SIZE):  # Limit to 5000 for performance
            songs_batch = list(songs_collection.find({}).skip(skip).limit(BATCH_SIZE))
            
            for song in songs_batch:
                song_name = song.get('song_name', '')
                artist_name = song.get('artist_name', '')
                language = song.get('language', '')
                combined_text = f"{song_name} {artist_name}"
                
                # Check Hebrew consistency
                has_hebrew_chars = self.detect_hebrew_chars(combined_text)
                if has_hebrew_chars and language != "Hebrew":
                    inconsistencies += 1
                    if inconsistencies <= 10:  # Log only first 10 to avoid spam
                        self.log_error("character_detection_inconsistency", "Hebrew characters found but language is not Hebrew", {
                            "song_name": song_name,
                            "artist_name": artist_name,
                            "current_language": language,
                            "expected_language": "Hebrew"
                        })
                
                # Check Japanese consistency
                has_japanese_chars = self.detect_japanese_chars(combined_text)
                if has_japanese_chars and language != "Japanese":
                    inconsistencies += 1
                    if inconsistencies <= 10:  # Log only first 10 to avoid spam
                        self.log_error("character_detection_inconsistency", "Japanese characters found but language is not Japanese", {
                            "song_name": song_name,
                            "artist_name": artist_name,
                            "current_language": language,
                            "expected_language": "Japanese"
                        })
        
        if inconsistencies > 10:
            logger.warning(f"Found {inconsistencies} character detection inconsistencies (showing first 10)")
        elif inconsistencies > 0:
            logger.warning(f"Found {inconsistencies} character detection inconsistencies")
        else:
            logger.info("✅ No character detection inconsistencies found")
    
    def validate_cross_collection_relationships(self):
        """Validate relationships between collections (sample check)"""
        logger.info("🔗 Validating cross-collection relationships...")
        
        streaming_collection = self.db[STREAMING_COLLECTION]
        songs_collection = self.db[SONGS_MASTER_COLLECTION]
        artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
        
        # Sample recent records for validation
        recent_records = list(streaming_collection.find().sort("ts_utc", -1).limit(100))
        
        missing_songs = 0
        missing_artists = 0
        
        for record in recent_records:
            track_name = record.get("track_name", "")
            artist_name = record.get("artist_name", "")
            
            if not track_name or not artist_name:
                continue
            
            # Check if song exists in songs_master
            song_exists = songs_collection.count_documents({
                "song_name": track_name,
                "artist_name": artist_name
            }) > 0
            
            if not song_exists:
                missing_songs += 1
            
            # Check if artist exists in artists_master
            artist_exists = artists_collection.count_documents({
                "artist_name": artist_name
            }) > 0
            
            if not artist_exists:
                missing_artists += 1
        
        if missing_songs > 0:
            self.log_error("missing_master_records", "Songs in StreamingHistory not found in songs_master", {
                "missing_songs_count": missing_songs,
                "sample_size": len(recent_records)
            })
        
        if missing_artists > 0:
            self.log_error("missing_master_records", "Artists in StreamingHistory not found in artists_master", {
                "missing_artists_count": missing_artists,
                "sample_size": len(recent_records)
            })
        
        if missing_songs == 0 and missing_artists == 0:
            logger.info("✅ Cross-collection relationships look good (sample check)")
    
    def validate_duplicates(self):
        """Check for duplicate records (sample check)"""
        logger.info("👥 Validating for duplicates...")
        
        # Check for duplicate songs
        songs_collection = self.db[SONGS_MASTER_COLLECTION]
        
        pipeline = [
            {"$group": {
                "_id": {
                    "song_name_lower": {"$toLower": "$song_name"},
                    "artist_name_lower": {"$toLower": "$artist_name"}
                },
                "count": {"$sum": 1},
                "docs": {"$push": {"song_name": "$song_name", "artist_name": "$artist_name"}}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": 10}  # Limit to first 10 duplicates
        ]
        
        duplicate_songs = list(songs_collection.aggregate(pipeline))
        
        if duplicate_songs:
            for dup in duplicate_songs:
                self.log_error("duplicates", "Duplicate songs found", {
                    "song_variations": [doc["song_name"] for doc in dup["docs"]],
                    "artist_variations": [doc["artist_name"] for doc in dup["docs"]],
                    "duplicate_count": dup["count"]
                })
        
        # Check for duplicate artists
        artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
        
        pipeline = [
            {"$group": {
                "_id": {"artist_name_lower": {"$toLower": "$artist_name"}},
                "count": {"$sum": 1},
                "docs": {"$push": {"artist_name": "$artist_name"}}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": 10}  # Limit to first 10 duplicates
        ]
        
        duplicate_artists = list(artists_collection.aggregate(pipeline))
        
        if duplicate_artists:
            for dup in duplicate_artists:
                self.log_error("duplicates", "Duplicate artists found", {
                    "artist_variations": [doc["artist_name"] for doc in dup["docs"]],
                    "duplicate_count": dup["count"]
                })
    
    def validate_spotify_data(self):
        """Validate Spotify URI and metadata (sample check)"""
        logger.info("🎵 Validating Spotify data...")
        
        songs_collection = self.db[SONGS_MASTER_COLLECTION]
        artists_collection = self.db[ARTISTS_MASTER_COLLECTION]
        
        # Check for missing Spotify URIs in songs (sample)
        missing_song_uris = songs_collection.count_documents({
            "$or": [
                {"spotify_track_uri": None},
                {"spotify_track_uri": ""},
                {"spotify_track_uri": {"$exists": False}}
            ]
        })
        
        if missing_song_uris > 0:
            self.log_error("missing_spotify_data", "Missing spotify_track_uri in songs", {
                "collection": "songs_master",
                "count": missing_song_uris
            })
        
        # Check for missing Spotify URIs in artists (sample)
        missing_artist_uris = artists_collection.count_documents({
            "$or": [
                {"artist_uri": None},
                {"artist_uri": ""},
                {"artist_uri": {"$exists": False}}
            ]
        })
        
        if missing_artist_uris > 0:
            self.log_error("missing_spotify_data", "Missing artist_uri in artists", {
                "collection": "artists_master",
                "count": missing_artist_uris
            })
        
        # Check for missing metadata
        missing_metadata_songs = songs_collection.count_documents({
            "$or": [
                {"duration_ms": None},
                {"popularity": None}
            ]
        })
        
        if missing_metadata_songs > 0:
            self.log_error("missing_spotify_data", "Missing metadata in songs", {
                "collection": "songs_master",
                "count": missing_metadata_songs,
                "missing_fields": ["duration_ms", "popularity"]
            })

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
    logger.info("✅ Starting Data Validation & Consistency Check")
    
    # Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        db = client[DATABASE_NAME]
        validator = DataValidator(db)
        
        # Run all validation checks
        logger.info("🔍 Starting comprehensive data validation...\n")
        
        # 1. Fix soundtrack consistency (auto-fix)
        validator.validate_soundtrack_consistency()
        
        # 2. Validate required fields
        validator.validate_required_fields()
        
        # 3. Validate boolean fields
        validator.validate_boolean_fields()
        
        # 4. Validate language fields
        validator.validate_language_fields()
        
        # 5. Validate detection methods
        validator.validate_detection_methods()
        
        # 6. Validate character detection consistency
        validator.validate_character_detection_consistency()
        
        # 7. Validate cross-collection relationships
        validator.validate_cross_collection_relationships()
        
        # 8. Check for duplicates
        validator.validate_duplicates()
        
        # 9. Validate Spotify data
        validator.validate_spotify_data()
        
        # Summary
        logger.info("=" * 60)
        logger.info("DATA VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total errors found: {len(validator.errors)}")
        logger.info(f"Automatic fixes applied: {validator.fixes_applied}")
        
        if len(validator.errors) == 0:
            logger.info("🎉 All data is consistent - no issues found!")
        else:
            logger.warning(f"📋 Found {len(validator.errors)} validation issues")
            
            # Group errors by category for summary
            error_categories = {}
            for error in validator.errors:
                cat = error["category"]
                if cat not in error_categories:
                    error_categories[cat] = 0
                error_categories[cat] += 1
            
            for category, count in error_categories.items():
                logger.info(f"  {category}: {count} issues")
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
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