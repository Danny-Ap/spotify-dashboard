#!/usr/bin/env python3
"""
Data Validation & Consistency Checker - REFACTORED

Validates data integrity across StreamingHistory, songs_master, and artists_master collections.
Checks for inconsistencies, missing data, and cross-collection relationship errors.

Key improvements:
- Uses shared utilities (MongoDBConnection, LanguageDetector)
- Uses new field names from STREAMING_FIELDS
- Validates enrichment_status field
- Auto-fixes missing enrichment_status

Auto-fixes:
- is_soundtrack: true -> language: "Soundtrack"
- Missing enrichment_status -> sets to 'processed' if has_lyrics is set

Collections:
- StreamingHistory: Main listening data
- songs_master: Unique songs with metadata
- artists_master: Unique artists with metadata
"""

import logging
from datetime import datetime
from typing import Dict, List, Any

from src.utils.database import MongoDBConnection
from src.utils.language_detection import LanguageDetector
from src.utils.config import (
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    STREAMING_FIELDS,
    ENRICHMENT_STATUS_PENDING,
    ENRICHMENT_STATUS_PROCESSED,
)

logger = logging.getLogger(__name__)

# Expected detection methods
VALID_DETECTION_METHODS = {
    "lyrics", "title", "artist_name", "character_detection",
    "soundtrack", "majority_songs", "unknown", "genre"
}

BATCH_SIZE = 1000


class DataValidator:
    """Validates data integrity across MongoDB collections."""

    def __init__(self, db_connection: MongoDBConnection):
        self.db = db_connection
        self.errors: List[Dict] = []
        self.fixes_applied = 0

    def log_error(self, category: str, error_type: str, details: Dict[str, Any]):
        """Log an error to the errors list."""
        self.errors.append({
            "category": category,
            "error_type": error_type,
            "details": details,
            "timestamp": datetime.now().isoformat()
        })
        logger.warning(f"{category} - {error_type}: {details}")

    def validate_soundtrack_consistency(self):
        """Validate and fix soundtrack language consistency."""
        logger.info("Validating soundtrack consistency...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)
        artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)

        # Fix songs with is_soundtrack: true but language != "Soundtrack"
        songs_to_fix = list(songs_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        }))

        for song in songs_to_fix:
            result = songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )

            if result.modified_count > 0:
                self.fixes_applied += 1
                logger.info(
                    f"  FIXED SONG: '{song['song_name']}' by {song['artist_name']}: "
                    f"{song.get('language')} -> Soundtrack"
                )

        # Fix artists with is_soundtrack: true but language != "Soundtrack"
        artists_to_fix = list(artists_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        }))

        for artist in artists_to_fix:
            result = artists_collection.update_one(
                {"_id": artist["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )

            if result.modified_count > 0:
                self.fixes_applied += 1
                logger.info(
                    f"  FIXED ARTIST: {artist['artist_name']}: "
                    f"{artist.get('language')} -> Soundtrack"
                )

        logger.info(f"Fixed {len(songs_to_fix)} songs and {len(artists_to_fix)} artists with soundtrack issues")

    def validate_enrichment_status(self):
        """Validate and fix enrichment_status field."""
        logger.info("Validating enrichment_status field...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)

        # Find songs without enrichment_status
        missing_status = list(songs_collection.find({
            "$or": [
                {"enrichment_status": {"$exists": False}},
                {"enrichment_status": None}
            ]
        }))

        if not missing_status:
            logger.info("  All songs have enrichment_status set")
            return

        logger.info(f"  Found {len(missing_status)} songs without enrichment_status")

        # Auto-fix: Set to 'processed' if has_lyrics is set, otherwise 'pending'
        for song in missing_status:
            has_lyrics = song.get('has_lyrics')

            if has_lyrics is not None:
                # Song has been processed (has_lyrics is True or False)
                new_status = ENRICHMENT_STATUS_PROCESSED
            else:
                # Song hasn't been processed yet
                new_status = ENRICHMENT_STATUS_PENDING

            result = songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {"enrichment_status": new_status}}
            )

            if result.modified_count > 0:
                self.fixes_applied += 1

        logger.info(f"  Fixed {len(missing_status)} songs with missing enrichment_status")

    def validate_required_fields(self):
        """Validate that required fields are not null/empty."""
        logger.info("Validating required fields...")

        # Use new field names for StreamingHistory
        collections_to_check = [
            (STREAMING_COLLECTION, [
                STREAMING_FIELDS['track_name'],
                STREAMING_FIELDS['artist_name'],
                "ts_utc"
            ]),
            (SONGS_MASTER_COLLECTION, ["song_name", "artist_name", "spotify_track_uri"]),
            (ARTISTS_MASTER_COLLECTION, ["artist_name"])
        ]

        for collection_name, required_fields in collections_to_check:
            collection = self.db.get_collection(collection_name)

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
        """Validate that is_soundtrack fields are proper booleans."""
        logger.info("Validating boolean fields...")

        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]

        for collection_name in collections_to_check:
            collection = self.db.get_collection(collection_name)

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
        """Validate that language fields contain proper values."""
        logger.info("Validating language fields...")

        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]

        for collection_name in collections_to_check:
            collection = self.db.get_collection(collection_name)

            null_language_count = collection.count_documents({"language": None})
            if null_language_count > 0:
                self.log_error("missing_language", "Language field is null", {
                    "collection": collection_name,
                    "count": null_language_count
                })

    def validate_detection_methods(self):
        """Validate detection_method field values."""
        logger.info("Validating detection methods...")

        collections_to_check = [SONGS_MASTER_COLLECTION, ARTISTS_MASTER_COLLECTION]

        for collection_name in collections_to_check:
            collection = self.db.get_collection(collection_name)

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
        """Validate Hebrew/Japanese character detection consistency."""
        logger.info("Validating character detection consistency...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)

        total_songs = songs_collection.count_documents({})
        inconsistencies = 0

        # Limit to 5000 songs for performance
        for skip in range(0, min(total_songs, 5000), BATCH_SIZE):
            songs_batch = list(songs_collection.find({}).skip(skip).limit(BATCH_SIZE))

            for song in songs_batch:
                song_name = song.get('song_name', '')
                artist_name = song.get('artist_name', '')
                language = song.get('language', '')
                combined_text = f"{song_name} {artist_name}"

                # Use shared LanguageDetector
                detected_lang = LanguageDetector.detect_by_characters(combined_text)

                if detected_lang and detected_lang != language:
                    # Skip if current language is Soundtrack (takes precedence)
                    if language == "Soundtrack":
                        continue

                    inconsistencies += 1
                    if inconsistencies <= 10:
                        self.log_error("character_detection_inconsistency",
                                       f"{detected_lang} characters found but language is {language}", {
                                           "song_name": song_name,
                                           "artist_name": artist_name,
                                           "current_language": language,
                                           "expected_language": detected_lang
                                       })

        if inconsistencies > 10:
            logger.warning(f"Found {inconsistencies} character detection inconsistencies (showing first 10)")
        elif inconsistencies > 0:
            logger.warning(f"Found {inconsistencies} character detection inconsistencies")
        else:
            logger.info("  No character detection inconsistencies found")

    def validate_cross_collection_relationships(self):
        """Validate relationships between collections (sample check)."""
        logger.info("Validating cross-collection relationships...")

        streaming_collection = self.db.get_collection(STREAMING_COLLECTION)
        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)
        artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)

        # Sample recent records
        recent_records = list(streaming_collection.find().sort("ts_utc", -1).limit(100))

        missing_songs = 0
        missing_artists = 0

        for record in recent_records:
            # Use new field names
            track_name = record.get(STREAMING_FIELDS['track_name'], "")
            artist_name = record.get(STREAMING_FIELDS['artist_name'], "")

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
            logger.info("  Cross-collection relationships look good (sample check)")

    def validate_duplicates(self):
        """Check for duplicate records (sample check)."""
        logger.info("Validating for duplicates...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)

        # Check for duplicate songs
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
            {"$limit": 10}
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
        artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)

        pipeline = [
            {"$group": {
                "_id": {"artist_name_lower": {"$toLower": "$artist_name"}},
                "count": {"$sum": 1},
                "docs": {"$push": {"artist_name": "$artist_name"}}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": 10}
        ]

        duplicate_artists = list(artists_collection.aggregate(pipeline))

        if duplicate_artists:
            for dup in duplicate_artists:
                self.log_error("duplicates", "Duplicate artists found", {
                    "artist_variations": [doc["artist_name"] for doc in dup["docs"]],
                    "duplicate_count": dup["count"]
                })

    def validate_spotify_data(self):
        """Validate Spotify URI and metadata."""
        logger.info("Validating Spotify data...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)
        artists_collection = self.db.get_collection(ARTISTS_MASTER_COLLECTION)

        # Check for missing Spotify URIs in songs
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

        # Check for missing Spotify URIs in artists
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

    def validate_album_names(self):
        """Validate album_name field in songs_master."""
        logger.info("Validating album names...")

        songs_collection = self.db.get_collection(SONGS_MASTER_COLLECTION)

        missing_albums = songs_collection.count_documents({
            "$or": [
                {"album_name": None},
                {"album_name": ""},
                {"album_name": {"$exists": False}}
            ]
        })

        if missing_albums > 0:
            self.log_error("missing_album_name", "Missing album_name in songs", {
                "collection": "songs_master",
                "count": missing_albums
            })
        else:
            logger.info("  All songs have album_name set")

    def get_collection_stats(self) -> Dict[str, int]:
        """Get document counts for all collections."""
        return {
            "streaming_history": self.db.get_collection(STREAMING_COLLECTION).count_documents({}),
            "songs_master": self.db.get_collection(SONGS_MASTER_COLLECTION).count_documents({}),
            "artists_master": self.db.get_collection(ARTISTS_MASTER_COLLECTION).count_documents({}),
        }


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("DATA VALIDATION & CONSISTENCY CHECK")
    logger.info("=" * 60)

    try:
        with MongoDBConnection() as db_conn:
            validator = DataValidator(db_conn)

            # Get collection stats
            stats = validator.get_collection_stats()
            logger.info(f"Collection counts:")
            logger.info(f"  StreamingHistory: {stats['streaming_history']}")
            logger.info(f"  songs_master: {stats['songs_master']}")
            logger.info(f"  artists_master: {stats['artists_master']}")
            logger.info("")

            logger.info("Starting comprehensive data validation...\n")

            # 1. Fix soundtrack consistency (auto-fix)
            validator.validate_soundtrack_consistency()

            # 2. Fix/validate enrichment_status (auto-fix)
            validator.validate_enrichment_status()

            # 3. Validate required fields
            validator.validate_required_fields()

            # 4. Validate boolean fields
            validator.validate_boolean_fields()

            # 5. Validate language fields
            validator.validate_language_fields()

            # 6. Validate detection methods
            validator.validate_detection_methods()

            # 7. Validate character detection consistency
            validator.validate_character_detection_consistency()

            # 8. Validate cross-collection relationships
            validator.validate_cross_collection_relationships()

            # 9. Check for duplicates
            validator.validate_duplicates()

            # 10. Validate Spotify data
            validator.validate_spotify_data()

            # 11. Validate album names
            validator.validate_album_names()

            # Summary
            logger.info("=" * 60)
            logger.info("DATA VALIDATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total errors found: {len(validator.errors)}")
            logger.info(f"Automatic fixes applied: {validator.fixes_applied}")

            if len(validator.errors) == 0:
                logger.info("All data is consistent - no issues found!")
            else:
                logger.warning(f"Found {len(validator.errors)} validation issues")

                # Group errors by category for summary
                error_categories: Dict[str, int] = {}
                for error in validator.errors:
                    cat = error["category"]
                    error_categories[cat] = error_categories.get(cat, 0) + 1

                for category, count in error_categories.items():
                    logger.info(f"  {category}: {count} issues")

    except Exception as e:
        logger.error(f"Error during validation: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
