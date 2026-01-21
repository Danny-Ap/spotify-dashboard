#!/usr/bin/env python3
"""
One-time Migration Script: Populate Language in StreamingHistory

Copies the language field from songs_master to StreamingHistory documents
using spotify_track_uri as the join key.

This denormalization makes dashboard queries much faster by avoiding
expensive $lookup operations on the 14MB songs_master collection.

Usage:
    python -m src.migrations.populate_streaming_language
"""

import logging
from typing import Dict

from src.utils.database import MongoDBConnection
from src.utils.config import (
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
)

logger = logging.getLogger(__name__)


def build_language_map(db_connection: MongoDBConnection) -> Dict[str, str]:
    """
    Build a mapping of spotify_track_uri -> language from songs_master.
    Only fetches URI and language fields to minimize memory usage.
    """
    collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)

    language_map = {}

    # Only get URIs with valid language
    cursor = collection.find(
        {
            "spotify_track_uri": {"$exists": True, "$ne": None},
            "language": {"$exists": True, "$ne": None}
        },
        {"spotify_track_uri": 1, "language": 1, "_id": 0}
    )

    for doc in cursor:
        language_map[doc["spotify_track_uri"]] = doc["language"]

    logger.info(f"Built language mapping with {len(language_map)} entries")
    return language_map


def get_streaming_records_without_language(db_connection: MongoDBConnection) -> int:
    """Count StreamingHistory records missing language field."""
    collection = db_connection.get_collection(STREAMING_COLLECTION)

    count = collection.count_documents({
        "spotify_track_uri": {"$exists": True, "$ne": None},
        "$or": [
            {"language": {"$exists": False}},
            {"language": None}
        ]
    })

    return count


def populate_language_in_streaming(
    db_connection: MongoDBConnection,
    language_map: Dict[str, str],
    batch_size: int = 1000
) -> tuple:
    """
    Update StreamingHistory records with language from songs_master.

    Uses bulk operations for efficiency.

    Returns:
        Tuple of (updated_count, not_found_count)
    """
    collection = db_connection.get_collection(STREAMING_COLLECTION)

    updated_count = 0
    not_found_count = 0
    processed = 0

    # Find records missing language
    cursor = collection.find(
        {
            "spotify_track_uri": {"$exists": True, "$ne": None},
            "$or": [
                {"language": {"$exists": False}},
                {"language": None}
            ]
        },
        {"_id": 1, "spotify_track_uri": 1}
    )

    from pymongo import UpdateOne
    bulk_operations = []

    for doc in cursor:
        track_uri = doc.get("spotify_track_uri")
        processed += 1

        if track_uri in language_map:
            language = language_map[track_uri]
            bulk_operations.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {"language": language}}
                )
            )
        else:
            not_found_count += 1

        # Execute batch
        if len(bulk_operations) >= batch_size:
            result = collection.bulk_write(bulk_operations)
            updated_count += result.modified_count
            logger.info(f"Processed {processed} records, updated {updated_count}")
            bulk_operations = []

    # Execute remaining operations
    if bulk_operations:
        result = collection.bulk_write(bulk_operations)
        updated_count += result.modified_count

    logger.info(f"Total processed: {processed}")
    logger.info(f"Total updated: {updated_count}")
    logger.info(f"Not found in songs_master: {not_found_count}")

    return updated_count, not_found_count


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("STREAMING HISTORY LANGUAGE MIGRATION")
    logger.info("=" * 60)
    logger.info("Populating language field in StreamingHistory from songs_master")
    logger.info("=" * 60)

    try:
        with MongoDBConnection() as db_conn:
            # Step 1: Check how many records need updating
            missing_count = get_streaming_records_without_language(db_conn)
            logger.info(f"StreamingHistory records missing language: {missing_count}")

            if missing_count == 0:
                logger.info("All StreamingHistory records already have language set!")
                return

            # Step 2: Build language mapping from songs_master
            language_map = build_language_map(db_conn)

            if not language_map:
                logger.error("No language data found in songs_master!")
                return

            # Step 3: Update StreamingHistory with languages
            updated, not_found = populate_language_in_streaming(db_conn, language_map)

            # Summary
            logger.info("=" * 60)
            logger.info("MIGRATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Records needing update: {missing_count}")
            logger.info(f"Successfully updated: {updated}")
            logger.info(f"No language in songs_master: {not_found}")

            if not_found > 0:
                logger.info(
                    f"{not_found} streaming records have no matching song in songs_master "
                    "or the song has no language set. This is expected for some tracks."
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
