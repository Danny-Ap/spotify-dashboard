#!/usr/bin/env python3
"""
Cleanup Script: Remove Duplicate Tracks and Fix Format

This script:
1. Finds duplicate tracks in StreamingHistory (same ts + spotify_track_uri)
2. Removes duplicates, keeping the oldest document (_id)
3. Fixes format inconsistency for API-sourced records:
   - Converts ts_utc from Date to string format
   - Ensures ts is the main Date field

Usage:
    python -m src.migrations.cleanup_duplicates

Configuration:
    Set START_DATE below to control which records to process.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any

from src.utils.database import MongoDBConnection
from src.utils.config import STREAMING_COLLECTION

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Change this date to control the cleanup range
# Format: DD-MM-YYYY
# ============================================================================
START_DATE = "18-01-2026"
# ============================================================================


def parse_date(date_str: str) -> datetime:
    """Parse date string in DD-MM-YYYY format to datetime."""
    return datetime.strptime(date_str, "%d-%m-%Y")


def find_duplicates(db_connection: MongoDBConnection, start_date: datetime) -> List[Dict[str, Any]]:
    """
    Find duplicate records based on ts + spotify_track_uri.

    Returns:
        List of duplicate groups, each containing the documents to review.
    """
    collection = db_connection.get_collection(STREAMING_COLLECTION)

    # Aggregation pipeline to find duplicates
    pipeline = [
        # Filter by date range and API source
        {
            "$match": {
                "data_source": "recently_played_api",
                "$or": [
                    {"ts": {"$gte": start_date}},
                    {"ts_utc": {"$gte": start_date}}
                ]
            }
        },
        # Group by ts + spotify_track_uri to find duplicates
        {
            "$group": {
                "_id": {
                    "ts": "$ts",
                    "spotify_track_uri": "$spotify_track_uri"
                },
                "count": {"$sum": 1},
                "docs": {"$push": {
                    "_id": "$_id",
                    "master_metadata_track_name": "$master_metadata_track_name",
                    "master_metadata_album_artist_name": "$master_metadata_album_artist_name",
                    "ts_utc": "$ts_utc"
                }}
            }
        },
        # Only keep groups with more than 1 document (duplicates)
        {
            "$match": {
                "count": {"$gt": 1}
            }
        },
        # Sort by count descending to see worst offenders first
        {
            "$sort": {"count": -1}
        }
    ]

    duplicates = list(collection.aggregate(pipeline))
    return duplicates


def get_api_records_count(db_connection: MongoDBConnection, start_date: datetime) -> int:
    """Get count of API-sourced records from start date."""
    collection = db_connection.get_collection(STREAMING_COLLECTION)

    count = collection.count_documents({
        "data_source": "recently_played_api",
        "$or": [
            {"ts": {"$gte": start_date}},
            {"ts_utc": {"$gte": start_date}}
        ]
    })

    return count


def print_dry_run_report(duplicates: List[Dict], api_records_count: int, start_date: datetime):
    """Print a detailed report of what will be cleaned up."""
    print("\n" + "=" * 70)
    print("DRY RUN REPORT - Duplicate Cleanup")
    print("=" * 70)
    print(f"Start date: {start_date.strftime('%d-%m-%Y')}")
    print(f"Total API-sourced records from start date: {api_records_count}")
    print(f"Duplicate groups found: {len(duplicates)}")

    total_duplicates = sum(group["count"] - 1 for group in duplicates)
    print(f"Total duplicate records to delete: {total_duplicates}")

    if duplicates:
        print("\n" + "-" * 70)
        print("Top duplicate tracks (showing first 20):")
        print("-" * 70)

        for i, group in enumerate(duplicates[:20], 1):
            track_name = group["docs"][0].get("master_metadata_track_name", "Unknown")
            artist_name = group["docs"][0].get("master_metadata_album_artist_name", "Unknown")
            ts = group["_id"].get("ts", "Unknown")
            count = group["count"]

            print(f"  {i}. '{track_name}' by {artist_name}")
            print(f"     Timestamp: {ts} | Duplicates: {count - 1} (keeping 1, deleting {count - 1})")

        if len(duplicates) > 20:
            print(f"\n  ... and {len(duplicates) - 20} more duplicate groups")

    print("\n" + "-" * 70)
    print("Format fix: All API-sourced records will have ts_utc converted to string")
    print("-" * 70)
    print()


def delete_duplicates(db_connection: MongoDBConnection, duplicates: List[Dict]) -> int:
    """
    Delete duplicate records, keeping the oldest one (smallest _id).

    Returns:
        Number of records deleted.
    """
    collection = db_connection.get_collection(STREAMING_COLLECTION)
    deleted_count = 0

    for group in duplicates:
        docs = group["docs"]

        # Sort by _id to keep the oldest (ObjectId is chronological)
        sorted_docs = sorted(docs, key=lambda x: x["_id"])

        # Keep the first one, delete the rest
        ids_to_delete = [doc["_id"] for doc in sorted_docs[1:]]

        if ids_to_delete:
            result = collection.delete_many({"_id": {"$in": ids_to_delete}})
            deleted_count += result.deleted_count

            track_name = docs[0].get("master_metadata_track_name", "Unknown")
            logger.info(f"Deleted {result.deleted_count} duplicates of '{track_name}'")

    return deleted_count


def fix_format(db_connection: MongoDBConnection, start_date: datetime) -> int:
    """
    Fix format for API-sourced records:
    - Convert ts_utc from Date to string format (matching Extended Streaming History)
    - Ensure ts is a proper Date field

    Returns:
        Number of records updated.
    """
    collection = db_connection.get_collection(STREAMING_COLLECTION)
    updated_count = 0

    # Find all API-sourced records that need format fixing
    # These have ts_utc as a Date object instead of string
    cursor = collection.find({
        "data_source": "recently_played_api",
        "ts_utc": {"$type": "date"}  # ts_utc is currently a Date, needs to be string
    })

    for doc in cursor:
        ts_utc_date = doc["ts_utc"]

        # Convert ts_utc to string format like Extended Streaming History
        # Format: "2017-07-16 19:58:18+00:00"
        ts_utc_string = ts_utc_date.strftime("%Y-%m-%d %H:%M:%S+00:00")

        # The ts field should be a Date (it's currently a string in API records)
        # Parse the current ts string to Date if needed
        ts_value = doc.get("ts")
        if isinstance(ts_value, str):
            # Parse "2026-01-22T08:52:12Z" format to datetime
            ts_date = datetime.fromisoformat(ts_value.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            ts_date = ts_value

        # Update the document
        result = collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "ts_utc": ts_utc_string,
                    "ts": ts_date
                }
            }
        )

        if result.modified_count > 0:
            updated_count += 1

    return updated_count


def main():
    """Main execution function."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("\n" + "=" * 70)
    print("STREAMING HISTORY CLEANUP SCRIPT")
    print("=" * 70)
    print("This script will:")
    print("  1. Remove duplicate tracks (same timestamp + track URI)")
    print("  2. Fix format inconsistency (ts_utc to string, ts to Date)")
    print("=" * 70)

    try:
        start_date = parse_date(START_DATE)
        print(f"\nProcessing records from: {START_DATE}")
    except ValueError as e:
        print(f"Error: Invalid START_DATE format. Use DD-MM-YYYY. Got: {START_DATE}")
        return

    try:
        with MongoDBConnection() as db_conn:
            # Step 1: Find duplicates
            print("\nSearching for duplicates...")
            duplicates = find_duplicates(db_conn, start_date)
            api_records_count = get_api_records_count(db_conn, start_date)

            # Step 2: Print dry run report
            print_dry_run_report(duplicates, api_records_count, start_date)

            if not duplicates:
                print("No duplicates found!")
                print("\nProceeding to format fix only...")
            else:
                total_to_delete = sum(group["count"] - 1 for group in duplicates)
                print(f"Ready to delete {total_to_delete} duplicate records.")

            # Step 3: Ask for confirmation
            print("\n" + "=" * 70)
            response = input("Do you want to proceed? (yes/no): ").strip().lower()

            if response != "yes":
                print("\nAborted. No changes made.")
                return

            # Step 4: Delete duplicates
            if duplicates:
                print("\nDeleting duplicates...")
                deleted_count = delete_duplicates(db_conn, duplicates)
                print(f"Deleted {deleted_count} duplicate records.")
            else:
                deleted_count = 0

            # Step 5: Fix format
            print("\nFixing format for API-sourced records...")
            fixed_count = fix_format(db_conn, start_date)
            print(f"Fixed format for {fixed_count} records.")

            # Summary
            print("\n" + "=" * 70)
            print("CLEANUP SUMMARY")
            print("=" * 70)
            print(f"Duplicate records deleted: {deleted_count}")
            print(f"Records with format fixed: {fixed_count}")
            print("=" * 70)
            print("\nCleanup complete!")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
