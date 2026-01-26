#!/usr/bin/env python3
"""
Fix False Soundtrack Classifications

One-time fix script to correct songs that were incorrectly marked as soundtracks
due to problematic keywords like 'remastered', 'live at', etc.

This script:
1. Finds songs where is_soundtrack=True AND has_lyrics=True (likely false positives)
2. Re-runs soundtrack classification WITHOUT the bad keywords
3. If no longer a soundtrack, re-detects language from lyrics
4. Updates both songs_master and artists_master collections
"""

import logging
import sys
from pathlib import Path
from collections import Counter
from typing import Optional, Tuple

# Add project root to path for direct script execution
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import MongoDBConnection
from src.utils.language_detection import LanguageDetector, SoundtrackClassifier
from src.utils.config import (
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    SOUNDTRACK_GENRES,
    FILM_COMPOSERS,
    CLASSICAL_COMPOSERS,
    ORCHESTRA_KEYWORDS,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def should_be_soundtrack(
    song_name: str,
    artist_name: str,
    album_name: str,
    genres: str
) -> bool:
    """
    Re-classify if song should be soundtrack WITHOUT the problematic keywords.

    Uses only:
    - Known composers
    - Soundtrack genres
    - Orchestra keywords (as whole words, not substrings)
    """
    # Check if artist is a known composer
    artist_lower = artist_name.lower().strip()
    if artist_lower in FILM_COMPOSERS or artist_lower in CLASSICAL_COMPOSERS:
        return True

    # Check genres
    if genres:
        genre_list = [g.strip().lower() for g in str(genres).split(',')]
        if any(g in SOUNDTRACK_GENRES for g in genre_list):
            return True

    # Check for orchestra keywords as whole words (not substrings)
    import re
    combined = f"{song_name} {artist_name} {album_name}".lower()
    words = set(re.findall(r'\b\w+\b', combined))
    if words & ORCHESTRA_KEYWORDS:
        return True

    return False


def get_artist_genres(db_connection: MongoDBConnection, artist_name: str) -> str:
    """Get genres for an artist from artists_master."""
    collection = db_connection.get_collection(ARTISTS_MASTER_COLLECTION)
    artist = collection.find_one({"artist_name": artist_name})
    if artist:
        return artist.get("genres", "") or ""
    return ""


def fix_false_soundtracks(db_connection: MongoDBConnection, dry_run: bool = False):
    """
    Find and fix songs incorrectly marked as soundtracks.

    Args:
        db_connection: MongoDB connection
        dry_run: If True, only report what would be changed without making changes
    """
    songs_collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)
    artists_collection = db_connection.get_collection(ARTISTS_MASTER_COLLECTION)

    # Find potential false positives: is_soundtrack=True AND has_lyrics=True
    # Songs with lyrics are likely NOT instrumentals/soundtracks
    query = {
        "is_soundtrack": True,
        "has_lyrics": True,
        "lyrics": {"$ne": None, "$ne": ""}
    }

    potential_false_positives = list(songs_collection.find(query))
    logger.info(f"Found {len(potential_false_positives)} songs marked as soundtrack but have lyrics")

    if not potential_false_positives:
        logger.info("No false positives found!")
        return

    fixed_songs = 0
    artists_to_recheck = set()

    for song in potential_false_positives:
        song_name = song.get("song_name", "")
        artist_name = song.get("artist_name", "")
        album_name = song.get("album_name", "")
        lyrics = song.get("lyrics", "")

        # Get artist genres
        genres = get_artist_genres(db_connection, artist_name)

        # Re-classify with strict rules (no problematic keywords)
        still_soundtrack = should_be_soundtrack(song_name, artist_name, album_name, genres)

        if still_soundtrack:
            # It's legitimately a soundtrack, skip
            continue

        # This is a false positive - fix it!
        logger.info(f"\nFALSE POSITIVE: '{song_name}' by {artist_name}")
        logger.info(f"  Album: {album_name}")
        logger.info(f"  Old: is_soundtrack=True, language=Soundtrack")

        # Re-detect language from lyrics
        new_language, detection_method = LanguageDetector.detect_language(
            song_name=song_name,
            artist_name=artist_name,
            lyrics=lyrics,
            is_soundtrack=False,  # Not a soundtrack anymore!
            genres=genres,
            album_name=album_name
        )

        logger.info(f"  New: is_soundtrack=False, language={new_language} (method: {detection_method})")

        if not dry_run:
            # Update the song
            songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {
                    "is_soundtrack": False,
                    "language": new_language,
                    "detection_method": detection_method
                }}
            )

        fixed_songs += 1
        artists_to_recheck.add(artist_name)

    logger.info(f"\n{'[DRY RUN] Would fix' if dry_run else 'Fixed'} {fixed_songs} songs")

    # Now fix artists that may have been incorrectly marked as soundtrack
    if artists_to_recheck and not dry_run:
        logger.info(f"\nRechecking {len(artists_to_recheck)} artists...")
        fix_artist_languages(db_connection, artists_to_recheck)


def fix_artist_languages(db_connection: MongoDBConnection, artist_names: set):
    """
    Recalculate artist language based on majority of their songs.
    """
    songs_collection = db_connection.get_collection(SONGS_MASTER_COLLECTION)
    artists_collection = db_connection.get_collection(ARTISTS_MASTER_COLLECTION)

    fixed_artists = 0

    for artist_name in artist_names:
        # Get all songs by this artist
        artist_songs = list(songs_collection.find({"artist_name": artist_name}))

        if not artist_songs:
            continue

        # Count languages (excluding Soundtrack and Unknown)
        lang_counter = Counter()
        soundtrack_count = 0

        for song in artist_songs:
            lang = song.get("language", "Unknown")
            is_st = song.get("is_soundtrack", False)

            if is_st:
                soundtrack_count += 1
            elif lang and lang not in ["Unknown", "Soundtrack"]:
                lang_counter[lang] += 1

        # Determine if artist should still be soundtrack
        total_songs = len(artist_songs)

        # Get artist genres for classification
        genres = get_artist_genres(db_connection, artist_name)
        artist_lower = artist_name.lower().strip()

        is_known_composer = (
            artist_lower in FILM_COMPOSERS or
            artist_lower in CLASSICAL_COMPOSERS
        )

        has_soundtrack_genre = False
        if genres:
            genre_list = [g.strip().lower() for g in str(genres).split(',')]
            has_soundtrack_genre = any(g in SOUNDTRACK_GENRES for g in genre_list)

        # Artist is soundtrack if: known composer OR has soundtrack genre OR majority of songs are soundtrack
        should_be_st = (
            is_known_composer or
            has_soundtrack_genre or
            (soundtrack_count > total_songs / 2)
        )

        if should_be_st:
            new_language = "Soundtrack"
            detection_method = "soundtrack"
        elif lang_counter:
            new_language = lang_counter.most_common(1)[0][0]
            detection_method = "majority_songs"
        else:
            new_language = "Unknown"
            detection_method = "unknown"

        # Get current artist record
        artist_record = artists_collection.find_one({"artist_name": artist_name})
        if not artist_record:
            continue

        old_language = artist_record.get("language", "Unknown")
        old_is_soundtrack = artist_record.get("is_soundtrack", False)

        # Check if anything changed
        if old_language != new_language or old_is_soundtrack != should_be_st:
            logger.info(f"\nARTIST: {artist_name}")
            logger.info(f"  Old: is_soundtrack={old_is_soundtrack}, language={old_language}")
            logger.info(f"  New: is_soundtrack={should_be_st}, language={new_language}")

            artists_collection.update_one(
                {"_id": artist_record["_id"]},
                {"$set": {
                    "is_soundtrack": should_be_st,
                    "language": new_language,
                    "detection_method": detection_method
                }}
            )
            fixed_artists += 1

    logger.info(f"\nFixed {fixed_artists} artists")


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("FIX FALSE SOUNDTRACK CLASSIFICATIONS")
    logger.info("=" * 60)

    # Set to True for a dry run (no changes made)
    DRY_RUN = True

    if DRY_RUN:
        logger.info("*** DRY RUN MODE - No changes will be made ***")

    try:
        with MongoDBConnection() as db_conn:
            fix_false_soundtracks(db_conn, dry_run=DRY_RUN)

        logger.info("\n" + "=" * 60)
        logger.info("COMPLETE!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during fix: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
