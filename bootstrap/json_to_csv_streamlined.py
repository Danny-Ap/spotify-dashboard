#!/usr/bin/env python3
"""
Spotify Streaming History Merger - Streamlined Version
Processes Spotify Extended Streaming History JSON files and creates:
1. Original CSV: All data with all columns
2. Cleaned CSV: Music-only data with date/duration columns
"""

import os
import json
import glob
import pandas as pd
from typing import List, Set

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_DIR = "Spotify Extended Streaming History - January 2026"
OUTPUT_DIR = "Spotify Extended Streaming History - January 2026"
ORIGINAL_CSV = "spotify_streaming_history_original.csv"
CLEANED_CSV = "spotify_streaming_history_cleaned.csv"

# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def read_json_file(file_path: str) -> dict:
    """Read JSON file with UTF-8-sig encoding (UTF-8 fallback)."""
    try:
        with open(file_path, "r", encoding='utf-8-sig') as f:
            return json.load(f)
    except:
        with open(file_path, "r", encoding='utf-8') as f:
            return json.load(f)

def discover_all_columns(json_files: List[str]) -> Set[str]:
    """Scan all JSON files to discover every possible column."""
    all_columns = set()

    for file_path in json_files:
        data = read_json_file(file_path)
        records = data if isinstance(data, list) else []

        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, list):
                    records.extend(value)

        for record in records:
            if isinstance(record, dict):
                all_columns.update(record.keys())

    return all_columns

def process_json_files(json_files: List[str], all_columns: Set[str]) -> pd.DataFrame:
    """Process all JSON files and create unified DataFrame."""
    all_records = []

    for i, file_path in enumerate(json_files, 1):
        print(f"Processing {i}/{len(json_files)}: {os.path.basename(file_path)}")

        data = read_json_file(file_path)
        records = data if isinstance(data, list) else []

        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, list):
                    records.extend(value)

        for record in records:
            if isinstance(record, dict):
                normalized_record = {col: record.get(col, None) for col in all_columns}
                all_records.append(normalized_record)

    print(f"✅ {len(json_files)} files processed successfully")
    return pd.DataFrame(all_records)

def create_original_csv(df: pd.DataFrame, output_path: str) -> pd.DataFrame:
    """Create original CSV with all data and columns."""
    print("\n📄 Creating original CSV...")

    original_df = df.copy()

    # Normalize timestamps
    if 'ts' in original_df.columns:
        original_df['ts_utc'] = pd.to_datetime(original_df['ts'], utc=True, errors='coerce')
        original_df = original_df[~original_df['ts_utc'].isna()].copy()
        original_df = original_df.sort_values('ts_utc').reset_index(drop=True)
        print(f"  ✅ Normalized timestamps using 'ts' column")

    # Convert ms_played to integer
    if 'ms_played' in original_df.columns:
        original_df['ms_played'] = pd.to_numeric(original_df['ms_played'], errors='coerce').fillna(0).astype('int64')

    # Save with UTF-8-sig encoding
    original_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  ✅ Saved: {output_path}")
    print(f"  📊 Total records: {len(original_df):,}")

    if 'ts_utc' in original_df.columns:
        print(f"  📅 Date range: {original_df['ts_utc'].min().strftime('%Y-%m-%d')} to {original_df['ts_utc'].max().strftime('%Y-%m-%d')}")

    return original_df

def create_cleaned_csv(df: pd.DataFrame, output_path: str) -> pd.DataFrame:
    """Create cleaned CSV with music-only data and additional columns."""
    print("\n🎵 Creating cleaned CSV...")

    cleaned_df = df.copy()

    # Filter to music only (remove podcasts and audiobooks)
    podcast_cols = ['episode_name', 'episode_show_name', 'spotify_episode_uri']
    audiobook_cols = ['audiobook_title', 'audiobook_uri', 'audiobook_chapter_uri', 'audiobook_chapter_title']

    is_podcast = False
    is_audiobook = False

    for col in podcast_cols:
        if col in cleaned_df.columns:
            is_podcast = is_podcast | cleaned_df[col].notna()

    for col in audiobook_cols:
        if col in cleaned_df.columns:
            is_audiobook = is_audiobook | cleaned_df[col].notna()

    cleaned_df = cleaned_df[~(is_podcast | is_audiobook)].copy()
    print(f"  ✅ Filtered to music only: {len(cleaned_df):,} records")

    # Drop podcast/audiobook columns
    columns_to_drop = podcast_cols + audiobook_cols
    existing_drops = [col for col in columns_to_drop if col in cleaned_df.columns]
    if existing_drops:
        cleaned_df = cleaned_df.drop(columns=existing_drops)

    # Add date-related columns
    if 'ts_utc' in cleaned_df.columns:
        cleaned_df['Date'] = cleaned_df['ts_utc'].dt.strftime('%Y-%m-%d')
        cleaned_df['Year'] = cleaned_df['ts_utc'].dt.year
        cleaned_df['Month'] = cleaned_df['ts_utc'].dt.strftime('%B')
        cleaned_df['Day'] = cleaned_df['ts_utc'].dt.strftime('%A')
        print(f"  ✅ Added date columns: Date, Year, Month, Day")

    # Add duration conversion columns
    if 'ms_played' in cleaned_df.columns:
        cleaned_df['duration_seconds'] = cleaned_df['ms_played'] / 1000.0
        cleaned_df['duration_minutes'] = cleaned_df['duration_seconds'] / 60.0
        cleaned_df['duration_hours'] = cleaned_df['duration_minutes'] / 60.0
        print(f"  ✅ Added duration columns: seconds, minutes, hours")

    # Reorder columns for better readability
    if 'ts_utc' in cleaned_df.columns:
        # Define preferred column order
        priority_cols = ['ts_utc', 'Date', 'Year', 'Month', 'Day']

        if 'ms_played' in cleaned_df.columns:
            priority_cols.extend(['ms_played', 'duration_seconds', 'duration_minutes', 'duration_hours'])

        # Add track metadata columns
        for col in ['master_metadata_track_name', 'master_metadata_album_artist_name', 'master_metadata_album_album_name']:
            if col in cleaned_df.columns:
                priority_cols.append(col)

        # Add remaining columns
        remaining = [col for col in cleaned_df.columns if col not in priority_cols]
        new_order = priority_cols + remaining
        cleaned_df = cleaned_df[new_order]
        print(f"  ✅ Reordered columns for readability")

    # Save with UTF-8-sig encoding
    cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  ✅ Saved: {output_path}")
    print(f"  📊 Total records: {len(cleaned_df):,}")

    return cleaned_df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function."""
    print("=" * 70)
    print("SPOTIFY STREAMING HISTORY MERGER - STREAMLINED VERSION")
    print("=" * 70)

    # Find JSON files
    print(f"\n🔍 Scanning for JSON files in: {INPUT_DIR}")
    json_files = glob.glob(os.path.join(INPUT_DIR, "*.json"), recursive=False)
    json_files += glob.glob(os.path.join(INPUT_DIR, "**", "*.json"), recursive=True)
    json_files = sorted(list(set(json_files)))

    if not json_files:
        print("❌ ERROR: No JSON files found!")
        return

    print(f"✅ Found {len(json_files)} JSON files")

    # Discover all columns
    print(f"\n🔍 Discovering columns across all files...")
    all_columns = discover_all_columns(json_files)
    print(f"✅ Found {len(all_columns)} unique columns")

    # Process all JSON files
    print(f"\n⚙️  Processing {len(json_files)} files...")
    df = process_json_files(json_files, all_columns)

    if df.empty:
        print("❌ ERROR: No data was processed!")
        return

    # Create output paths
    original_path = os.path.join(OUTPUT_DIR, ORIGINAL_CSV)
    cleaned_path = os.path.join(OUTPUT_DIR, CLEANED_CSV)

    # Create both CSVs
    original_df = create_original_csv(df, original_path)
    cleaned_df = create_cleaned_csv(original_df, cleaned_path)

    # Summary
    print("\n" + "=" * 70)
    print("✅ PROCESSING COMPLETE!")
    print("=" * 70)
    print(f"📁 Original CSV: {original_path}")
    print(f"   • Records: {len(original_df):,}")
    print(f"   • Columns: {len(original_df.columns)}")
    print(f"\n📁 Cleaned CSV: {cleaned_path}")
    print(f"   • Music records: {len(cleaned_df):,}")
    print(f"   • Columns: {len(cleaned_df.columns)}")
    print("=" * 70)

if __name__ == "__main__":
    main()
