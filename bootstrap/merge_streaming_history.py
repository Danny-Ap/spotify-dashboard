"""
Script to merge new Spotify streaming history from MongoDB export into the cleaned CSV.
Finds the last entry in cleaned.csv and appends all newer entries from the MongoDB export.
"""

import pandas as pd
from datetime import datetime
import shutil
from pathlib import Path

# File paths
CLEANED_CSV = r"Spotify Extended Streaming History - January 2026\spotify_streaming_history_cleaned.csv"
MONGODB_EXPORT = r"Spotify.StreamingHistory.csv"

def create_backup(file_path: str) -> str:
    """Create a timestamped backup of the file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.backup_{timestamp}"
    shutil.copy2(file_path, backup_path)
    print(f"Backup created: {backup_path}")
    return backup_path

def parse_timestamp(ts_str: str) -> datetime:
    """Parse various timestamp formats to datetime (always returns naive UTC)."""
    if pd.isna(ts_str):
        return None
    ts_str = str(ts_str)
    # Use pandas to parse, then convert to naive UTC datetime
    try:
        dt = pd.to_datetime(ts_str, utc=True)
        # Remove timezone info to get naive datetime (already in UTC)
        return dt.to_pydatetime().replace(tzinfo=None)
    except:
        return None

def format_date_for_cleaned(dt: datetime) -> str:
    """Format date as DD/MM/YYYY for the cleaned CSV."""
    if dt is None:
        return ""
    return dt.strftime("%d/%m/%Y")

def get_day_name(dt: datetime) -> str:
    """Get the day name (Monday, Tuesday, etc.)."""
    if dt is None:
        return ""
    return dt.strftime("%A")

def get_month_name(dt: datetime) -> str:
    """Get the month name (January, February, etc.)."""
    if dt is None:
        return ""
    return dt.strftime("%B")

def format_ts_utc_for_cleaned(ts_str: str) -> str:
    """Format ts_utc to match cleaned.csv format: YYYY-MM-DD HH:MM:SS+00:00"""
    dt = parse_timestamp(ts_str)
    if dt is None:
        return ts_str
    return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

def format_ts_for_cleaned(ts_str: str) -> str:
    """Format ts to match cleaned.csv format: YYYY-MM-DDTHH:MM:SSZ"""
    dt = parse_timestamp(ts_str)
    if dt is None:
        return ts_str
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def bool_to_uppercase(val) -> str:
    """Convert boolean/string to uppercase TRUE/FALSE."""
    if pd.isna(val):
        return ""
    val_str = str(val).lower()
    if val_str in ['true', '1']:
        return "TRUE"
    elif val_str in ['false', '0']:
        return "FALSE"
    return str(val)

def transform_mongodb_row_to_cleaned_format(row: pd.Series) -> dict:
    """Transform a MongoDB export row to match the cleaned.csv format."""
    # Parse the timestamp
    dt = parse_timestamp(row['ts_utc'])

    return {
        'ts_utc': format_ts_utc_for_cleaned(row['ts_utc']),
        'Date': format_date_for_cleaned(dt),
        'Year': dt.year if dt else "",
        'Month': get_month_name(dt),
        'Day': get_day_name(dt),
        'ms_played': row.get('ms_played', ''),
        'duration_seconds': row.get('s_played', ''),
        'duration_minutes': row.get('min_played', ''),
        'duration_hours': row.get('h_played', ''),
        'master_metadata_track_name': row.get('track_name', ''),
        'master_metadata_album_artist_name': row.get('artist_name', ''),
        'master_metadata_album_album_name': row.get('album_name', ''),
        'ts': format_ts_for_cleaned(row['ts_utc']),
        'incognito_mode': bool_to_uppercase(row.get('incognito_mode', '')),
        'skipped': bool_to_uppercase(row.get('skipped', '')),
        'reason_start': row.get('reason_start', '') if pd.notna(row.get('reason_start', '')) else '',
        'offline': bool_to_uppercase(row.get('offline', '')),
        'shuffle': bool_to_uppercase(row.get('shuffle', '')),
        'platform': row.get('platform', '') if pd.notna(row.get('platform', '')) else '',
        'conn_country': row.get('conn_country', '') if pd.notna(row.get('conn_country', '')) else '',
        'spotify_track_uri': row.get('spotify_track_uri', ''),
        'reason_end': row.get('reason_end', '') if pd.notna(row.get('reason_end', '')) else '',
        'ip_addr': row.get('ip_addr', '') if pd.notna(row.get('ip_addr', '')) else '',
        'offline_timestamp': row.get('offline_timestamp', '') if pd.notna(row.get('offline_timestamp', '')) else '',
    }

def main():
    print("=" * 60)
    print("Spotify Streaming History Merger")
    print("=" * 60)

    # Read the cleaned CSV
    print(f"\nReading cleaned CSV: {CLEANED_CSV}")
    cleaned_df = pd.read_csv(CLEANED_CSV, encoding='utf-8-sig')
    print(f"  Total rows in cleaned CSV: {len(cleaned_df)}")

    # Get the last row from cleaned CSV
    last_row = cleaned_df.iloc[-1]
    last_ts = str(last_row['ts_utc'])
    last_track_uri = str(last_row['spotify_track_uri'])
    last_track_name = str(last_row['master_metadata_track_name'])

    print(f"\nLast entry in cleaned CSV:")
    print(f"  Timestamp: {last_ts}")
    print(f"  Track: {last_track_name}")
    print(f"  URI: {last_track_uri}")

    # Parse the last timestamp
    last_dt = parse_timestamp(last_ts)

    # Read the MongoDB export
    print(f"\nReading MongoDB export: {MONGODB_EXPORT}")
    mongodb_df = pd.read_csv(MONGODB_EXPORT, encoding='utf-8-sig', low_memory=False)
    print(f"  Total rows in MongoDB export: {len(mongodb_df)}")

    # Parse all timestamps in MongoDB export and filter for entries AFTER the last cleaned entry
    print(f"\nFinding all entries after {last_ts}...")

    new_rows_list = []
    for idx, row in mongodb_df.iterrows():
        row_dt = parse_timestamp(row['ts_utc'])
        if row_dt and last_dt and row_dt > last_dt:
            new_rows_list.append(row)

    if len(new_rows_list) == 0:
        print("\nNo new entries to add. The cleaned CSV is already up to date!")
        return

    new_rows = pd.DataFrame(new_rows_list)

    if len(new_rows) == 0:
        print("\nNo new entries to add. The cleaned CSV is already up to date!")
        return

    print(f"\nFound {len(new_rows)} new entries to add.")
    print(f"\nFirst new entry:")
    first_new = new_rows.iloc[0]
    print(f"  Timestamp: {first_new['ts_utc']}")
    print(f"  Track: {first_new['track_name']}")

    print(f"\nLast new entry:")
    last_new = new_rows.iloc[-1]
    print(f"  Timestamp: {last_new['ts_utc']}")
    print(f"  Track: {last_new['track_name']}")

    # Create backup
    print("\n" + "-" * 40)
    create_backup(CLEANED_CSV)

    # Transform new rows to cleaned format
    print("\nTransforming new entries to cleaned CSV format...")
    transformed_rows = []
    for idx, row in new_rows.iterrows():
        transformed_rows.append(transform_mongodb_row_to_cleaned_format(row))

    new_df = pd.DataFrame(transformed_rows)

    # Ensure column order matches the original
    new_df = new_df[cleaned_df.columns]

    # Append to cleaned CSV
    print(f"\nAppending {len(new_df)} new entries to cleaned CSV...")
    new_df.to_csv(CLEANED_CSV, mode='a', header=False, index=False, encoding='utf-8-sig')

    # Verify
    verification_df = pd.read_csv(CLEANED_CSV, encoding='utf-8-sig')
    print(f"\nVerification:")
    print(f"  Original row count: {len(cleaned_df)}")
    print(f"  New entries added: {len(new_rows)}")
    print(f"  Final row count: {len(verification_df)}")
    print(f"  Expected row count: {len(cleaned_df) + len(new_rows)}")

    if len(verification_df) == len(cleaned_df) + len(new_rows):
        print("\n✓ SUCCESS! All new entries have been added.")
    else:
        print("\n⚠ WARNING: Row count mismatch. Please verify the file manually.")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

if __name__ == "__main__":
    main()
