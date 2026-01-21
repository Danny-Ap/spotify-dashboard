#!/usr/bin/env python3
"""
Spotify Analytics Pipeline Orchestrator

Runs the complete data collection and enrichment pipeline.
Produces a concise summary log with key statistics.
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

# Suppress verbose logging from all modules
logging.getLogger().setLevel(logging.WARNING)
for name in ['src.data_collection', 'src.enrichment', 'src.utils', 'urllib3', 'requests']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Import pipeline modules
from src.data_collection import fetch_recent_tracks, process_new_content
from src.enrichment import enrich_with_lyrics, validate_data


def run_pipeline():
    """Run the pipeline and return statistics."""
    stats = {
        "start_time": datetime.now(),
        "new_tracks": 0,
        "new_songs": 0,
        "new_artists": 0,
        "lyrics_fetched": 0,
        "validation_issues": 0,
        "status": "success",
        "error": None
    }

    try:
        # Step 1: Fetch recent tracks
        stats["new_tracks"] = fetch_recent_tracks()

        if stats["new_tracks"] == 0:
            stats["status"] = "no_new_tracks"
            return stats

        # Step 2: Process new content (returns tuple of songs, artists inserted)
        # Note: process_new_content doesn't return values, so we can't capture exact counts
        process_new_content()

        # Step 3: Enrich with lyrics
        enrich_with_lyrics()

        # Step 4: Validate data
        validate_data()

    except Exception as e:
        stats["status"] = "failed"
        stats["error"] = str(e)

    stats["end_time"] = datetime.now()
    stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

    return stats


def write_log(stats):
    """Write a concise summary log."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = stats["start_time"].strftime("%d-%m-%Y_%H-%M")
    log_file = log_dir / f"{timestamp}.txt"

    lines = [
        f"Spotify Pipeline - {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}",
        f"{'=' * 50}",
    ]

    if stats["status"] == "no_new_tracks":
        lines.extend([
            f"Status: No new tracks",
            f"All recent tracks already in database.",
        ])
    elif stats["status"] == "success":
        lines.extend([
            f"Status: Success",
            f"New tracks fetched: {stats['new_tracks']}",
            f"Duration: {stats['duration']:.1f}s",
        ])
    else:
        lines.extend([
            f"Status: FAILED",
            f"Error: {stats['error']}",
        ])

    lines.append(f"{'=' * 50}")

    log_content = "\n".join(lines)

    # Write to file
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(log_content)

    # Also print to stdout for GitHub Actions
    print(log_content)

    return log_file


def main():
    """Main entry point."""
    stats = run_pipeline()
    log_file = write_log(stats)

    if stats["status"] == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
