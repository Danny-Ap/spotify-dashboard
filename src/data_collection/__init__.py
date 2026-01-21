"""
Data collection module for the Spotify pipeline.

Contains scripts for fetching and processing streaming data:
- fetch_recent_tracks: Fetches recently played tracks from Spotify API
- process_new_content: Identifies and processes new songs/artists
"""

from .fetch_recent_tracks import main as fetch_recent_tracks
from .process_new_content import main as process_new_content

__all__ = ["fetch_recent_tracks", "process_new_content"]
