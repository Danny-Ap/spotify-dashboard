"""
Enrichment module for the Spotify pipeline.

Contains scripts for enriching and validating streaming data:
- enrich_with_lyrics: Fetches lyrics and performs language detection
- validate_data: Validates data integrity across collections
"""

from .enrich_with_lyrics import main as enrich_with_lyrics
from .validate_data import main as validate_data

__all__ = ["enrich_with_lyrics", "validate_data"]
