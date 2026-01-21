"""
Shared utilities for the Spotify data pipeline.

Provides centralized configuration, database connection management,
Spotify API client, and language detection utilities.
"""

from .config import (
    DATABASE_NAME,
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    STREAMING_FIELDS,
    ENRICHMENT_STATUS_PENDING,
    ENRICHMENT_STATUS_PROCESSED,
    ENRICHMENT_STATUS_FAILED,
    CONFIDENCE_THRESHOLD,
    MIN_LYRICS_LENGTH,
    VALID_DETECTION_METHODS,
)

from .database import MongoDBConnection

from .spotify_api import SpotifyClient

from .language_detection import (
    LanguageDetector,
    SoundtrackClassifier,
)

__all__ = [
    # Config
    "DATABASE_NAME",
    "STREAMING_COLLECTION",
    "SONGS_MASTER_COLLECTION",
    "ARTISTS_MASTER_COLLECTION",
    "STREAMING_FIELDS",
    "ENRICHMENT_STATUS_PENDING",
    "ENRICHMENT_STATUS_PROCESSED",
    "ENRICHMENT_STATUS_FAILED",
    "CONFIDENCE_THRESHOLD",
    "MIN_LYRICS_LENGTH",
    "VALID_DETECTION_METHODS",
    # Database
    "MongoDBConnection",
    # Spotify
    "SpotifyClient",
    # Language
    "LanguageDetector",
    "SoundtrackClassifier",
]
