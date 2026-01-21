"""
Language detection utilities for the Spotify data pipeline.

Includes character-based detection, ML detection, genre-based detection,
and soundtrack classification.
"""

import re
import logging
from typing import Optional, Tuple

from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException

from .config import (
    CONFIDENCE_THRESHOLD,
    MIN_LYRICS_LENGTH,
    MIN_TITLE_LENGTH,
    LANGUAGE_MAP,
    GENRE_LANGUAGE_MAP,
    SOUNDTRACK_GENRES,
    FILM_COMPOSERS,
    CLASSICAL_COMPOSERS,
    ORCHESTRA_KEYWORDS,
)

logger = logging.getLogger(__name__)


class LanguageDetector:
    """
    Language detection with multiple strategies.

    Priority order:
    1. Soundtrack classification -> "Soundtrack"
    2. Character detection (Hebrew, Japanese, Korean, etc.)
    3. Lyrics ML detection (>=70% confidence)
    4. Genre-based detection
    5. Title/Artist ML detection (>=80% confidence)
    6. Fallback -> "Unknown"
    """

    # Character range patterns for non-Latin scripts
    HEBREW_PATTERN = re.compile(r'[\u0590-\u05FF\uFB1D-\uFB4F]')
    JAPANESE_PATTERN = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]')
    KOREAN_PATTERN = re.compile(r'[\uAC00-\uD7AF\u1100-\u11FF]')
    CYRILLIC_PATTERN = re.compile(r'[\u0400-\u04FF]')
    ARABIC_PATTERN = re.compile(r'[\u0600-\u06FF\u0750-\u077F]')
    THAI_PATTERN = re.compile(r'[\u0E00-\u0E7F]')
    GREEK_PATTERN = re.compile(r'[\u0370-\u03FF]')
    CHINESE_PATTERN = re.compile(r'[\u4E00-\u9FFF]')  # CJK Unified
    DEVANAGARI_PATTERN = re.compile(r'[\u0900-\u097F]')  # Hindi, Sanskrit, etc.

    @classmethod
    def has_hebrew_chars(cls, text: str) -> bool:
        """Check for Hebrew characters."""
        return bool(text and cls.HEBREW_PATTERN.search(text))

    @classmethod
    def has_japanese_chars(cls, text: str) -> bool:
        """Check for Japanese characters (Hiragana, Katakana, Kanji)."""
        return bool(text and cls.JAPANESE_PATTERN.search(text))

    @classmethod
    def has_korean_chars(cls, text: str) -> bool:
        """Check for Korean characters (Hangul)."""
        return bool(text and cls.KOREAN_PATTERN.search(text))

    @classmethod
    def has_cyrillic_chars(cls, text: str) -> bool:
        """Check for Cyrillic characters (Russian, Ukrainian, etc.)."""
        return bool(text and cls.CYRILLIC_PATTERN.search(text))

    @classmethod
    def has_arabic_chars(cls, text: str) -> bool:
        """Check for Arabic characters."""
        return bool(text and cls.ARABIC_PATTERN.search(text))

    @classmethod
    def has_thai_chars(cls, text: str) -> bool:
        """Check for Thai characters."""
        return bool(text and cls.THAI_PATTERN.search(text))

    @classmethod
    def has_greek_chars(cls, text: str) -> bool:
        """Check for Greek characters."""
        return bool(text and cls.GREEK_PATTERN.search(text))

    @classmethod
    def has_chinese_chars(cls, text: str) -> bool:
        """Check for Chinese characters (CJK)."""
        return bool(text and cls.CHINESE_PATTERN.search(text))

    @classmethod
    def has_devanagari_chars(cls, text: str) -> bool:
        """Check for Devanagari characters (Hindi, etc.)."""
        return bool(text and cls.DEVANAGARI_PATTERN.search(text))

    @classmethod
    def detect_by_characters(cls, text: str) -> Optional[str]:
        """
        Detect language by character set.

        This is the highest priority detection method for non-Latin scripts
        as it's very reliable.

        Returns:
            Language name if detected, None otherwise.
        """
        if not text:
            return None

        # Order matters - check more specific scripts first
        if cls.has_hebrew_chars(text):
            return "Hebrew"
        if cls.has_korean_chars(text):
            return "Korean"
        if cls.has_japanese_chars(text):
            # Japanese uses Hiragana/Katakana which Chinese doesn't
            return "Japanese"
        if cls.has_thai_chars(text):
            return "Thai"
        if cls.has_arabic_chars(text):
            return "Arabic"
        if cls.has_devanagari_chars(text):
            return "Hindi"
        if cls.has_greek_chars(text):
            return "Greek"
        if cls.has_cyrillic_chars(text):
            return "Russian"
        # Chinese check last since Japanese also uses some Chinese chars
        if cls.has_chinese_chars(text) and not cls.has_japanese_chars(text):
            return "Chinese"

        return None

    @staticmethod
    def detect_ml(text: str) -> Tuple[Optional[str], float]:
        """
        ML-based language detection using langdetect.

        Args:
            text: Text to analyze.

        Returns:
            Tuple of (language_code, confidence_percentage).
            Returns (None, 0.0) if detection fails.
        """
        if not text or len(text.strip()) < 3:
            return None, 0.0

        try:
            results = detect_langs(text)
            if results:
                best = results[0]
                return best.lang, best.prob * 100
        except (LangDetectException, Exception):
            pass

        return None, 0.0

    @staticmethod
    def normalize_code(code: str) -> str:
        """
        Convert language code to readable name.

        Args:
            code: ISO 639-1 language code (e.g., 'en', 'es').

        Returns:
            Human-readable language name (e.g., 'English', 'Spanish').
        """
        if not code:
            return 'Unknown'
        return LANGUAGE_MAP.get(code, code.title())

    @staticmethod
    def detect_from_genres(genres: str) -> Optional[str]:
        """
        Detect language from genre-specific indicators.

        Args:
            genres: Comma-separated genre string.

        Returns:
            Language name if genre maps to a specific language, None otherwise.
        """
        if not genres:
            return None

        genre_list = [g.strip().lower() for g in str(genres).split(',')]

        for genre in genre_list:
            if genre in GENRE_LANGUAGE_MAP:
                return GENRE_LANGUAGE_MAP[genre]

        return None

    @classmethod
    def detect_language(
        cls,
        song_name: str,
        artist_name: str,
        lyrics: Optional[str] = None,
        is_soundtrack: bool = False,
        genres: str = "",
        album_name: str = ""
    ) -> Tuple[str, str]:
        """
        Detect language with priority order.

        Priority:
        1. Soundtrack -> "Soundtrack"
        2. Character detection (Hebrew, Japanese, Korean, etc.)
        3. Lyrics ML (>=100 chars, >=70% confidence)
        4. Genre-based detection
        5. Title+Artist ML (>=20 chars, >=80% confidence)
        6. Fallback -> "Unknown"

        Args:
            song_name: Name of the song.
            artist_name: Name of the artist.
            lyrics: Song lyrics (optional).
            is_soundtrack: Whether the song is classified as soundtrack.
            genres: Comma-separated genre string (optional).
            album_name: Name of the album (optional).

        Returns:
            Tuple of (language, detection_method).
        """
        # Priority 1: Soundtrack classification
        if is_soundtrack:
            return "Soundtrack", "soundtrack"

        # Combine text for analysis
        combined = f"{song_name} {artist_name}"
        if album_name:
            combined = f"{combined} {album_name}"

        # Priority 2: Character detection (very reliable for non-Latin)
        char_lang = cls.detect_by_characters(combined)
        if char_lang:
            return char_lang, "character_detection"

        # Priority 3: Lyrics ML detection
        if lyrics and len(lyrics.strip()) >= MIN_LYRICS_LENGTH:
            lang, conf = cls.detect_ml(lyrics)
            if lang and conf >= CONFIDENCE_THRESHOLD:
                return cls.normalize_code(lang), "lyrics"

        # Priority 4: Genre-based detection
        genre_lang = cls.detect_from_genres(genres)
        if genre_lang:
            return genre_lang, "genre"

        # Priority 5: Title+Artist ML (stricter threshold)
        if combined and len(combined.strip()) >= MIN_TITLE_LENGTH:
            lang, conf = cls.detect_ml(combined)
            # Use stricter threshold for title-based detection
            if lang and conf >= 80.0:
                return cls.normalize_code(lang), "title"

        # Priority 6: Fallback
        return "Unknown", "unknown"


class SoundtrackClassifier:
    """
    Classify songs and artists as soundtrack/orchestral/instrumental.

    Uses multiple heuristics:
    - Genre matching
    - Known composer matching
    - Orchestra/ensemble keyword detection
    """

    @staticmethod
    def has_soundtrack_genre(genres: str) -> bool:
        """
        Check if genres contain soundtrack-related genres.

        Args:
            genres: Comma-separated genre string.

        Returns:
            True if any genre indicates soundtrack/orchestral.
        """
        if not genres:
            return False

        genre_list = [g.strip().lower() for g in str(genres).split(',')]
        return any(g in SOUNDTRACK_GENRES for g in genre_list)

    @staticmethod
    def is_known_composer(artist_name: str) -> bool:
        """
        Check if artist is a known film/classical composer.

        Args:
            artist_name: Name of the artist.

        Returns:
            True if artist is a known composer.
        """
        if not artist_name:
            return False

        normalized = artist_name.lower().strip()
        return normalized in FILM_COMPOSERS or normalized in CLASSICAL_COMPOSERS

    @staticmethod
    def has_orchestra_keywords(text: str) -> bool:
        """
        Check if text contains orchestra keywords as complete words.

        Args:
            text: Text to check (song name, artist name, album).

        Returns:
            True if orchestra-related keywords found.
        """
        if not text:
            return False

        words = re.findall(r'\b\w+\b', text.lower())
        return any(w in ORCHESTRA_KEYWORDS for w in words)

    @classmethod
    def classify_artist(cls, artist_name: str, genres: str = "") -> bool:
        """
        Classify if artist is soundtrack/orchestral.

        Args:
            artist_name: Name of the artist.
            genres: Comma-separated genre string.

        Returns:
            True if artist should be classified as soundtrack.
        """
        if cls.has_soundtrack_genre(genres):
            return True
        if cls.is_known_composer(artist_name):
            return True
        if cls.has_orchestra_keywords(artist_name):
            return True
        return False

    @classmethod
    def classify_song(
        cls,
        song_name: str,
        artist_name: str,
        album_name: str = "",
        genres: str = ""
    ) -> bool:
        """
        Classify if song is soundtrack/orchestral.

        Args:
            song_name: Name of the song.
            artist_name: Name of the artist.
            album_name: Name of the album.
            genres: Comma-separated genre string.

        Returns:
            True if song should be classified as soundtrack.
        """
        # Check artist first
        if cls.classify_artist(artist_name, genres):
            return True

        # Check song and album names
        combined = f"{song_name} {album_name}"
        if cls.has_orchestra_keywords(combined):
            return True

        return False
