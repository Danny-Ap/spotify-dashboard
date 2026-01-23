#!/usr/bin/env python3
"""
Master Collection Creator - Consolidated Script
================================================
Creates songs_master.csv and artists_master.csv from streaming history.

MODES:
- FETCH_NEW_LYRICS = False (default): Use existing lyrics from songs_master.csv,
  re-detect all languages fresh. Fast, no Genius API calls.
- FETCH_NEW_LYRICS = True: Fetch new lyrics from Genius for songs without lyrics.
  Slower, requires GENIUS_TOKEN.

This script does:
1. Extracts unique songs and artists from cleaned CSV
2. Fetches Spotify API metadata (duration, release date, popularity, genres, followers)
3. Classifies soundtracks (composers, genres, orchestra keywords, classical terms)
4. Loads existing lyrics from songs_master.csv (if FETCH_NEW_LYRICS=False)
5. Optionally fetches lyrics from Genius API for songs without lyrics (if FETCH_NEW_LYRICS=True)
6. Detects language with priority:
   - Soundtrack -> "Soundtrack"
   - Character detection (Hebrew, Japanese, Korean, Cyrillic, Arabic, Thai, Greek)
   - Lyrics ML (>=100 chars, >=70% confidence)
   - Genre-based detection
   - Title+Artist ML (>=20 chars, >=80% confidence)
   - Unknown
7. Determines artist language based on majority of their songs

Input:  spotify_streaming_history_cleaned.csv
Output: songs_master.csv, artists_master.csv

Run after: json_to_csv_streamlined.py
"""

import os
import re
import time
import base64
import pandas as pd
from typing import List, Dict, Tuple, Optional
from collections import Counter
from requests import post, get
from dotenv import load_dotenv
import lyricsgenius
from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_DIR = "Spotify Extended Streaming History - January 2026"
INPUT_FILE = os.path.join(INPUT_DIR, "spotify_streaming_history_cleaned.csv")
SONGS_OUTPUT_FILE = "songs_master.csv"
ARTISTS_OUTPUT_FILE = "artists_master.csv"

# Spotify API Configuration
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

# Genius API Configuration
GENIUS_TOKEN = os.getenv('GENIUS_TOKEN')

# =============================================================================
# LYRICS FETCHING MODE
# =============================================================================
# Set to False to use existing lyrics from songs_master.csv (fast, no Genius API)
# Set to True to fetch new lyrics from Genius (slower, only fetches for songs without lyrics)
FETCH_NEW_LYRICS = False

# Processing Configuration
REQUEST_DELAY = 0.2          # Delay between Spotify API calls
GENIUS_DELAY = 0.5           # Delay between Genius API calls
BATCH_SIZE = 50              # Spotify API batch limit
SAVE_INTERVAL = 100          # Save progress every N songs
CONFIDENCE_THRESHOLD = 70.0  # Language detection confidence for lyrics (0-100)
TITLE_CONFIDENCE_THRESHOLD = 80.0  # Stricter threshold for title+artist detection
MIN_LYRICS_LENGTH = 100      # Minimum chars for valid lyrics
MIN_TITLE_LENGTH = 20        # Minimum chars for title+artist ML detection

# =============================================================================
# SOUNDTRACK CLASSIFICATION DATA (EXPANDED)
# =============================================================================

SOUNDTRACK_GENRES = {
    # Film & TV
    'soundtrack', 'film score', 'movie tunes', 'tv soundtrack', 'anime score',
    'disney', 'pixar', 'dreamworks',

    # Video Games
    'video game music', 'vgm', 'japanese vgm', 'chiptune', 'game score',

    # Classical
    'classical', 'classical piano', 'classical performance', 'classical era',
    'baroque', 'romantic era', 'early romantic era', 'late romantic era',
    'post-romantic era', 'impressionism', 'expressionism', 'serialism',
    'modern classical', 'contemporary classical', '20th century classical',
    '21st century classical', 'early music', 'renaissance', 'medieval',

    # Orchestral
    'orchestra', 'orchestral', 'symphonic', 'symphony', 'philharmonic',
    'chamber music', 'string quartet', 'wind ensemble', 'brass ensemble',

    # Choral & Vocal Classical
    'choral', 'choir', 'opera', 'operetta', 'oratorio', 'cantata',
    'requiem', 'mass', 'gregorian chant', 'sacred music',

    # Piano & Instrumental
    'piano', 'solo piano', 'classical piano', 'piano cover',
    'violin', 'cello', 'flute', 'guitar classical', 'classical guitar',
    'harpsichord', 'organ', 'harp',

    # Neoclassical & Modern
    'neoclassical', 'neoclassicism', 'minimalism', 'post-minimalism',
    'modern composition', 'experimental classical', 'avant-garde classical',

    # Ambient & Instrumental
    'ambient', 'drone', 'space music', 'new age', 'meditation',
    'sleep', 'relaxation', 'spa', 'yoga', 'nature sounds',
    'easy listening', 'lounge', 'elevator music', 'muzak',

    # World Traditional (Instrumental)
    'traditional', 'folk instrumental', 'world music instrumental',
    'celtic instrumental', 'asian traditional', 'indian classical',
    'japanese classical', 'chinese classical', 'gamelan',

    # Dance Forms (Classical/Traditional)
    'waltz', 'minuet', 'polonaise', 'mazurka', 'nocturne', 'etude',
    'prelude', 'fugue', 'sonata', 'concerto', 'overture', 'suite',
    'ragtime', 'tango', 'bolero', 'polka', 'march',

    # Other Instrumental
    'instrumental', 'downtempo', 'post-rock', 'shoegaze instrumental',
    'math rock instrumental', 'jazz instrumental', 'fusion instrumental',
    'exotica', 'lo-fi beats', 'study music', 'focus music',
}

KNOWN_COMPOSERS = {
    # ===================
    # FILM COMPOSERS
    # ===================
    # Golden Age (1930s-1960s)
    'max steiner', 'alfred newman', 'bernard herrmann', 'dimitri tiomkin',
    'miklos rozsa', 'franz waxman', 'erich wolfgang korngold', 'alex north',
    'elmer bernstein', 'henry mancini', 'maurice jarre', 'john barry',

    # New Hollywood (1970s-1990s)
    'john williams', 'jerry goldsmith', 'james horner', 'alan silvestri',
    'danny elfman', 'hans zimmer', 'james newton howard', 'randy newman',
    'thomas newman', 'alan menken', 'howard shore', 'michael kamen',
    'basil poledouris', 'bill conti', 'dave grusin', 'lalo schifrin',
    'ennio morricone', 'nino rota', 'vangelis', 'giorgio moroder',

    # Modern (2000s-present)
    'michael giacchino', 'alexandre desplat', 'ramin djawadi', 'brian tyler',
    'henry jackman', 'steve jablonsky', 'harry gregson-williams', 'john powell',
    'marco beltrami', 'christophe beck', 'carter burwell', 'cliff martinez',
    'clint mansell', 'dario marianelli', 'patrick doyle', 'rachel portman',
    'gabriel yared', 'elliot goldenthal', 'mychael danna', 'mark isham',
    'nicholas britell', 'justin hurwitz', 'ludwig goransson', 'hildur gudnadottir',
    'jonny greenwood', 'mica levi', 'daniel pemberton', 'benjamin wallfisch',
    'steven price', 'jed kurzel', 'tom holkenborg', 'junkie xl',
    'bear mccreary', 'jeff russo', 'nathan barr', 'russ landau',

    # Anime/Japanese Film
    'joe hisaishi', 'yoko kanno', 'hiroyuki sawano', 'yuki kajiura',
    'ryuichi sakamoto', 'toru takemitsu', 'kenji kawai', 'shiro sagisu',
    'michiru oshima', 'taro iwashiro', 'naoki sato', 'akira ifukube',
    'masashi hamauzu', 'nobuo uematsu', 'koji kondo', 'yasunori mitsuda',

    # Other International
    'ennio morricone', 'nino rota', 'nicola piovani', 'luis bacalov',
    'gustavo santaolalla', 'alberto iglesias', 'a.r. rahman', 'tan dun',
    'alexandre desplat', 'yann tiersen', 'jean-michel jarre',

    # ===================
    # CLASSICAL COMPOSERS
    # ===================
    # Baroque (1600-1750)
    'johann sebastian bach', 'j.s. bach', 'bach', 'george frideric handel',
    'handel', 'antonio vivaldi', 'vivaldi', 'henry purcell', 'jean-philippe rameau',
    'domenico scarlatti', 'georg philipp telemann', 'arcangelo corelli',
    'claudio monteverdi', 'jean-baptiste lully', 'heinrich schutz',

    # Classical (1750-1820)
    'wolfgang amadeus mozart', 'mozart', 'joseph haydn', 'haydn',
    'ludwig van beethoven', 'beethoven', 'christoph willibald gluck',
    'muzio clementi', 'carl philipp emanuel bach', 'johann christian bach',

    # Romantic (1820-1900)
    'franz schubert', 'schubert', 'frederic chopin', 'chopin', 'frédéric chopin',
    'robert schumann', 'schumann', 'felix mendelssohn', 'mendelssohn',
    'franz liszt', 'liszt', 'johannes brahms', 'brahms',
    'pyotr ilyich tchaikovsky', 'tchaikovsky', 'antonin dvorak', 'dvorak',
    'richard wagner', 'wagner', 'giuseppe verdi', 'verdi',
    'hector berlioz', 'berlioz', 'giacomo puccini', 'puccini',
    'edvard grieg', 'grieg', 'jean sibelius', 'sibelius',
    'nikolai rimsky-korsakov', 'rimsky-korsakov', 'modest mussorgsky',
    'alexander borodin', 'cesar franck', 'camille saint-saens',
    'gabriel faure', 'fauré', 'georges bizet', 'bizet',

    # Late Romantic / Early Modern (1890-1950)
    'gustav mahler', 'mahler', 'richard strauss', 'r. strauss',
    'claude debussy', 'debussy', 'maurice ravel', 'ravel',
    'igor stravinsky', 'stravinsky', 'sergei rachmaninoff', 'rachmaninoff',
    'sergei prokofiev', 'prokofiev', 'dmitri shostakovich', 'shostakovich',
    'bela bartok', 'bartok', 'aaron copland', 'copland',
    'george gershwin', 'gershwin', 'leonard bernstein', 'l. bernstein',
    'ralph vaughan williams', 'vaughan williams', 'gustav holst', 'holst',
    'benjamin britten', 'britten', 'edward elgar', 'elgar',
    'erik satie', 'satie', 'arnold schoenberg', 'schoenberg',
    'alban berg', 'anton webern', 'paul hindemith', 'carl orff',

    # 20th Century / Contemporary
    'john cage', 'philip glass', 'steve reich', 'terry riley',
    'john adams', 'arvo part', 'arvo pärt', 'henryk gorecki', 'górecki',
    'max richter', 'ludovico einaudi', 'yiruma', 'olafur arnalds',
    'ólafur arnalds', 'nils frahm', 'johann johannsson', 'jóhann jóhannsson',
    'dustin ohalloran', "dustin o'halloran", 'hauschka', 'joep beving',

    # ===================
    # FAMOUS PERFORMERS (Classical)
    # ===================
    'yo-yo ma', 'itzhak perlman', 'lang lang', 'yuja wang',
    'martha argerich', 'vladimir horowitz', 'arthur rubinstein',
    'glenn gould', 'murray perahia', 'andras schiff', 'evgeny kissin',
    'daniel barenboim', 'zubin mehta', 'herbert von karajan',
    'leonard bernstein', 'gustavo dudamel', 'simon rattle',
}

SOUNDTRACK_KEYWORDS = {
    # Film/TV/Game
    'soundtrack', 'ost', 'o.s.t.', 'original motion picture', 'original soundtrack',
    'original score', 'film score', 'movie soundtrack', 'tv soundtrack',
    'television soundtrack', 'game soundtrack', 'video game soundtrack',
    'from the motion picture', 'from the film', 'from the movie',
    'from the tv series', 'from the video game', 'from the game',
    'music from', 'score', 'theme from', 'main theme', 'main title',
    'end credits', 'opening theme', 'closing theme',

    # Classical terms that indicate instrumental
    'opus', 'op.', 'no.', 'in c major', 'in d minor', 'in g major',
    'in a minor', 'in e flat', 'in b flat', 'k.', 'bwv', 'hwv', 'rv',
    'symphony no', 'concerto no', 'sonata no', 'quartet no', 'trio no',
    'etude', 'étude', 'etudes', 'études', 'prelude', 'prélude',
    'nocturne', 'waltz', 'polonaise', 'mazurka', 'ballade', 'scherzo',
    'impromptu', 'fantaisie', 'fantasy', 'rhapsody', 'variations',
    'fugue', 'toccata', 'invention', 'partita', 'suite no',
    'overture', 'ouverture', 'serenade', 'divertimento',
    'requiem', 'mass in', 'magnificat', 'stabat mater', 'te deum',
    'cantata', 'oratorio', 'passion',

    # Performance/Recording indicators
    'live at', 'recorded at', 'remastered', 'arrangement', 'transcription',
    'performed by', 'conducted by', 'featuring the',
}

ORCHESTRA_KEYWORDS = {
    'orchestra', 'symphony', 'philharmonic', 'ensemble', 'quartet', 'quintet',
    'trio', 'sextet', 'septet', 'octet', 'chamber', 'conservatory', 'academy',
    'chorale', 'choir', 'chorus', 'concerto', 'orchestral', 'symphonic',
    'strings', 'winds', 'brass', 'woodwinds', 'soloists', 'virtuosi',
    'sinfonietta', 'camerata', 'collegium', 'consort',
}

# =============================================================================
# GENRE-TO-LANGUAGE MAPPING (Only language-specific genres)
# =============================================================================

GENRE_LANGUAGE_MAP = {
    # Korean
    'k-pop': 'Korean', 'korean pop': 'Korean', 'korean r&b': 'Korean',
    'korean ost': 'Korean', 'korean indie': 'Korean', 'korean hip hop': 'Korean',
    'k-rap': 'Korean', 'k-rock': 'Korean', 'trot': 'Korean',

    # Japanese
    'j-pop': 'Japanese', 'j-rock': 'Japanese', 'japanese pop': 'Japanese',
    'japanese rock': 'Japanese', 'visual kei': 'Japanese', 'j-idol': 'Japanese',
    'anime': 'Japanese', 'vocaloid': 'Japanese', 'japanese r&b': 'Japanese',
    'city pop': 'Japanese', 'shibuya-kei': 'Japanese', 'enka': 'Japanese',
    'kayokyoku': 'Japanese',

    # Spanish
    'reggaeton': 'Spanish', 'latin pop': 'Spanish', 'latin': 'Spanish',
    'musica mexicana': 'Spanish', 'spanish pop': 'Spanish', 'salsa': 'Spanish',
    'bachata': 'Spanish', 'cumbia': 'Spanish', 'regional mexican': 'Spanish',
    'corrido': 'Spanish', 'corridos tumbados': 'Spanish', 'norteno': 'Spanish',
    'banda': 'Spanish', 'mariachi': 'Spanish', 'ranchera': 'Spanish',
    'latin rock': 'Spanish', 'latin hip hop': 'Spanish', 'latin trap': 'Spanish',
    'urbano latino': 'Spanish', 'perreo': 'Spanish', 'dembow': 'Spanish',
    'flamenco': 'Spanish', 'flamenco pop': 'Spanish', 'nuevo flamenco': 'Spanish',
    'spanish hip hop': 'Spanish', 'spanish rock': 'Spanish',

    # French
    'french pop': 'French', 'chanson': 'French', 'chanson francaise': 'French',
    'french hip hop': 'French', 'variete francaise': 'French', 'french rock': 'French',
    'french indie': 'French', 'french house': 'French', 'french touch': 'French',
    'raï': 'French', 'zouk': 'French',

    # German
    'schlager': 'German', 'german pop': 'German', 'german hip hop': 'German',
    'neue deutsche welle': 'German', 'ndw': 'German', 'german rock': 'German',
    'deutsche schlager': 'German', 'volkstümliche musik': 'German',
    'german metal': 'German', 'krautrock': 'German',

    # Hebrew
    'israeli': 'Hebrew', 'mizrahi': 'Hebrew', 'hebrew': 'Hebrew',
    'israeli pop': 'Hebrew', 'israeli rock': 'Hebrew', 'israeli hip hop': 'Hebrew',
    'oriental': 'Hebrew',  # Often Israeli music

    # Dutch
    'dutch pop': 'Dutch', 'nederpop': 'Dutch', 'dutch hip hop': 'Dutch',
    'levenslied': 'Dutch', 'dutch house': 'Dutch', 'gabber': 'Dutch',
    'hardcore': 'Dutch',  # Gabber originated in Netherlands
    'flemish': 'Dutch',

    # Italian
    'italian pop': 'Italian', 'musica italiana': 'Italian',
    'italian hip hop': 'Italian', 'italian rock': 'Italian',
    'canzone italiana': 'Italian', 'italo disco': 'Italian',
    'neapolitan': 'Italian', 'napoli': 'Italian',

    # Portuguese (Portuguese & Brazilian)
    'brazilian': 'Portuguese', 'mpb': 'Portuguese', 'bossa nova': 'Portuguese',
    'sertanejo': 'Portuguese', 'funk carioca': 'Portuguese', 'pagode': 'Portuguese',
    'forro': 'Portuguese', 'axe': 'Portuguese', 'brazilian rock': 'Portuguese',
    'brazilian pop': 'Portuguese', 'brazilian hip hop': 'Portuguese',
    'portuguese pop': 'Portuguese', 'fado': 'Portuguese', 'pimba': 'Portuguese',

    # Russian
    'russian pop': 'Russian', 'russian hip hop': 'Russian', 'russian rock': 'Russian',
    'russische': 'Russian', 'shanson': 'Russian',

    # Arabic
    'arabic pop': 'Arabic', 'khaliji': 'Arabic', 'egyptian pop': 'Arabic',
    'levantine': 'Arabic', 'maghreb': 'Arabic', 'arab pop': 'Arabic',
    'mahraganat': 'Arabic', 'shaabi': 'Arabic',

    # Turkish
    'turkish pop': 'Turkish', 'arabesque': 'Turkish', 'turkish rock': 'Turkish',
    'turkish hip hop': 'Turkish', 'turk pop': 'Turkish', 'anatolian rock': 'Turkish',

    # Swedish
    'swedish pop': 'Swedish', 'svensk pop': 'Swedish', 'swedish hip hop': 'Swedish',
    'swedish indie': 'Swedish', 'dansband': 'Swedish',

    # Norwegian
    'norwegian pop': 'Norwegian', 'norsk pop': 'Norwegian', 'norwegian hip hop': 'Norwegian',

    # Finnish
    'finnish pop': 'Finnish', 'suomi pop': 'Finnish', 'finnish metal': 'Finnish',
    'suomi rock': 'Finnish', 'finnish hip hop': 'Finnish',

    # Danish
    'danish pop': 'Danish', 'dansk pop': 'Danish', 'danish hip hop': 'Danish',

    # Polish
    'polish pop': 'Polish', 'polish hip hop': 'Polish', 'polish rock': 'Polish',
    'disco polo': 'Polish',

    # Greek
    'greek pop': 'Greek', 'laiko': 'Greek', 'greek hip hop': 'Greek',
    'rebetiko': 'Greek', 'entexno': 'Greek',

    # Hindi/Indian
    'bollywood': 'Hindi', 'hindi pop': 'Hindi', 'indian pop': 'Hindi',
    'filmi': 'Hindi', 'desi hip hop': 'Hindi', 'punjabi': 'Hindi',

    # Chinese
    'mandopop': 'Chinese', 'c-pop': 'Chinese', 'cantopop': 'Chinese',
    'chinese pop': 'Chinese', 'chinese hip hop': 'Chinese',

    # Thai
    'thai pop': 'Thai', 't-pop': 'Thai', 'luk thung': 'Thai', 'mor lam': 'Thai',

    # Vietnamese
    'v-pop': 'Vietnamese', 'vietnamese pop': 'Vietnamese',

    # Indonesian
    'indonesian pop': 'Indonesian', 'dangdut': 'Indonesian',

    # Afrikaans
    'afrikaans': 'Afrikaans', 'afrikaanse': 'Afrikaans',
}

# =============================================================================
# LANGUAGE DETECTION DATA
# =============================================================================

LANGUAGE_MAP = {
    'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German',
    'it': 'Italian', 'pt': 'Portuguese', 'ru': 'Russian', 'ja': 'Japanese',
    'ko': 'Korean', 'zh-cn': 'Chinese', 'zh': 'Chinese', 'ar': 'Arabic',
    'tr': 'Turkish', 'nl': 'Dutch', 'pl': 'Polish', 'sv': 'Swedish',
    'no': 'Norwegian', 'da': 'Danish', 'fi': 'Finnish', 'he': 'Hebrew',
    'hi': 'Hindi', 'th': 'Thai', 'vi': 'Vietnamese', 'id': 'Indonesian',
    'ro': 'Romanian', 'hu': 'Hungarian', 'cs': 'Czech', 'uk': 'Ukrainian',
    'el': 'Greek', 'bg': 'Bulgarian', 'hr': 'Croatian', 'sk': 'Slovak',
    'ca': 'Catalan', 'af': 'Afrikaans', 'cy': 'Welsh', 'et': 'Estonian',
    'lv': 'Latvian', 'lt': 'Lithuanian', 'sl': 'Slovenian', 'mk': 'Macedonian',
    'sq': 'Albanian', 'tl': 'Filipino', 'ms': 'Malay', 'sw': 'Swahili'
}

# =============================================================================
# SPOTIFY API CLASS
# =============================================================================

class SpotifyAPI:
    def __init__(self):
        self.token = None
        self._get_token()

    def _get_token(self):
        """Get Spotify API access token"""
        if not CLIENT_ID or not CLIENT_SECRET:
            raise ValueError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in .env file")

        auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}

        result = post(url, headers=headers, data=data)
        result.raise_for_status()
        self.token = result.json()["access_token"]
        print("  Spotify API token obtained")

    def _get_auth_header(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_batch_tracks(self, track_ids: List[str]) -> Dict[str, Dict]:
        """Get track details for up to 50 tracks"""
        if not track_ids:
            return {}

        ids_string = ",".join(track_ids[:BATCH_SIZE])
        url = f"https://api.spotify.com/v1/tracks?ids={ids_string}"

        try:
            result = get(url, headers=self._get_auth_header())
            result.raise_for_status()
            tracks_data = result.json().get("tracks", [])

            batch_results = {}
            for i, track in enumerate(tracks_data):
                track_id = track_ids[i]
                if track is None:
                    batch_results[track_id] = {"status": "error"}
                else:
                    album = track.get("album", {})
                    release_date = album.get("release_date", "")

                    release_year = None
                    if release_date:
                        match = re.match(r'^(\d{4})', release_date)
                        if match:
                            release_year = int(match.group(1))

                    batch_results[track_id] = {
                        "status": "success",
                        "duration_ms": track.get("duration_ms"),
                        "release_date": release_date,
                        "release_date_year": release_year,
                        "popularity": track.get("popularity"),
                        "album_name": album.get("name"),
                        "artists": [
                            {"id": a["id"], "name": a["name"], "uri": a["uri"]}
                            for a in track.get("artists", [])
                        ]
                    }
            return batch_results

        except Exception as e:
            return {tid: {"status": "error", "error": str(e)} for tid in track_ids}

    def get_batch_artists(self, artist_ids: List[str]) -> Dict[str, Dict]:
        """Get artist details for up to 50 artists"""
        if not artist_ids:
            return {}

        ids_string = ",".join(artist_ids[:BATCH_SIZE])
        url = f"https://api.spotify.com/v1/artists?ids={ids_string}"

        try:
            result = get(url, headers=self._get_auth_header())
            result.raise_for_status()
            artists_data = result.json().get("artists", [])

            batch_results = {}
            for i, artist in enumerate(artists_data):
                artist_id = artist_ids[i]
                if artist is None:
                    batch_results[artist_id] = {"status": "error"}
                else:
                    batch_results[artist_id] = {
                        "status": "success",
                        "name": artist.get("name"),
                        "uri": artist.get("uri"),
                        "genres": ", ".join(artist.get("genres", [])),
                        "followers": artist.get("followers", {}).get("total"),
                        "popularity": artist.get("popularity")
                    }
            return batch_results

        except Exception as e:
            return {aid: {"status": "error", "error": str(e)} for aid in artist_ids}

# =============================================================================
# SOUNDTRACK CLASSIFIER (EXPANDED)
# =============================================================================

class SoundtrackClassifier:
    @staticmethod
    def has_soundtrack_genre(genres: str) -> bool:
        """Check if genres contain soundtrack-related genres"""
        if not genres:
            return False
        genre_list = [g.strip().lower() for g in str(genres).split(',')]
        return any(g in SOUNDTRACK_GENRES for g in genre_list)

    @staticmethod
    def is_known_composer(artist_name: str) -> bool:
        """Check if artist is a known composer"""
        if not artist_name:
            return False
        return artist_name.lower().strip() in KNOWN_COMPOSERS

    @staticmethod
    def has_soundtrack_keywords(text: str) -> bool:
        """Check if text contains soundtrack keywords"""
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in SOUNDTRACK_KEYWORDS)

    @staticmethod
    def has_orchestra_keywords(text: str) -> bool:
        """Check if text contains orchestra keywords as complete words"""
        if not text:
            return False
        words = re.findall(r'\b\w+\b', text.lower())
        return any(w in ORCHESTRA_KEYWORDS for w in words)

    @classmethod
    def classify_artist(cls, artist_name: str, genres: str) -> bool:
        """Classify if artist is soundtrack/orchestral"""
        if cls.has_soundtrack_genre(genres):
            return True
        if cls.is_known_composer(artist_name):
            return True
        if cls.has_orchestra_keywords(artist_name):
            return True
        return False

    @classmethod
    def classify_song(cls, song_name: str, album_name: str, artist_is_soundtrack: bool) -> bool:
        """Classify if song is soundtrack/orchestral"""
        if artist_is_soundtrack:
            return True
        if cls.has_soundtrack_keywords(song_name):
            return True
        if cls.has_soundtrack_keywords(album_name):
            return True
        return False

# =============================================================================
# LANGUAGE DETECTOR (WITH GENRE SUPPORT)
# =============================================================================

class LanguageDetector:
    @staticmethod
    def has_hebrew_chars(text: str) -> bool:
        """Check for Hebrew characters"""
        if not text:
            return False
        return bool(re.search(r'[\u0590-\u05FF\uFB1D-\uFB4F]', text))

    @staticmethod
    def has_japanese_chars(text: str) -> bool:
        """Check for Japanese characters (Hiragana, Katakana, Kanji)"""
        if not text:
            return False
        return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]', text))

    @staticmethod
    def has_korean_chars(text: str) -> bool:
        """Check for Korean characters (Hangul)"""
        if not text:
            return False
        return bool(re.search(r'[\uAC00-\uD7AF\u1100-\u11FF]', text))

    @staticmethod
    def has_cyrillic_chars(text: str) -> bool:
        """Check for Cyrillic characters (Russian, etc.)"""
        if not text:
            return False
        return bool(re.search(r'[\u0400-\u04FF]', text))

    @staticmethod
    def has_arabic_chars(text: str) -> bool:
        """Check for Arabic characters"""
        if not text:
            return False
        return bool(re.search(r'[\u0600-\u06FF\u0750-\u077F]', text))

    @staticmethod
    def has_thai_chars(text: str) -> bool:
        """Check for Thai characters"""
        if not text:
            return False
        return bool(re.search(r'[\u0E00-\u0E7F]', text))

    @staticmethod
    def has_greek_chars(text: str) -> bool:
        """Check for Greek characters"""
        if not text:
            return False
        return bool(re.search(r'[\u0370-\u03FF]', text))

    @staticmethod
    def detect_from_genres(genres: str) -> Optional[str]:
        """Detect language from genre-specific indicators"""
        if not genres:
            return None
        genre_list = [g.strip().lower() for g in str(genres).split(',')]
        for genre in genre_list:
            if genre in GENRE_LANGUAGE_MAP:
                return GENRE_LANGUAGE_MAP[genre]
        return None

    @staticmethod
    def detect_ml(text: str) -> Tuple[Optional[str], float]:
        """ML-based language detection"""
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
        """Convert language code to readable name"""
        return LANGUAGE_MAP.get(code, code.title() if code else 'Unknown')

    @classmethod
    def detect_language(cls, song_name: str, artist_name: str,
                       lyrics: Optional[str], is_soundtrack: bool,
                       genres: str = "") -> Tuple[str, str]:
        """
        Detect language with priority:
        1. Soundtrack -> "Soundtrack"
        2. Character detection (Hebrew, Japanese, Korean, Cyrillic, Arabic, Thai, Greek)
        3. Lyrics ML (>=100 chars, >=70% confidence)
        4. Genre-based detection
        5. Title+Artist ML (>=20 chars combined, >=80% confidence) - stricter
        6. Fallback -> "Unknown"

        Returns: (language, detection_method)
        """
        # Priority 1: Soundtrack
        if is_soundtrack:
            return "Soundtrack", "soundtrack"

        combined = f"{song_name} {artist_name}"

        # Priority 2: Character detection (most reliable for non-Latin scripts)
        if cls.has_hebrew_chars(combined):
            return "Hebrew", "character_detection"
        if cls.has_japanese_chars(combined):
            return "Japanese", "character_detection"
        if cls.has_korean_chars(combined):
            return "Korean", "character_detection"
        if cls.has_arabic_chars(combined):
            return "Arabic", "character_detection"
        if cls.has_thai_chars(combined):
            return "Thai", "character_detection"
        if cls.has_greek_chars(combined):
            return "Greek", "character_detection"
        if cls.has_cyrillic_chars(combined):
            return "Russian", "character_detection"

        # Priority 3: Lyrics ML (most reliable for Latin-script languages)
        if lyrics and len(lyrics.strip()) >= MIN_LYRICS_LENGTH:
            lang, conf = cls.detect_ml(lyrics)
            if lang and conf >= CONFIDENCE_THRESHOLD:
                return cls.normalize_code(lang), "lyrics"

        # Priority 4: Genre-based detection
        genre_lang = cls.detect_from_genres(genres)
        if genre_lang:
            return genre_lang, "genre"

        # Priority 5: Title+Artist ML (stricter - needs more text and higher confidence)
        if combined and len(combined.strip()) >= MIN_TITLE_LENGTH:
            lang, conf = cls.detect_ml(combined)
            # Debug: show all language probabilities for specific songs
            if "Ik Wil Niet In Bad" in song_name:
                try:
                    all_results = detect_langs(combined)
                    print(f"\n    [DEBUG] langdetect results for '{combined}':")
                    for r in all_results:
                        print(f"      {r.lang}: {r.prob*100:.1f}%")
                except:
                    pass
            if lang and conf >= TITLE_CONFIDENCE_THRESHOLD:
                return cls.normalize_code(lang), "title"

        return "Unknown", "unknown"

# =============================================================================
# LYRICS FETCHER
# =============================================================================

class LyricsFetcher:
    def __init__(self):
        self.genius = None
        if GENIUS_TOKEN:
            try:
                self.genius = lyricsgenius.Genius(GENIUS_TOKEN)
                self.genius.verbose = False
                self.genius.remove_section_headers = False
                self.genius.timeout = 10
                print("  Genius API connected")
            except Exception as e:
                print(f"  Warning: Could not connect to Genius API: {e}")

    def fetch(self, song_name: str, artist_name: str) -> Tuple[Optional[str], bool]:
        """
        Fetch lyrics for a song.
        Returns: (lyrics_text or None, has_lyrics boolean)
        """
        if not self.genius:
            return None, False

        try:
            song = self.genius.search_song(song_name, artist_name)
            if song and song.lyrics:
                lyrics = song.lyrics.strip()
                if len(lyrics) >= MIN_LYRICS_LENGTH:
                    return lyrics, True
            return None, False
        except Exception:
            return None, False

# =============================================================================
# EXISTING DATA LOADER
# =============================================================================

def load_existing_lyrics_data() -> Dict[str, Dict]:
    """
    Load existing lyrics data from songs_master.csv.
    Only loads lyrics - language will be re-detected fresh.
    """
    existing_data = {}

    if not os.path.exists(SONGS_OUTPUT_FILE):
        print(f"  No existing songs file found at {SONGS_OUTPUT_FILE}")
        return existing_data

    print(f"\n Loading existing lyrics from {SONGS_OUTPUT_FILE}...")

    try:
        df = pd.read_csv(SONGS_OUTPUT_FILE, encoding='utf-8-sig')

        for _, row in df.iterrows():
            uri = str(row.get('spotify_track_uri', ''))
            if uri and uri.startswith('spotify:track:'):
                existing_data[uri] = {
                    'lyrics': row.get('lyrics') if pd.notna(row.get('lyrics')) else None,
                    # Don't load language - we want to re-detect it fresh
                }

        print(f"  Loaded {len(existing_data):,} songs")

        # Count songs with lyrics
        with_lyrics = sum(1 for d in existing_data.values() if d.get('lyrics'))
        print(f"  Songs with lyrics: {with_lyrics:,}")

    except Exception as e:
        print(f"  Warning: Could not load existing lyrics data: {e}")

    return existing_data

# =============================================================================
# MAIN PROCESSING
# =============================================================================

def load_streaming_history() -> pd.DataFrame:
    """Load and prepare streaming history"""
    print(f"\n Loading streaming history from {INPUT_FILE}...")

    df = pd.read_csv(INPUT_FILE, encoding='utf-8-sig')
    print(f"  Loaded {len(df):,} records")

    return df

def extract_unique_songs(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique songs from streaming history"""
    print("\n Extracting unique songs...")

    songs = df[['master_metadata_track_name', 'master_metadata_album_artist_name',
                'spotify_track_uri']].copy()
    songs.columns = ['song_name', 'artist_name', 'spotify_track_uri']

    songs = songs.fillna("").apply(lambda s: s.astype(str).str.strip())
    songs = songs[(songs['song_name'] != "") &
                  (songs['artist_name'] != "") &
                  (songs['spotify_track_uri'] != "")]

    unique_songs = songs.drop_duplicates(
        subset=['song_name', 'artist_name'],
        keep='first'
    ).reset_index(drop=True)

    unique_songs['track_id'] = unique_songs['spotify_track_uri'].str.replace('spotify:track:', '', regex=False)

    print(f"  Found {len(unique_songs):,} unique songs")
    return unique_songs

def process_songs(unique_songs: pd.DataFrame, spotify: SpotifyAPI,
                  lyrics_fetcher: Optional[LyricsFetcher], existing_data: Dict[str, Dict]) -> Tuple[List[Dict], Dict[str, Dict]]:
    """Process all songs: fetch metadata, lyrics (if FETCH_NEW_LYRICS=True), detect language"""
    print(f"\n Processing {len(unique_songs):,} songs...")

    # Count existing lyrics
    have_existing = 0
    for _, row in unique_songs.iterrows():
        uri = row['spotify_track_uri']
        if uri in existing_data and existing_data[uri].get('lyrics'):
            have_existing += 1

    print(f"  Songs with existing lyrics: {have_existing:,}")
    if FETCH_NEW_LYRICS:
        print(f"  Mode: FETCH_NEW_LYRICS=True (will fetch missing lyrics from Genius)")
    else:
        print(f"  Mode: FETCH_NEW_LYRICS=False (using existing lyrics only, re-detecting languages)")

    all_songs = []
    all_artists = {}

    total = len(unique_songs)
    lyrics_fetched = 0
    lyrics_from_existing = 0
    no_lyrics = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = unique_songs.iloc[batch_start:batch_end]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n  Batch {batch_num}/{total_batches} (songs {batch_start+1}-{batch_end})")

        # Get Spotify track data
        track_ids = batch['track_id'].tolist()
        track_results = spotify.get_batch_tracks(track_ids)
        time.sleep(REQUEST_DELAY)

        for idx, row in batch.iterrows():
            song_num = batch_start + (idx - batch.index[0]) + 1
            track_id = row['track_id']
            song_name = row['song_name']
            artist_name = row['artist_name']
            spotify_uri = row['spotify_track_uri']

            track_data = track_results.get(track_id, {"status": "error"})

            # Collect artists for later processing
            if track_data.get("status") == "success":
                for artist_info in track_data.get("artists", []):
                    artist_key = artist_info["name"].lower().strip()
                    if artist_key not in all_artists:
                        all_artists[artist_key] = {
                            "artist_id": artist_info["id"],
                            "artist_name": artist_info["name"],
                            "artist_uri": artist_info["uri"]
                        }

            # Get existing lyrics if available
            existing = existing_data.get(spotify_uri, {})
            existing_lyrics = existing.get('lyrics')

            # Check if artist is soundtrack (will be refined after we get artist genres)
            is_artist_soundtrack = (
                SoundtrackClassifier.is_known_composer(artist_name) or
                SoundtrackClassifier.has_orchestra_keywords(artist_name)
            )

            # Check if song is soundtrack
            album_name = track_data.get("album_name", "") if track_data.get("status") == "success" else ""
            is_soundtrack = SoundtrackClassifier.classify_song(
                song_name, album_name, is_artist_soundtrack
            )

            # Determine lyrics
            lyrics = existing_lyrics
            has_lyrics = bool(existing_lyrics)
            lyrics_source = "E" if has_lyrics else "-"  # E=existing

            # Only fetch from Genius if FETCH_NEW_LYRICS=True and we don't have lyrics
            if FETCH_NEW_LYRICS and not has_lyrics and not is_soundtrack and lyrics_fetcher and lyrics_fetcher.genius:
                lyrics, has_lyrics = lyrics_fetcher.fetch(song_name, artist_name)
                if has_lyrics:
                    lyrics_fetched += 1
                    lyrics_source = "N"  # N=new
                    print(f"    [NEW LYRICS] {song_name[:30]}")
                time.sleep(GENIUS_DELAY)

            # Count lyrics sources
            if has_lyrics:
                if lyrics_source == "E":
                    lyrics_from_existing += 1
            else:
                no_lyrics += 1

            # Detect language fresh (genres will be empty for now, updated after artist processing)
            language, detection_method = LanguageDetector.detect_language(
                song_name, artist_name, lyrics, is_soundtrack,
                genres=""
            )

            # Create song record
            song_record = {
                "song_name": song_name,
                "artist_name": artist_name,
                "spotify_track_uri": spotify_uri,
                "duration_ms": track_data.get("duration_ms"),
                "duration_s": round(track_data.get("duration_ms", 0) / 1000, 2) if track_data.get("duration_ms") else None,
                "release_date": track_data.get("release_date"),
                "release_date_year": track_data.get("release_date_year"),
                "popularity": track_data.get("popularity"),
                "is_soundtrack": is_soundtrack,
                "has_lyrics": has_lyrics,
                "lyrics": lyrics,
                "language": language,
                "detection_method": detection_method
            }

            all_songs.append(song_record)

            # Progress indicator
            status = "ok" if track_data.get("status") == "success" else "err"
            lyrics_status = "L" if has_lyrics else "-"
            print(f"    [{song_num}/{total}] {status} {lyrics_status}{lyrics_source} {language[:3]:3s} | {song_name[:40]}")

        # Save progress periodically
        if batch_end % SAVE_INTERVAL == 0 or batch_end == total:
            save_songs(all_songs)

    print(f"\n  Lyrics summary:")
    print(f"    From existing data: {lyrics_from_existing:,}")
    if FETCH_NEW_LYRICS:
        print(f"    Fetched from Genius: {lyrics_fetched:,}")
    print(f"    No lyrics: {no_lyrics:,}")

    return all_songs, all_artists

def process_artists(all_artists: Dict[str, Dict], all_songs: List[Dict],
                    spotify: SpotifyAPI) -> Tuple[List[Dict], Dict[str, str]]:
    """Process artists: fetch metadata, determine language from song majority"""
    print(f"\n Processing {len(all_artists):,} artists...")

    # Build song language counts per artist
    artist_languages = {}
    artist_soundtrack_counts = {}

    for song in all_songs:
        artist_key = song["artist_name"].lower().strip()
        if artist_key not in artist_languages:
            artist_languages[artist_key] = Counter()
            artist_soundtrack_counts[artist_key] = []

        lang = song["language"]
        if lang and lang != "Unknown":
            artist_languages[artist_key][lang] += 1

        artist_soundtrack_counts[artist_key].append(song["is_soundtrack"])

    # Get Spotify artist details in batches
    artists_list = list(all_artists.values())
    final_artists = []
    artist_genres_map = {}  # For returning to update songs

    for batch_start in range(0, len(artists_list), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(artists_list))
        batch = artists_list[batch_start:batch_end]

        artist_ids = [a["artist_id"] for a in batch]
        artist_results = spotify.get_batch_artists(artist_ids)
        time.sleep(REQUEST_DELAY)

        for artist_info in batch:
            artist_id = artist_info["artist_id"]
            artist_name = artist_info["artist_name"]
            artist_key = artist_name.lower().strip()

            api_data = artist_results.get(artist_id, {"status": "error"})

            # Get genres from API
            genres = api_data.get("genres", "") if api_data.get("status") == "success" else ""
            artist_genres_map[artist_key] = genres

            # Determine if artist is soundtrack
            soundtrack_counts = artist_soundtrack_counts.get(artist_key, [])
            is_soundtrack = (
                SoundtrackClassifier.classify_artist(artist_name, genres) or
                (soundtrack_counts and sum(soundtrack_counts) > len(soundtrack_counts) / 2)
            )

            # Determine artist language by majority of songs
            lang_counts = artist_languages.get(artist_key, Counter())
            if is_soundtrack:
                language = "Soundtrack"
                detection_method = "soundtrack"
            elif lang_counts:
                non_soundtrack = {k: v for k, v in lang_counts.items() if k != "Soundtrack"}
                if non_soundtrack:
                    language = max(non_soundtrack, key=non_soundtrack.get)
                    detection_method = "majority_songs"
                else:
                    language = "Soundtrack"
                    detection_method = "soundtrack"
            else:
                # Try genre-based detection for artist
                genre_lang = LanguageDetector.detect_from_genres(genres)
                if genre_lang:
                    language = genre_lang
                    detection_method = "genre"
                else:
                    language = "Unknown"
                    detection_method = "unknown"

            artist_record = {
                "artist_name": artist_name,
                "artist_uri": artist_info["artist_uri"],
                "genres": genres,
                "followers": api_data.get("followers"),
                "popularity": api_data.get("popularity"),
                "is_soundtrack": is_soundtrack,
                "language": language,
                "detection_method": detection_method
            }

            final_artists.append(artist_record)

    return final_artists, artist_genres_map

def refine_song_languages(all_songs: List[Dict], artist_genres_map: Dict[str, str]) -> List[Dict]:
    """Second pass: refine song languages using artist genres"""
    print("\n Refining song languages with artist genres...")

    refined = 0
    for song in all_songs:
        # Only refine if language is still Unknown
        if song["language"] == "Unknown":
            artist_key = song["artist_name"].lower().strip()
            genres = artist_genres_map.get(artist_key, "")

            if genres:
                genre_lang = LanguageDetector.detect_from_genres(genres)
                if genre_lang:
                    song["language"] = genre_lang
                    song["detection_method"] = "genre"
                    refined += 1

    print(f"  Refined {refined} songs using artist genres")
    return all_songs

def save_songs(songs: List[Dict]):
    """Save songs to CSV"""
    if not songs:
        return
    df = pd.DataFrame(songs)
    df.to_csv(SONGS_OUTPUT_FILE, index=False, encoding='utf-8-sig')

def save_artists(artists: List[Dict]):
    """Save artists to CSV"""
    if not artists:
        return
    df = pd.DataFrame(artists)
    df.to_csv(ARTISTS_OUTPUT_FILE, index=False, encoding='utf-8-sig')

def print_summary(songs: List[Dict], artists: List[Dict]):
    """Print processing summary"""
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE!")
    print("=" * 70)

    total_songs = len(songs)
    soundtrack_songs = sum(1 for s in songs if s.get("is_soundtrack"))
    lyrics_found = sum(1 for s in songs if s.get("has_lyrics"))

    print(f"\n SONGS ({SONGS_OUTPUT_FILE}):")
    print(f"  Total: {total_songs:,}")
    print(f"  With lyrics: {lyrics_found:,} ({lyrics_found/total_songs*100:.1f}%)")
    print(f"  Soundtracks: {soundtrack_songs:,} ({soundtrack_songs/total_songs*100:.1f}%)")

    lang_counts = Counter(s["language"] for s in songs)
    print(f"\n  Language distribution:")
    for lang, count in lang_counts.most_common(15):
        print(f"    {lang}: {count:,} ({count/total_songs*100:.1f}%)")

    # Detection method breakdown
    method_counts = Counter(s["detection_method"] for s in songs)
    print(f"\n  Detection method breakdown:")
    for method, count in method_counts.most_common():
        print(f"    {method}: {count:,} ({count/total_songs*100:.1f}%)")

    total_artists = len(artists)
    soundtrack_artists = sum(1 for a in artists if a.get("is_soundtrack"))

    print(f"\n ARTISTS ({ARTISTS_OUTPUT_FILE}):")
    print(f"  Total: {total_artists:,}")
    print(f"  Soundtracks: {soundtrack_artists:,} ({soundtrack_artists/total_artists*100:.1f}%)")

    artist_lang_counts = Counter(a["language"] for a in artists)
    print(f"\n  Language distribution:")
    for lang, count in artist_lang_counts.most_common(10):
        print(f"    {lang}: {count:,} ({count/total_artists*100:.1f}%)")

    print("\n" + "=" * 70)

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("MASTER COLLECTION CREATOR")
    print("=" * 70)
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {SONGS_OUTPUT_FILE}, {ARTISTS_OUTPUT_FILE}")
    print(f"Mode:   {'FETCH NEW LYRICS from Genius' if FETCH_NEW_LYRICS else 'USE EXISTING LYRICS (re-detect languages)'}")
    print("=" * 70)

    # Check for required credentials
    print("\n Checking credentials...")
    if not CLIENT_ID or not CLIENT_SECRET:
        print("  ERROR: SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET required in .env")
        return
    print("  Spotify credentials found")

    # Initialize lyrics fetcher only if needed
    lyrics_fetcher = None
    if FETCH_NEW_LYRICS:
        if not GENIUS_TOKEN:
            print("  WARNING: GENIUS_TOKEN not found - cannot fetch new lyrics")
            print("  Set FETCH_NEW_LYRICS=False to use existing lyrics only")
            return
        print("  Genius credentials found")

    # Load existing lyrics data (always load to preserve lyrics)
    existing_data = load_existing_lyrics_data()

    # Initialize APIs
    print("\n Initializing APIs...")
    spotify = SpotifyAPI()
    if FETCH_NEW_LYRICS:
        lyrics_fetcher = LyricsFetcher()

    # Load data
    df = load_streaming_history()

    # Extract unique songs
    unique_songs = extract_unique_songs(df)

    # Process songs (metadata, lyrics if FETCH_NEW_LYRICS, fresh language detection)
    all_songs, all_artists = process_songs(unique_songs, spotify, lyrics_fetcher, existing_data)

    # Process artists (always fetches fresh data from Spotify API)
    final_artists, artist_genres_map = process_artists(all_artists, all_songs, spotify)

    # Second pass: refine song languages with artist genres
    all_songs = refine_song_languages(all_songs, artist_genres_map)

    # Final save
    print("\n Saving final results...")
    save_songs(all_songs)
    save_artists(final_artists)

    # Print summary
    print_summary(all_songs, final_artists)

    print(f"\n Output files ready for MongoDB import:")
    print(f"  - {SONGS_OUTPUT_FILE}")
    print(f"  - {ARTISTS_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
