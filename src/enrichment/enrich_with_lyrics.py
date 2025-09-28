#!/usr/bin/env python3
"""
Lyrics Fetcher & Language Detection Script for MongoDB - OPTIMIZED
Processes songs with has_lyrics: null, fetches lyrics using Genius API,
detects languages using character detection and langdetect.

OPTIMIZATION: Removed heavy artist language conflict resolution that was taking 3+ minutes.
Artist languages are now only updated when new artists are processed, not every run.

Processing Flow:
1. Find unprocessed songs (has_lyrics: null)
2. For each song: fetch lyrics → update database → detect language → update database
3. Handle soundtrack classification
4. Skip artist conflict resolution (moved to separate weekly script)

Collections:
- songs_master: Target for lyrics and language updates
- artists_master: Target for artist language updates (minimal updates only)
"""

import os
import re
import time
from typing import Optional, Tuple, Dict, List
from datetime import datetime
from collections import Counter
import lyricsgenius
from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('enrichment.enrich_with_lyrics')

# Configuration
DATABASE_NAME = "Spotify"
SONGS_MASTER_COLLECTION = "songs_master"
ARTISTS_MASTER_COLLECTION = "artists_master"

# MongoDB Configuration
MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')
GENIUS_TOKEN = os.getenv('GENIUS_TOKEN')

# Language Detection Configuration
CONFIDENCE_THRESHOLD = 70.0  # Minimum confidence for language detection (0-100)
MIN_LYRICS_LENGTH = 100      # Minimum characters for lyrics to be considered valid

# Soundtrack/Orchestral/Non-vocal genres (exact matches only)
SOUNDTRACK_GENRES = {
    'soundtrack', 'japanese vgm', 'classical', 'orchestra', 'classical piano', 
    'neoclassical', 'chamber music', 'minimalism', 'requiem', 'choral', 
    'ragtime', 'ambient', 'drone', 'space music', 'new age', 'medieval', 
    'sea shanties', 'traditional music', 'traditional folk', 'japanese classical', 
    'polka', 'tango', 'bolero', 'exotica', 'easy listening', 'gregorian chant',
    'avant-garde', 'lounge', 'downtempo', 'breakbeat', 'idm'
}

# Top film composers (exact name matches only)
FILM_COMPOSERS = {
    'john williams', 'hans zimmer', 'ennio morricone', 'bernard herrmann', 
    'jerry goldsmith', 'elmer bernstein', 'max steiner', 'dmitri tiomkin',
    'miklos rozsa', 'alex north', 'maurice jarre', 'lalo schifrin',
    'henry mancini', 'dave grusin', 'thomas newman', 'james newton howard',
    'alan silvestri', 'danny elfman', 'carter burwell', 'elliot goldenthal',
    'gabriel yared', 'rachel portman', 'patrick doyle', 'david arnold',
    'michael giacchino', 'alexandre desplat', 'clint mansell', 'trent reznor',
    'atticus ross', 'jonny greenwood', 'mica levi', 'ludwig goransson',
    'ramin djawadi', 'bear mccreary', 'joe hisaishi', 'ryuichi sakamoto',
    'toru takemitsu', 'akira ifukube', 'isao tomita', 'vangelis',
    'giorgio moroder', 'jean-michel jarre', 'wenzel fuchs', 'klaus badelt',
    'steve jablonsky', 'mark mothersbaugh', 'christophe beck', 'alan menken',
    'randy newman', 'phil collins', 'lin-manuel miranda', 'benj pasek',
    'justin paul', 'kristen anderson-lopez', 'robert lopez', 'stephen schwartz',
    'andrew lloyd webber', 'tim rice', 'sherman brothers', 'richard sherman',
    'robert sherman', 'bill conti', 'quincy jones', 'john barry',
    'basil poledouris', 'james horner', 'howard shore', 'craig armstrong',
    'dario marianelli', 'gustavo santaolalla', 'ar rahman', 'tan dun',
    'yann tiersen', 'cliff martinez', 'junkie xl', 'tom holkenborg',
    'brian tyler', 'tyler bates', 'rupert gregson-williams', 'henry jackman',
    'fernando velazquez', 'jed kurzel', 'benjamin wallfisch', 'daniel pemberton',
    'hildur guðnadóttir', 'nicholas britell', 'justin hurwitz', 'steven price',
    'antonio pinto', 'volker bertelmann', 'hauschka', 'max richter',
    'ólafur arnalds', 'nils frahm', 'dustin ohalloran', 'adam peters',
    'roberto cacciapaglia', 'emilie simon', 'cliff eidelman', 'marc shaiman',
    'rachel elkind', 'wendy carlos', 'brad fiedel', 'graeme revell',
    'stewart copeland', 'mark isham', 'david newman', 'randy edelman',
    'joel mcneely', 'don davis', 'jan kaczmarek', 'mychael danna',
    'jeff danna', 'christoph beck', 'terence blanchard', 'branford marsalis'
}

# Top classical/orchestral composers (exact name matches only)
CLASSICAL_COMPOSERS = {
    'johann sebastian bach', 'ludwig van beethoven', 'wolfgang amadeus mozart',
    'franz schubert', 'frederic chopin', 'johannes brahms', 'pyotr ilyich tchaikovsky',
    'claude debussy', 'maurice ravel', 'igor stravinsky', 'sergei rachmaninoff',
    'franz liszt', 'robert schumann', 'felix mendelssohn', 'antonio vivaldi',
    'george frideric handel', 'haydn', 'joseph haydn', 'hector berlioz',
    'richard wagner', 'gustav mahler', 'dmitri shostakovich', 'sergei prokofiev',
    'aaron copland', 'leonard bernstein', 'george gershwin', 'erik satie',
    'camille saint-saens', 'cesar franck', 'gabriel faure', 'edvard grieg',
    'jean sibelius', 'antonin dvorak', 'bedrich smetana', 'leos janacek',
    'bela bartok', 'zoltan kodaly', 'ralph vaughan williams', 'gustav holst',
    'benjamin britten', 'edward elgar', 'frederick delius', 'william walton',
    'michael tippett', 'harrison birtwistle', 'peter maxwell davies',
    'olivier messiaen', 'pierre boulez', 'karlheinz stockhausen', 'gyorgy ligeti',
    'luciano berio', 'luigi nono', 'iannis xenakis', 'witold lutoslawski',
    'krzysztof penderecki', 'henryk gorecki', 'arvo part', 'john tavener',
    'steve reich', 'philip glass', 'terry riley', 'la monte young',
    'john adams', 'michael nyman', 'gavin bryars', 'howard skempton',
    'morton feldman', 'john cage', 'earle brown', 'christian wolff',
    'giacinto scelsi', 'salvatore sciarrino', 'helmut lachenmann',
    'brian ferneyhough', 'michael finnissy', 'richard barrett',
    'wolfgang rihm', 'georges aperghis', 'pascal dusapin',
    'philippe manoury', 'tristan murail', 'gerard grisey', 'kaija saariaho',
    'magnus lindberg', 'esa-pekka salonen', 'thomas ades', 'george benjamin',
    'julian anderson', 'mark-anthony turnage', 'judith weir', 'sally beamish',
    'james macmillan', 'john corigliano', 'christopher rouse', 'william bolcom',
    'david del tredici', 'frederic rzewski', 'louis andriessen', 'martijn padding',
    'michel van der aa', 'anna clyne', 'caroline shaw',
    'nico muhly', 'mason bates', 'andrew norman', 'john luther adams',
    'david lang', 'julia wolfe', 'michael gordon'
}

# Orchestra/ensemble keywords for word matching (single words only)
ORCHESTRA_KEYWORDS = {
    'orchestra', 'symphony', 'philharmonic', 'ensemble', 'quartet', 'quintet',
    'chamber', 'conservatory', 'academy', 'chorale', 'choir', 'concerto',
    'orchestral', 'symphonic', 'operatic', 'ballet', 'strings', 'winds',
    'brass', 'woodwinds', 'percussion', 'conducting', 'conductor'
}

class LanguageDetector:
    def __init__(self):
        pass
    
    def detect_hebrew_chars(self, text: str) -> bool:
        """Check if text contains Hebrew characters."""
        if not text:
            return False
        
        hebrew_pattern = re.compile(r'[\u0590-\u05FF\uFB1D-\uFB4F]')
        return bool(hebrew_pattern.search(text))
    
    def detect_japanese_chars(self, text: str) -> bool:
        """Check if text contains Japanese characters (Hiragana, Katakana, Kanji)."""
        if not text:
            return False
        
        # Hiragana: U+3040–U+309F, Katakana: U+30A0–U+30FF, Kanji: U+4E00–U+9FAF
        japanese_pattern = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]')
        return bool(japanese_pattern.search(text))
    
    def detect_language_ml(self, text: str) -> Tuple[Optional[str], float]:
        """Use machine learning to detect language with confidence score."""
        if not text or len(text.strip()) < 3:
            return None, 0.0
        
        try:
            lang_probs = detect_langs(text)
            if lang_probs:
                best_lang = lang_probs[0]
                confidence_percent = best_lang.prob * 100
                return best_lang.lang, confidence_percent
            return None, 0.0
        except (LangDetectException, Exception):
            return None, 0.0
    
    def normalize_language_code(self, lang_code: str) -> str:
        """Convert language codes to readable names."""
        lang_map = {
            'en': 'English', 'es': 'Spanish', 'fr': 'French', 'de': 'German',
            'it': 'Italian', 'pt': 'Portuguese', 'ru': 'Russian', 'ja': 'Japanese',
            'ko': 'Korean', 'zh-cn': 'Chinese', 'zh': 'Chinese', 'ar': 'Arabic',
            'tr': 'Turkish', 'nl': 'Dutch', 'pl': 'Polish', 'sv': 'Swedish',
            'no': 'Norwegian', 'da': 'Danish', 'fi': 'Finnish', 'he': 'Hebrew',
            'hi': 'Hindi', 'th': 'Thai', 'vi': 'Vietnamese', 'id': 'Indonesian',
            'ms': 'Malay', 'tl': 'Filipino', 'ro': 'Romanian', 'hu': 'Hungarian',
            'cs': 'Czech', 'sk': 'Slovak', 'bg': 'Bulgarian', 'hr': 'Croatian',
            'sr': 'Serbian', 'sl': 'Slovenian', 'et': 'Estonian', 'lv': 'Latvian',
            'lt': 'Lithuanian', 'uk': 'Ukrainian', 'be': 'Belarusian',
            'mk': 'Macedonian', 'sq': 'Albanian', 'ca': 'Catalan', 'eu': 'Basque',
            'gl': 'Galician', 'cy': 'Welsh', 'ga': 'Irish', 'is': 'Icelandic',
            'mt': 'Maltese'
        }
        return lang_map.get(lang_code, lang_code.title() if lang_code else 'Unknown')

class SoundtrackClassifier:
    @staticmethod
    def contains_soundtrack_genre(genres: str) -> bool:
        """Check if any genre matches soundtrack genres"""
        if not genres:
            return False
        
        genre_list = [genre.strip().lower() for genre in str(genres).split(',')]
        return any(genre in SOUNDTRACK_GENRES for genre in genre_list)

    @staticmethod
    def is_known_composer(artist_name: str) -> bool:
        """Check if artist name exactly matches known composers"""
        if not artist_name:
            return False
        
        normalized_name = artist_name.lower().strip()
        return normalized_name in FILM_COMPOSERS or normalized_name in CLASSICAL_COMPOSERS

    @staticmethod
    def contains_orchestra_keywords(text: str) -> bool:
        """Check if text contains orchestra keywords as complete words"""
        if not text:
            return False
        
        words = re.findall(r'\b\w+\b', text.lower())
        return any(word in ORCHESTRA_KEYWORDS for word in words)

def connect_to_mongodb():
    """Connect to MongoDB Atlas"""
    if not MONGODB_CONNECTION_STRING:
        logger.error("MongoDB connection string not found in environment variables")
        return None
    
    try:
        logger.info("Connecting to MongoDB Atlas...")
        client = MongoClient(MONGODB_CONNECTION_STRING)
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB Atlas")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def initialize_genius_api():
    """Initialize Genius API client"""
    if not GENIUS_TOKEN:
        logger.error("GENIUS_TOKEN not found in environment variables")
        return None
    
    try:
        genius = lyricsgenius.Genius(GENIUS_TOKEN)
        genius.verbose = False  # Reduce API output noise
        genius.remove_section_headers = False  # Keep original formatting
        logger.info("✅ Successfully connected to Genius API")
        return genius
    except Exception as e:
        logger.error(f"Error initializing Genius API: {e}")
        return None

def get_unprocessed_songs(db):
    """Get songs with has_lyrics: null (unprocessed songs)"""
    try:
        collection = db[SONGS_MASTER_COLLECTION]
        songs = list(collection.find({"has_lyrics": None}))
        logger.info(f"✅ Found {len(songs)} unprocessed songs")
        return songs
    except Exception as e:
        logger.error(f"Error getting unprocessed songs: {e}")
        return []

def fetch_lyrics(genius, song_name: str, artist_name: str) -> Tuple[Optional[str], bool]:
    """
    Fetch lyrics for a single song
    Returns: (lyrics_text, has_lyrics_boolean)
    """
    try:
        logger.debug(f"Searching for lyrics: '{song_name}' by '{artist_name}'")
        
        # Search for the song
        song = genius.search_song(song_name, artist_name)
        
        if song and song.lyrics:
            lyrics = song.lyrics.strip()
            
            # Check if lyrics are long enough
            if len(lyrics) >= MIN_LYRICS_LENGTH:
                logger.debug(f"✅ Found lyrics ({len(lyrics)} characters)")
                return lyrics, True
            else:
                logger.debug(f"❌ Lyrics too short ({len(lyrics)} characters, minimum {MIN_LYRICS_LENGTH})")
                return None, False
        else:
            logger.debug(f"❌ No lyrics found")
            return None, False
            
    except Exception as e:
        logger.debug(f"❌ Error fetching lyrics: {e}")
        return None, False

def detect_song_language(detector, song_data, lyrics: Optional[str]) -> Tuple[str, str]:
    """
    Detect language for a song following priority: Soundtrack > Hebrew > Japanese > Lyrics > Title
    Returns: (language, detection_method)
    """
    song_name = song_data.get('song_name', '')
    artist_name = song_data.get('artist_name', '')
    
    # Step 1: Check if it's soundtrack (highest priority)
    is_soundtrack = song_data.get('is_soundtrack', False)
    if is_soundtrack:
        return "Soundtrack", "soundtrack"
    
    # Step 2: Check for Hebrew characters in song/artist name
    combined_text = f"{song_name} {artist_name}"
    if detector.detect_hebrew_chars(combined_text):
        return "Hebrew", "character_detection"
    
    # Step 3: Check for Japanese characters in song/artist name
    if detector.detect_japanese_chars(combined_text):
        return "Japanese", "character_detection"
    
    # Step 4: Try lyrics if available
    if lyrics and len(lyrics.strip()) >= MIN_LYRICS_LENGTH:
        lang, confidence = detector.detect_language_ml(lyrics)
        if lang and confidence >= CONFIDENCE_THRESHOLD:
            return detector.normalize_language_code(lang), "lyrics"
    
    # Step 5: Try song title
    if song_name:
        lang, confidence = detector.detect_language_ml(song_name)
        if lang and confidence >= CONFIDENCE_THRESHOLD:
            return detector.normalize_language_code(lang), "title"
    
    # Step 6: Try artist name
    if artist_name:
        lang, confidence = detector.detect_language_ml(artist_name)
        if lang and confidence >= CONFIDENCE_THRESHOLD:
            return detector.normalize_language_code(lang), "artist_name"
    
    return "Unknown", "unknown"

def classify_soundtrack(song_data) -> bool:
    """Classify if a song is soundtrack based on artist info"""
    artist_name = song_data.get('artist_name', '')
    song_name = song_data.get('song_name', '')
    
    # Check if artist is a known composer
    if SoundtrackClassifier.is_known_composer(artist_name):
        return True
    
    # Check if artist/song name contains orchestra keywords
    if SoundtrackClassifier.contains_orchestra_keywords(f"{artist_name} {song_name}"):
        return True
    
    return False

def update_song_in_db(db, song_id, lyrics: Optional[str], has_lyrics: bool, 
                     language: str, detection_method: str, is_soundtrack: bool):
    """Update song record in database"""
    try:
        collection = db[SONGS_MASTER_COLLECTION]
        
        update_data = {
            "has_lyrics": has_lyrics,
            "language": language,
            "detection_method": detection_method,
            "is_soundtrack": is_soundtrack
        }
        
        # Add lyrics if they exist
        if lyrics:
            update_data["lyrics"] = lyrics
        
        result = collection.update_one(
            {"_id": song_id},
            {"$set": update_data}
        )
        
        return result.modified_count > 0
        
    except Exception as e:
        logger.error(f"Error updating song in database: {e}")
        return False

def update_new_artist_language(db, artist_name: str, language: str, detection_method: str):
    """Update language for a NEW artist only (lightweight operation)"""
    try:
        artists_collection = db[ARTISTS_MASTER_COLLECTION]
        
        # Only update if the artist exists and doesn't have a language set
        result = artists_collection.update_one(
            {
                "artist_name": artist_name,
                "$or": [
                    {"language": None},
                    {"language": "Unknown"},
                    {"language": {"$exists": False}}
                ]
            },
            {"$set": {
                "language": language,
                "detection_method": detection_method
            }}
        )
        
        if result.modified_count > 0:
            logger.info(f"  🎭 Updated new artist language: {artist_name} → {language}")
        
        return result.modified_count > 0
        
    except Exception as e:
        logger.error(f"Error updating artist language: {e}")
        return False

def fix_soundtrack_language_issues(db):
    """Fix existing soundtrack songs/artists that don't have language: 'Soundtrack'"""
    try:
        songs_collection = db[SONGS_MASTER_COLLECTION]
        artists_collection = db[ARTISTS_MASTER_COLLECTION]
        
        logger.info("🎬 Fixing soundtrack language issues...")
        
        # Fix songs with is_soundtrack: true but language != "Soundtrack"
        songs_fixed = 0
        soundtrack_songs = songs_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        })
        
        for song in soundtrack_songs:
            result = songs_collection.update_one(
                {"_id": song["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )
            
            if result.modified_count > 0:
                songs_fixed += 1
                old_lang = song.get('language', 'None')
                logger.info(f"  🎬 SONG: '{song['song_name']}' by {song['artist_name']}: {old_lang} → Soundtrack")
        
        # Fix artists with is_soundtrack: true but language != "Soundtrack"
        artists_fixed = 0
        soundtrack_artists = artists_collection.find({
            "is_soundtrack": True,
            "language": {"$ne": "Soundtrack"}
        })
        
        for artist in soundtrack_artists:
            result = artists_collection.update_one(
                {"_id": artist["_id"]},
                {"$set": {
                    "language": "Soundtrack",
                    "detection_method": "soundtrack"
                }}
            )
            
            if result.modified_count > 0:
                artists_fixed += 1
                old_lang = artist.get('language', 'None')
                logger.info(f"  🎭 ARTIST: {artist['artist_name']}: {old_lang} → Soundtrack")
        
        if songs_fixed == 0 and artists_fixed == 0:
            logger.info("  ✅ No soundtrack language issues found")
        else:
            logger.info(f"✅ Fixed {songs_fixed} songs and {artists_fixed} artists with soundtrack language issues")
        
    except Exception as e:
        logger.error(f"Error fixing soundtrack language issues: {e}")

def main():
    """Main execution function"""
    logger.info("="*80)
    logger.info("LYRICS FETCHER & LANGUAGE DETECTION SCRIPT - OPTIMIZED")
    logger.info("="*80)
    logger.info(f"Target Collections: {DATABASE_NAME}.{SONGS_MASTER_COLLECTION}")
    logger.info(f"Processing: Songs with has_lyrics: null")
    logger.info(f"Optimization: Removed heavy artist conflict resolution")
    logger.info("="*80)
    
    # Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    # Initialize Genius API
    genius = initialize_genius_api()
    if not genius:
        client.close()
        return
    
    # Initialize language detector
    detector = LanguageDetector()
    
    try:
        db = client[DATABASE_NAME]
        
        # Get unprocessed songs
        unprocessed_songs = get_unprocessed_songs(db)
        
        if not unprocessed_songs:
            logger.info("🎉 No unprocessed songs found! All songs have been processed.")
            return
        
        logger.info(f"🔍 Processing {len(unprocessed_songs)} songs...")
        
        processed_count = 0
        lyrics_found_count = 0
        soundtrack_count = 0
        new_artist_languages = set()
        
        # Process each song individually
        for i, song in enumerate(unprocessed_songs, 1):
            song_id = song['_id']
            song_name = song.get('song_name', '')
            artist_name = song.get('artist_name', '')
            
            logger.info(f"[{i}/{len(unprocessed_songs)}] Processing: '{song_name}' by {artist_name}")
            
            # Step 1: Classify as soundtrack
            is_soundtrack = classify_soundtrack(song)
            if is_soundtrack:
                soundtrack_count += 1
                logger.info(f"  🎬 Classified as soundtrack")
            
            # Step 2: Fetch lyrics (skip if soundtrack)
            lyrics = None
            has_lyrics = False
            
            if not is_soundtrack:
                lyrics, has_lyrics = fetch_lyrics(genius, song_name, artist_name)
                if has_lyrics:
                    lyrics_found_count += 1
                time.sleep(0.1)  # Small delay between API calls
            else:
                logger.info(f"  🎬 Skipping lyrics fetch for soundtrack")
                has_lyrics = False
            
            # Step 3: Detect language (pass the newly classified is_soundtrack value)
            song_with_soundtrack = song.copy()
            song_with_soundtrack['is_soundtrack'] = is_soundtrack
            language, detection_method = detect_song_language(detector, song_with_soundtrack, lyrics)
            logger.info(f"  🌍 Language detected: {language} (method: {detection_method})")
            
            # Step 4: Update database
            success = update_song_in_db(
                db, song_id, lyrics, has_lyrics, 
                language, detection_method, is_soundtrack
            )
            
            if success:
                processed_count += 1
                logger.info(f"  ✅ Updated in database")
                
                # Step 5: Update artist language if it's a new artist with a detected language
                if language not in ['Unknown', 'Soundtrack'] and artist_name not in new_artist_languages:
                    update_new_artist_language(db, artist_name, language, detection_method)
                    new_artist_languages.add(artist_name)
            else:
                logger.error(f"  ❌ Failed to update database")
        
        # Step 6: Fix any remaining soundtrack language issues (lightweight)
        fix_soundtrack_language_issues(db)
        
        # Summary
        logger.info("="*80)
        logger.info("PROCESSING COMPLETE!")
        logger.info("="*80)
        logger.info(f"Songs processed: {processed_count}/{len(unprocessed_songs)}")
        logger.info(f"Lyrics found: {lyrics_found_count}")
        logger.info(f"Soundtracks identified: {soundtrack_count}")
        logger.info(f"New artist languages updated: {len(new_artist_languages)}")
        logger.info(f"Language detection completed for all processed songs")
        logger.info("⚡ Artist conflict resolution skipped for performance")
        
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        client.close()
        logger.info("✅ MongoDB connection closed")

if __name__ == "__main__":
    main()
