"""
Configuration constants for the Spotify data pipeline.

Central location for all field names, collection names, mappings,
and classification data (soundtracks, composers, etc.).
"""

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

DATABASE_NAME = "Spotify"

# Collection Names
STREAMING_COLLECTION = "StreamingHistory"
SONGS_MASTER_COLLECTION = "songs_master"
ARTISTS_MASTER_COLLECTION = "artists_master"

# =============================================================================
# API CONFIGURATION
# =============================================================================

SPOTIFY_BATCH_SIZE = 50
REQUEST_DELAY = 0.2  # Seconds between API calls
GENIUS_DELAY = 0.1   # Seconds between Genius API calls

# =============================================================================
# LANGUAGE DETECTION CONFIGURATION
# =============================================================================

CONFIDENCE_THRESHOLD = 70.0   # Minimum confidence for ML detection (0-100)
MIN_LYRICS_LENGTH = 100       # Minimum characters for lyrics to be valid
MIN_TITLE_LENGTH = 20         # Minimum chars for title-based detection

# =============================================================================
# ENRICHMENT STATUS VALUES
# =============================================================================

ENRICHMENT_STATUS_PENDING = "pending"
ENRICHMENT_STATUS_PROCESSED = "processed"
ENRICHMENT_STATUS_FAILED = "failed"

# =============================================================================
# FIELD NAME MAPPINGS - StreamingHistory
# =============================================================================

# Maps logical field names to actual MongoDB field names (matching CSV schema)
STREAMING_FIELDS = {
    # Timestamps
    "ts_utc": "ts_utc",
    "ts": "ts",
    "date": "Date",
    "year": "Year",
    "month": "Month",
    "day": "Day",

    # Duration fields
    "ms_played": "ms_played",
    "duration_seconds": "duration_seconds",
    "duration_minutes": "duration_minutes",
    "duration_hours": "duration_hours",

    # Track metadata
    "track_name": "master_metadata_track_name",
    "artist_name": "master_metadata_album_artist_name",
    "album_name": "master_metadata_album_album_name",
    "spotify_track_uri": "spotify_track_uri",

    # Playback context
    "conn_country": "conn_country",
    "platform": "platform",
    "shuffle": "shuffle",
    "skipped": "skipped",
    "offline": "offline",
    "offline_timestamp": "offline_timestamp",
    "incognito_mode": "incognito_mode",
    "reason_start": "reason_start",
    "reason_end": "reason_end",
    "ip_addr": "ip_addr",
}

# =============================================================================
# VALID DETECTION METHODS
# =============================================================================

VALID_DETECTION_METHODS = {
    "lyrics",              # Detected from lyrics text
    "title",               # Detected from song/artist title
    "artist_name",         # Detected from artist name
    "character_detection", # Detected from non-Latin characters
    "soundtrack",          # Classified as soundtrack
    "majority_songs",      # Based on majority of artist's songs
    "genre",               # Based on genre mapping
    "unknown",             # Could not detect
}

# =============================================================================
# LANGUAGE CODE NORMALIZATION
# =============================================================================

LANGUAGE_MAP = {
    'en': 'English',
    'es': 'Spanish',
    'fr': 'French',
    'de': 'German',
    'it': 'Italian',
    'pt': 'Portuguese',
    'ru': 'Russian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh-cn': 'Chinese',
    'zh': 'Chinese',
    'zh-tw': 'Chinese',
    'ar': 'Arabic',
    'tr': 'Turkish',
    'nl': 'Dutch',
    'pl': 'Polish',
    'sv': 'Swedish',
    'no': 'Norwegian',
    'da': 'Danish',
    'fi': 'Finnish',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'th': 'Thai',
    'vi': 'Vietnamese',
    'id': 'Indonesian',
    'ms': 'Malay',
    'tl': 'Filipino',
    'ro': 'Romanian',
    'hu': 'Hungarian',
    'cs': 'Czech',
    'sk': 'Slovak',
    'bg': 'Bulgarian',
    'hr': 'Croatian',
    'sr': 'Serbian',
    'sl': 'Slovenian',
    'et': 'Estonian',
    'lv': 'Latvian',
    'lt': 'Lithuanian',
    'uk': 'Ukrainian',
    'be': 'Belarusian',
    'mk': 'Macedonian',
    'sq': 'Albanian',
    'ca': 'Catalan',
    'eu': 'Basque',
    'gl': 'Galician',
    'cy': 'Welsh',
    'ga': 'Irish',
    'is': 'Icelandic',
    'mt': 'Maltese',
    'el': 'Greek',
    'af': 'Afrikaans',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'bn': 'Bengali',
    'mr': 'Marathi',
    'gu': 'Gujarati',
    'kn': 'Kannada',
    'ml': 'Malayalam',
    'pa': 'Punjabi',
    'ur': 'Urdu',
    'fa': 'Persian',
    'ne': 'Nepali',
    'si': 'Sinhala',
}

# =============================================================================
# GENRE TO LANGUAGE MAPPING
# =============================================================================

GENRE_LANGUAGE_MAP = {
    # Korean
    'k-pop': 'Korean',
    'korean pop': 'Korean',
    'korean r&b': 'Korean',
    'korean hip hop': 'Korean',
    'korean indie': 'Korean',
    'korean ost': 'Korean',

    # Japanese
    'j-pop': 'Japanese',
    'j-rock': 'Japanese',
    'japanese pop': 'Japanese',
    'japanese rock': 'Japanese',
    'anime': 'Japanese',
    'japanese vgm': 'Japanese',
    'vocaloid': 'Japanese',

    # Spanish
    'reggaeton': 'Spanish',
    'latin pop': 'Spanish',
    'latin': 'Spanish',
    'latin hip hop': 'Spanish',
    'bachata': 'Spanish',
    'salsa': 'Spanish',
    'cumbia': 'Spanish',
    'merengue': 'Spanish',
    'flamenco': 'Spanish',
    'spanish pop': 'Spanish',
    'latin rock': 'Spanish',
    'urbano latino': 'Spanish',

    # French
    'french pop': 'French',
    'french hip hop': 'French',
    'french rap': 'French',
    'chanson': 'French',

    # German
    'german pop': 'German',
    'german hip hop': 'German',
    'schlager': 'German',
    'neue deutsche welle': 'German',

    # Portuguese/Brazilian
    'brazilian pop': 'Portuguese',
    'mpb': 'Portuguese',
    'bossa nova': 'Portuguese',
    'samba': 'Portuguese',
    'funk carioca': 'Portuguese',
    'sertanejo': 'Portuguese',
    'fado': 'Portuguese',

    # Italian
    'italian pop': 'Italian',
    'italian hip hop': 'Italian',

    # Russian
    'russian pop': 'Russian',
    'russian hip hop': 'Russian',
    'russian rock': 'Russian',

    # Arabic
    'arabic pop': 'Arabic',
    'khaliji': 'Arabic',
    'egyptian pop': 'Arabic',

    # Turkish
    'turkish pop': 'Turkish',
    'turkish rock': 'Turkish',

    # Hebrew
    'israeli pop': 'Hebrew',
    'mizrahi': 'Hebrew',

    # Hindi
    'bollywood': 'Hindi',
    'filmi': 'Hindi',
    'indian pop': 'Hindi',

    # Chinese
    'mandopop': 'Chinese',
    'cantopop': 'Chinese',
    'c-pop': 'Chinese',

    # Thai
    'thai pop': 'Thai',
    't-pop': 'Thai',

    # Dutch
    'dutch pop': 'Dutch',
    'nederpop': 'Dutch',

    # Swedish
    'swedish pop': 'Swedish',

    # Norwegian
    'norwegian pop': 'Norwegian',

    # Danish
    'danish pop': 'Danish',
    'dansk pop': 'Danish',

    # Finnish
    'finnish pop': 'Finnish',
    'suomi rock': 'Finnish',

    # Polish
    'polish pop': 'Polish',
    'polish hip hop': 'Polish',

    # Greek
    'greek pop': 'Greek',
    'laiko': 'Greek',
}

# =============================================================================
# SOUNDTRACK CLASSIFICATION - GENRES
# =============================================================================

SOUNDTRACK_GENRES = {
    'soundtrack',
    'japanese vgm',
    'classical',
    'orchestra',
    'classical piano',
    'neoclassical',
    'chamber music',
    'minimalism',
    'requiem',
    'choral',
    'ragtime',
    'ambient',
    'drone',
    'space music',
    'new age',
    'medieval',
    'sea shanties',
    'traditional music',
    'traditional folk',
    'japanese classical',
    'polka',
    'tango',
    'bolero',
    'exotica',
    'easy listening',
    'gregorian chant',
    'avant-garde',
    'lounge',
    'downtempo',
    'breakbeat',
    'idm',
    'film score',
    'video game music',
    'orchestral',
    'cinematic',
    'epic music',
    'trailer music',
}

# =============================================================================
# SOUNDTRACK CLASSIFICATION - FILM COMPOSERS
# =============================================================================

FILM_COMPOSERS = {
    'john williams',
    'hans zimmer',
    'ennio morricone',
    'bernard herrmann',
    'jerry goldsmith',
    'elmer bernstein',
    'max steiner',
    'dmitri tiomkin',
    'miklos rozsa',
    'alex north',
    'maurice jarre',
    'lalo schifrin',
    'henry mancini',
    'dave grusin',
    'thomas newman',
    'james newton howard',
    'alan silvestri',
    'danny elfman',
    'carter burwell',
    'elliot goldenthal',
    'gabriel yared',
    'rachel portman',
    'patrick doyle',
    'david arnold',
    'michael giacchino',
    'alexandre desplat',
    'clint mansell',
    'trent reznor',
    'atticus ross',
    'jonny greenwood',
    'mica levi',
    'ludwig goransson',
    'ramin djawadi',
    'bear mccreary',
    'joe hisaishi',
    'ryuichi sakamoto',
    'toru takemitsu',
    'akira ifukube',
    'isao tomita',
    'vangelis',
    'giorgio moroder',
    'jean-michel jarre',
    'wenzel fuchs',
    'klaus badelt',
    'steve jablonsky',
    'mark mothersbaugh',
    'christophe beck',
    'alan menken',
    'randy newman',
    'lin-manuel miranda',
    'benj pasek',
    'justin paul',
    'kristen anderson-lopez',
    'robert lopez',
    'stephen schwartz',
    'andrew lloyd webber',
    'tim rice',
    'richard sherman',
    'robert sherman',
    'bill conti',
    'quincy jones',
    'john barry',
    'basil poledouris',
    'james horner',
    'howard shore',
    'craig armstrong',
    'dario marianelli',
    'gustavo santaolalla',
    'ar rahman',
    'tan dun',
    'yann tiersen',
    'cliff martinez',
    'junkie xl',
    'tom holkenborg',
    'brian tyler',
    'tyler bates',
    'rupert gregson-williams',
    'henry jackman',
    'fernando velazquez',
    'jed kurzel',
    'benjamin wallfisch',
    'daniel pemberton',
    'hildur gudnadottir',
    'nicholas britell',
    'justin hurwitz',
    'steven price',
    'antonio pinto',
    'volker bertelmann',
    'hauschka',
    'max richter',
    'olafur arnalds',
    'nils frahm',
    'dustin ohalloran',
    'graeme revell',
    'stewart copeland',
    'mark isham',
    'david newman',
    'randy edelman',
    'joel mcneely',
    'don davis',
    'jan kaczmarek',
    'mychael danna',
    'jeff danna',
    'terence blanchard',
    'branford marsalis',
}

# =============================================================================
# SOUNDTRACK CLASSIFICATION - CLASSICAL COMPOSERS
# =============================================================================

CLASSICAL_COMPOSERS = {
    'johann sebastian bach',
    'ludwig van beethoven',
    'wolfgang amadeus mozart',
    'franz schubert',
    'frederic chopin',
    'johannes brahms',
    'pyotr ilyich tchaikovsky',
    'claude debussy',
    'maurice ravel',
    'igor stravinsky',
    'sergei rachmaninoff',
    'franz liszt',
    'robert schumann',
    'felix mendelssohn',
    'antonio vivaldi',
    'george frideric handel',
    'joseph haydn',
    'haydn',
    'hector berlioz',
    'richard wagner',
    'gustav mahler',
    'dmitri shostakovich',
    'sergei prokofiev',
    'aaron copland',
    'leonard bernstein',
    'george gershwin',
    'erik satie',
    'camille saint-saens',
    'cesar franck',
    'gabriel faure',
    'edvard grieg',
    'jean sibelius',
    'antonin dvorak',
    'bedrich smetana',
    'leos janacek',
    'bela bartok',
    'zoltan kodaly',
    'ralph vaughan williams',
    'gustav holst',
    'benjamin britten',
    'edward elgar',
    'frederick delius',
    'william walton',
    'michael tippett',
    'harrison birtwistle',
    'peter maxwell davies',
    'olivier messiaen',
    'pierre boulez',
    'karlheinz stockhausen',
    'gyorgy ligeti',
    'luciano berio',
    'luigi nono',
    'iannis xenakis',
    'witold lutoslawski',
    'krzysztof penderecki',
    'henryk gorecki',
    'arvo part',
    'john tavener',
    'steve reich',
    'philip glass',
    'terry riley',
    'la monte young',
    'john adams',
    'michael nyman',
    'gavin bryars',
    'howard skempton',
    'morton feldman',
    'john cage',
    'earle brown',
    'christian wolff',
    'giacinto scelsi',
    'salvatore sciarrino',
    'helmut lachenmann',
    'brian ferneyhough',
    'michael finnissy',
    'richard barrett',
    'wolfgang rihm',
    'georges aperghis',
    'pascal dusapin',
    'philippe manoury',
    'tristan murail',
    'gerard grisey',
    'kaija saariaho',
    'magnus lindberg',
    'esa-pekka salonen',
    'thomas ades',
    'george benjamin',
    'julian anderson',
    'mark-anthony turnage',
    'judith weir',
    'sally beamish',
    'james macmillan',
    'john corigliano',
    'christopher rouse',
    'william bolcom',
    'david del tredici',
    'frederic rzewski',
    'louis andriessen',
    'martijn padding',
    'michel van der aa',
    'anna clyne',
    'caroline shaw',
    'nico muhly',
    'mason bates',
    'andrew norman',
    'john luther adams',
    'david lang',
    'julia wolfe',
    'michael gordon',
}

# =============================================================================
# SOUNDTRACK CLASSIFICATION - ORCHESTRA KEYWORDS
# =============================================================================

ORCHESTRA_KEYWORDS = {
    'orchestra',
    'symphony',
    'philharmonic',
    'ensemble',
    'quartet',
    'quintet',
    'chamber',
    'conservatory',
    'academy',
    'chorale',
    'choir',
    'concerto',
    'orchestral',
    'symphonic',
    'operatic',
    'ballet',
    'strings',
    'winds',
    'brass',
    'woodwinds',
    'percussion',
    'conducting',
    'conductor',
}
