# Spotify Analytics Dashboard

A comprehensive personal analytics project that processes Spotify streaming data to provide insights into listening habits, language patterns, and music preferences through an interactive dashboard.

🎧 **[Live Dashboard](https://danny-spotify-dashboard.streamlit.app/)**

## Project Overview

This project transforms raw Spotify streaming data into meaningful insights through automated data collection, processing, and visualization. The system processes extended streaming history, fetches additional metadata, detects song languages, and presents everything through a real-time dashboard that updates every 2 hours with recently played tracks.

## Key Features

- **Real-time Updates**: Automatically fetches recently played songs every 2 hours
- **Language Detection**: Advanced language classification for songs across multiple languages
- **Soundtrack Classification**: Identifies instrumental and soundtrack content
- **Interactive Dashboard**: Comprehensive analytics with filtering capabilities
- **Data Validation**: Automated error checking and data consistency validation

## Technology Stack

- **APIs**: Spotify Web API for track metadata, Genius API for lyrics retrieval
- **Language Detection**: Custom implementation using `langdetect` library with character detection for Hebrew/Japanese
- **Database**: MongoDB for data storage and management
- **Visualization**: Streamlit dashboard with Altair charts
- **Data Processing**: Python with pandas for data manipulation

## Architecture & Workflow

## Data Pipeline

### 1. Data Collection
- **Historical Data**: Processes Spotify's extended streaming history export
- **Real-time Data**: Fetches recently played tracks via Spotify API
- **Metadata Enhancement**: Retrieves detailed track and artist information

### 2. Language Detection System
Since Spotify API doesn't provide language information for songs, I developed a sophisticated detection system:
- **Lyrics Analysis**: Uses Genius API to fetch song lyrics for language detection
- **Character Detection**: Identifies Hebrew and Japanese text using Unicode patterns
- **Soundtrack Classification**: Detects instrumental/orchestral content using artist genres and composer databases
- **Priority System**: Soundtrack → Hebrew → Japanese → Lyrics → Song Title → Artist Name

### 3. Data Storage (MongoDB)
- **StreamingHistory**: Main listening data with enriched metadata
- **songs_master**: Unique songs with language and soundtrack classifications  
- **artists_master**: Unique artists with aggregated language information

### 4. Validation & Quality Control
- Cross-collection relationship validation
- Data consistency checks
- Automated error detection and reporting
- Soundtrack language consistency fixes

## Dashboard Features

- **Key Metrics**: Total listening hours, unique songs/artists/albums
- **Top Charts**: Most played songs, artists, albums by hours and play count
- **Temporal Analysis**: Listening patterns by day, month, year
- **Language Distribution**: Song language breakdown with evolution over time
- **Release Year Analysis**: Historical music preferences
- **Listening Heatmap**: Hour-by-hour and day-of-week activity patterns
- **Advanced Filtering**: Date range, artist, album, language, and year filters

## File Structure

### Data Collection & Processing
- `json_to_csv_claude4.py` - Converts Spotify export JSON to clean CSV format
- `recently_played2.py` - Fetches recent tracks from Spotify API
- `unique_songs_artists_mongodb.py` - Identifies and processes new songs/artists

### Language & Classification
- `lyrics_language_mongodb.py` - Fetches lyrics and detects languages
- `language_detection_script4.py` - Advanced language classification system

### Spotify API Integration
- `unique_songs_details.py` - Fetches detailed track metadata
- `unique_artists_details.py` - Retrieves artist information and genres

### Validation & Quality
- `validation_mongodb.py` - Comprehensive data validation and error checking

### Visualization
- `streamlit_app9.py` - Interactive dashboard application

## Requirements

- Spotify API credentials (Client ID, Client Secret, Access Token)
- Genius API token for lyrics retrieval
- MongoDB database connection
- Python environment with required dependencies

## Technical Highlights

- **Language Detection**: Overcame Spotify API limitations by building custom language detection using multiple data sources
- **Real-time Processing**: Automated pipeline that continuously updates with new listening data
- **Data Quality**: Robust validation system ensuring data consistency across collections
- **Scalable Architecture**: MongoDB-based design capable of handling large datasets
- **Interactive Analytics**: Dynamic dashboard with comprehensive filtering and visualization options

---

*This project demonstrates end-to-end data engineering skills including API integration, data processing, language detection, database design, and interactive visualization.*
