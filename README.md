# Spotify Analytics Dashboard

A comprehensive personal analytics project that processes Spotify streaming data to provide insights into listening habits, language patterns, and music preferences through an interactive dashboard.

🎧 **[Live Dashboard](https://danny-spotify-dashboard.streamlit.app/)**

<img width="1908" height="895" alt="dashboardpicture" src="https://github.com/user-attachments/assets/fcf3dae2-85f7-4fdc-9674-f635cf94ba2a" />


## Project Overview

This project transforms raw Spotify streaming data into meaningful insights through automated data collection, processing, and visualization. The system started with historical Spotify extended streaming history data and evolved into a real-time analytics platform that automatically fetches and processes recently played tracks every 2 hours.

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

## Data Pipeline

### 1. Initial Setup
- **Historical Processing**: Uploaded and processed extended Spotify streaming history
- **Database Population**: Established MongoDB collections with comprehensive track and artist data

### 2. Real-time Automation
- **Continuous Updates**: Automated system fetches recently played tracks every 2 hours
- **New Content Detection**: Identifies and processes previously unseen songs and artists
- **Metadata Enhancement**: Retrieves detailed track and artist information via Spotify API

### 3. Language Detection System
Since Spotify API doesn't provide language information for songs, I developed a sophisticated detection system:
- **Lyrics Analysis**: Uses Genius API to fetch song lyrics for language detection
- **Character Detection**: Identifies Hebrew and Japanese text using Unicode patterns
- **Soundtrack Classification**: Detects instrumental/orchestral content using artist genres and composer databases
- **Priority System**: Soundtrack → Hebrew → Japanese → Lyrics → Song Title → Artist Name

### 4. Data Storage (MongoDB)
- **StreamingHistory**: Main listening data with enriched metadata
- **songs_master**: Unique songs with language and soundtrack classifications  
- **artists_master**: Unique artists with aggregated language information

### 5. Validation & Quality Control
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

## Project Structure

### Core Pipeline
- `main_pipeline.py` - Main orchestration script for the automated pipeline

### Data Collection (`src/data_collection/`)
- `fetch_recent_tracks.py` - Fetches recent tracks from Spotify API
- `process_new_content.py` - Identifies and processes new songs/artists

### Data Enrichment (`src/enrichment/`)
- `enrich_with_lyrics.py` - Fetches lyrics and detects languages for new content
- `validate_data.py` - Comprehensive data validation and error checking

### Dashboard (`src/dashboard/`)
- `dashboard.py` - Interactive Streamlit dashboard application

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

*This project demonstrates end-to-end data analysis skills including API integration, data processing, language detection, database design, and interactive visualization.*
