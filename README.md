# Spotify Analytics Dashboard

A comprehensive personal analytics project that processes 10 years of Spotify streaming data to uncover listening patterns, language preferences, and music trends through an interactive dashboard.

🎧 **[Live Dashboard](https://danny-spotify-dashboard.streamlit.app/)**

<img width="1908" height="895" alt="dashboardpicture" src="https://github.com/user-attachments/assets/fcf3dae2-85f7-4fdc-9674-f635cf94ba2a" />

## Project Overview

After accumulating over 10 years of listening history and more than 1,200 hours on Spotify, I was curious: what trends exist in my music preferences over the years? What languages do I listen to most? How have my tastes evolved?

This project is an end-to-end data engineering endeavor built over a few months, touching on database management, API integration, automated workflows, language detection, and interactive visualization. The goal was to learn a bit of everything while creating something personally meaningful.

## Key Insights from My Data

The analysis revealed some fascinating patterns:
- **Language Evolution**: English dominated my early years (2015-2017), Dutch took over during 2021-2023, and movie soundtracks became the primary listening choice from 2023-2025
- **Top Artist**: Queen reigns supreme, with "Don't Stop Me Now" being the most played song at over 170 plays
- **Nostalgia Factor**: Most songs are from 2010-2020, showing that nostalgia plays a significant role in music preferences

## How It Works

This project is split into two main parts:

### Part 1: Historical Data Processing
The journey began with Spotify's Extended Streaming History - a complete export of listening data spanning 10 years. This raw data needed cleaning, enrichment, and structure. The process involved:

Working through the extended streaming history data, cleaning it up to handle issues like missing values and character encoding problems (especially with Hebrew text), then enriching each track with additional metadata from the Spotify API. For language detection, lyrics were fetched from the Genius API and analyzed using the langdetect library. Since not all songs have lyrics, a fallback system was built: first attempting lyrics-based detection, then using song titles, and finally checking other songs by the same artist. Soundtracks were identified by looking for keywords like "orchestra" or "OST" in song metadata, or recognizing composer names like Hans Zimmer and John Williams. Once processed, all this data was organized into clear MongoDB schemas.

### Part 2: Real-Time Automation
After establishing the historical baseline, the next challenge was keeping the data current. A GitHub Actions workflow was set up to automatically fetch recently played songs every 2 hours. When new tracks are detected, the same enrichment pipeline runs: fetching Spotify metadata, retrieving lyrics when available, detecting languages, and updating the MongoDB database. This ensures the dashboard always reflects current listening habits without manual intervention.

## Technology Stack

- **APIs**: Spotify Web API for track metadata, Genius API for lyrics
- **Language Detection**: langdetect library with custom logic for multilingual content
- **Database**: MongoDB (chosen as a free alternative to cloud databases like AWS, with excellent Python connectivity)
- **Automation**: GitHub Actions for scheduled data collection
- **Visualization**: Streamlit (free hosting for accessible dashboard)
- **Data Processing**: Python with pandas

## Database Structure

The MongoDB database contains three main collections:

- **StreamingHistory**: Every song play with timestamp, duration, and enriched metadata
- **songs_master**: Unique songs with detected language and soundtrack classification
- **artists_master**: Unique artists with genre information and aggregated language data

## Language Detection System

Since Spotify's API doesn't provide language information for songs, a custom detection system was developed. The approach uses multiple data sources in priority order:

First, soundtracks are identified based on keywords (orchestra, OST) or known composer names. For remaining songs, the system attempts to fetch lyrics from Genius API and uses langdetect to identify the language. When lyrics aren't available, it falls back to analyzing the song title or checking language patterns from other songs by the same artist. This multi-layered approach handles the reality that not all songs have accessible lyrics, while still achieving reasonable language classification across the entire library.

## Dashboard

The Streamlit dashboard provides a comprehensive view of listening habits over time. It displays key metrics like total listening hours and unique songs, alongside top charts for most-played songs, artists, and albums. Temporal visualizations show listening patterns by day, month, and year, while language distribution charts reveal how music preferences evolved. The dashboard includes heatmaps for hour-by-hour activity patterns and release year analysis to understand the era of music being consumed. Multiple filters enable drilling down into specific time periods, artists, albums, languages, or release years to uncover personalized trends.

## Project Structure

### Core Pipeline
- `main_pipeline.py` - Main orchestration script for the automated pipeline

### Data Collection (`src/data_collection/`)
- `fetch_recent_tracks.py` - Fetches recent tracks from Spotify API
- `process_new_content.py` - Identifies and processes new songs/artists

### Data Enrichment (`src/enrichment/`)
- `enrich_with_lyrics.py` - Fetches lyrics and detects languages for new content
- `validate_data.py` - Data validation and error checking

### Dashboard (`src/dashboard/`)
- `dashboard.py` - Interactive Streamlit dashboard application

## What I Learned

Building this project provided hands-on experience with multiple aspects of data engineering:

- **API Integration**: Working with both Spotify and Genius APIs, handling rate limits, authentication, and parsing responses
- **Database Design**: Structuring MongoDB collections for efficient queries and maintaining data relationships
- **Automated Workflows**: Setting up GitHub Actions to run Python scripts on a schedule
- **Data Visualization**: Creating interactive dashboards with Streamlit and making data insights accessible
- **Multilingual Data Handling**: Dealing with character encoding issues and language detection for Hebrew, Dutch, English, and other languages
- **Real-World Data Challenges**: Handling missing data, inconsistent formats, and building fallback systems when primary data sources fail

The biggest challenge wasn't learning individual technologies - it was making them all work together smoothly. Coordinating API calls, database updates, language detection, and automated scheduling into a reliable pipeline required careful planning and plenty of debugging.

---

*This project demonstrates end-to-end data engineering skills, from initial data collection and cleaning through automated pipelines to interactive visualization.*
