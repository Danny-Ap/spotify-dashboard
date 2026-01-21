import sys
from pathlib import Path

# Add project root to Python path for Streamlit Cloud compatibility
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timezone, timedelta
import pytz
import itertools

from src.utils.database import MongoDBConnection
from src.utils.config import (
    DATABASE_NAME,
    STREAMING_COLLECTION,
    SONGS_MASTER_COLLECTION,
    ARTISTS_MASTER_COLLECTION,
    STREAMING_FIELDS,
)

# =============================================================================
# CONFIGURATION
# =============================================================================
DB_NAME = DATABASE_NAME

# Field name mappings for StreamingHistory (using new schema)
TRACK_NAME = STREAMING_FIELDS['track_name']  # master_metadata_track_name
ARTIST_NAME = STREAMING_FIELDS['artist_name']  # master_metadata_album_artist_name
ALBUM_NAME = STREAMING_FIELDS['album_name']  # master_metadata_album_album_name
DURATION_HOURS = STREAMING_FIELDS['duration_hours']  # duration_hours
DURATION_MINUTES = STREAMING_FIELDS['duration_minutes']  # duration_minutes
DURATION_SECONDS = STREAMING_FIELDS['duration_seconds']  # duration_seconds
DAY_OF_WEEK = STREAMING_FIELDS['day_of_week']  # Day
MONTH = STREAMING_FIELDS['month']  # Month
YEAR = STREAMING_FIELDS['year']  # Year
DATE = STREAMING_FIELDS['date']  # Date

# Page configuration
st.set_page_config(
    page_title="Spotify Analytics Dashboard",
    page_icon="🎧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-container {
        border: 2px solid #1DB954;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        margin: 5px;
        background-color: white;
    }

    .metric-value {
        font-size: 48px !important;
        font-weight: bold !important;
        color: #1DB954;
    }

    .metric-label {
        font-size: 18px !important;
        font-weight: bold !important;
        color: #333;
        margin-bottom: 10px;
    }

    .last-song-container {
        border: 2px solid #1DB954;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        margin: 10px 0;
        background-color: #f8f9fa;
    }

    .last-song-text {
        font-size: 24px !important;
        font-weight: bold !important;
        color: #1DB954;
        margin: 0;
    }

    .connection-status {
        font-size: 12px;
        color: #666;
        text-align: center;
        padding: 5px;
    }

    .filter-section {
        margin-bottom: 15px;
        padding: 12px;
        border-radius: 8px;
        background-color: #f8f9fa;
    }

    .filter-section h5 {
        font-size: 14px !important;
        margin-bottom: 8px !important;
        color: #1DB954;
        font-weight: bold !important;
    }

    .stAlert > div {
        padding: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_connection():
    """Create and cache MongoDB connection."""
    import os

    # Try environment variable first (local), then Streamlit secrets (cloud)
    connection_string = os.getenv("MONGODB_CONNECTION_STRING")

    if not connection_string:
        try:
            connection_string = st.secrets["MONGODB_CONNECTION_STRING"]
        except:
            pass

    if not connection_string:
        return None, "MONGODB_CONNECTION_STRING not found in .env or secrets"

    try:
        db_conn = MongoDBConnection(connection_string)
        if db_conn.connect():
            return db_conn, "Connected to MongoDB Atlas"
        else:
            return None, "Connection failed"
    except Exception as e:
        return None, f"Connection failed: {str(e)}"


def get_next_update_time():
    """Calculate time until next 2-hour update interval."""
    brussels_tz = pytz.timezone('Europe/Brussels')
    now = datetime.now(brussels_tz)
    current_hour = now.hour
    next_update_hour = ((current_hour // 2) + 1) * 2
    if next_update_hour >= 24:
        next_update_hour = 0
        next_update_date = now.date() + timedelta(days=1)
    else:
        next_update_date = now.date()
    next_update = brussels_tz.localize(
        datetime.combine(next_update_date, datetime.min.time().replace(hour=next_update_hour))
    )
    time_diff = next_update - now
    total_seconds = int(time_diff.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return hours, minutes, seconds, next_update.strftime("%H:%M")


@st.cache_data(ttl=300)
def get_filter_options():
    """Get all unique values for dropdown filters."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return {}, None, None, status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Use new field names
        songs = list(collection.distinct(TRACK_NAME, {TRACK_NAME: {"$ne": None, "$ne": ""}}))
        artists = list(collection.distinct(ARTIST_NAME, {ARTIST_NAME: {"$ne": None, "$ne": ""}}))
        albums = list(collection.distinct(ALBUM_NAME, {ALBUM_NAME: {"$ne": None, "$ne": ""}}))
        years = list(collection.distinct(YEAR, {YEAR: {"$ne": None}}))

        # Get languages from songs_master (more accurate)
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)
        languages = list(songs_collection.distinct("language", {"language": {"$ne": None, "$ne": "Unknown"}}))

        date_pipeline = [
            {"$match": {DATE: {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": None,
                "min_date": {"$min": f"${DATE}"},
                "max_date": {"$max": f"${DATE}"}
            }}
        ]
        date_result = list(collection.aggregate(date_pipeline))
        min_date = date_result[0]["min_date"] if date_result else None
        max_date = date_result[0]["max_date"] if date_result else None

        if min_date and isinstance(min_date, str):
            # Try multiple date formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                try:
                    min_date = datetime.strptime(min_date, fmt).date()
                    break
                except ValueError:
                    continue
        if max_date and isinstance(max_date, str):
            # Try multiple date formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                try:
                    max_date = datetime.strptime(max_date, fmt).date()
                    break
                except ValueError:
                    continue

        return {
            "songs": sorted([s for s in songs if s]),
            "artists": sorted([a for a in artists if a]),
            "albums": sorted([al for al in albums if al]),
            "languages": sorted([l for l in languages if l]),
            "years": sorted([y for y in years if y])
        }, min_date, max_date, status

    except Exception as e:
        return {}, None, None, f"Error loading filter options: {str(e)}"


def apply_filters(base_pipeline, filters):
    """Apply filters to MongoDB aggregation pipeline."""
    if not filters:
        return base_pipeline

    match_conditions = {}

    if "songs" in filters and filters["songs"]:
        match_conditions[TRACK_NAME] = {"$in": filters["songs"]}
    if "artists" in filters and filters["artists"]:
        match_conditions[ARTIST_NAME] = {"$in": filters["artists"]}
    if "albums" in filters and filters["albums"]:
        match_conditions[ALBUM_NAME] = {"$in": filters["albums"]}
    if "years" in filters and filters["years"]:
        match_conditions[YEAR] = {"$in": filters["years"]}
    if "date_range" in filters and filters["date_range"]:
        start_date, end_date = filters["date_range"]
        # Use DD/MM/YYYY format to match stored data format
        match_conditions[DATE] = {"$gte": start_date.strftime("%d/%m/%Y"), "$lte": end_date.strftime("%d/%m/%Y")}

    if match_conditions:
        base_pipeline.insert(0, {"$match": match_conditions})

    return base_pipeline


@st.cache_data(ttl=300)
def get_last_song_played(filters=None):
    """Get the most recently played song."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return None, status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None, "$ne": ""},
                "ts_utc": {"$exists": True, "$ne": None}
            }},
            {"$sort": {"ts_utc": -1}},
            {"$limit": 1}
        ]

        if filters:
            pipeline = apply_filters(pipeline, filters)

        result = list(collection.aggregate(pipeline))

        if result:
            song = result[0]
            utc_time = song.get("ts_utc")
            if isinstance(utc_time, datetime):
                if utc_time.tzinfo is None:
                    utc_time = utc_time.replace(tzinfo=timezone.utc)
                local_tz = pytz.timezone('Europe/Brussels')
                local_time = utc_time.astimezone(local_tz)

                return {
                    "datetime": local_time,
                    "song_name": song.get(TRACK_NAME, "Unknown"),
                    "artist_name": song.get(ARTIST_NAME, "Unknown")
                }, status

        return None, status

    except Exception as e:
        return None, f"Error getting last song: {str(e)}"


@st.cache_data(ttl=300)
def get_kpi_metrics(filters=None):
    """Get KPI metrics: total hours, unique songs, artists, albums."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return None, status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Use new field name for hours
        total_hours_pipeline = [{"$group": {"_id": None, "total_hours": {"$sum": f"${DURATION_HOURS}"}}}]
        unique_songs_pipeline = [
            {"$match": {TRACK_NAME: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": f"${TRACK_NAME}"}},
            {"$count": "unique_songs"}
        ]
        unique_artists_pipeline = [
            {"$match": {ARTIST_NAME: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": f"${ARTIST_NAME}"}},
            {"$count": "unique_artists"}
        ]
        unique_albums_pipeline = [
            {"$match": {ALBUM_NAME: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": f"${ALBUM_NAME}"}},
            {"$count": "unique_albums"}
        ]

        if filters:
            total_hours_pipeline = apply_filters(total_hours_pipeline, filters)
            unique_songs_pipeline = apply_filters(unique_songs_pipeline, filters)
            unique_artists_pipeline = apply_filters(unique_artists_pipeline, filters)
            unique_albums_pipeline = apply_filters(unique_albums_pipeline, filters)

        total_hours_result = list(collection.aggregate(total_hours_pipeline, allowDiskUse=True))
        total_hours = total_hours_result[0]["total_hours"] if total_hours_result else 0

        unique_songs_result = list(collection.aggregate(unique_songs_pipeline, allowDiskUse=True))
        unique_songs = unique_songs_result[0]["unique_songs"] if unique_songs_result else 0

        unique_artists_result = list(collection.aggregate(unique_artists_pipeline, allowDiskUse=True))
        unique_artists = unique_artists_result[0]["unique_artists"] if unique_artists_result else 0

        unique_albums_result = list(collection.aggregate(unique_albums_pipeline, allowDiskUse=True))
        unique_albums = unique_albums_result[0]["unique_albums"] if unique_albums_result else 0

        return {
            "total_hours": total_hours or 0,
            "unique_songs": unique_songs,
            "unique_artists": unique_artists,
            "unique_albums": unique_albums
        }, status

    except Exception as e:
        return None, f"Error getting KPI metrics: {str(e)}"


@st.cache_data(ttl=300)
def get_top_data(data_type="songs", limit=20, filters=None):
    """Get top songs, artists, albums, or play counts by criteria."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        if data_type == "play_count":
            pipeline = [
                {"$match": {TRACK_NAME: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": {
                        "track_name": f"${TRACK_NAME}",
                        "artist_name": f"${ARTIST_NAME}"
                    },
                    "play_count": {"$sum": 1}
                }},
                {"$project": {
                    "_id": 0,
                    "track_name": "$_id.track_name",
                    "artist_name": "$_id.artist_name",
                    "name": {"$concat": ["$_id.track_name", " - ", "$_id.artist_name"]},
                    "count": "$play_count"
                }},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]
        else:
            field_map = {
                "songs": TRACK_NAME,
                "artists": ARTIST_NAME,
                "albums": ALBUM_NAME
            }

            field_name = field_map[data_type]

            pipeline = [
                {"$match": {field_name: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": f"${field_name}",
                    "total_hours": {"$sum": f"${DURATION_HOURS}"}
                }},
                {"$project": {
                    "_id": 0,
                    "name": "$_id",
                    "hours": "$total_hours"
                }},
                {"$sort": {"hours": -1}},
                {"$limit": limit}
            ]

        if filters:
            pipeline = apply_filters(pipeline, filters)

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty:
            df['display_name'] = df['name'].apply(lambda x: x[:40] + "..." if len(str(x)) > 40 else str(x))

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting top {data_type}: {str(e)}"


@st.cache_data(ttl=300)
def get_time_aggregation(time_type="Day", filters=None):
    """Get hours aggregated by Day, Month, or Year."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Use new field names
        field_map = {
            "Day": DAY_OF_WEEK,
            "Month": MONTH,
            "Year": YEAR
        }

        field_name = field_map.get(time_type, time_type)

        pipeline = [
            {"$match": {field_name: {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": f"${field_name}",
                "total_hours": {"$sum": f"${DURATION_HOURS}"}
            }},
            {"$project": {
                "_id": 0,
                "period": "$_id",
                "hours": "$total_hours"
            }}
        ]

        if filters:
            pipeline = apply_filters(pipeline, filters)

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty:
            if time_type == "Day":
                day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                df['period'] = pd.Categorical(df['period'], categories=day_order, ordered=True)
                df = df.sort_values('period').reset_index(drop=True)
            elif time_type == "Month":
                month_order = ["September", "October", "November", "December", "January", "February",
                              "March", "April", "May", "June", "July", "August"]
                df['period'] = pd.Categorical(df['period'], categories=month_order, ordered=True)
                df = df.sort_values('period').reset_index(drop=True)
            elif time_type == "Year":
                df = df.sort_values('period').reset_index(drop=True)

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting time aggregation: {str(e)}"


@st.cache_data(ttl=300)
def get_listening_heatmap_data(filters=None):
    """Get listening data for hour vs day of week heatmap."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Use ts field (MongoDB Date object) to extract hour
        pipeline = [
            {"$match": {
                "ts": {"$exists": True, "$ne": None},
                DAY_OF_WEEK: {"$exists": True, "$ne": None}
            }},
            {"$project": {
                "day_of_week": f"${DAY_OF_WEEK}",
                "hour": {"$hour": "$ts"},
                "h_played": f"${DURATION_HOURS}"
            }},
            {"$group": {
                "_id": {
                    "day": "$day_of_week",
                    "hour": "$hour"
                },
                "total_hours": {"$sum": "$h_played"}
            }},
            {"$project": {
                "_id": 0,
                "day": "$_id.day",
                "hour": "$_id.hour",
                "hours": "$total_hours"
            }}
        ]

        if filters:
            pipeline = apply_filters(pipeline, filters)

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty:
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            hours = list(range(24))
            all_combinations = list(itertools.product(days, hours))
            full_df = pd.DataFrame(all_combinations, columns=['day', 'hour'])
            df = full_df.merge(df, on=['day', 'hour'], how='left')
            df['hours'] = df['hours'].fillna(0)

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting heatmap data: {str(e)}"


@st.cache_data(ttl=300)
def get_language_evolution_data():
    """Get language listening evolution over time (monthly) for top 5 languages."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        # Get top languages from songs_master
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        top_languages_pipeline = [
            {"$match": {"language": {"$exists": True, "$ne": None, "$ne": "Unknown"}}},
            {"$group": {
                "_id": "$language",
                "song_count": {"$sum": 1}
            }},
            {"$sort": {"song_count": -1}},
            {"$limit": 5},
            {"$project": {"_id": 0, "language": "$_id"}}
        ]

        top_languages_result = list(songs_collection.aggregate(top_languages_pipeline, allowDiskUse=True))
        top_languages = [lang["language"] for lang in top_languages_result]

        if not top_languages:
            return pd.DataFrame(), status

        # Get streaming data and join with songs_master for language
        streaming_collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Use ts field (MongoDB Date) to extract year and month
        pipeline = [
            {"$match": {
                "ts": {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$lookup": {
                "from": SONGS_MASTER_COLLECTION,
                "localField": "spotify_track_uri",
                "foreignField": "spotify_track_uri",
                "as": "song_info"
            }},
            {"$unwind": "$song_info"},
            {"$match": {"song_info.language": {"$in": top_languages}}},
            {"$group": {
                "_id": {
                    "language": "$song_info.language",
                    "year": {"$year": "$ts"},
                    "month": {"$month": "$ts"}
                },
                "total_hours": {"$sum": f"${DURATION_HOURS}"}
            }},
            {"$project": {
                "_id": 0,
                "language": "$_id.language",
                "year": "$_id.year",
                "month": "$_id.month",
                "hours": "$total_hours"
            }},
            {"$sort": {"year": 1, "month": 1}}
        ]

        results = list(streaming_collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty:
            df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
            df = df.sort_values('date')

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting language evolution data: {str(e)}"


@st.cache_data(ttl=300)
def get_distribution_data(data_type="countries", filters=None):
    """Get data for pie charts (countries, languages by songs, languages by hours)."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        if data_type == "countries":
            collection = db_conn.get_collection(STREAMING_COLLECTION)
            pipeline = [
                {"$match": {"conn_country": {"$exists": True, "$ne": None, "$ne": ""}}},
                {"$group": {
                    "_id": "$conn_country",
                    "total_hours": {"$sum": f"${DURATION_HOURS}"}
                }},
                {"$project": {
                    "_id": 0,
                    "category": "$_id",
                    "value": "$total_hours"
                }},
                {"$sort": {"value": -1}}
            ]

            if filters:
                pipeline = apply_filters(pipeline, filters)

        elif data_type == "languages_songs":
            # Get from songs_master (more accurate)
            collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)
            pipeline = [
                {"$match": {"language": {"$exists": True, "$ne": None, "$ne": "Unknown"}}},
                {"$group": {
                    "_id": "$language",
                    "song_count": {"$sum": 1}
                }},
                {"$project": {
                    "_id": 0,
                    "category": "$_id",
                    "value": "$song_count"
                }},
                {"$sort": {"value": -1}}
            ]

        elif data_type == "languages_hours":
            # Join streaming with songs_master for language
            collection = db_conn.get_collection(STREAMING_COLLECTION)
            pipeline = [
                {"$match": {"spotify_track_uri": {"$exists": True, "$ne": None}}},
                {"$lookup": {
                    "from": SONGS_MASTER_COLLECTION,
                    "localField": "spotify_track_uri",
                    "foreignField": "spotify_track_uri",
                    "as": "song_info"
                }},
                {"$unwind": "$song_info"},
                {"$match": {"song_info.language": {"$exists": True, "$ne": None, "$ne": "Unknown"}}},
                {"$group": {
                    "_id": "$song_info.language",
                    "total_hours": {"$sum": f"${DURATION_HOURS}"}
                }},
                {"$project": {
                    "_id": 0,
                    "category": "$_id",
                    "value": "$total_hours"
                }},
                {"$sort": {"value": -1}}
            ]

            if filters:
                pipeline = apply_filters(pipeline, filters)

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty and len(df) > 5:
            top_5 = df.head(5)
            others_value = df.tail(len(df) - 5)['value'].sum()

            if others_value > 0:
                others_row = pd.DataFrame([{"category": "Others", "value": others_value}])
                df = pd.concat([top_5, others_row], ignore_index=True)
            else:
                df = top_5

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting {data_type} data: {str(e)}"


@st.cache_data(ttl=300)
def get_release_years_data():
    """Get count of unique songs by release year from songs_master collection."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {
                "release_date_year": {
                    "$exists": True,
                    "$ne": None,
                    "$type": "number",
                    "$gt": 0
                }
            }},
            {"$group": {
                "_id": "$release_date_year",
                "song_count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "year": "$_id",
                "count": "$song_count"
            }},
            {"$sort": {"year": 1}}
        ]

        results = list(songs_collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)

        if not df.empty:
            min_year = int(df['year'].min())
            max_year = int(df['year'].max())

            all_years = pd.DataFrame({'year': range(min_year, max_year + 1)})
            df = all_years.merge(df, on='year', how='left')
            df['count'] = df['count'].fillna(0)
            df['year'] = df['year'].astype(int)
            df['count'] = df['count'].astype(int)

        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting release years data: {str(e)}"


@st.cache_data(ttl=300)
def get_songs_by_year(selected_year):
    """Get songs released in a specific year."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {"release_date_year": selected_year}},
            {"$project": {
                "_id": 0,
                "song_name": 1,
                "artist_name": 1,
                "release_date": 1
            }},
            {"$sort": {"song_name": 1}}
        ]

        results = list(songs_collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting songs for year {selected_year}: {str(e)}"


@st.cache_data(ttl=300)
def get_song_popularity_data():
    """Get song popularity distribution from songs_master."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {
                "popularity": {"$exists": True, "$ne": None, "$type": "number"}
            }},
            {"$group": {
                "_id": "$popularity",
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "popularity": "$_id",
                "count": "$count"
            }},
            {"$sort": {"popularity": 1}}
        ]

        results = list(songs_collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)
        if not df.empty:
            df['popularity'] = df['popularity'].astype(int)
        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting song popularity data: {str(e)}"


@st.cache_data(ttl=300)
def get_songs_by_popularity(popularity_value):
    """Get songs with specific popularity value."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {"popularity": popularity_value}},
            {"$project": {
                "_id": 0,
                "song_name": 1,
                "artist_name": 1,
                "release_date": 1,
                "popularity": 1
            }},
            {"$sort": {"song_name": 1}}
        ]

        results = list(songs_collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting songs for popularity {popularity_value}: {str(e)}"


@st.cache_data(ttl=300)
def get_artist_popularity_data():
    """Get artist popularity distribution from artists_master."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        artists_collection = db_conn.get_collection(ARTISTS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {
                "popularity": {"$exists": True, "$ne": None, "$type": "number"}
            }},
            {"$group": {
                "_id": "$popularity",
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "popularity": "$_id",
                "count": "$count"
            }},
            {"$sort": {"popularity": 1}}
        ]

        results = list(artists_collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)
        if not df.empty:
            df['popularity'] = df['popularity'].astype(int)
        return df, status

    except Exception as e:
        return pd.DataFrame(), f"Error getting artist popularity data: {str(e)}"


@st.cache_data(ttl=300)
def get_artists_by_popularity(popularity_value):
    """Get artists with specific popularity value."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        artists_collection = db_conn.get_collection(ARTISTS_MASTER_COLLECTION)

        pipeline = [
            {"$match": {"popularity": popularity_value}},
            {"$project": {
                "_id": 0,
                "artist_name": 1,
                "popularity": 1,
                "followers": 1
            }},
            {"$sort": {"artist_name": 1}}
        ]

        results = list(artists_collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting artists for popularity {popularity_value}: {str(e)}"


# =============================================================================
# NEW FEATURES: Listening Streaks, Discovery Metrics, Soundtrack Analytics
# =============================================================================

@st.cache_data(ttl=300)
def get_listening_streaks_data():
    """Get listening streak and consistency metrics."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return None, status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Get all unique dates with plays
        pipeline = [
            {"$match": {DATE: {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": f"${DATE}",
                "total_hours": {"$sum": f"${DURATION_HOURS}"}
            }},
            {"$sort": {"_id": 1}}
        ]

        results = list(collection.aggregate(pipeline, allowDiskUse=True))

        if not results:
            return None, status

        # Convert to dates and calculate streaks
        dates_with_hours = [(r["_id"], r["total_hours"]) for r in results]

        # Parse dates
        parsed_dates = []
        for date_str, hours in dates_with_hours:
            try:
                if isinstance(date_str, str):
                    # Try multiple date formats
                    parsed_date = None
                    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    if parsed_date is None:
                        continue
                else:
                    parsed_date = date_str
                parsed_dates.append((parsed_date, hours))
            except:
                continue

        if not parsed_dates:
            return None, status

        parsed_dates.sort(key=lambda x: x[0])

        # Calculate longest streak
        longest_streak = 1
        current_streak = 1
        longest_streak_start = parsed_dates[0][0]
        longest_streak_end = parsed_dates[0][0]
        current_streak_start = parsed_dates[0][0]

        for i in range(1, len(parsed_dates)):
            prev_date = parsed_dates[i - 1][0]
            curr_date = parsed_dates[i][0]

            if (curr_date - prev_date).days == 1:
                current_streak += 1
                if current_streak > longest_streak:
                    longest_streak = current_streak
                    longest_streak_start = current_streak_start
                    longest_streak_end = curr_date
            else:
                current_streak = 1
                current_streak_start = curr_date

        # Calculate current streak (from most recent date backwards)
        today = datetime.now().date()
        current_active_streak = 0
        for i in range(len(parsed_dates) - 1, -1, -1):
            date = parsed_dates[i][0]
            expected_date = today - timedelta(days=(len(parsed_dates) - 1 - i))
            if i == len(parsed_dates) - 1:
                # Check if most recent play was today or yesterday
                days_since = (today - date).days
                if days_since <= 1:
                    current_active_streak = 1
                    for j in range(i - 1, -1, -1):
                        if (parsed_dates[j + 1][0] - parsed_dates[j][0]).days == 1:
                            current_active_streak += 1
                        else:
                            break
                break

        # Find most active day ever
        most_active_day = max(parsed_dates, key=lambda x: x[1])

        # Calculate average daily hours
        total_hours = sum(h for _, h in parsed_dates)
        total_days = len(parsed_dates)
        avg_daily_hours = total_hours / total_days if total_days > 0 else 0

        return {
            "longest_streak": longest_streak,
            "longest_streak_start": longest_streak_start,
            "longest_streak_end": longest_streak_end,
            "current_streak": current_active_streak,
            "most_active_date": most_active_day[0],
            "most_active_hours": most_active_day[1],
            "avg_daily_hours": avg_daily_hours,
            "total_listening_days": total_days
        }, status

    except Exception as e:
        return None, f"Error getting listening streaks: {str(e)}"


@st.cache_data(ttl=300)
def get_discovery_metrics():
    """Get discovery metrics: new songs, one-hit wonders, rediscovered songs."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return None, status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        # Get current month boundaries
        now = datetime.now()
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        first_of_month_str = first_of_month.strftime("%d/%m/%Y")  # Match stored date format

        # New songs this month: songs whose first play was this month
        new_songs_pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": "$spotify_track_uri",
                "song_name": {"$first": f"${TRACK_NAME}"},
                "artist_name": {"$first": f"${ARTIST_NAME}"},
                "first_play": {"$min": f"${DATE}"},
                "play_count": {"$sum": 1}
            }},
            {"$match": {"first_play": {"$gte": first_of_month_str}}},
            {"$count": "new_songs"}
        ]

        new_songs_result = list(collection.aggregate(new_songs_pipeline, allowDiskUse=True))
        new_songs_count = new_songs_result[0]["new_songs"] if new_songs_result else 0

        # One-hit wonders: songs played only once ever
        one_hit_pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": "$spotify_track_uri",
                "play_count": {"$sum": 1}
            }},
            {"$match": {"play_count": 1}},
            {"$count": "one_hit_wonders"}
        ]

        one_hit_result = list(collection.aggregate(one_hit_pipeline, allowDiskUse=True))
        one_hit_count = one_hit_result[0]["one_hit_wonders"] if one_hit_result else 0

        # Rediscovered: songs with >30 day gap between plays, played again this month
        rediscovered_pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$sort": {DATE: 1}},
            {"$group": {
                "_id": "$spotify_track_uri",
                "song_name": {"$first": f"${TRACK_NAME}"},
                "artist_name": {"$first": f"${ARTIST_NAME}"},
                "play_dates": {"$push": f"${DATE}"},
                "play_count": {"$sum": 1}
            }},
            {"$match": {"play_count": {"$gte": 2}}}
        ]

        rediscovered_results = list(collection.aggregate(rediscovered_pipeline, allowDiskUse=True))

        rediscovered_count = 0
        for song in rediscovered_results:
            dates = song.get("play_dates", [])
            if len(dates) < 2:
                continue

            # Check for gaps > 30 days and recent play
            has_long_gap = False
            has_recent_play = False

            for i in range(1, len(dates)):
                try:
                    # Parse dates with multiple formats
                    def parse_date(d):
                        if isinstance(d, str):
                            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                                try:
                                    return datetime.strptime(d, fmt)
                                except ValueError:
                                    continue
                            return None
                        return d

                    prev = parse_date(dates[i-1])
                    curr = parse_date(dates[i])
                    if prev is None or curr is None:
                        continue
                    if (curr - prev).days > 30:
                        has_long_gap = True
                    if isinstance(curr, datetime):
                        curr_date = curr
                    else:
                        curr_date = datetime.combine(curr, datetime.min.time())
                    if curr_date >= first_of_month:
                        has_recent_play = True
                except:
                    continue

            if has_long_gap and has_recent_play:
                rediscovered_count += 1

        # Get total unique songs for context
        total_unique_pipeline = [
            {"$match": {"spotify_track_uri": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$spotify_track_uri"}},
            {"$count": "total"}
        ]
        total_result = list(collection.aggregate(total_unique_pipeline, allowDiskUse=True))
        total_unique = total_result[0]["total"] if total_result else 0

        return {
            "new_songs_this_month": new_songs_count,
            "one_hit_wonders": one_hit_count,
            "rediscovered": rediscovered_count,
            "total_unique_songs": total_unique,
            "month_name": now.strftime("%B")
        }, status

    except Exception as e:
        return None, f"Error getting discovery metrics: {str(e)}"


@st.cache_data(ttl=300)
def get_recently_discovered_songs(limit=10):
    """Get list of recently discovered songs (first played this month)."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        now = datetime.now()
        first_of_month = now.replace(day=1)
        first_of_month_str = first_of_month.strftime("%d/%m/%Y")  # Match stored date format

        pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": "$spotify_track_uri",
                "song_name": {"$first": f"${TRACK_NAME}"},
                "artist_name": {"$first": f"${ARTIST_NAME}"},
                "first_play": {"$min": f"${DATE}"},
                "play_count": {"$sum": 1}
            }},
            {"$match": {"first_play": {"$gte": first_of_month_str}}},
            {"$sort": {"first_play": -1}},
            {"$limit": limit},
            {"$project": {
                "_id": 0,
                "song_name": 1,
                "artist_name": 1,
                "first_play": 1,
                "play_count": 1
            }}
        ]

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting discovered songs: {str(e)}"


@st.cache_data(ttl=300)
def get_one_hit_wonders_list(limit=10):
    """Get list of one-hit wonder songs."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        collection = db_conn.get_collection(STREAMING_COLLECTION)

        pipeline = [
            {"$match": {
                TRACK_NAME: {"$exists": True, "$ne": None},
                "spotify_track_uri": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": "$spotify_track_uri",
                "song_name": {"$first": f"${TRACK_NAME}"},
                "artist_name": {"$first": f"${ARTIST_NAME}"},
                "play_date": {"$first": f"${DATE}"},
                "play_count": {"$sum": 1}
            }},
            {"$match": {"play_count": 1}},
            {"$sort": {"play_date": -1}},
            {"$limit": limit},
            {"$project": {
                "_id": 0,
                "song_name": 1,
                "artist_name": 1,
                "play_date": 1
            }}
        ]

        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting one-hit wonders: {str(e)}"


@st.cache_data(ttl=300)
def get_soundtrack_analytics():
    """Get soundtrack listening analytics using songs_master is_soundtrack field."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return None, status

    try:
        streaming_collection = db_conn.get_collection(STREAMING_COLLECTION)
        songs_collection = db_conn.get_collection(SONGS_MASTER_COLLECTION)

        # Get soundtrack vs regular music hours via $lookup
        comparison_pipeline = [
            {"$match": {"spotify_track_uri": {"$exists": True, "$ne": None}}},
            {"$lookup": {
                "from": SONGS_MASTER_COLLECTION,
                "localField": "spotify_track_uri",
                "foreignField": "spotify_track_uri",
                "as": "song_info"
            }},
            {"$unwind": "$song_info"},
            {"$group": {
                "_id": "$song_info.is_soundtrack",
                "total_hours": {"$sum": f"${DURATION_HOURS}"},
                "play_count": {"$sum": 1}
            }}
        ]

        comparison_results = list(streaming_collection.aggregate(comparison_pipeline, allowDiskUse=True))

        soundtrack_hours = 0
        regular_hours = 0
        soundtrack_plays = 0
        regular_plays = 0

        for result in comparison_results:
            if result["_id"] is True:
                soundtrack_hours = result["total_hours"]
                soundtrack_plays = result["play_count"]
            else:
                regular_hours = result["total_hours"]
                regular_plays = result["play_count"]

        # Get soundtrack song count from songs_master
        soundtrack_songs = songs_collection.count_documents({"is_soundtrack": True})
        regular_songs = songs_collection.count_documents({"is_soundtrack": {"$ne": True}})

        return {
            "soundtrack_hours": soundtrack_hours,
            "regular_hours": regular_hours,
            "soundtrack_plays": soundtrack_plays,
            "regular_plays": regular_plays,
            "soundtrack_songs": soundtrack_songs,
            "regular_songs": regular_songs,
            "total_hours": soundtrack_hours + regular_hours
        }, status

    except Exception as e:
        return None, f"Error getting soundtrack analytics: {str(e)}"


@st.cache_data(ttl=300)
def get_top_soundtrack_artists(limit=10):
    """Get top soundtrack artists/composers by hours listened."""
    db_conn, status = get_db_connection()

    if db_conn is None:
        return pd.DataFrame(), status

    try:
        streaming_collection = db_conn.get_collection(STREAMING_COLLECTION)

        pipeline = [
            {"$match": {"spotify_track_uri": {"$exists": True, "$ne": None}}},
            {"$lookup": {
                "from": SONGS_MASTER_COLLECTION,
                "localField": "spotify_track_uri",
                "foreignField": "spotify_track_uri",
                "as": "song_info"
            }},
            {"$unwind": "$song_info"},
            {"$match": {"song_info.is_soundtrack": True}},
            {"$group": {
                "_id": f"${ARTIST_NAME}",
                "total_hours": {"$sum": f"${DURATION_HOURS}"},
                "song_count": {"$addToSet": "$spotify_track_uri"}
            }},
            {"$project": {
                "_id": 0,
                "artist_name": "$_id",
                "hours": "$total_hours",
                "unique_songs": {"$size": "$song_count"}
            }},
            {"$sort": {"hours": -1}},
            {"$limit": limit}
        ]

        results = list(streaming_collection.aggregate(pipeline, allowDiskUse=True))
        return pd.DataFrame(results), status

    except Exception as e:
        return pd.DataFrame(), f"Error getting top soundtrack artists: {str(e)}"


def create_horizontal_bar_chart(df, title, value_col="hours", height=600):
    """Create a horizontal bar chart."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=400, height=300)

    if value_col == "count":
        format_str = ",.0f"
        value_title = "Play Count"
    else:
        format_str = ",.2f"
        value_title = "Hours"

    chart = alt.Chart(df).mark_bar(
        color='#1DB954',
        opacity=0.8
    ).encode(
        x=alt.X(f'{value_col}:Q',
                title=value_title,
                axis=alt.Axis(format=format_str)),
        y=alt.Y('display_name:N',
                sort=alt.SortField(field=value_col, order='descending'),
                title=None,
                axis=alt.Axis(labelLimit=300)),
        tooltip=[
            alt.Tooltip('name:N', title=title),
            alt.Tooltip(f'{value_col}:Q', title=value_title, format=format_str)
        ]
    ).properties(
        width=500,
        height=height,
        title=alt.TitleParams(
            text=f"Top 20 {title}",
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def create_time_chart(df, time_type):
    """Create vertical bar chart for time aggregation with proper ordering."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=700, height=400)

    if time_type == "Day":
        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    elif time_type == "Month":
        order = ["September", "October", "November", "December", "January", "February",
                "March", "April", "May", "June", "July", "August"]
    else:
        order = None

    chart = alt.Chart(df).mark_bar(
        color='#1DB954',
        opacity=0.8
    ).encode(
        x=alt.X('period:N' if time_type != "Year" else 'period:O',
                title=time_type,
                sort=order if order else None,
                axis=alt.Axis(labelAngle=-45 if time_type == "Month" else 0)),
        y=alt.Y('hours:Q',
                title='Hours Listened',
                axis=alt.Axis(format='~s')),
        tooltip=[
            alt.Tooltip('period:N' if time_type != "Year" else 'period:O', title=time_type),
            alt.Tooltip('hours:Q', title='Hours', format=',.2f')
        ]
    ).properties(
        width=700,
        height=400,
        title=alt.TitleParams(
            text=f"Listening Hours by {time_type}",
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def create_heatmap_chart(df):
    """Create heatmap for hour vs day of week listening patterns."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=700, height=400)

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    chart = alt.Chart(df).mark_rect().encode(
        x=alt.X('hour:O', title='Hour of Day'),
        y=alt.Y('day:N', title='Day of Week', sort=day_order),
        color=alt.Color('hours:Q',
                       title='Hours Listened',
                       scale=alt.Scale(scheme='greens')),
        tooltip=[
            alt.Tooltip('day:N', title='Day'),
            alt.Tooltip('hour:O', title='Hour'),
            alt.Tooltip('hours:Q', title='Hours', format=',.2f')
        ]
    ).properties(
        width=600,
        height=500,
        title=alt.TitleParams(
            text="Listening Intensity by Hour and Day",
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def create_language_evolution_chart(df):
    """Create line chart for language evolution over time."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=700, height=400)

    chart = alt.Chart(df).mark_line(
        point=True,
        strokeWidth=2
    ).encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('hours:Q', title='Hours Listened'),
        color=alt.Color('language:N',
                       title='Language',
                       scale=alt.Scale(scheme='category10')),
        tooltip=[
            alt.Tooltip('date:T', title='Date', format='%Y-%m'),
            alt.Tooltip('language:N', title='Language'),
            alt.Tooltip('hours:Q', title='Hours', format=',.2f')
        ]
    ).properties(
        width=700,
        height=400,
        title=alt.TitleParams(
            text="Language Listening Evolution Over Time",
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def create_pie_chart(df, title):
    """Create a pie chart with largest slice starting at 0 degrees (top)."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=300, height=300)

    df_sorted = df.sort_values('value', ascending=False).reset_index(drop=True)

    chart = alt.Chart(df_sorted).mark_arc(
        innerRadius=50,
        outerRadius=120,
        stroke="#fff",
        strokeWidth=2
    ).encode(
        theta=alt.Theta('value:Q',
                       sort=alt.SortField(field='value', order='descending')),
        color=alt.Color(
            'category:N',
            scale=alt.Scale(scheme='category10'),
            sort=alt.SortField(field='value', order='descending'),
            legend=alt.Legend(
                orient="right",
                titleFontSize=12,
                labelFontSize=10,
                symbolSize=100
            )
        ),
        order=alt.Order('value:Q', sort='descending'),
        tooltip=[
            alt.Tooltip('category:N', title='Category'),
            alt.Tooltip('value:Q', title='Value', format=',.0f')
        ]
    ).properties(
        width=250,
        height=300,
        title=alt.TitleParams(
            text=title,
            fontSize=14,
            anchor='start'
        )
    )

    return chart


def create_release_years_chart(df):
    """Create vertical bar chart for release years."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=700, height=400)

    min_year = int(df['year'].min())
    max_year = int(df['year'].max())

    start_decade = (min_year // 10) * 10
    end_decade = ((max_year // 10) + 1) * 10

    decade_labels = list(range(start_decade, end_decade + 1, 10))

    chart = alt.Chart(df).mark_bar(
        color='#1DB954',
        opacity=0.8,
        size=8,
        stroke='white',
        strokeWidth=0.5
    ).encode(
        x=alt.X('year:O',
                title='Release Year',
                axis=alt.Axis(
                    values=decade_labels,
                    labelAngle=0
                ),
                scale=alt.Scale(paddingInner=0.1)),
        y=alt.Y('count:Q',
                title='Number of Songs',
                axis=alt.Axis(format='~s')),
        tooltip=[
            alt.Tooltip('year:O', title='Year'),
            alt.Tooltip('count:Q', title='Songs', format=',.0f')
        ]
    ).properties(
        width=500,
        height=400,
        title=alt.TitleParams(
            text="Songs by Release Year",
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def create_popularity_distribution_chart(df, title, chart_type="songs"):
    """Create vertical bar chart for popularity distribution."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=700, height=400)

    chart = alt.Chart(df).mark_bar(
        color='#1DB954',
        opacity=0.8,
        size=5
    ).encode(
        x=alt.X('popularity:O',
                title='Popularity Score',
                axis=alt.Axis(labelAngle=0)),
        y=alt.Y('count:Q',
                title=f'Number of {chart_type.capitalize()}',
                axis=alt.Axis(format='~s')),
        tooltip=[
            alt.Tooltip('popularity:O', title='Popularity'),
            alt.Tooltip('count:Q', title='Count', format=',.0f')
        ]
    ).properties(
        width=500,
        height=400,
        title=alt.TitleParams(
            text=title,
            fontSize=16,
            anchor='start'
        )
    )

    return chart


def main():
    """Main Streamlit app."""

    # Initialize session state for sidebar
    if 'sidebar_open' not in st.session_state:
        st.session_state.sidebar_open = False

    # Get filter options and connection status
    filter_options, min_date, max_date, connection_status = get_filter_options()

    # Connection status
    st.markdown(f'<div class="connection-status">{connection_status}</div>', unsafe_allow_html=True)

    # Sidebar toggle button
    if st.button("Filters" if not st.session_state.sidebar_open else "Close", key="sidebar_toggle"):
        st.session_state.sidebar_open = not st.session_state.sidebar_open

    # App header
    st.title("Spotify Analytics Dashboard")
    st.caption(f"Data from MongoDB: `{DB_NAME}` | Automated via GitHub Actions")

    # Check connection
    if "failed" in connection_status.lower() or "not found" in connection_status.lower():
        st.error("Database Connection Failed")
        st.error(connection_status)
        st.info("Troubleshooting:")
        st.info("1. Check if MongoDB connection string is set in Streamlit secrets")
        st.info("2. Verify your MongoDB Atlas cluster is running")
        st.info("3. Check network connectivity")
        st.stop()

    # Sidebar for filters
    current_filters = {}
    if st.session_state.sidebar_open:
        with st.sidebar:
            # Next update timer (above title)
            hours, minutes, seconds, next_time = get_next_update_time()
            st.info(f"Next update in: {hours}h {minutes}m {seconds}s (at {next_time} Brussels time)")

            st.title("Filters")

            # Reset filters button
            if st.button("Reset Filters", type="primary", width="stretch"):
                st.rerun()

            st.markdown("---")

            # Date range filter
            if min_date and max_date:
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Date Range")
                date_range = st.slider(
                    "Select date range:",
                    min_value=min_date,
                    max_value=max_date,
                    value=(min_date, max_date),
                    format="YYYY-MM-DD"
                )
                if date_range != (min_date, max_date):
                    current_filters["date_range"] = date_range
                st.markdown('</div>', unsafe_allow_html=True)

            # Year filter
            if filter_options.get("years"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Years")
                selected_years = st.multiselect(
                    "Select years:",
                    options=filter_options.get("years", []),
                    default=[]
                )
                if selected_years:
                    current_filters["years"] = selected_years
                st.markdown('</div>', unsafe_allow_html=True)

            # Song filter
            if filter_options.get("songs"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Songs")
                selected_songs = st.multiselect(
                    "Select songs:",
                    options=filter_options.get("songs", []),
                    default=[]
                )
                if selected_songs:
                    current_filters["songs"] = selected_songs
                st.markdown('</div>', unsafe_allow_html=True)

            # Artist filter
            if filter_options.get("artists"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Artists")
                selected_artists = st.multiselect(
                    "Select artists:",
                    options=filter_options.get("artists", []),
                    default=[]
                )
                if selected_artists:
                    current_filters["artists"] = selected_artists
                st.markdown('</div>', unsafe_allow_html=True)

            # Album filter
            if filter_options.get("albums"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Albums")
                selected_albums = st.multiselect(
                    "Select albums:",
                    options=filter_options.get("albums", []),
                    default=[]
                )
                if selected_albums:
                    current_filters["albums"] = selected_albums
                st.markdown('</div>', unsafe_allow_html=True)

            # Language filter
            if filter_options.get("languages"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### Languages")
                selected_languages = st.multiselect(
                    "Select languages:",
                    options=filter_options.get("languages", []),
                    default=[]
                )
                if selected_languages:
                    current_filters["languages"] = selected_languages
                st.markdown('</div>', unsafe_allow_html=True)

    # Get KPI data
    kpi_data, kpi_status = get_kpi_metrics(current_filters if current_filters else None)

    if kpi_data is None:
        st.error(kpi_status)
        st.stop()

    # Last song played section
    last_song_data, _ = get_last_song_played(current_filters if current_filters else None)

    if last_song_data:
        st.markdown(f'''
        <div class="last-song-container">
            <div class="last-song-text">
                Last Played: <strong>{last_song_data["song_name"]}</strong> by <strong>{last_song_data["artist_name"]}</strong>
                | {last_song_data["datetime"].strftime("%Y-%m-%d at %H:%M")}
            </div>
        </div>
        ''', unsafe_allow_html=True)

    # KPI Metrics
    st.markdown("### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f'''
        <div class="metric-container">
            <div class="metric-label">Total Hours</div>
            <div class="metric-value">{kpi_data["total_hours"]:,.1f}</div>
        </div>
        ''', unsafe_allow_html=True)

    with col2:
        st.markdown(f'''
        <div class="metric-container">
            <div class="metric-label">Unique Songs</div>
            <div class="metric-value">{kpi_data["unique_songs"]:,}</div>
        </div>
        ''', unsafe_allow_html=True)

    with col3:
        st.markdown(f'''
        <div class="metric-container">
            <div class="metric-label">Unique Artists</div>
            <div class="metric-value">{kpi_data["unique_artists"]:,}</div>
        </div>
        ''', unsafe_allow_html=True)

    with col4:
        st.markdown(f'''
        <div class="metric-container">
            <div class="metric-label">Unique Albums</div>
            <div class="metric-value">{kpi_data["unique_albums"]:,}</div>
        </div>
        ''', unsafe_allow_html=True)

    st.divider()

    # Top charts with inline radio buttons
    col_title, col_radio = st.columns([3, 1])
    with col_title:
        st.subheader("Top 20 Most Played")
    with col_radio:
        top_data_type = st.radio(
            "View by:",
            ["Songs", "Artists", "Albums", "Play Count"],
            horizontal=True,
            key="top_data_radio"
        )

    data_type_map = {
        "Songs": "songs",
        "Artists": "artists",
        "Albums": "albums",
        "Play Count": "play_count"
    }

    selected_data_type = data_type_map[top_data_type]
    top_df, top_status = get_top_data(
        selected_data_type,
        limit=20,
        filters=current_filters if current_filters else None
    )

    if not top_df.empty:
        value_col = "count" if selected_data_type == "play_count" else "hours"
        top_chart = create_horizontal_bar_chart(top_df, top_data_type, value_col)
        st.altair_chart(top_chart, width="stretch")
    else:
        st.info(f"No {top_data_type.lower()} data available with current filters")

    st.divider()

    # Time patterns with inline radio buttons
    col_title, col_radio = st.columns([3, 1])
    with col_title:
        st.subheader("Listening Patterns")
    with col_radio:
        time_type = st.radio(
            "View by:",
            ["Day", "Month", "Year"],
            horizontal=True,
            key="time_pattern_radio"
        )

    time_df, time_status = get_time_aggregation(time_type, current_filters if current_filters else None)

    if not time_df.empty:
        time_chart = create_time_chart(time_df, time_type)
        st.altair_chart(time_chart, width="stretch")
    else:
        st.info(f"No {time_type.lower()} data available with current filters")

    st.divider()

    # Pie chart (1/3) + Heatmap (2/3)
    col_dist, col_heatmap = st.columns([1, 2])

    with col_dist:
        st.subheader("Distribution Analysis")

        dist_type = st.radio(
            "View:",
            ["Countries", "Languages (Songs)", "Languages (Hours)"],
            key="distribution_radio"
        )

        dist_data_map = {
            "Countries": "countries",
            "Languages (Songs)": "languages_songs",
            "Languages (Hours)": "languages_hours"
        }

        selected_dist_type = dist_data_map[dist_type]
        dist_df, _ = get_distribution_data(selected_dist_type, current_filters if current_filters else None)

        if not dist_df.empty:
            dist_chart = create_pie_chart(dist_df, dist_type)
            st.altair_chart(dist_chart, width="stretch")
        else:
            st.info(f"No {dist_type.lower()} data available")

    with col_heatmap:
        st.subheader("Listening Intensity Heatmap")

        heatmap_df, _ = get_listening_heatmap_data(current_filters if current_filters else None)

        if not heatmap_df.empty:
            heatmap_chart = create_heatmap_chart(heatmap_df)
            st.altair_chart(heatmap_chart, width="stretch")
        else:
            st.info("No heatmap data available")

    st.divider()

    # Release Years + Song Popularity + Artist Popularity with radio buttons
    col_title, col_radio = st.columns([3, 1])
    with col_title:
        st.subheader("Music Catalog Analytics")
    with col_radio:
        catalog_type = st.radio(
            "View:",
            ["Release Years", "Song Popularity", "Artist Popularity"],
            horizontal=True,
            key="catalog_radio"
        )

    if catalog_type == "Release Years":
        release_years_df, _ = get_release_years_data()

        if not release_years_df.empty:
            col_chart, col_dropdown = st.columns([3, 1])

            with col_chart:
                release_years_chart = create_release_years_chart(release_years_df)
                st.altair_chart(release_years_chart, width="stretch")

            with col_dropdown:
                available_years = sorted(release_years_df['year'].tolist(), reverse=True)
                selected_year = st.selectbox(
                    "Select Year:",
                    options=available_years,
                    index=0,
                    key="year_selector"
                )

                year_count = release_years_df[release_years_df['year'] == selected_year]['count'].iloc[0]
                st.metric("Songs", f"{year_count:,}")

            if selected_year:
                with st.expander(f"Songs from {selected_year} ({year_count:,} songs)", expanded=True):
                    songs_df, _ = get_songs_by_year(selected_year)

                    if not songs_df.empty:
                        st.dataframe(
                            songs_df,
                            column_config={
                                "song_name": st.column_config.TextColumn("Song", width="medium"),
                                "artist_name": st.column_config.TextColumn("Artist", width="medium"),
                                "release_date": st.column_config.TextColumn("Release Date", width="small")
                            },
                            hide_index=True,
                            width="stretch",
                            height=300
                        )
                    else:
                        st.info("No songs found for this year")
        else:
            st.info("No release year data available")

    elif catalog_type == "Song Popularity":
        song_pop_df, _ = get_song_popularity_data()

        if not song_pop_df.empty:
            col_chart, col_dropdown = st.columns([3, 1])

            with col_chart:
                song_pop_chart = create_popularity_distribution_chart(
                    song_pop_df,
                    "Songs by Popularity Score",
                    "songs"
                )
                st.altair_chart(song_pop_chart, width="stretch")

            with col_dropdown:
                available_popularities = sorted(song_pop_df['popularity'].tolist(), reverse=True)
                selected_popularity = st.selectbox(
                    "Select Popularity:",
                    options=available_popularities,
                    index=0,
                    key="song_popularity_selector"
                )

                pop_count = song_pop_df[song_pop_df['popularity'] == selected_popularity]['count'].iloc[0]
                st.metric("Songs", f"{pop_count:,}")

            if selected_popularity is not None:
                with st.expander(f"Songs with Popularity {selected_popularity} ({pop_count:,} songs)", expanded=True):
                    songs_df, _ = get_songs_by_popularity(selected_popularity)

                    if not songs_df.empty:
                        st.dataframe(
                            songs_df,
                            column_config={
                                "song_name": st.column_config.TextColumn("Song", width="medium"),
                                "artist_name": st.column_config.TextColumn("Artist", width="medium"),
                                "release_date": st.column_config.TextColumn("Release Date", width="small"),
                                "popularity": st.column_config.NumberColumn("Popularity", width="small")
                            },
                            hide_index=True,
                            width="stretch",
                            height=300
                        )
                    else:
                        st.info("No songs found for this popularity level")
        else:
            st.info("No song popularity data available")

    elif catalog_type == "Artist Popularity":
        artist_pop_df, _ = get_artist_popularity_data()

        if not artist_pop_df.empty:
            col_chart, col_dropdown = st.columns([3, 1])

            with col_chart:
                artist_pop_chart = create_popularity_distribution_chart(
                    artist_pop_df,
                    "Artists by Popularity Score",
                    "artists"
                )
                st.altair_chart(artist_pop_chart, width="stretch")

            with col_dropdown:
                available_popularities = sorted(artist_pop_df['popularity'].tolist(), reverse=True)
                selected_popularity = st.selectbox(
                    "Select Popularity:",
                    options=available_popularities,
                    index=0,
                    key="artist_popularity_selector"
                )

                pop_count = artist_pop_df[artist_pop_df['popularity'] == selected_popularity]['count'].iloc[0]
                st.metric("Artists", f"{pop_count:,}")

            if selected_popularity is not None:
                with st.expander(f"Artists with Popularity {selected_popularity} ({pop_count:,} artists)", expanded=True):
                    artists_df, _ = get_artists_by_popularity(selected_popularity)

                    if not artists_df.empty:
                        st.dataframe(
                            artists_df,
                            column_config={
                                "artist_name": st.column_config.TextColumn("Artist", width="medium"),
                                "popularity": st.column_config.NumberColumn("Popularity", width="small"),
                                "followers": st.column_config.NumberColumn("Followers", width="medium")
                            },
                            hide_index=True,
                            width="stretch",
                            height=300
                        )
                    else:
                        st.info("No artists found for this popularity level")
        else:
            st.info("No artist popularity data available")

    st.divider()

    # Language Evolution Over Time
    st.subheader("Language Evolution Over Time")

    lang_evolution_df, _ = get_language_evolution_data()

    if not lang_evolution_df.empty:
        lang_evolution_chart = create_language_evolution_chart(lang_evolution_df)
        st.altair_chart(lang_evolution_chart, width="stretch")
    else:
        st.info("No language evolution data available")

    st.divider()

    # ==========================================================================
    # NEW SECTION: Listening Streaks & Consistency
    # ==========================================================================
    st.subheader("Listening Streaks & Consistency")

    streaks_data, _ = get_listening_streaks_data()

    if streaks_data:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Longest Streak",
                f"{streaks_data['longest_streak']} days",
                help=f"From {streaks_data['longest_streak_start']} to {streaks_data['longest_streak_end']}"
            )

        with col2:
            st.metric(
                "Current Streak",
                f"{streaks_data['current_streak']} days",
                help="Consecutive days including today/yesterday"
            )

        with col3:
            st.metric(
                "Avg Daily Hours",
                f"{streaks_data['avg_daily_hours']:.1f}h",
                help=f"Average across {streaks_data['total_listening_days']} listening days"
            )

        with col4:
            st.metric(
                "Most Active Day",
                f"{streaks_data['most_active_hours']:.1f}h",
                help=f"On {streaks_data['most_active_date']}"
            )
    else:
        st.info("No listening streak data available")

    st.divider()

    # ==========================================================================
    # NEW SECTION: Discovery Metrics
    # ==========================================================================
    st.subheader("Discovery Metrics")

    discovery_data, _ = get_discovery_metrics()

    if discovery_data:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                f"New Songs ({discovery_data['month_name']})",
                f"{discovery_data['new_songs_this_month']:,}",
                help="Songs you played for the first time this month"
            )

        with col2:
            one_hit_pct = (discovery_data['one_hit_wonders'] / discovery_data['total_unique_songs'] * 100) if discovery_data['total_unique_songs'] > 0 else 0
            st.metric(
                "One-Hit Wonders",
                f"{discovery_data['one_hit_wonders']:,}",
                help=f"{one_hit_pct:.1f}% of your library - songs played only once"
            )

        with col3:
            st.metric(
                "Rediscovered",
                f"{discovery_data['rediscovered']:,}",
                help="Songs you returned to after 30+ days, played again this month"
            )

        # Expandable sections for details
        col_new, col_one_hit = st.columns(2)

        with col_new:
            with st.expander(f"Recently Discovered Songs", expanded=False):
                new_songs_df, _ = get_recently_discovered_songs(limit=15)
                if not new_songs_df.empty:
                    st.dataframe(
                        new_songs_df,
                        column_config={
                            "song_name": st.column_config.TextColumn("Song", width="medium"),
                            "artist_name": st.column_config.TextColumn("Artist", width="medium"),
                            "first_play": st.column_config.TextColumn("First Play", width="small"),
                            "play_count": st.column_config.NumberColumn("Plays", width="small")
                        },
                        hide_index=True,
                        width="stretch",
                        height=300
                    )
                else:
                    st.info("No new songs this month")

        with col_one_hit:
            with st.expander(f"One-Hit Wonders (Recent)", expanded=False):
                one_hit_df, _ = get_one_hit_wonders_list(limit=15)
                if not one_hit_df.empty:
                    st.dataframe(
                        one_hit_df,
                        column_config={
                            "song_name": st.column_config.TextColumn("Song", width="medium"),
                            "artist_name": st.column_config.TextColumn("Artist", width="medium"),
                            "play_date": st.column_config.TextColumn("Played On", width="small")
                        },
                        hide_index=True,
                        width="stretch",
                        height=300
                    )
                else:
                    st.info("No one-hit wonders found")
    else:
        st.info("No discovery metrics available")

    st.divider()

    # ==========================================================================
    # NEW SECTION: Soundtrack Analytics
    # ==========================================================================
    st.subheader("Soundtrack & Instrumental Analytics")

    soundtrack_data, _ = get_soundtrack_analytics()

    if soundtrack_data and soundtrack_data['total_hours'] > 0:
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        soundtrack_pct = (soundtrack_data['soundtrack_hours'] / soundtrack_data['total_hours'] * 100) if soundtrack_data['total_hours'] > 0 else 0

        with col1:
            st.metric(
                "Soundtrack Hours",
                f"{soundtrack_data['soundtrack_hours']:.1f}h",
                help=f"{soundtrack_pct:.1f}% of total listening"
            )

        with col2:
            st.metric(
                "Regular Music Hours",
                f"{soundtrack_data['regular_hours']:.1f}h"
            )

        with col3:
            st.metric(
                "Soundtrack Songs",
                f"{soundtrack_data['soundtrack_songs']:,}",
                help="Unique soundtrack songs in your library"
            )

        with col4:
            st.metric(
                "Regular Songs",
                f"{soundtrack_data['regular_songs']:,}"
            )

        # Comparison pie chart and top composers
        col_pie, col_composers = st.columns([1, 2])

        with col_pie:
            comparison_df = pd.DataFrame([
                {"category": "Soundtrack", "value": soundtrack_data['soundtrack_hours']},
                {"category": "Regular Music", "value": soundtrack_data['regular_hours']}
            ])
            if not comparison_df.empty and comparison_df['value'].sum() > 0:
                comparison_chart = create_pie_chart(comparison_df, "Hours by Type")
                st.altair_chart(comparison_chart, width="stretch")

        with col_composers:
            st.markdown("**Top Soundtrack Composers/Artists**")
            composers_df, _ = get_top_soundtrack_artists(limit=10)
            if not composers_df.empty:
                st.dataframe(
                    composers_df,
                    column_config={
                        "artist_name": st.column_config.TextColumn("Artist/Composer", width="medium"),
                        "hours": st.column_config.NumberColumn("Hours", width="small", format="%.1f"),
                        "unique_songs": st.column_config.NumberColumn("Songs", width="small")
                    },
                    hide_index=True,
                    width="stretch",
                    height=300
                )
            else:
                st.info("No soundtrack artists found")
    else:
        st.info("No soundtrack data available")

    # Footer with GitHub Actions info
    st.divider()
    st.markdown("""
    ---
    **Automated Data Pipeline:** This dashboard is automatically updated every 2 hours via GitHub Actions
    **Data Source:** MongoDB Atlas
    **Pipeline:** Recently Played -> Process New Content -> Enrich with Lyrics -> Validate Data
    **Brussels Time Zone**
    """)


if __name__ == "__main__":
    main()
