import streamlit as st
import pymongo
import pandas as pd
import altair as alt
import os
from datetime import datetime, timezone, timedelta
import pytz
import itertools

# =============================================================================
# CONFIGURATION - For GitHub Actions and Streamlit Cloud deployment
# =============================================================================
DB_NAME = "Spotify"
STREAMING_COLLECTION = "StreamingHistory"
SONGS_MASTER_COLLECTION = "songs_master"
ARTISTS_MASTER_COLLECTION = "artists_master"

# Page configuration
st.set_page_config(
    page_title="🎧 Spotify Analytics Dashboard", 
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
def get_mongo_client():
    """Create and cache MongoDB client connection."""
    # Try different sources for connection string
    connection_string = (
        st.secrets.get("MONGODB_CONNECTION_STRING") or  # Streamlit Cloud secrets
        os.getenv("MONGODB_CONNECTION_STRING")  # Environment variable
    )
    
    if not connection_string:
        return None, "❌ MONGODB_CONNECTION_STRING not found in secrets or environment"
    
    try:
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command("ping")
        return client, "✅ Connected to MongoDB Atlas"
    except Exception as e:
        return None, f"❌ Connection failed: {str(e)}"

def get_next_update_time():
    """Calculate time until next 2-hour update interval."""
    # Get current time in Brussels timezone
    brussels_tz = pytz.timezone('Europe/Brussels')
    now = datetime.now(brussels_tz)
    
    # Update intervals: 00:00, 02:00, 04:00, ..., 22:00
    current_hour = now.hour
    
    # Find next update hour
    next_update_hour = ((current_hour // 2) + 1) * 2
    if next_update_hour >= 24:
        next_update_hour = 0
        next_update_date = now.date() + timedelta(days=1)
    else:
        next_update_date = now.date()
    
    # Create next update datetime
    next_update = brussels_tz.localize(
        datetime.combine(next_update_date, datetime.min.time().replace(hour=next_update_hour))
    )
    
    # Calculate time difference
    time_diff = next_update - now
    
    # Convert to hours, minutes, seconds
    total_seconds = int(time_diff.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    return hours, minutes, seconds, next_update.strftime("%H:%M")

@st.cache_data(ttl=300)
def get_filter_options():
    """Get all unique values for dropdown filters."""
    client, status = get_mongo_client()
    
    if client is None:
        return {}, None, None, status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        # Get unique songs, artists, albums, years, languages
        songs = list(collection.distinct("track_name", {"track_name": {"$ne": None, "$ne": ""}}))
        artists = list(collection.distinct("artist_name", {"artist_name": {"$ne": None, "$ne": ""}}))
        albums = list(collection.distinct("album_name", {"album_name": {"$ne": None, "$ne": ""}}))
        years = list(collection.distinct("year", {"year": {"$ne": None}}))
        languages = list(collection.distinct("language", {"language": {"$ne": None, "$ne": "Unknown"}}))
        
        # Get date range
        date_pipeline = [
            {"$match": {"date": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": None,
                "min_date": {"$min": "$date"},
                "max_date": {"$max": "$date"}
            }}
        ]
        date_result = list(collection.aggregate(date_pipeline))
        min_date = date_result[0]["min_date"] if date_result else None
        max_date = date_result[0]["max_date"] if date_result else None
        
        # Convert string dates to datetime objects if needed
        if min_date and isinstance(min_date, str):
            min_date = datetime.strptime(min_date, "%Y-%m-%d").date()
        if max_date and isinstance(max_date, str):
            max_date = datetime.strptime(max_date, "%Y-%m-%d").date()
        
        return {
            "songs": sorted([s for s in songs if s]),
            "artists": sorted([a for a in artists if a]),
            "albums": sorted([al for al in albums if al]),
            "languages": sorted([l for l in languages if l]),
            "years": sorted([y for y in years if y])
        }, min_date, max_date, status
        
    except Exception as e:
        return {}, None, None, f"❌ Error loading filter options: {str(e)}"

def apply_filters(base_pipeline, filters):
    """Apply filters to MongoDB aggregation pipeline."""
    match_stage = {}
    
    # Date filter
    if filters.get("date_range"):
        start_date, end_date = filters["date_range"]
        match_stage["date"] = {
            "$gte": start_date.strftime("%Y-%m-%d"),
            "$lte": end_date.strftime("%Y-%m-%d")
        }
    
    # Year filter
    if filters.get("years") and len(filters["years"]) > 0:
        match_stage["year"] = {"$in": filters["years"]}
    
    # Song filter
    if filters.get("songs") and len(filters["songs"]) > 0:
        match_stage["track_name"] = {"$in": filters["songs"]}
    
    # Artist filter
    if filters.get("artists") and len(filters["artists"]) > 0:
        match_stage["artist_name"] = {"$in": filters["artists"]}
    
    # Album filter
    if filters.get("albums") and len(filters["albums"]) > 0:
        match_stage["album_name"] = {"$in": filters["albums"]}
    
    # Language filter
    if filters.get("languages") and len(filters["languages"]) > 0:
        match_stage["language"] = {"$in": filters["languages"]}
    
    if match_stage:
        return [{"$match": match_stage}] + base_pipeline
    
    return base_pipeline

@st.cache_data(ttl=300)
def get_absolute_last_song():
    """Get the absolute last song listened to (ignores filters)."""
    client, status = get_mongo_client()
    
    if client is None:
        return None, status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        # Get absolute last song
        pipeline = [
            {"$match": {
                "track_name": {"$exists": True, "$ne": None, "$ne": ""},
                "ts_utc": {"$exists": True, "$ne": None}
            }},
            {"$sort": {"ts_utc": -1}},
            {"$limit": 1}
        ]
        
        result = list(collection.aggregate(pipeline))
        
        if result:
            song = result[0]
            timestamp = song.get("ts_utc")
            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                local_tz = pytz.timezone('Europe/Brussels')
                local_time = timestamp.astimezone(local_tz)
                
                return {
                    "datetime": local_time,
                    "song_name": song.get("track_name", "Unknown"),
                    "artist_name": song.get("artist_name", "Unknown")
                }, status
        
        return None, status
        
    except Exception as e:
        return None, f"❌ Error getting last song: {str(e)}"

@st.cache_data(ttl=300)
def get_kpi_metrics(filters=None):
    """Get KPI metrics with applied filters."""
    client, status = get_mongo_client()
    
    if client is None:
        return None, status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        # Base pipelines
        total_hours_pipeline = [{"$group": {"_id": None, "total_hours": {"$sum": "$h_played"}}}]
        unique_songs_pipeline = [
            {"$match": {"track_name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$track_name"}},
            {"$count": "unique_songs"}
        ]
        unique_artists_pipeline = [
            {"$match": {"artist_name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$artist_name"}},
            {"$count": "unique_artists"}
        ]
        unique_albums_pipeline = [
            {"$match": {"album_name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$album_name"}},
            {"$count": "unique_albums"}
        ]
        
        # Apply filters if provided
        if filters:
            total_hours_pipeline = apply_filters(total_hours_pipeline, filters)
            unique_songs_pipeline = apply_filters(unique_songs_pipeline, filters)
            unique_artists_pipeline = apply_filters(unique_artists_pipeline, filters)
            unique_albums_pipeline = apply_filters(unique_albums_pipeline, filters)
        
        # Execute queries
        total_hours_result = list(collection.aggregate(total_hours_pipeline, allowDiskUse=True))
        total_hours = total_hours_result[0]["total_hours"] if total_hours_result else 0
        
        unique_songs_result = list(collection.aggregate(unique_songs_pipeline, allowDiskUse=True))
        unique_songs = unique_songs_result[0]["unique_songs"] if unique_songs_result else 0
        
        unique_artists_result = list(collection.aggregate(unique_artists_pipeline, allowDiskUse=True))
        unique_artists = unique_artists_result[0]["unique_artists"] if unique_artists_result else 0
        
        unique_albums_result = list(collection.aggregate(unique_albums_pipeline, allowDiskUse=True))
        unique_albums = unique_albums_result[0]["unique_albums"] if unique_albums_result else 0
        
        return {
            "total_hours": total_hours,
            "unique_songs": unique_songs,
            "unique_artists": unique_artists,
            "unique_albums": unique_albums
        }, status
        
    except Exception as e:
        return None, f"❌ Error getting KPI metrics: {str(e)}"

@st.cache_data(ttl=300)
def get_top_data(data_type="songs", limit=20, filters=None):
    """Get top songs, artists, albums, or play counts by criteria."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        if data_type == "play_count":
            # Count individual plays per song
            pipeline = [
                {"$match": {"track_name": {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": {
                        "track_name": "$track_name",
                        "artist_name": "$artist_name"
                    },
                    "play_count": {"$sum": 1}
                }},
                {"$project": {
                    "_id": 0,
                    "name": {"$concat": ["$_id.track_name", " - ", "$_id.artist_name"]},
                    "count": "$play_count"
                }},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]
        else:
            field_map = {
                "songs": "track_name",
                "artists": "artist_name", 
                "albums": "album_name"
            }
            
            field_name = field_map[data_type]
            
            pipeline = [
                {"$match": {field_name: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": f"${field_name}",
                    "total_hours": {"$sum": "$h_played"}
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
        
        # Truncate long names and add display column
        if not df.empty:
            df['display_name'] = df['name'].apply(lambda x: x[:40] + "..." if len(str(x)) > 40 else str(x))
        
        return df, status
        
    except Exception as e:
        return pd.DataFrame(), f"❌ Error getting top {data_type}: {str(e)}"

@st.cache_data(ttl=300)
def get_time_aggregation(time_type="day_of_week", filters=None):
    """Get hours aggregated by Day, Month, or Year."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        # Map display names to database fields
        field_map = {
            "Day": "day_of_week",
            "Month": "month", 
            "Year": "year"
        }
        
        field_name = field_map.get(time_type, time_type)
        
        pipeline = [
            {"$match": {field_name: {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": f"${field_name}",
                "total_hours": {"$sum": "$h_played"}
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
        
        # Proper ordering for days/months
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
        return pd.DataFrame(), f"❌ Error getting time aggregation: {str(e)}"

@st.cache_data(ttl=300)
def get_listening_heatmap_data(filters=None):
    """Get listening data for hour vs day of week heatmap."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        pipeline = [
            {"$match": {
                "ts_utc": {"$exists": True, "$ne": None},
                "day_of_week": {"$exists": True, "$ne": None}
            }},
            {"$project": {
                "day_of_week": "$day_of_week",
                "hour": {"$hour": "$ts_utc"},
                "h_played": "$h_played"
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
            # Create complete hour x day grid
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            hours = list(range(24))
            
            # Create all combinations
            all_combinations = list(itertools.product(days, hours))
            full_df = pd.DataFrame(all_combinations, columns=['day', 'hour'])
            
            # Merge with actual data
            df = full_df.merge(df, on=['day', 'hour'], how='left')
            df['hours'] = df['hours'].fillna(0)
        
        return df, status
        
    except Exception as e:
        return pd.DataFrame(), f"❌ Error getting heatmap data: {str(e)}"

@st.cache_data(ttl=300)
def get_language_evolution_data():
    """Get language listening evolution over time (monthly) for top 5 languages only."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        collection = db[STREAMING_COLLECTION]
        
        # First, get top 5 languages by total hours
        top_languages_pipeline = [
            {"$match": {"language": {"$exists": True, "$ne": None, "$ne": "Unknown"}}},
            {"$group": {
                "_id": "$language",
                "total_hours": {"$sum": "$h_played"}
            }},
            {"$sort": {"total_hours": -1}},
            {"$limit": 5},
            {"$project": {"_id": 0, "language": "$_id"}}
        ]
        
        top_languages_result = list(collection.aggregate(top_languages_pipeline, allowDiskUse=True))
        top_languages = [lang["language"] for lang in top_languages_result]
        
        if not top_languages:
            return pd.DataFrame(), status
        
        # Now get evolution data for only these top 5 languages
        pipeline = [
            {"$match": {
                "language": {"$in": top_languages},
                "ts_utc": {"$exists": True, "$ne": None}
            }},
            {"$project": {
                "language": "$language",
                "year": {"$year": "$ts_utc"},
                "month": {"$month": "$ts_utc"},
                "h_played": "$h_played"
            }},
            {"$group": {
                "_id": {
                    "language": "$language",
                    "year": "$year",
                    "month": "$month"
                },
                "total_hours": {"$sum": "$h_played"}
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
        
        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        df = pd.DataFrame(results)
        
        if not df.empty:
            # Create date column for easier plotting
            df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
            df = df.sort_values('date')
        
        return df, status
        
    except Exception as e:
        return pd.DataFrame(), f"❌ Error getting language evolution data: {str(e)}"

@st.cache_data(ttl=300)
def get_distribution_data(data_type="countries", filters=None):
    """Get data for pie charts (countries, languages by songs, languages by hours)."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        
        if data_type == "countries":
            collection = db[STREAMING_COLLECTION]
            pipeline = [
                {"$match": {"conn_country": {"$exists": True, "$ne": None, "$ne": ""}}},
                {"$group": {
                    "_id": "$conn_country",
                    "total_hours": {"$sum": "$h_played"}
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
            collection = db[SONGS_MASTER_COLLECTION]
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
            collection = db[STREAMING_COLLECTION]
            pipeline = [
                {"$match": {"language": {"$exists": True, "$ne": None, "$ne": "Unknown"}}},
                {"$group": {
                    "_id": "$language",
                    "total_hours": {"$sum": "$h_played"}
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
        
        # Keep top 5, combine rest as "Others"
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
        return pd.DataFrame(), f"❌ Error getting {data_type} data: {str(e)}"

@st.cache_data(ttl=300)
def get_release_years_data():
    """Get count of unique songs by release year from songs_master collection."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        songs_collection = db[SONGS_MASTER_COLLECTION]
        
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
            # Fill in missing years with 0 count
            min_year = int(df['year'].min())
            max_year = int(df['year'].max())
            
            all_years = pd.DataFrame({'year': range(min_year, max_year + 1)})
            df = all_years.merge(df, on='year', how='left')
            df['count'] = df['count'].fillna(0)
            df['year'] = df['year'].astype(int)
            df['count'] = df['count'].astype(int)
        
        return df, status
        
    except Exception as e:
        return pd.DataFrame(), f"❌ Error getting release years data: {str(e)}"

@st.cache_data(ttl=300)
def get_songs_by_year(selected_year):
    """Get songs released in a specific year."""
    client, status = get_mongo_client()
    
    if client is None:
        return pd.DataFrame(), status
    
    try:
        db = client[DB_NAME]
        songs_collection = db[SONGS_MASTER_COLLECTION]
        
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
        return pd.DataFrame(), f"❌ Error getting songs for year {selected_year}: {str(e)}"

def create_horizontal_bar_chart(df, title, value_col="hours", height=600):
    """Create a horizontal bar chart."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text(
            text="No data available",
            fontSize=16,
            color="gray"
        ).properties(width=400, height=300)
    
    # Determine value format and title
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
    
    # Define proper ordering
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
    
    # Define day order
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
        height=300,
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
    
    # Sort by value descending to ensure largest slice starts at top
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
        width=300,
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
    
    # Create decade labels for x-axis
    min_year = int(df['year'].min())
    max_year = int(df['year'].max())
    
    # Round down to nearest decade for start, up for end
    start_decade = (min_year // 10) * 10
    end_decade = ((max_year // 10) + 1) * 10
    
    decade_labels = list(range(start_decade, end_decade + 1, 10))
    
    chart = alt.Chart(df).mark_bar(
        color='#1DB954',
        opacity=0.8,
        size=8,  # Make bars thicker
        stroke='white',
        strokeWidth=0.5
    ).encode(
        x=alt.X('year:O', 
                title='Release Year',
                axis=alt.Axis(
                    values=decade_labels,
                    labelAngle=0
                ),
                scale=alt.Scale(paddingInner=0.1)),  # Make bars closer together
        y=alt.Y('count:Q', 
                title='Number of Songs',
                axis=alt.Axis(format='~s')),
        tooltip=[
            alt.Tooltip('year:O', title='Year'),
            alt.Tooltip('count:Q', title='Songs', format=',.0f')
        ]
    ).properties(
        width=500,
        height=400,  # Increased height to match pie chart
        title=alt.TitleParams(
            text="Songs by Release Year",
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
    
    # Connection status (small message at top)
    st.markdown(f'<div class="connection-status">{connection_status}</div>', unsafe_allow_html=True)
    
    # Sidebar toggle button (always visible)
    if st.button("🔧 Filters" if not st.session_state.sidebar_open else "✖️ Close", key="sidebar_toggle"):
        st.session_state.sidebar_open = not st.session_state.sidebar_open
    
    # App header
    st.title("🎧 Spotify Analytics Dashboard")
    st.caption(f"Data from MongoDB: `{DB_NAME}` → `{STREAMING_COLLECTION}` | Automated via GitHub Actions")
    
    # Check if we can connect to MongoDB
    if "❌" in connection_status:
        st.error("🚨 **Database Connection Failed**")
        st.error(connection_status)
        st.info("💡 **Troubleshooting:**")
        st.info("1. Check if MongoDB connection string is set in Streamlit secrets")
        st.info("2. Verify your MongoDB Atlas cluster is running")
        st.info("3. Check if your IP address is whitelisted in MongoDB Atlas")
        st.stop()
    
    # Last song section (full width, at top)
    last_song_data, _ = get_absolute_last_song()
    
    if last_song_data:
        formatted_date = last_song_data["datetime"].strftime("%d %B %Y %H:%M").lstrip('0')
        last_song_text = f'🎵 Last played: "{last_song_data["song_name"]}" by {last_song_data["artist_name"]} on {formatted_date}'
    else:
        last_song_text = "🎵 No recent listening data available"
    
    st.markdown(f'''
    <div class="last-song-container">
        <div class="last-song-text">{last_song_text}</div>
    </div>
    ''', unsafe_allow_html=True)
    
    # Sidebar filters
    current_filters = {}
    if st.session_state.sidebar_open:
        with st.sidebar:
            # Timer for next update
            hours, minutes, seconds, next_time = get_next_update_time()
            st.markdown(f"""
            <div style="
                background-color: #f0f8f0; 
                border: 2px solid #1DB954; 
                border-radius: 8px; 
                padding: 10px; 
                text-align: center; 
                margin-bottom: 15px;
            ">
                <div style="font-size: 14px; color: #1DB954; font-weight: bold;">
                    ⏰ Next GitHub Actions Run: {next_time}
                </div>
                <div style="font-size: 18px; color: #333; font-weight: bold;">
                    {hours:02d}:{minutes:02d}:{seconds:02d}
                </div>
                <div style="font-size: 12px; color: #666;">
                    Time until next automation
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.header("🔍 Filters")
            
            # Reset button at the top with red background
            if st.button("🔄 Reset All Filters", help="Clear all filters", key="reset_filters"):
                st.rerun()
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Date filter section
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("##### 📅 Date Range")
            if min_date and max_date:
                date_range = st.slider(
                    "Select date range:",
                    min_value=min_date,
                    max_value=max_date,
                    value=(min_date, max_date),
                    format="YYYY-MM-DD"
                )
                current_filters["date_range"] = date_range
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Year filter section
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("##### 📆 Years")
            selected_years = st.multiselect(
                "Select years:",
                options=filter_options.get("years", []),
                default=[]
            )
            if selected_years:
                current_filters["years"] = selected_years
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Song filter section
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("##### 🎵 Songs")
            selected_songs = st.multiselect(
                "Select songs:",
                options=filter_options.get("songs", []),
                default=[]
            )
            if selected_songs:
                current_filters["songs"] = selected_songs
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Artist filter section
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("##### 🎤 Artists")
            selected_artists = st.multiselect(
                "Select artists:",
                options=filter_options.get("artists", []),
                default=[]
            )
            if selected_artists:
                current_filters["artists"] = selected_artists
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Album filter section
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            st.markdown("##### 💿 Albums")
            selected_albums = st.multiselect(
                "Select albums:",
                options=filter_options.get("albums", []),
                default=[]
            )
            if selected_albums:
                current_filters["albums"] = selected_albums
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Language filter section
            if filter_options.get("languages"):
                st.markdown('<div class="filter-section">', unsafe_allow_html=True)
                st.markdown("##### 🌍 Languages")
                selected_languages = st.multiselect(
                    "Select languages:",
                    options=filter_options.get("languages", []),
                    default=[]
                )
                if selected_languages:
                    current_filters["languages"] = selected_languages
                st.markdown('</div>', unsafe_allow_html=True)
    
    # Get data with filters
    kpi_data, kpi_status = get_kpi_metrics(current_filters if current_filters else None)
    
    if kpi_data is None:
        st.error(kpi_status)
        st.stop()
    
    # KPI Metrics - Bigger, centered, bold, in boxes
    st.markdown("### 📊 Key Metrics")
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
        st.subheader("🔥 Top 20 Most Played")
    with col_radio:
        top_data_type = st.radio(
            "View by:",
            ["Songs", "Artists", "Albums", "Play Count"],
            horizontal=True,
            key="top_data_radio"
        )
    
    # Map radio selection to data type
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
        st.altair_chart(top_chart, use_container_width=True)
    else:
        st.info(f"No {top_data_type.lower()} data available with current filters")
    
    st.divider()
    
    # Time patterns with inline radio buttons
    col_title, col_radio = st.columns([3, 1])
    with col_title:
        st.subheader("📈 Listening Patterns")
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
        st.altair_chart(time_chart, use_container_width=True)
    else:
        st.info(f"No {time_type.lower()} data available with current filters")
    
    st.divider()
    
    # Combined section: Distribution Analysis (1/3) + Release Years (2/3)
    col_dist, col_release = st.columns([1, 2])
    
    with col_dist:
        st.subheader("📊 Distribution Analysis")
        
        dist_type = st.radio(
            "View:",
            ["Countries", "Languages (Songs)", "Languages (Hours)"],
            key="distribution_radio"
        )
        
        # Map radio selection to data type
        dist_data_map = {
            "Countries": "countries",
            "Languages (Songs)": "languages_songs", 
            "Languages (Hours)": "languages_hours"
        }
        
        selected_dist_type = dist_data_map[dist_type]
        dist_df, _ = get_distribution_data(selected_dist_type, current_filters if current_filters else None)
        
        if not dist_df.empty:
            dist_chart = create_pie_chart(dist_df, dist_type)
            st.altair_chart(dist_chart, use_container_width=True)
        else:
            st.info(f"No {dist_type.lower()} data available")
    
    with col_release:
        st.subheader("📅 Songs by Release Year")
        
        release_years_df, _ = get_release_years_data()
        
        if not release_years_df.empty:
            # Create dropdown for year selection
            col_chart, col_dropdown = st.columns([3, 1])
            
            with col_chart:
                release_years_chart = create_release_years_chart(release_years_df)
                st.altair_chart(release_years_chart, use_container_width=True)
            
            with col_dropdown:
                # Year selection dropdown
                available_years = sorted(release_years_df['year'].tolist(), reverse=True)
                selected_year = st.selectbox(
                    "Select Year:",
                    options=available_years,
                    index=0,
                    key="year_selector"
                )
                
                # Show count for selected year
                year_count = release_years_df[release_years_df['year'] == selected_year]['count'].iloc[0]
                st.metric("Songs", f"{year_count:,}")
            
            # Show songs for selected year
            if selected_year:
                with st.expander(f"🎼 Songs from {selected_year} ({year_count:,} songs)", expanded=True):
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
                            use_container_width=True,
                            height=300
                        )
                    else:
                        st.info("No songs found for this year")
        else:
            st.info("No release year data available")
    
    st.divider()
    
    # Listening Habits Heatmap
    st.subheader("🔥 Listening Intensity Heatmap")
    
    heatmap_df, _ = get_listening_heatmap_data(current_filters if current_filters else None)
    
    if not heatmap_df.empty:
        heatmap_chart = create_heatmap_chart(heatmap_df)
        st.altair_chart(heatmap_chart, use_container_width=True)
    else:
        st.info("No heatmap data available")
    
    st.divider()
    
    # Language Evolution Over Time
    st.subheader("🌍 Language Evolution Over Time")
    
    lang_evolution_df, _ = get_language_evolution_data()
    
    if not lang_evolution_df.empty:
        lang_evolution_chart = create_language_evolution_chart(lang_evolution_df)
        st.altair_chart(lang_evolution_chart, use_container_width=True)
    else:
        st.info("No language evolution data available")
    
    # Footer with GitHub Actions info
    st.divider()
    st.markdown("""
    ---
    **🤖 Automated Data Pipeline:** This dashboard is automatically updated every 2 hours via GitHub Actions  
    **📊 Data Source:** MongoDB Atlas  
    **🔧 Pipeline:** Recently Played → Process New Content → Enrich with Lyrics → Validate Data  
    **⏰ Brussels Time Zone**
    """)

if __name__ == "__main__":
    main()
