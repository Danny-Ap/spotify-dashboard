# streamlit_app.py
import os
from datetime import datetime
import pandas as pd
import streamlit as st
import altair as alt
from dotenv import load_dotenv
from pymongo import MongoClient

# ----------------------------
# App & page configuration
# ----------------------------
st.set_page_config(page_title="Spotify Hours by Year", page_icon="🎧", layout="wide")
st.title("🎧 Spotify Listening — Hours per Year")

# ----------------------------
# Load environment & connect
# ----------------------------
load_dotenv()  # expects .env containing: MONGODB_CONNECTION_STRING="mongodb+srv://..."
MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING", "")

if not MONGO_URI:
    st.error("MONGODB_CONNECTION_STRING not found. Add it to your .env file.")
    st.stop()

@st.cache_resource
def get_client(uri: str):
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

try:
    client = get_client(MONGO_URI)
    db = client["Spotify"]
    coll = db["StreamingHistory2"]
    # Trigger a quick ping to surface connection issues early
    client.admin.command("ping")
except Exception as e:
    st.error(f"Could not connect to MongoDB: {e}")
    st.stop()

# ----------------------------
# Data access (cached)
# ----------------------------
@st.cache_data(ttl=300)
def hours_by_year_dataframe() -> pd.DataFrame:
    """
    Aggregates total duration_hours per Years from MongoDB.
    Returns a DataFrame with columns: year (int), hours (float),
    filling any missing years between min..max with 0.
    """
    pipeline = [
        {"$group": {"_id": "$Years", "hours": {"$sum": "$duration_hours"}}},
        {"$project": {"year": "$_id", "hours": 1, "_id": 0}},
        {"$sort": {"year": 1}},
    ]

    rows = list(coll.aggregate(pipeline, allowDiskUse=True))
    if not rows:
        return pd.DataFrame(columns=["year", "hours"])

    df = pd.DataFrame(rows)  # year (int), hours (float)

    # Fill missing years with 0 between min and max present
    min_year = int(df["year"].min())
    max_year = int(df["year"].max())
    full_index = pd.Index(range(min_year, max_year + 1), name="year")
    df_full = (
        df.set_index("year")
          .reindex(full_index, fill_value=0.0)
          .reset_index()
    )

    # Ensure sorting ascending and consistent types
    df_full["year"] = df_full["year"].astype(int)
    df_full["hours"] = df_full["hours"].astype(float)

    return df_full

df = hours_by_year_dataframe()

if df.empty:
    st.info("No data found in Spotify.StreamingHistory2.")
    st.stop()

# ----------------------------
# Chart
# ----------------------------
st.subheader("Hours listened per year")

bars = alt.Chart(df).mark_bar().encode(
    x=alt.X("year:O", title="Year", sort=None),
    y=alt.Y("hours:Q", title="Hours listened", axis=alt.Axis(format="~s")),
    tooltip=[alt.Tooltip("year:O", title="Year"), alt.Tooltip("hours:Q", title="Hours", format=",.2f")],
)

labels = alt.Chart(df).mark_text(dy=-5).encode(
    x="year:O",
    y="hours:Q",
    text=alt.Text("hours:Q", format=",.1f"),
)

chart = (bars + labels).properties(height=420)
st.altair_chart(chart, use_container_width=True)

# (Optional) show the table below
with st.expander("Show aggregated table"):
    st.dataframe(df, use_container_width=True)

# Footer note
st.caption("Data source: MongoDB Atlas • Collection: Spotify.StreamingHistory2 • Aggregated with $group.")
