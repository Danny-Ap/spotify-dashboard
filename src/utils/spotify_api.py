"""
Spotify API client for the data pipeline.

Handles authentication (both user auth and client credentials),
token refresh, and batch API requests for tracks and artists.
"""

import os
import base64
import time
import re
import logging
from typing import Dict, List, Optional, Any
import requests
from dotenv import load_dotenv

from .config import SPOTIFY_BATCH_SIZE, REQUEST_DELAY

load_dotenv()
logger = logging.getLogger(__name__)


class SpotifyClient:
    """
    Unified Spotify API client with token management.

    Supports two authentication modes:
    1. User Auth (use_user_auth=True): Uses access/refresh tokens for
       endpoints requiring user authorization (e.g., recently-played)
    2. Client Credentials (use_user_auth=False): Uses client ID/secret
       for public endpoints (e.g., track/artist details)

    Usage:
        # For recently-played endpoint (requires user auth)
        client = SpotifyClient(use_user_auth=True)
        if client.authenticate():
            tracks = client.get_recently_played()

        # For public endpoints
        client = SpotifyClient(use_user_auth=False)
        if client.authenticate():
            details = client.get_batch_tracks(track_ids)
    """

    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(self, use_user_auth: bool = False):
        """
        Initialize Spotify client.

        Args:
            use_user_auth: If True, use user authentication (access/refresh tokens).
                          If False, use client credentials flow.
        """
        self.client_id = os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        self.access_token = os.getenv('SPOTIFY_ACCESS_TOKEN') if use_user_auth else None
        self.refresh_token = os.getenv('SPOTIFY_REFRESH_TOKEN') if use_user_auth else None
        self.use_user_auth = use_user_auth
        self.token: Optional[str] = None

    def authenticate(self) -> bool:
        """
        Authenticate with Spotify API.

        Returns:
            True if authentication successful, False otherwise.
        """
        if not self.client_id or not self.client_secret:
            logger.error("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set")
            return False

        if self.use_user_auth:
            if self.access_token:
                self.token = self.access_token
                logger.info("Using existing Spotify access token")
                return True
            else:
                logger.error("User auth requested but no access token available")
                return False

        return self._get_client_credentials_token()

    def _get_client_credentials_token(self) -> bool:
        """
        Get token using client credentials flow.

        Returns:
            True if token obtained successfully, False otherwise.
        """
        try:
            auth_string = f"{self.client_id}:{self.client_secret}"
            auth_bytes = auth_string.encode('utf-8')
            auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')

            headers = {
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {"grant_type": "client_credentials"}

            response = requests.post(self.TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()

            self.token = response.json()["access_token"]
            logger.info("Spotify API token obtained via client credentials")
            return True

        except Exception as e:
            logger.error(f"Error getting Spotify token: {e}")
            return False

    def refresh_access_token(self) -> Optional[str]:
        """
        Refresh the user access token using refresh token.

        Returns:
            New access token if successful, None otherwise.
        """
        if not self.refresh_token:
            logger.error("No refresh token available")
            return None

        if not self.client_id or not self.client_secret:
            logger.error("Missing client credentials for token refresh")
            return None

        try:
            auth_header = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            logger.info("Refreshing Spotify access token...")
            response = requests.post(
                self.TOKEN_URL,
                auth=auth_header,
                data=data,
                headers=headers
            )

            if response.status_code == 200:
                new_token = response.json()['access_token']
                self.token = new_token
                logger.info("Access token refreshed successfully")
                return new_token
            else:
                logger.error(f"Failed to refresh token: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error refreshing access token: {e}")
            return None

    def _get_auth_header(self) -> Dict[str, str]:
        """Get authorization header for API requests."""
        return {"Authorization": f"Bearer {self.token}"}

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        retry_on_401: bool = True
    ) -> Optional[Dict]:
        """
        Make an authenticated API request with retry on 401.

        Args:
            endpoint: API endpoint (without base URL).
            params: Query parameters.
            retry_on_401: Whether to retry with refreshed token on 401.

        Returns:
            JSON response as dict, or None on failure.
        """
        url = f"{self.API_BASE}/{endpoint}"

        try:
            response = requests.get(url, headers=self._get_auth_header(), params=params)

            # Handle 401 with token refresh
            if response.status_code == 401 and retry_on_401 and self.use_user_auth:
                logger.warning("Access token expired, attempting refresh...")
                if self.refresh_access_token():
                    response = requests.get(
                        url,
                        headers=self._get_auth_header(),
                        params=params
                    )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None

    def get_recently_played(self, limit: int = 50) -> Optional[List[Dict]]:
        """
        Get recently played tracks (requires user auth).

        Args:
            limit: Maximum number of tracks (max 50).

        Returns:
            List of track items, or None on failure.
        """
        if not self.use_user_auth:
            logger.error("get_recently_played requires user authentication")
            return None

        result = self._make_request("me/player/recently-played", {"limit": min(limit, 50)})

        if result:
            tracks = result.get("items", [])
            logger.info(f"Retrieved {len(tracks)} recently played tracks")
            return tracks

        return None

    def get_batch_tracks(self, track_ids: List[str]) -> Dict[str, Dict]:
        """
        Get track details for up to 50 tracks in a single API call.

        Args:
            track_ids: List of Spotify track URIs or IDs.

        Returns:
            Dict mapping track_id to track details.
            Each entry has 'status' key ('success' or 'error').
        """
        if not track_ids:
            return {}

        # Clean track IDs (remove spotify:track: prefix if present)
        clean_ids = []
        id_mapping = {}  # Map clean ID back to original
        for tid in track_ids[:SPOTIFY_BATCH_SIZE]:
            clean_id = tid.replace('spotify:track:', '')
            clean_ids.append(clean_id)
            id_mapping[clean_id] = tid

        result = self._make_request("tracks", {"ids": ",".join(clean_ids)})

        batch_results = {}

        if not result:
            # Return error for all tracks
            for tid in track_ids:
                batch_results[tid] = {
                    "status": "error",
                    "error_message": "API request failed"
                }
            return batch_results

        tracks = result.get("tracks", [])

        for i, track in enumerate(tracks):
            if i >= len(clean_ids):
                break

            clean_id = clean_ids[i]
            original_id = id_mapping.get(clean_id, track_ids[i] if i < len(track_ids) else clean_id)

            if track is None:
                batch_results[original_id] = {
                    "status": "error",
                    "error_message": "Track not found or not available"
                }
            else:
                album = track.get("album", {})
                release_date = album.get("release_date", "")
                duration_ms = track.get("duration_ms")

                batch_results[original_id] = {
                    "status": "success",
                    "duration_ms": duration_ms,
                    "duration_s": round(duration_ms / 1000, 2) if duration_ms else None,
                    "release_date": release_date,
                    "release_date_year": self._extract_year(release_date),
                    "popularity": track.get("popularity"),
                    "album_name": album.get("name"),
                    "artists": [
                        {
                            "id": a.get("id"),
                            "name": a.get("name"),
                            "uri": a.get("uri")
                        }
                        for a in track.get("artists", [])
                    ]
                }

        time.sleep(REQUEST_DELAY)
        return batch_results

    def get_batch_artists(self, artist_ids: List[str]) -> Dict[str, Dict]:
        """
        Get artist details for up to 50 artists in a single API call.

        Args:
            artist_ids: List of Spotify artist IDs.

        Returns:
            Dict mapping artist_id to artist details.
            Each entry has 'status' key ('success' or 'error').
        """
        if not artist_ids:
            return {}

        # Ensure we don't exceed batch size
        ids_to_fetch = artist_ids[:SPOTIFY_BATCH_SIZE]

        result = self._make_request("artists", {"ids": ",".join(ids_to_fetch)})

        batch_results = {}

        if not result:
            for aid in artist_ids:
                batch_results[aid] = {
                    "status": "error",
                    "error_message": "API request failed"
                }
            return batch_results

        artists = result.get("artists", [])

        for i, artist in enumerate(artists):
            if i >= len(ids_to_fetch):
                break

            artist_id = ids_to_fetch[i]

            if artist is None:
                batch_results[artist_id] = {
                    "status": "error",
                    "error_message": "Artist not found or not available"
                }
            else:
                batch_results[artist_id] = {
                    "status": "success",
                    "name": artist.get("name"),
                    "uri": artist.get("uri"),
                    "genres": ", ".join(artist.get("genres", [])),
                    "followers": artist.get("followers", {}).get("total"),
                    "popularity": artist.get("popularity")
                }

        time.sleep(REQUEST_DELAY)
        return batch_results

    @staticmethod
    def _extract_year(release_date: str) -> Optional[int]:
        """
        Extract year from release date string.

        Args:
            release_date: Date string (e.g., "2023-01-15" or "2023").

        Returns:
            Year as integer, or None if parsing fails.
        """
        if not release_date:
            return None

        match = re.match(r'^(\d{4})', release_date)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None

        return None
