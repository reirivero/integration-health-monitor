import os
import time
import logging
import base64
import httpx
from typing import Any
from src.collector.base import ApiCollector

logger = logging.getLogger(__name__)

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_BASE_URL = "https://api.spotify.com/v1"


class SpotifyCollector(ApiCollector):
    """Collects track and artist data from Spotify using OAuth2 Client Credentials."""

    def __init__(self) -> None:
        super().__init__(
            name="spotify",
            base_url=SPOTIFY_BASE_URL,
            max_retries=3,
            timeout=10.0,
        )
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise ValueError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set.")

        # Token state — starts empty, fetched on first request
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    def _get_access_token(self) -> str:
        """Request a new access token using Client Credentials Flow.
        
        Encodes credentials as Base64 per Spotify's OAuth2 spec.
        Token is cached and reused until expiry.
        """
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        response = httpx.post(
            SPOTIFY_AUTH_URL,
            headers={
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        token_data = response.json()

        self._access_token = token_data["access_token"]
        # Store expiry time with a 60s safety buffer
        self._token_expires_at = time.time() + token_data["expires_in"] - 60
        logger.info("[%s] Access token obtained. Expires in ~%ds", self.name, token_data["expires_in"])

        return self._access_token

    def _get_valid_token(self) -> str:
        """Return cached token if still valid, otherwise fetch a new one."""
        if self._access_token is None or time.time() >= self._token_expires_at:
            return self._get_access_token()
        return self._access_token

    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Override fetch to inject Bearer token. Auto-refreshes on expiry."""
        url = f"{self.base_url}{endpoint}"
        attempt = 0

        while attempt < self.max_retries:
            try:
                token = self._get_valid_token()
                response = httpx.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                logger.info("[%s] OK %s", self.name, endpoint)
                return response.json()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                logger.warning("[%s] HTTP %s on %s (attempt %d/%d)",
                    self.name, status, endpoint, attempt + 1, self.max_retries)

                if status == 401:
                    # Token rejected — force refresh on next attempt
                    logger.info("[%s] Token invalid, forcing refresh...", self.name)
                    self._access_token = None
                elif status == 429:
                    wait = 2 ** attempt
                    logger.info("[%s] Rate limited. Waiting %ds...", self.name, wait)
                    time.sleep(wait)
                elif status == 403:
                    raise PermissionError(f"[{self.name}] Insufficient permissions for this endpoint.")

            except httpx.RequestError as e:
                logger.warning("[%s] Request error: %s (attempt %d/%d)",
                    self.name, e, attempt + 1, self.max_retries)

            attempt += 1

        raise RuntimeError(f"[{self.name}] Failed after {self.max_retries} attempts on {endpoint}")

    def collect(self) -> list[dict[str, Any]]:
        """Search for tracks by genre and return normalized events."""
        # Search endpoint is available to all app types
        raw = self.fetch("/search", params={
            "q": "genre:electronic",
            "type": "track",
            "limit": 10,
            "market": "US",
        })
        tracks = raw.get("tracks", {}).get("items", [])
        logger.info("[%s] Collected %d tracks", self.name, len(tracks))
        return self._normalize(tracks)

    def _normalize(self, tracks: list[dict]) -> list[dict[str, Any]]:
        """Convert raw Spotify track data into flat, consistent events."""
        events = []
        for track in tracks:
            artists = ", ".join(a.get("name", "unknown") for a in track.get("artists", []))
            album = track.get("album", {})
            events.append({
                "source": self.name,
                "event_type": "track_found",
                "track_id": track.get("id", ""),
                "track_name": track.get("name", ""),
                "artists": artists,
                "album_name": album.get("name", ""),
                "release_date": album.get("release_date", ""),
                "popularity": track.get("popularity", 0),
                "duration_ms": track.get("duration_ms", 0),
                "spotify_url": track.get("external_urls", {}).get("spotify", ""),
                "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
        return events