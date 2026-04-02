import os
import time
import logging
from typing import Any
from src.collector.base import ApiCollector

logger = logging.getLogger(__name__)

FOOTBALL_BASE_URL = "https://api.football-data.org/v4"


class FootballCollector(ApiCollector):
    """Collects match and competition data from football-data.org."""

    def __init__(self) -> None:
        super().__init__(
            name="football-data",
            base_url=FOOTBALL_BASE_URL,
            max_retries=3,
            timeout=10.0,
        )
        self.api_key = os.getenv("FOOTBALL_API_KEY")
        if not self.api_key:
            raise ValueError("FOOTBALL_API_KEY is not set in environment variables.")

    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Override fetch to inject the API key header."""
        import httpx

        url = f"{self.base_url}{endpoint}"
        headers = {"X-Auth-Token": self.api_key}
        attempt = 0

        while attempt < self.max_retries:
            try:
                response = httpx.get(url, headers=headers, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                logger.info("[%s] OK %s", self.name, endpoint)
                return data

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                logger.warning("[%s] HTTP %s on %s (attempt %d/%d)",
                    self.name, status, endpoint, attempt + 1, self.max_retries)

                if status == 429:
                    wait = 2 ** attempt
                    logger.info("[%s] Rate limited. Waiting %ds...", self.name, wait)
                    time.sleep(wait)
                elif status == 403:
                    raise PermissionError(f"[{self.name}] Invalid or missing API key.")

            except httpx.RequestError as e:
                logger.warning("[%s] Request error: %s (attempt %d/%d)",
                    self.name, e, attempt + 1, self.max_retries)

            attempt += 1

        raise RuntimeError(f"[{self.name}] Failed after {self.max_retries} attempts on {endpoint}")

    def collect(self) -> list[dict[str, Any]]:
        """Fetch today's matches and return normalized events."""
        raw = self.fetch("/matches", params={"status": "SCHEDULED"})
        matches = raw.get("matches", [])
        logger.info("[%s] Collected %d matches", self.name, len(matches))
        return self._normalize(matches)

    def _normalize(self, matches: list[dict]) -> list[dict[str, Any]]:
        """Convert raw API response into a flat, consistent event format."""
        events = []
        for match in matches:
            events.append({
                "source": self.name,
                "event_type": "match_scheduled",
                "match_id": str(match.get("id")),
                "competition": match.get("competition", {}).get("name", "unknown"),
                "home_team": match.get("homeTeam", {}).get("name", "unknown"),
                "away_team": match.get("awayTeam", {}).get("name", "unknown"),
                "match_date": match.get("utcDate", ""),
                "status": match.get("status", ""),
                "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
        return events