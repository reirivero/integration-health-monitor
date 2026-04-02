import time
import logging
import httpx
from abc import ABC, abstractmethod
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ApiCollector(ABC):
    """Base class for all API collectors."""

    def __init__(
        self,
        name: str,
        base_url: str,
        max_retries: int = 3,
        timeout: float = 10.0,
    ) -> None:
        self.name = name
        self.base_url = base_url
        self.max_retries = max_retries
        self.timeout = timeout

    def fetch(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetch an endpoint with retry logic. Returns parsed JSON or raises."""
        url = f"{self.base_url}{endpoint}"
        attempt = 0

        while attempt < self.max_retries:
            try:
                response = httpx.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                logger.info("[%s] OK %s", self.name, endpoint)
                return data

            except httpx.HTTPStatusError as e:
                logger.warning(
                    "[%s] HTTP %s on %s (attempt %d/%d)",
                    self.name, e.response.status_code, endpoint, attempt + 1, self.max_retries,
                )
                if e.response.status_code == 429:
                    # Rate limited — wait before retrying
                    wait = 2 ** attempt
                    logger.info("[%s] Rate limited. Waiting %ds...", self.name, wait)
                    time.sleep(wait)

            except httpx.RequestError as e:
                logger.warning("[%s] Request error: %s (attempt %d/%d)", self.name, e, attempt + 1, self.max_retries)

            attempt += 1

        raise RuntimeError(f"[{self.name}] Failed after {self.max_retries} attempts on {endpoint}")

    @abstractmethod
    def collect(self) -> list[dict[str, Any]]:
        """Each collector implements this to return normalized events."""
        ...