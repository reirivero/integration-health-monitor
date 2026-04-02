import os
import logging
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import Any

logger = logging.getLogger(__name__)


def get_client() -> Client:
    """Create and return an authenticated ClickHouse client.

    Reads credentials from environment variables.
    Uses TLS by default — ClickHouse Cloud requires it.
    """
    host = os.getenv("CLICKHOUSE_HOST")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    database = os.getenv("CLICKHOUSE_DB", "default")

    if not host or not password:
        raise ValueError("CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD must be set.")

    client = clickhouse_connect.get_client(
        host=host,
        user=user,
        password=password,
        database=database,
        secure=True,  # TLS required by ClickHouse Cloud
    )
    logger.info("[clickhouse] Connected to %s", host)
    return client


def setup_table(client: Client) -> None:
    """Create the api_events table if it does not exist.

    Uses MergeTree engine — ClickHouse's default for most use cases.
    Partitioned by month for query performance.
    Ordered by source and collection time for fast filtering.
    """
    client.command("""
        CREATE TABLE IF NOT EXISTS api_events (
            source          LowCardinality(String),
            event_type      LowCardinality(String),
            collected_at    DateTime,
            match_id        Nullable(String),
            competition     Nullable(String),
            home_team       Nullable(String),
            away_team       Nullable(String),
            match_date      Nullable(String),
            status          Nullable(String),
            track_id        Nullable(String),
            track_name      Nullable(String),
            artists         Nullable(String),
            album_name      Nullable(String),
            release_date    Nullable(String),
            popularity      Nullable(Int32),
            duration_ms     Nullable(Int32),
            spotify_url     Nullable(String)
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(collected_at)
        ORDER BY (source, collected_at)
    """)
    logger.info("[clickhouse] Table api_events ready.")


def insert_events(client: Client, events: list[dict[str, Any]]) -> None:
    """Insert a list of normalized events into api_events.

    clickhouse-connect insert() expects rows as a list of lists,
    not a list of dicts. We extract column names and convert accordingly.
    """
    if not events:
        logger.info("[clickhouse] No events to insert.")
        return
    
    from datetime import datetime, timezone

    # Define the canonical column order matching the table schema
    columns = [
        "source", "event_type", "collected_at",
        "match_id", "competition", "home_team", "away_team",
        "match_date", "status",
        "track_id", "track_name", "artists", "album_name",
        "release_date", "popularity", "duration_ms", "spotify_url",
    ]

    def parse_datetime(value: str) -> datetime:
        """Convert ISO 8601 string to timezone-aware datetime object."""
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


    # Convert each dict to a row of values in the correct column order
    # Fields missing from an event (e.g. football events have no track_id)
    # default to None — ClickHouse stores them as NULL
    rows = [
        [
            parse_datetime(event[col]) if col == "collected_at" else event.get(col)
            for col in columns
        ]
        for event in events
    ]

    client.insert("api_events", rows, column_names=columns)
    logger.info("[clickhouse] Inserted %d events.", len(events))