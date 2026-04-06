import os
import logging
import httpx
from datetime import datetime, timezone, timedelta
from clickhouse_connect.driver.client import Client
from typing import Any

logger = logging.getLogger(__name__)

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "google/gemini-2.0-flash-001"


def fetch_recent_events(client: Client, hours: int = 24) -> list[dict[str, Any]]:
    """Pull events from the last N hours from ClickHouse."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    result = client.query(f"""
        SELECT
            source,
            event_type,
            collected_at,
            match_id,
            competition,
            home_team,
            away_team,
            track_id,
            track_name,
            artists,
            popularity,
            duration_ms
        FROM api_events
        WHERE collected_at >= '{since_str}'
        ORDER BY source, collected_at DESC
    """)
    return list(result.named_results())


def detect_anomalies(events: list[dict[str, Any]]) -> list[str]:
    """Rule-based anomaly detection — deterministic, no LLM needed.

    Returns a list of human-readable findings.
    """
    findings = []

    by_source: dict[str, list] = {}
    for e in events:
        by_source.setdefault(e["source"], []).append(e)

    # Check 1 — missing sources
    expected_sources = {"football-data", "spotify"}
    missing = expected_sources - set(by_source.keys())
    for s in missing:
        findings.append(f"WARNING: No events received from '{s}' in the last 24h — possible API failure or outage.")

    # Check 2 — low event count per source
    for source, evts in by_source.items():
        if len(evts) < 3:
            findings.append(f"WARNING: '{source}' returned only {len(evts)} events — unusually low volume.")

    # Check 3 — Spotify popularity at zero (known data quality issue)
    spotify_events = by_source.get("spotify", [])
    zero_popularity = [e for e in spotify_events if e.get("popularity") == 0]
    if zero_popularity and len(zero_popularity) == len(spotify_events):
        findings.append(
            f"INFO: All {len(zero_popularity)} Spotify tracks have popularity=0 — "
            "Spotify does not populate this field under Client Credentials Flow for certain markets."
        )

    # Check 4 — duplicate match IDs (football)
    football_events = by_source.get("football-data", [])
    match_ids = [e["match_id"] for e in football_events if e.get("match_id")]
    if len(match_ids) != len(set(match_ids)):
        findings.append("WARNING: Duplicate match_id detected in football-data events — possible double-collection.")

    if not findings:
        findings.append("INFO: No anomalies detected. All sources reporting within normal parameters.")

    return findings


def build_prompt(events: list[dict[str, Any]], findings: list[str]) -> str:
    """Build the LLM prompt with event summary and anomaly findings."""
    source_counts = {}
    for e in events:
        source_counts[e["source"]] = source_counts.get(e["source"], 0) + 1

    summary_lines = [f"  - {src}: {count} events" for src, count in source_counts.items()]
    findings_text = "\n".join(f"  - {f}" for f in findings)

    return f"""You are an API health monitoring assistant. Analyze the following pipeline report and write a concise, professional summary.

            ## Pipeline Run Summary
            - Total events collected: {len(events)}
            - Sources:
            {chr(10).join(summary_lines)}

            ## Anomaly Detection Findings
            {findings_text}

            ## Instructions
            Write a short report (3-5 sentences) that:
            1. Summarizes the overall pipeline health
            2. Highlights any findings that require attention
            3. Notes any data quality observations
            4. Ends with a recommended action if any issue exists

            Be direct and technical. Do not add headers or bullet points — write in prose.
            """


def call_llm(prompt: str) -> str:
    """Send prompt to OpenRouter and return the model's response text."""
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY is not set.")

    response = httpx.post(
        OPENROUTER_API_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def run_analysis(client: Client) -> tuple[str, list[str], dict[str, int]]:
    """Full analysis cycle: fetch → detect → summarize with LLM.

    Returns (report_text, findings, event_counts_by_source).
    """
    logger.info("[analyzer] Fetching recent events...")
    events = fetch_recent_events(client, hours=24)
    logger.info("[analyzer] %d events retrieved from ClickHouse.", len(events))

    findings = detect_anomalies(events)
    for f in findings:
        logger.info("[analyzer] %s", f)

    event_counts: dict[str, int] = {}
    for e in events:
        event_counts[e["source"]] = event_counts.get(e["source"], 0) + 1

    prompt = build_prompt(events, findings)
    logger.info("[analyzer] Calling LLM for report generation...")
    report = call_llm(prompt)

    return report, findings, event_counts