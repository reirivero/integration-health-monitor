import logging
from dotenv import load_dotenv

load_dotenv()

from src.collector.spotify import SpotifyCollector
from src.collector.football import FootballCollector
from src.db.clickhouse import get_client, setup_table, insert_events


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

def run_pipeline() -> None:
    """Execute one full collection cycle across all sources."""
    logger.info("Pipeline started.")

    client = get_client()
    setup_table(client)

    collectors = [
        SpotifyCollector(),
        FootballCollector(),
    ]

    all_events: list[dict] = []

    for collector in collectors:
        try:
            events = collector.collect()
            all_events.extend(events)
            logger.info("[%s] %d events collected.", collector.name, len(events))
        except Exception as e:
            logger.error("[%s] Collection failed: %s", collector.name, e)

    if all_events:
        insert_events(client, all_events)

    logger.info("Pipeline finished. Total events inserted: %d", len(all_events))

if __name__ == "__main__":
    run_pipeline()