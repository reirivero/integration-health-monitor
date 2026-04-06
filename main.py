import logging
from dotenv import load_dotenv

load_dotenv()

from src.collector.football import FootballCollector
from src.collector.spotify import SpotifyCollector
from src.db.clickhouse import get_client, setup_table, insert_events
from src.agent.analyzer import run_analysis
from src.reporter.markdown import save_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """Execute one full collection cycle across all sources."""
    logger.info("Pipeline started.")

    client = get_client()
    setup_table(client)

    collectors = [
        FootballCollector(),
        SpotifyCollector(),
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

    # Run analysis on accumulated data
    logger.info("Starting analysis cycle...")
    report, findings, event_counts = run_analysis(client)

    filepath = save_report(report, findings, event_counts)

    print("\n" + "="*60)
    print("PIPELINE HEALTH REPORT")
    print("="*60)
    print(report)
    print(f"\nFull report saved to: {filepath}")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_pipeline()