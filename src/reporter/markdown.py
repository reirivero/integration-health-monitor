import os
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

REPORTS_DIR = "reports"


def save_report(
    report_text: str,
    findings: list[str],
    event_counts: dict[str, int],
) -> str:
    """Save the pipeline report as a Markdown file with timestamp.

    Returns the path of the saved file.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)

    now = datetime.now(timezone.utc)
    filename = now.strftime("%Y-%m-%d_%H-%M") + "_pipeline_report.md"
    filepath = os.path.join(REPORTS_DIR, filename)

    total = sum(event_counts.values())
    counts_md = "\n".join(f"| {src} | {count} |" for src, count in event_counts.items())
    findings_md = "\n".join(f"- {f}" for f in findings)

    content = f"""# Pipeline Health Report
**Generated:** {now.strftime("%Y-%m-%d %H:%M UTC")}

## Event Summary
| Source | Events |
|--------|--------|
{counts_md}
| **Total** | **{total}** |

## Anomaly Findings
{findings_md}

## LLM Analysis
{report_text}
"""

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    logger.info("[reporter] Report saved to %s", filepath)
    return filepath