"""P90 HELP TTR from JSM Time-to-resolution SLA (trailing window).

Value is the 90th percentile completed ``customfield_10665`` elapsed time, in
hours, for HELP tickets resolved in the window.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

DEFAULT_P90_TTR_DAYS = 30


def get_p90_ttr(
    client: JiraClient,
    *,
    days: int = DEFAULT_P90_TTR_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - search uses client timeouts
) -> dict[str, Any]:
    """Return ``{"value": <hours>}`` P90 JSM TTR SLA elapsed time.

    Fails loud when Jira is unavailable or no completed TTR SLA cycles exist.
    """
    if days < 1:
        return {"error": "days must be >= 1", "days": days}

    result = client.get_help_p90_ttr(days=int(days))
    if result.get("error"):
        return result

    value = result.get("value")
    if value is None:
        return {
            "error": "HELP P90 TTR response missing value",
            "window_days": int(days),
        }

    logger.info(
        "P90 TTR: %s hour(s) (measured=%s, window=%sd)",
        value,
        result.get("measured"),
        days,
    )
    return {
        "value": int(value),
        "p90_hours": result.get("p90_hours"),
        "p90_ms": result.get("p90_ms"),
        "measured": result.get("measured"),
        "window_days": int(days),
        "definition": result.get("definition"),
        "sla_field": result.get("sla_field"),
    }
