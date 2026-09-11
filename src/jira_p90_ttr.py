"""P90 HELP TTR from JSM Time-to-resolution SLA (trailing window).

Value is the 90th percentile completed ``customfield_10665`` elapsed time, in
days, for HELP tickets resolved in the window.
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
    as_of=None,
    timeout: float = 60.0,  # noqa: ARG001 - search uses client timeouts
) -> dict[str, Any]:
    """Return ``{"value": <days>}`` P90 JSM TTR SLA elapsed time.

    Fails loud when Jira is unavailable or no completed TTR SLA cycles exist.
    """
    if days < 1:
        return {"error": "days must be >= 1", "days": days}

    result = client.get_help_p90_ttr(days=int(days), as_of=as_of)
    if result.get("error"):
        return result

    value = result.get("value")
    if value is None:
        return {
            "error": "HELP P90 TTR response missing value",
            "window_days": int(days),
        }

    hours = float(value)
    days_value = round(hours / 24.0, 2)
    logger.info(
        "P90 TTR: %s day(s) (%s h, measured=%s, window=%sd)",
        days_value,
        hours,
        result.get("measured"),
        days,
    )
    return {
        "value": days_value,
        "p90_hours": hours,
        "p90_ms": result.get("p90_ms"),
        "measured": result.get("measured"),
        "window_days": int(days),
        "definition": result.get("definition"),
        "sla_field": result.get("sla_field"),
    }
