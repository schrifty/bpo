"""Median HELP TTR from JSM Time-to-resolution SLA (LeanDNA metric 2171).

Value is the median completed ``customfield_10665`` elapsed time, in hours, for
HELP tickets resolved in a trailing window. Written to LeanDNA by ``metrics-upsert``.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

DEFAULT_MEDIAN_TTR_DAYS = 30


def get_median_ttr(
    client: JiraClient,
    *,
    days: int = DEFAULT_MEDIAN_TTR_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - search uses client timeouts
) -> dict[str, Any]:
    """Return ``{"value": <hours>}`` median JSM TTR SLA elapsed time.

    Fails loud (``{"error": ...}``) when Jira is unavailable or no completed
    TTR SLA cycles exist in the window so ``metrics-upsert`` does not write a
    placeholder.
    """
    if days < 1:
        return {"error": "days must be >= 1", "days": days}

    result = client.get_help_median_ttr(days=int(days))
    if result.get("error"):
        return result

    value = result.get("value")
    if value is None:
        return {
            "error": "HELP median TTR response missing value",
            "window_days": int(days),
        }

    logger.info(
        "Median TTR: %s hour(s) (measured=%s, window=%sd)",
        value,
        (result.get("ttr") or {}).get("measured"),
        days,
    )
    return {
        "value": int(value),
        "median_hours": result.get("median_hours"),
        "median_ms": result.get("median_ms"),
        "measured": (result.get("ttr") or {}).get("measured"),
        "window_days": int(days),
        "definition": result.get("definition"),
        "sla_field": result.get("sla_field"),
    }
