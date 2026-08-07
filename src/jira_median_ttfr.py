"""Median HELP TTFR from JSM Time-to-first-response SLA.

Value is the median completed ``customfield_10666`` elapsed time, in hours, for
HELP tickets resolved in a trailing window. Written to LeanDNA by ``metrics-upsert``.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

DEFAULT_MEDIAN_TTFR_DAYS = 30


def get_median_ttfr(
    client: JiraClient,
    *,
    days: int = DEFAULT_MEDIAN_TTFR_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - search uses client timeouts
) -> dict[str, Any]:
    """Return ``{"value": <hours>}`` median JSM TTFR SLA elapsed time.

    Fails loud (``{"error": ...}``) when Jira is unavailable or no completed
    TTFR SLA cycles exist in the window so ``metrics-upsert`` does not write a
    placeholder.
    """
    if days < 1:
        return {"error": "days must be >= 1", "days": days}

    result = client.get_help_median_ttfr(days=int(days))
    if result.get("error"):
        return result

    value = result.get("value")
    if value is None:
        return {
            "error": "HELP median TTFR response missing value",
            "window_days": int(days),
        }

    logger.info(
        "Median TTFR: %s hour(s) (measured=%s, window=%sd)",
        value,
        (result.get("ttfr") or {}).get("measured"),
        days,
    )
    return {
        "value": int(value),
        "median_hours": result.get("median_hours"),
        "median_ms": result.get("median_ms"),
        "measured": (result.get("ttfr") or {}).get("measured"),
        "window_days": int(days),
        "definition": result.get("definition"),
        "sla_field": result.get("sla_field"),
    }
