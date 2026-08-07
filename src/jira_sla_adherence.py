"""HELP SLA adherence % from completed JSM TTFR/TTR cycles.

Among HELP tickets resolved in a trailing window with at least one completed
TTFR or TTR SLA cycle, the percent that did not breach any measured cycle.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

DEFAULT_SLA_ADHERENCE_DAYS = 30


def get_sla_adherence(
    client: JiraClient,
    *,
    days: int = DEFAULT_SLA_ADHERENCE_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - search uses client timeouts
) -> dict[str, Any]:
    """Return ``{"value": <percent>}`` HELP SLA adherence.

    Fails loud when Jira is unavailable or no completed SLA cycles exist in the
    window so ``metrics-upsert`` does not write a placeholder.
    """
    if days < 1:
        return {"error": "days must be >= 1", "days": days}

    result = client.get_help_sla_adherence(days=int(days))
    if result.get("error"):
        return result

    value = result.get("value")
    if value is None:
        return {
            "error": "HELP SLA adherence response missing value",
            "window_days": int(days),
        }

    adherence = result.get("sla_adherence") or {}
    logger.info(
        "SLA Adherence: %s%% (met=%s / measured=%s, window=%sd)",
        value,
        adherence.get("met"),
        adherence.get("measured"),
        days,
    )
    measured = adherence.get("measured")
    met = adherence.get("met")
    return {
        "value": float(value),
        "numerator": result.get("numerator"),
        "denominator": result.get("denominator"),
        "met": met,
        "measured": measured,
        "breached": (
            int(measured) - int(met)
            if measured is not None and met is not None
            else None
        ),
        "waiting": adherence.get("waiting"),
        "window_days": int(days),
        "definition": result.get("definition"),
    }
