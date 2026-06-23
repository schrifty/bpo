"""Count of open HELP tickets beyond service thresholds (LeanDNA metric 2028).

The metric counts unresolved high-priority HELP (JSM) tickets, excluding
operational noise labelled ``Outage`` / ``healthcheck``. The count is the value
written to LeanDNA by ``metrics-upsert``.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

# Priorities that count as "beyond service thresholds".
SERVICE_THRESHOLD_PRIORITIES: tuple[str, ...] = (
    "Critical: Significant operational impact",
    "Blocker: The platform is completely down",
    "Major: Workaround available, not essential",
)

# Exact JQL used to count tickets (no ORDER BY needed for a count).
SERVICE_THRESHOLD_TICKETS_JQL: str = (
    "project = HELP "
    "AND statusCategory NOT IN (Done) "
    "AND ((labels IS EMPTY) OR (labels not in (Outage, healthcheck))) "
    'AND (priority IN ("Critical: Significant operational impact", '
    '"Blocker: The platform is completely down", '
    '"Major: Workaround available, not essential"))'
)


def get_service_threshold_ticket_count(
    client: JiraClient,
    *,
    timeout: float = 60.0,  # noqa: ARG001 - count endpoint uses its own fixed timeout
) -> dict[str, Any]:
    """Return ``{"value": <count>}`` of open HELP tickets beyond service thresholds.

    Fails loud (``{"error": ...}``) when the Jira count endpoint is unavailable so
    ``metrics-upsert`` does not write a placeholder value.
    """
    count = client.jql_match_count(
        SERVICE_THRESHOLD_TICKETS_JQL,
        data_description="Tickets Beyond Service Thresholds (open HELP, high priority)",
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Tickets Beyond Service Thresholds "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            )
        }
    logger.info("Tickets Beyond Service Thresholds: %s open HELP ticket(s)", count)
    return {
        "value": int(count),
        "jql": SERVICE_THRESHOLD_TICKETS_JQL,
        "priorities": list(SERVICE_THRESHOLD_PRIORITIES),
    }
