"""Count of open customer-reported LEAN bugs (LeanDNA metric 2035).

The metric counts unresolved ``Bug`` issues in the LEAN engineering project that
are in active engineering statuses—the same slice the engineering portfolio
**Bug Health** slide uses (``open_bugs`` from in-flight LEAN work).

The count is the value written to LeanDNA by ``metrics-upsert``.
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

# Active LEAN engineering statuses (matches get_engineering_portfolio in-flight JQL).
_CUSTOMER_REPORTED_BUG_STATUSES: tuple[str, ...] = (
    "In Progress",
    "In Review",
    "Open",
    "Reopened",
)

_STATUS_JQL = ", ".join(f'"{s}"' for s in _CUSTOMER_REPORTED_BUG_STATUSES)

CUSTOMER_REPORTED_BUGS_JQL: str = (
    f"project = LEAN AND issuetype = Bug AND status in ({_STATUS_JQL})"
)


def get_customer_reported_bug_count(
    client: JiraClient,
    *,
    timeout: float = 60.0,  # noqa: ARG001 - count endpoint uses its own fixed timeout
) -> dict[str, Any]:
    """Return ``{"value": <count>}`` of open LEAN bugs in active engineering statuses.

    Fails loud (``{"error": ...}``) when the Jira count endpoint is unavailable so
    ``metrics-upsert`` does not write a placeholder value.
    """
    count = client.jql_match_count(
        CUSTOMER_REPORTED_BUGS_JQL,
        data_description="Customer-Reported Bugs (open LEAN Bug issues)",
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Customer-Reported Bugs "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            )
        }
    logger.info("Customer-Reported Bugs: %s open LEAN bug(s)", count)
    return {
        "value": int(count),
        "jql": CUSTOMER_REPORTED_BUGS_JQL,
        "statuses": list(_CUSTOMER_REPORTED_BUG_STATUSES),
    }
