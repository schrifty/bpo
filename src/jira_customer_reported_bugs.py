"""Customer-Reported Bugs: LEAN ``Bug`` issues created last calendar month.

Month-close inflow of customer-escalated bugs (``labels = jira_escalated``).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from .eng_scorecard_metrics import (
    _jql_half_open_day_range,
    _previous_calendar_month_bounds,
)
from .jira_client import JiraClient

logger = logging.getLogger("cortex")


def _as_of_datetime(as_of: date | datetime | None) -> datetime | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None:
            return as_of.replace(tzinfo=timezone.utc)
        return as_of.astimezone(timezone.utc)
    return datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc)


def customer_reported_bugs_created_jql(*, as_of: date | datetime | None = None) -> str:
    """LEAN escalated bugs created in the previous completed calendar month."""
    start, end, _month = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    return (
        "project = LEAN AND type = Bug AND "
        + _jql_half_open_day_range("createdDate", start, end)
        + ' AND labels = "jira_escalated"'
    )


def get_customer_reported_bugs_created(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001 - count endpoint uses its own fixed timeout
) -> dict[str, Any]:
    """Return how many LEAN ``Bug`` issues with ``jira_escalated`` were created
    in the previous calendar month.

    Matches the board query shape
    ``project = LEAN AND type = Bug AND createdDate >= … AND createdDate < …
    AND labels = jira_escalated``. Fails loud when the Jira count endpoint is
    unavailable so ``metrics-upsert`` and ``kpi-snapshot`` do not write a
    placeholder.
    """
    start, end, month_key = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    jql = customer_reported_bugs_created_jql(as_of=as_of)
    count = client.jql_match_count(
        jql,
        data_description=(
            f"Customer-Reported Bugs (LEAN jira_escalated Bugs created {month_key})"
        ),
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Customer-Reported Bugs "
                f"(LEAN jira_escalated Bugs created {month_key}; "
                "approximate-count returned no count)"
            )
        }
    value = int(count)
    logger.info(
        "Customer-Reported Bugs: %s LEAN jira_escalated bug(s) created in %s",
        value,
        month_key,
    )
    return {
        "value": value,
        "jql": jql,
        "month": month_key,
        "method": "actual_previous_month",
        "as_of": start.date().isoformat(),
        "created_start": start.date().isoformat(),
        "created_end_exclusive": end.date().isoformat(),
    }


# Registry / grain alias — month-close is created-in-month, not EOM stock.
get_customer_reported_bugs_eom = get_customer_reported_bugs_created
