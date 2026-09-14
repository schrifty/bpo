"""Customer-Reported Bugs: LEAN ``Bug`` issues customers escalated.

Month-close inflow of customer-escalated bugs (``labels = jira_escalated``) for the
KPI registry, plus a trailing-window aggregate for the engineering review deck.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from .eng_scorecard_metrics import (
    _jql_half_open_day_range,
    _previous_calendar_month_bounds,
)
from .jira_client import JiraClient, _jql_escape_string

logger = logging.getLogger("cortex")

# Customers report bugs through support; the ones escalated into engineering carry this
# label on the LEAN project. It is the only customer-reported signal the engineering
# deck uses — raw HELP desk volume belongs to the support decks.
CUSTOMER_REPORTED_LABEL = "jira_escalated"
_CRITICAL_PRIORITIES = ("Blocker", "Critical")


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
        + f' AND labels = "{CUSTOMER_REPORTED_LABEL}"'
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


def customer_reported_bugs_window_jql(*, days: int) -> str:
    """LEAN escalated bugs created in the trailing *days* window."""
    return (
        f'project = LEAN AND issuetype = Bug AND labels = "{CUSTOMER_REPORTED_LABEL}" '
        f"AND created >= -{int(days)}d"
    )


def build_customer_reported_bug_pressure(
    client: JiraClient,
    *,
    days: int = 30,
    timeout: float = 60.0,  # noqa: ARG001 - search uses the client's own timeout
) -> dict[str, Any]:
    """Trailing-window aggregate of bugs customers escalated into engineering.

    Returns reported/open/resolved counts, the open blocker-critical count, and a
    priority mix with per-priority Jira links for the engineering review deck.
    """
    base = customer_reported_bugs_window_jql(days=days)
    try:
        raw = client._search(
            f"{base} ORDER BY created DESC",
            max_results=2000,
            fields=["summary", "status", "priority", "created", "resolution", "labels"],
            data_description=f"LEAN customer-reported bugs (created last {days} days)",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Customer-reported bug fetch failed: %s", e)
        return {"error": str(e), "total": 0, "days": days}

    by_priority: dict[str, int] = {}
    priority_full_name: dict[str, str] = {}
    open_count = 0
    open_blocker_critical = 0
    for issue in raw:
        fields = issue.get("fields") or {}
        full = ((fields.get("priority") or {}).get("name") or "").strip()
        short = (full.split(":")[0] if ":" in full else full) or "Unknown"
        by_priority[short] = by_priority.get(short, 0) + 1
        if full and short not in priority_full_name:
            priority_full_name[short] = full
        if not fields.get("resolution"):
            open_count += 1
            if short in _CRITICAL_PRIORITIES:
                open_blocker_critical += 1

    aggregate_jql = f"{base} ORDER BY created DESC"
    jql_by_priority_short: dict[str, str] = {}
    for short in by_priority:
        full = priority_full_name.get(short)
        if short == "Unknown" or not full:
            jql_by_priority_short[short] = (
                f"{base} AND priority is EMPTY ORDER BY created DESC"
                if short == "Unknown"
                else aggregate_jql
            )
        else:
            jql_by_priority_short[short] = (
                f'{base} AND priority = "{_jql_escape_string(full)}" ORDER BY created DESC'
            )

    logger.info(
        "Customer-reported bug pressure: %s reported / %s open (last %sd)",
        len(raw),
        open_count,
        days,
    )
    return {
        "total": len(raw),
        "open": open_count,
        "resolved": len(raw) - open_count,
        "open_blocker_critical": open_blocker_critical,
        "by_priority": dict(sorted(by_priority.items(), key=lambda x: -x[1])),
        "jql_by_priority_short": jql_by_priority_short,
        "aggregate_jql": aggregate_jql,
        "label": CUSTOMER_REPORTED_LABEL,
        "days": days,
        "error": None,
    }
