"""Open customer-reported LEAN bugs: live stock and month-end stock.

The live metric counts unresolved ``Bug`` issues in the LEAN engineering project
that are in active engineering statuses—the same slice the engineering portfolio
**Bug Health** slide uses (``open_bugs`` from in-flight LEAN work). That value is
written to LeanDNA metric 2035 (**Open Customer-Reported Bugs**).

**Customer-Reported Bugs** (month-close) is end-of-month stock: the daily open
count on the last calendar day of the previous month, read from the KPI store.
Historical daily points use Jira ``status WAS IN (…) ON \"YYYY-MM-DD\"``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .jira_client import JiraClient
from .kpi_store import (
    CUSTOMER_REPORTED_BUGS_METRIC,
    GRAIN_DAILY,
    OPEN_CUSTOMER_REPORTED_BUGS_METRIC,
    connect,
    get_open_customer_reported_bugs_eom,
    previous_calendar_month_end,
)
from .kpi_store_s3 import prepare_kpi_store_for_read

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


def _as_of_date(as_of: date | datetime | None) -> date | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        return as_of.date()
    return as_of


def customer_reported_bugs_jql(*, as_of: date | datetime | None = None) -> str:
    """Live ``status in`` for today; ``status WAS IN … ON`` for a past calendar day."""
    as_of_d = _as_of_date(as_of)
    if as_of_d is None or as_of_d >= date.today():
        return CUSTOMER_REPORTED_BUGS_JQL
    return (
        f"project = LEAN AND issuetype = Bug "
        f'AND status WAS IN ({_STATUS_JQL}) ON "{as_of_d.isoformat()}"'
    )


def get_customer_reported_bug_count(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001 - count endpoint uses its own fixed timeout
) -> dict[str, Any]:
    """Return ``{"value": <count>}`` of open LEAN bugs in active engineering statuses.

    *as_of* in the past uses Jira history (``WAS ON``). Today or omitted uses current
    status. Fails loud when the Jira count endpoint is unavailable so
    ``metrics-upsert`` does not write a placeholder value.
    """
    jql = customer_reported_bugs_jql(as_of=as_of)
    as_of_d = _as_of_date(as_of)
    count = client.jql_match_count(
        jql,
        data_description="Open Customer-Reported Bugs (open LEAN Bug issues)",
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Open Customer-Reported Bugs "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            )
        }
    logger.info(
        "Open Customer-Reported Bugs: %s open LEAN bug(s) as_of=%s",
        count,
        as_of_d.isoformat() if as_of_d else "today",
    )
    payload: dict[str, Any] = {
        "value": int(count),
        "jql": jql,
        "statuses": list(_CUSTOMER_REPORTED_BUG_STATUSES),
    }
    if as_of_d is not None:
        payload["as_of"] = as_of_d.isoformat()
    return payload


def get_customer_reported_bugs_eom(
    *,
    as_of: date | datetime | None = None,
    db_path: str | Path | None = None,
    skip_s3: bool = False,
) -> dict[str, Any]:
    """Return previous-calendar-month EOM open-bug stock from the daily KPI store.

    Fails loud when the last day of that month has no successful daily snapshot
    of **Open Customer-Reported Bugs** (or the pre-split daily name).
    """
    if as_of is None:
        as_of_d = date.today()
    elif isinstance(as_of, datetime):
        as_of_d = as_of.date()
    else:
        as_of_d = as_of
    eom = previous_calendar_month_end(as_of_d)
    month_key = f"{eom.year:04d}-{eom.month:02d}"
    path = prepare_kpi_store_for_read(db_path, skip_s3=skip_s3)
    conn = connect(path)
    try:
        row = get_open_customer_reported_bugs_eom(conn, as_of_d)
    finally:
        conn.close()
    if row is None:
        return {
            "error": (
                f"No daily {OPEN_CUSTOMER_REPORTED_BUGS_METRIC} snapshot for "
                f"{eom.isoformat()} (EOM stock for {month_key} "
                f"{CUSTOMER_REPORTED_BUGS_METRIC})"
            )
        }
    err = (row.observation.error or "").strip()
    if err:
        return {
            "error": (
                f"Daily {row.metric_name} snapshot for {eom.isoformat()} failed: {err}"
            )
        }
    if row.observation.value is None:
        return {
            "error": (
                f"Daily {row.metric_name} snapshot for {eom.isoformat()} has no value"
            )
        }
    value = int(row.observation.value)
    logger.info(
        "%s: %s open LEAN bug(s) on %s (source=%s %s/%s)",
        CUSTOMER_REPORTED_BUGS_METRIC,
        value,
        eom.isoformat(),
        row.metric_name,
        GRAIN_DAILY,
        eom.isoformat(),
    )
    return {
        "value": value,
        "as_of": eom.isoformat(),
        "source_metric": row.metric_name,
        "source_grain": GRAIN_DAILY,
        "source_period_key": eom.isoformat(),
    }
