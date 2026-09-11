"""Operational HELP support KPIs for the metrics registry.

Month-close volume, throughput, and escalation counts share the previous
calendar month. Open HELP stock is a point-in-time snapshot on *as_of*.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .eng_scorecard_metrics import (
    _jql_half_open_day_range,
    _previous_calendar_month_bounds,
)
from .jira_client import JiraClient

logger = logging.getLogger("cortex")

_HELP_TRANSIENT = "NOT (labels = Outage OR labels = Healthcheck)"
_CUSTOMER_LEAN_EXCL = "issuetype not in (Epic, SUT)"
OPEN_HELP_AGE_DAYS = 30


def _count_or_error(
    client: JiraClient,
    jql: str,
    *,
    label: str,
) -> dict[str, Any]:
    count = client.jql_match_count(jql, data_description=label)
    if count is None:
        return {
            "error": (
                f"Jira count unavailable for {label} "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            ),
            "jql": jql,
        }
    return {"value": int(count), "jql": jql}


def _as_of_datetime(as_of: date | datetime | None) -> datetime | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None:
            return as_of.replace(tzinfo=timezone.utc)
        return as_of.astimezone(timezone.utc)
    return datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc)


def _as_of_iso(as_of: date | datetime | None) -> str | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        dt = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date().isoformat()
    return as_of.isoformat()[:10]


def _weekday_count(start: datetime, end: datetime) -> int:
    """Inclusive-of-start, exclusive-of-end weekday count (Mon–Fri)."""
    n = 0
    day = start.date()
    stop = end.date()
    while day < stop:
        if day.weekday() < 5:
            n += 1
        day += timedelta(days=1)
    return n


def _snapshot_day_and_end(as_of: date | datetime | None) -> tuple[date, datetime]:
    dt = _as_of_datetime(as_of) or datetime.now(timezone.utc)
    day = dt.date()
    end = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1)
    return day, end


def help_ticket_count_jql(*, as_of: date | datetime | None = None) -> str:
    """HELP tickets created in the previous completed calendar month."""
    start, end, _month = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    return (
        f"project = HELP AND {_HELP_TRANSIENT} AND "
        + _jql_half_open_day_range("createdDate", start, end)
    )


def get_help_ticket_count(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """HELP tickets created in the previous calendar month (excl. Outage/Healthcheck)."""
    start, end, month_key = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    jql = help_ticket_count_jql(as_of=as_of)
    counted = _count_or_error(
        client, jql, label=f"HELP tickets created {month_key}"
    )
    if counted.get("error"):
        return counted
    value = int(counted["value"])
    weekdays = _weekday_count(start, end)
    per_day = round(value / weekdays, 2) if weekdays else None
    logger.info("Ticket Count: %s HELP ticket(s) created in %s", value, month_key)
    return {
        "value": value,
        "jql": jql,
        "month": month_key,
        "method": "actual_previous_month",
        "as_of": start.date().isoformat(),
        "created_start": start.date().isoformat(),
        "created_end_exclusive": end.date().isoformat(),
        "business_days": weekdays,
        "created_per_business_day": per_day,
    }


def get_help_resolved_created_ratio(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    days: int | None = None,  # noqa: ARG001 — month-close; kept for invoker compat
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """HELP resolved ÷ created × 100 for the previous calendar month."""
    start, end, month_key = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    created_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND "
        + _jql_half_open_day_range("createdDate", start, end)
    )
    resolved_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND resolution is not EMPTY AND "
        + _jql_half_open_day_range("resolved", start, end)
    )
    created = _count_or_error(
        client, created_jql, label=f"HELP created {month_key}"
    )
    if created.get("error"):
        return created
    resolved = _count_or_error(
        client, resolved_jql, label=f"HELP resolved {month_key}"
    )
    if resolved.get("error"):
        return resolved

    created_n = int(created["value"])
    resolved_n = int(resolved["value"])
    if created_n <= 0:
        return {
            "error": (
                f"HELP created count is 0 in {month_key} — "
                "cannot compute Resolved / Created"
            ),
            "created": created_n,
            "resolved": resolved_n,
            "month": month_key,
        }

    pct = round(100.0 * resolved_n / created_n, 2)
    logger.info(
        "HELP Resolved / Created: %s / %s = %s%% (%s)",
        resolved_n,
        created_n,
        pct,
        month_key,
    )
    return {
        "value": pct,
        "numerator": float(resolved_n),
        "denominator": float(created_n),
        "resolved": resolved_n,
        "created": created_n,
        "month": month_key,
        "method": "actual_previous_month",
        "created_jql": created_jql,
        "resolved_jql": resolved_jql,
        "created_start": start.date().isoformat(),
        "created_end_exclusive": end.date().isoformat(),
        "as_of": start.date().isoformat(),
    }


def _escalation_month(
    client: JiraClient,
    *,
    project: str,
    rate_label: str,
    as_of: date | datetime | None,
) -> dict[str, Any]:
    start, end, month_key = _previous_calendar_month_bounds(_as_of_datetime(as_of))
    range_clause = _jql_half_open_day_range("createdDate", start, end)
    help_created_jql = f"project = HELP AND {_HELP_TRANSIENT} AND {range_clause}"
    escalated_jql = (
        f'project = {project} AND labels = "jira_escalated" AND {_CUSTOMER_LEAN_EXCL} '
        f"AND {range_clause}"
    )
    help_created = _count_or_error(
        client, help_created_jql, label=f"HELP created {month_key}"
    )
    if help_created.get("error"):
        return help_created
    escalated = _count_or_error(
        client,
        escalated_jql,
        label=f"{project} jira_escalated created {month_key}",
    )
    if escalated.get("error"):
        return escalated
    return {
        "help_created": int(help_created["value"]),
        "escalated": int(escalated["value"]),
        "project": project,
        "month": month_key,
        "method": "actual_previous_month",
        "help_created_jql": help_created_jql,
        "escalated_jql": escalated_jql,
        "created_start": start.date().isoformat(),
        "created_end_exclusive": end.date().isoformat(),
        "as_of": start.date().isoformat(),
        "rate_label": rate_label,
    }


def get_engineering_escalation_count(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """LEAN ``jira_escalated`` tickets created in the previous calendar month."""
    raw = _escalation_month(
        client, project="LEAN", rate_label="Engineering Escalations", as_of=as_of
    )
    if raw.get("error"):
        return raw
    value = int(raw["escalated"])
    logger.info(
        "Engineering Escalations: %s LEAN in %s", value, raw["month"]
    )
    return {**raw, "value": value, "numerator": float(value), "denominator": 1.0}


def get_data_escalation_count(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """CUSTOMER ``jira_escalated`` tickets created in the previous calendar month."""
    raw = _escalation_month(
        client, project="CUSTOMER", rate_label="Data Escalations", as_of=as_of
    )
    if raw.get("error"):
        return raw
    value = int(raw["escalated"])
    logger.info("Data Escalations: %s CUSTOMER in %s", value, raw["month"])
    return {**raw, "value": value, "numerator": float(value), "denominator": 1.0}


def get_engineering_escalation_rate(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    days: int | None = None,  # noqa: ARG001
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """LEAN ``jira_escalated`` created ÷ HELP created (previous calendar month)."""
    return _escalation_rate_from_month(
        client, project="LEAN", rate_label="Engineering Escalation Rate", as_of=as_of
    )


def get_data_escalation_rate(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    days: int | None = None,  # noqa: ARG001
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """CUSTOMER ``jira_escalated`` created ÷ HELP created (previous calendar month)."""
    return _escalation_rate_from_month(
        client, project="CUSTOMER", rate_label="Data Escalation Rate", as_of=as_of
    )


def _escalation_rate_from_month(
    client: JiraClient,
    *,
    project: str,
    rate_label: str,
    as_of: date | datetime | None,
) -> dict[str, Any]:
    raw = _escalation_month(
        client, project=project, rate_label=rate_label, as_of=as_of
    )
    if raw.get("error"):
        return raw
    help_n = int(raw["help_created"])
    escalated_n = int(raw["escalated"])
    if help_n <= 0:
        return {
            "error": (
                f"HELP created count is 0 in {raw['month']} — "
                f"cannot compute {rate_label}"
            ),
            **raw,
        }
    pct = round(100.0 * escalated_n / help_n, 2)
    logger.info(
        "%s: %s %s / %s HELP created = %s%% (%s)",
        rate_label,
        escalated_n,
        project,
        help_n,
        pct,
        raw["month"],
    )
    return {
        **raw,
        "value": pct,
        "numerator": float(escalated_n),
        "denominator": float(help_n),
    }


def open_help_jql(
    *,
    as_of: date | datetime | None = None,
    min_age_days: int | None = None,
) -> str:
    """HELP still open at the end of *as_of*'s calendar day."""
    day, end = _snapshot_day_and_end(as_of)
    end_s = end.strftime("%Y-%m-%d")
    parts = [
        f"project = HELP AND {_HELP_TRANSIENT}",
        f'createdDate < "{end_s}"',
        f'(resolution is EMPTY OR resolved >= "{end_s}")',
    ]
    if min_age_days is not None:
        cutoff = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) - timedelta(
            days=int(min_age_days)
        )
        parts.append(f'createdDate < "{cutoff.strftime("%Y-%m-%d")}"')
    return " AND ".join(parts)


def get_open_help(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """Open HELP stock at end of *as_of* (excl. Outage/Healthcheck)."""
    day, _end = _snapshot_day_and_end(as_of)
    jql = open_help_jql(as_of=as_of)
    counted = _count_or_error(client, jql, label=f"Open HELP as of {day.isoformat()}")
    if counted.get("error"):
        return counted
    value = int(counted["value"])
    logger.info("Open HELP: %s ticket(s) open as of %s", value, day.isoformat())
    return {
        "value": value,
        "jql": jql,
        "as_of": day.isoformat(),
        "method": "open_as_of",
    }


def get_open_help_over_30d_pct(
    client: JiraClient,
    *,
    as_of: date | datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """Share of open HELP whose created date is more than 30 days before *as_of*."""
    day, _end = _snapshot_day_and_end(as_of)
    open_jql = open_help_jql(as_of=as_of)
    aged_jql = open_help_jql(as_of=as_of, min_age_days=OPEN_HELP_AGE_DAYS)
    opened = _count_or_error(client, open_jql, label=f"Open HELP as of {day.isoformat()}")
    if opened.get("error"):
        return opened
    aged = _count_or_error(
        client,
        aged_jql,
        label=f"Open HELP >{OPEN_HELP_AGE_DAYS}d as of {day.isoformat()}",
    )
    if aged.get("error"):
        return aged
    open_n = int(opened["value"])
    aged_n = int(aged["value"])
    if open_n <= 0:
        return {
            "error": (
                f"Open HELP count is 0 as of {day.isoformat()} — "
                "cannot compute Open HELP >30d %"
            ),
            "open": open_n,
            "aged": aged_n,
            "as_of": day.isoformat(),
        }
    pct = round(100.0 * aged_n / open_n, 2)
    logger.info(
        "Open HELP >30d %%: %s / %s = %s%% as of %s",
        aged_n,
        open_n,
        pct,
        day.isoformat(),
    )
    return {
        "value": pct,
        "numerator": float(aged_n),
        "denominator": float(open_n),
        "open": open_n,
        "aged": aged_n,
        "as_of": day.isoformat(),
        "age_days": OPEN_HELP_AGE_DAYS,
        "open_jql": open_jql,
        "aged_jql": aged_jql,
        "method": "open_as_of",
    }
