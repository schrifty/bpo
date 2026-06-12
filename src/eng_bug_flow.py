"""Weekly bug inflow vs. outflow for the LEAN engineering project.

A static open-bug count cannot tell a VP whether the bug backlog is growing or
shrinking. This builds a trailing weekly *created vs. resolved* series so the slide
can show net flow and the backlog trend instead of a single snapshot number.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

logger = logging.getLogger("bpo")

if TYPE_CHECKING:  # pragma: no cover
    from .jira_client import JiraClient

_PROJECT = "LEAN"


def _iso_key(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _parse_day(raw: Any) -> date | None:
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def build_eng_bug_flow(
    client: "JiraClient",
    *,
    window_days: int = 84,
    timeout: float = 60.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Weekly bug created-vs-resolved series for LEAN over the trailing window.

    Returns a chart-ready payload with continuous (zero-filled) weeks oldest→newest,
    window totals, the current open-bug count, and a growing/shrinking/flat trend.
    """
    ref = (now or datetime.now(timezone.utc)).date()
    weeks_n = max(1, round(window_days / 7))
    this_monday = ref - timedelta(days=ref.weekday())
    anchors = [this_monday - timedelta(weeks=(weeks_n - 1 - i)) for i in range(weeks_n)]
    buckets: dict[str, dict[str, Any]] = {
        _iso_key(a): {"week": _iso_key(a), "label": a.strftime("%b %-d"), "created": 0, "resolved": 0}
        for a in anchors
    }

    try:
        issues = client._search(
            f"project = {_PROJECT} AND issuetype = Bug "
            f"AND (created >= -{int(window_days)}d OR resolutiondate >= -{int(window_days)}d)",
            max_results=5000,
            fields=["created", "resolutiondate", "resolution"],
            data_description=f"LEAN bugs created or resolved in last {window_days} days",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Bug flow fetch failed: %s", e)
        return {"error": str(e), "weeks": [], "window_days": window_days}

    for issue in issues:
        f = issue.get("fields") or {}
        cre = _parse_day(f.get("created"))
        if cre is not None:
            b = buckets.get(_iso_key(cre))
            if b is not None:
                b["created"] += 1
        if f.get("resolution"):
            res = _parse_day(f.get("resolutiondate"))
            if res is not None:
                b = buckets.get(_iso_key(res))
                if b is not None:
                    b["resolved"] += 1

    weeks: list[dict[str, Any]] = []
    for a in anchors:
        b = buckets[_iso_key(a)]
        b["net"] = b["created"] - b["resolved"]
        weeks.append(b)

    created_total = sum(w["created"] for w in weeks)
    resolved_total = sum(w["resolved"] for w in weeks)
    net_total = created_total - resolved_total

    open_now: int | None = None
    try:
        open_now = client.jql_match_count(
            f"project = {_PROJECT} AND issuetype = Bug AND resolution = EMPTY",
            data_description="LEAN open bug count",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Open bug count failed: %s", e)

    # A handful of bugs either way over 12 weeks is noise, not a trend. Treat a net
    # within ~8% of inflow (floor of 10) as "treading water" so a marginal -9 on 145
    # created doesn't get oversold as "shrinking".
    flat_band = max(10, round(0.08 * created_total))
    if net_total > flat_band:
        trend = "growing"
    elif net_total < -flat_band:
        trend = "shrinking"
    else:
        trend = "flat"

    return {
        "window_days": window_days,
        "weeks_count": weeks_n,
        "weeks": weeks,
        "created_total": created_total,
        "resolved_total": resolved_total,
        "net_total": net_total,
        "open_now": open_now,
        "avg_weekly_created": round(created_total / weeks_n, 1),
        "avg_weekly_resolved": round(resolved_total / weeks_n, 1),
        "trend": trend,
        "error": None,
    }
