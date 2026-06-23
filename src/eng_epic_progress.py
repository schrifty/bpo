"""Active epic ("big rock") progress for the LEAN engineering project.

The deck otherwise has no line of sight from day-to-day activity to the larger
initiatives. This summarizes the *in-flight* epics by remaining work and recent
movement so a VP can see whether the big rocks are actually moving — and which
ones are at risk — rather than near-done maintenance umbrellas.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

logger = logging.getLogger("cortex")

if TYPE_CHECKING:  # pragma: no cover
    from .jira_client import JiraClient

# Engineering initiatives live in both the core SDLC project (LEAN) and the customer
# implementation/escalation project (CUSTOMER) — the same scope the Team Scorecard and
# Velocity slides already report (CUSTOMER Active Scrum + Data Integration board).
_PROJECTS = ("LEAN", "CUSTOMER")

# Selection thresholds. Ranking by raw child count promotes large maintenance
# umbrellas (e.g. "Data Pipeline Maintenance", "LC Write Back") over real
# initiatives, so we exclude near-complete epics and oversized umbrellas and rank
# what remains by open work + recent activity.
_NEAR_DONE_PCT = 95          # >= this % complete is effectively finished, not a "rock"
_MIN_REMAINING = 3           # fewer than this many open children → not worth tracking
_UMBRELLA_TOTAL_CAP = 120    # epics larger than this are maintenance buckets, not initiatives
_RECENT_DAYS = 30            # child activity window used as the "is it moving" signal


def _days_since(raw: Any, *, now: datetime) -> int | None:
    if not raw:
        return None
    try:
        dt = datetime.strptime(str(raw)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    return (now - dt).days


def build_eng_epic_progress(
    client: "JiraClient",
    *,
    max_epics: int = 8,
    candidate_cap: int = 50,
    umbrella_total_cap: int = _UMBRELLA_TOTAL_CAP,
    near_done_pct: int = _NEAR_DONE_PCT,
    min_remaining: int = _MIN_REMAINING,
    recent_days: int = _RECENT_DAYS,
    timeout: float = 60.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Top in-flight LEAN epics by remaining work + recent activity, with risk flags.

    Progress is done child issues ÷ total child issues (``parent = EPIC``). Epics that
    are near-complete, barely have open work, or are oversized maintenance umbrellas are
    excluded so the slide shows the initiatives actually in flight. ``active_30d`` (child
    issues touched in the last ``recent_days``) drives a due-date-free "stalled" flag.
    """
    ref = now or datetime.now(timezone.utc)
    proj_clause = ", ".join(_PROJECTS)
    try:
        epics = client._search(
            f"project in ({proj_clause}) AND issuetype = Epic AND statusCategory != Done ORDER BY updated DESC",
            max_results=candidate_cap,
            fields=["summary", "status", "duedate", "assignee", "updated"],
            data_description="LEAN + CUSTOMER active epics",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Epic fetch failed: %s", e)
        return {"error": str(e), "epics": []}

    def _counts(key: str) -> tuple[str, int | None, int | None, int | None]:
        total = client.jql_match_count(f"parent = {key}", data_description="epic child count")
        done = client.jql_match_count(
            f"parent = {key} AND statusCategory = Done", data_description="epic done-child count"
        )
        active = client.jql_match_count(
            f"parent = {key} AND updated >= -{int(recent_days)}d",
            data_description="epic recently-active child count",
        )
        return key, total, done, active

    counts: dict[str, tuple[int | None, int | None, int | None]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_counts, e["key"]) for e in epics if e.get("key")]
        for fut in as_completed(futures):
            try:
                key, total, done, active = fut.result()
                counts[key] = (total, done, active)
            except Exception as e:  # noqa: BLE001
                logger.warning("Epic child count failed: %s", e)

    rows: list[dict[str, Any]] = []
    any_due = False
    for e in epics:
        key = e.get("key")
        f = e.get("fields") or {}
        total, done, active = counts.get(key, (None, None, None))
        if not total:
            continue
        done = done or 0
        active = active or 0
        remaining = total - done
        pct = round(done / total * 100)
        # Exclude effectively-done epics, trivially-small remainders, and oversized
        # maintenance umbrellas — none of these are "big rocks" a VP needs to track.
        if remaining < min_remaining or pct >= near_done_pct or total > umbrella_total_cap:
            continue
        due = f.get("duedate")
        if due:
            any_due = True
        status = str((f.get("status") or {}).get("name") or "")
        days_due = _days_since(due, now=ref)
        overdue = bool(due and days_due is not None and days_due > 0)
        # Without due dates the actionable risk signal is "has open work but isn't moving".
        stalled = bool(remaining > 0 and active == 0)
        rows.append({
            "key": key,
            "project": str(key).split("-")[0] if key else "",
            "summary": str(f.get("summary") or ""),
            "status": status,
            "owner": str((f.get("assignee") or {}).get("displayName") or ""),
            "due": due,
            "total": int(total),
            "done": int(done),
            "remaining": int(remaining),
            "active_30d": int(active),
            "pct": pct,
            "overdue": overdue,
            "stalled": stalled,
            "reopened": status.lower() == "reopened",
            "at_risk": bool(overdue or stalled),
        })

    # Most open work + most recent movement first: that is where delivery attention goes.
    rows.sort(key=lambda r: (-r["remaining"], -r["active_30d"]))
    rows = rows[:max_epics]

    pcts = [r["pct"] for r in rows]
    median_pct = sorted(pcts)[len(pcts) // 2] if pcts else None
    return {
        "epics": rows,
        "projects": list(_PROJECTS),
        "epic_count": len(rows),
        "median_pct": median_pct,
        "total_remaining": sum(r["remaining"] for r in rows),
        "early_stage_count": sum(1 for r in rows if r["pct"] < 50),
        "at_risk_count": sum(1 for r in rows if r["at_risk"]),
        "has_due_dates": any_due,
        "error": None,
    }
