"""Active epic ("big rock") progress for the LEAN engineering project.

The deck otherwise has no line of sight from day-to-day activity to the larger
initiatives. This summarizes the active epics by completion (done vs. total child
issues) so a VP can see whether the big rocks are actually moving.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

logger = logging.getLogger("bpo")

if TYPE_CHECKING:  # pragma: no cover
    from .jira_client import JiraClient

_PROJECT = "LEAN"


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
    candidate_cap: int = 25,
    stale_days: int = 30,
    timeout: float = 60.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Top active LEAN epics by remaining work, with completion and risk flags.

    Each epic's progress is done child issues ÷ total child issues (``parent = EPIC``).
    Epics with no remaining work are excluded (e.g. catch-all/"done" epics), and the
    rest are sorted by total size so the largest in-flight initiatives surface first.
    """
    ref = now or datetime.now(timezone.utc)
    try:
        epics = client._search(
            f"project = {_PROJECT} AND issuetype = Epic AND statusCategory != Done ORDER BY updated DESC",
            max_results=candidate_cap,
            fields=["summary", "status", "duedate", "assignee", "updated"],
            data_description="LEAN active epics",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Epic fetch failed: %s", e)
        return {"error": str(e), "epics": []}

    def _counts(key: str) -> tuple[str, int | None, int | None]:
        total = client.jql_match_count(f"parent = {key}", data_description="epic child count")
        done = client.jql_match_count(
            f"parent = {key} AND statusCategory = Done", data_description="epic done-child count"
        )
        return key, total, done

    counts: dict[str, tuple[int | None, int | None]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_counts, e["key"]) for e in epics if e.get("key")]
        for fut in as_completed(futures):
            try:
                key, total, done = fut.result()
                counts[key] = (total, done)
            except Exception as e:  # noqa: BLE001
                logger.warning("Epic child count failed: %s", e)

    rows: list[dict[str, Any]] = []
    any_due = False
    for e in epics:
        key = e.get("key")
        f = e.get("fields") or {}
        total, done = counts.get(key, (None, None))
        if not total:
            continue
        done = done or 0
        remaining = total - done
        if remaining <= 0:
            continue  # catch-all / effectively-complete epics are not "in flight"
        due = f.get("duedate")
        if due:
            any_due = True
        days_stale = _days_since(f.get("updated"), now=ref)
        overdue = bool(due and _days_since(due, now=ref) is not None and _days_since(due, now=ref) > 0)
        rows.append({
            "key": key,
            "summary": str(f.get("summary") or ""),
            "status": str((f.get("status") or {}).get("name") or ""),
            "owner": str((f.get("assignee") or {}).get("displayName") or ""),
            "due": due,
            "total": int(total),
            "done": int(done),
            "remaining": int(remaining),
            "pct": round(done / total * 100),
            "overdue": overdue,
            "stale": bool(days_stale is not None and days_stale > stale_days),
        })

    rows.sort(key=lambda r: (-r["total"], -r["remaining"]))
    rows = rows[:max_epics]

    pcts = [r["pct"] for r in rows]
    median_pct = sorted(pcts)[len(pcts) // 2] if pcts else None
    return {
        "epics": rows,
        "epic_count": len(rows),
        "median_pct": median_pct,
        "total_remaining": sum(r["remaining"] for r in rows),
        "has_due_dates": any_due,
        "error": None,
    }
