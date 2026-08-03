"""LeanDNA metric generators for the Engineering MFR scorecard KPIs.

Board-level AI adoption / impact metrics from Cursor, GitHub, and Jira, scoped to the
Engineering Department (Atlassian ``Dev - *`` teams) where applicable. Generators
return ``{"value": …}`` or ``{"numerator": …, "denominator": …}`` for percentages,
and ``{"error": …}`` on failure so ``metrics-upsert`` fails loud.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger("cortex")

DEFAULT_WINDOW_DAYS = 30
WAU_WINDOW_DAYS = 7

ISSUES_SHIPPED_JQL_TEMPLATE = (
    "project = LEAN AND statusCategory = Done AND resolved >= -{days}d"
)


def _engineer_scope(jira: Any, *, timeout: float) -> dict[str, Any]:
    from .eng_team_roster import build_engineer_audience_scope

    scope = build_engineer_audience_scope(jira, timeout=timeout)
    if scope.get("error"):
        return {"error": f"Engineering Department roster unavailable: {scope['error']}"}
    headcount = int(scope.get("headcount") or 0)
    if headcount <= 0:
        return {"error": "Engineering Department headcount is 0 (no Dev - * Atlassian teams)"}
    emails = {str(e).strip().casefold() for e in (scope.get("emails") or []) if e}
    return {"headcount": headcount, "emails": emails}


def _cursor_events(client: Any, *, days: int) -> list[dict[str, Any]] | dict[str, Any]:
    window = max(1, int(days))
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=window)
    try:
        return client.get_usage_events(start, end)
    except Exception as e:  # noqa: BLE001 — surface as metric error
        return {"error": f"Cursor usage events unavailable: {e}"}


def _engineer_event_stats(
    events: list[dict[str, Any]],
    *,
    engineer_emails: set[str],
) -> dict[str, Any]:
    from .cursor_usage_report import _event_cost_cents, _event_io_tokens

    active: set[str] = set()
    tokens = 0
    charged_cents = 0.0
    for event in events:
        email = str(event.get("userEmail") or "").strip().casefold()
        if not email or email not in engineer_emails:
            continue
        active.add(email)
        in_t, out_t = _event_io_tokens(event)
        tokens += in_t + out_t
        charged_cents += _event_cost_cents(event)
    return {
        "active_users": len(active),
        "tokens": tokens,
        "charged_cents": round(charged_cents, 2),
    }


def get_weekly_active_ai_users(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = WAU_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Weekly Active AI Users: active Cursor users ÷ Engineering Department headcount.

    Scope is the Engineering Department only — members of Atlassian ``Dev - *`` teams
    (see :func:`eng_team_roster.build_engineer_audience_scope`). Non-engineering Cursor
    users are excluded from both the numerator and the denominator.
    """
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    window = max(1, int(days) or WAU_WINDOW_DAYS)
    events = _cursor_events(cursor_client, days=window)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    active = int(stats["active_users"])
    pct = round(100.0 * active / headcount, 2) if headcount else 0.0
    logger.info(
        "Weekly Active AI Users: %s / %s Engineering Department (window=%sd) = %s%%",
        active,
        headcount,
        window,
        pct,
    )
    return {
        "numerator": float(active),
        "denominator": float(headcount),
        "value": pct,
        "active_users": active,
        "headcount": headcount,
        "window_days": window,
        "scope": "engineering_department",
    }


def get_wau_pct(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = WAU_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Deprecated alias for :func:`get_weekly_active_ai_users`."""
    return get_weekly_active_ai_users(
        cursor_client, jira_client, days=days, timeout=timeout
    )


def get_tokens_per_dev(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Tokens / Dev: total model tokens ÷ engineering headcount."""
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    tokens = int(stats["tokens"])
    per_dev = round(tokens / headcount, 1) if headcount else 0.0
    logger.info("Tokens / Dev: %s tokens / %s eng = %s", tokens, headcount, per_dev)
    return {
        "numerator": float(tokens),
        "denominator": float(headcount),
        "value": per_dev,
        "tokens": tokens,
        "headcount": headcount,
        "window_days": max(1, int(days)),
    }


def get_prs_merged(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """PRs Merged: engineer-scoped merged PRs in the window (falls back to org total)."""
    from .github_client import github_configured
    from .github_productivity_report import build_github_productivity_report

    if not github_configured():
        return {"error": "GitHub not configured (GITHUB_TOKEN / org)"}

    identity = None
    try:
        from .engineer_identity_map import build_engineer_identity_map
        from .jira_client import get_shared_jira_client

        identity = build_engineer_identity_map(jira_client=get_shared_jira_client())
        if not identity.get("configured"):
            identity = None
    except Exception as e:  # noqa: BLE001
        logger.warning("PRs Merged: identity map unavailable (%s); using org totals", e)

    report = build_github_productivity_report(window_days=days, identity=identity)
    if not report or not report.get("configured", True):
        return {"error": "GitHub productivity report unavailable"}

    eng = report.get("company_engineers") or {}
    all_co = report.get("company_all") or {}
    if identity is not None:
        merged = int(eng.get("merged_prs") or 0)
        scope = "engineers"
    else:
        merged = int(all_co.get("merged_prs") or 0)
        scope = "org"
    logger.info("PRs Merged: %s (%s, window=%sd)", merged, scope, days)
    return {
        "value": merged,
        "scope": scope,
        "window_days": max(1, int(days)),
    }


def get_issues_shipped(
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """Issues Shipped: LEAN issues completed (Done) with resolved date in the window."""
    window = max(1, int(days))
    jql = ISSUES_SHIPPED_JQL_TEMPLATE.format(days=window)
    count = jira_client.jql_match_count(
        jql,
        data_description=f"Issues Shipped (LEAN Done, resolved last {window}d)",
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Issues Shipped "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            )
        }
    logger.info("Issues Shipped: %s (window=%sd)", count, window)
    return {"value": int(count), "jql": jql, "window_days": window}


def get_growth_allocation_pct(
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """% Growth Allocation: planned/roadmap closed work ÷ total closed engineering work."""
    from .jira_client import compute_eng_work_split

    window = max(1, int(days))
    fields = ["summary", "status", "issuetype", "labels", "resolved"]
    try:
        closed_raw = jira_client._search(
            f"project = LEAN AND statusCategory = Done AND resolved >= -{window}d "
            "ORDER BY resolved DESC",
            max_results=2000,
            fields=fields,
            data_description=f"LEAN Done issues for growth allocation ({window}d)",
        )
    except Exception as e:  # noqa: BLE001
        return {"error": f"Jira search unavailable for % Growth Allocation: {e}"}

    closed = []
    for issue in closed_raw or []:
        f = issue.get("fields") or {}
        closed.append(
            {
                "type": (f.get("issuetype") or {}).get("name", ""),
                "labels": f.get("labels") or [],
            }
        )
    split = compute_eng_work_split([], closed)
    closed_split = split.get("closed") or {}
    planned = int(closed_split.get("planned") or 0)
    total = int(closed_split.get("total") or 0)
    if total <= 0:
        return {"error": f"no LEAN Done issues in last {window}d for growth allocation"}
    logger.info(
        "%% Growth Allocation: planned=%s / total=%s (window=%sd)",
        planned,
        total,
        window,
    )
    return {
        "numerator": float(planned),
        "denominator": float(total),
        "planned": planned,
        "unplanned": int(closed_split.get("unplanned") or 0),
        "window_days": window,
    }


def get_ai_spend_per_issue(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Spend / Issue: engineer Cursor spend (USD) ÷ issues shipped."""
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    shipped = get_issues_shipped(jira_client, days=days, timeout=timeout)
    if shipped.get("error"):
        return shipped
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {"error": "Issues Shipped is 0 — cannot compute AI Spend / Issue"}
    spend_usd = round(float(stats["charged_cents"]) / 100.0, 4)
    per_issue = round(spend_usd / issues, 4)
    logger.info(
        "AI Spend / Issue: $%s / %s issues = $%s (window=%sd)",
        spend_usd,
        issues,
        per_issue,
        days,
    )
    return {
        "numerator": spend_usd,
        "denominator": float(issues),
        "value": per_issue,
        "spend_usd": spend_usd,
        "issues_shipped": issues,
        "window_days": max(1, int(days)),
    }
