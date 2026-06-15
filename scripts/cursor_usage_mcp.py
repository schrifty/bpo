#!/usr/bin/env python3
"""Read-only MCP server exposing Cursor Team Admin usage data for ad-hoc analysis.

This wraps the in-house :class:`src.cursor_client.CursorClient` (and the slide report
aggregator) as MCP tools so an agent can answer questions like "what's our spend
trend?" or "who are the idle seats?" without writing code first. It deliberately
exposes **only read** operations — no member removal, spend-limit, or billing-group
mutations — matching the project's read-only/security posture. It reuses the same
on-disk hourly cache and fail-loud error handling as the deck pipeline.

Auth: ``CURSOR_ADMIN_API_KEY`` from ``.env`` (Cursor dashboard → Settings). When the
key is missing, every tool returns a clear ``{"error": ...}`` instead of crashing.

Run via ``.cursor/mcp.json`` (stdio transport); see that file for the launch command.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from mcp.server.fastmcp import FastMCP  # noqa: E402

from src.cursor_client import (  # noqa: E402
    CursorClientError,
    cursor_configured,
    get_shared_cursor_client,
)

mcp = FastMCP("cursor-usage")


def _err(msg: str) -> dict[str, Any]:
    return {"error": msg}


def _require_client():
    """Return the shared client, or raise CursorClientError if unconfigured."""
    if not cursor_configured():
        raise CursorClientError(
            "CURSOR_ADMIN_API_KEY not set — add it to .env (Cursor dashboard → Settings)."
        )
    return get_shared_cursor_client()


@mcp.tool()
def cursor_team_members(include_removed: bool = False) -> dict[str, Any]:
    """List Cursor team members (email, role, active/removed status) and a seat count.

    Removed members are excluded unless ``include_removed`` is true. Read-only.
    """
    try:
        client = _require_client()
        members = client.get_team_members(include_removed=include_removed)
    except CursorClientError as e:
        return _err(str(e))
    roster = [
        {
            "email": m.get("email"),
            "name": m.get("name"),
            "role": m.get("role"),
            "removed": bool(m.get("isRemoved")),
        }
        for m in members
    ]
    return {"seats": len(roster), "members": roster}


@mcp.tool()
def cursor_usage_summary(days: int = 30) -> dict[str, Any]:
    """Aggregate Cursor daily-usage totals over a trailing window (default 30 days).

    Returns active users, lines added/accepted, accept/reject counts and acceptance
    rate, tab usage, and request counts by surface (agent/chat/composer/cmd+k/bugbot).
    Read-only; backed by the hourly-cached daily-usage-data endpoint.
    """
    try:
        client = _require_client()
        summary = client.get_usage_summary(days=max(1, days))
    except CursorClientError as e:
        return _err(str(e))
    out = dict(summary.__dict__)
    out["acceptance_rate"] = summary.acceptance_rate
    return out


@mcp.tool()
def cursor_spend(top: int = 25) -> dict[str, Any]:
    """Per-member Cursor spend for the current billing cycle, highest first.

    Returns the total cycle spend (USD) and the top ``top`` spenders. Read-only.
    """
    try:
        client = _require_client()
        rows = client.get_spend()
    except CursorClientError as e:
        return _err(str(e))
    rows.sort(key=lambda r: float(r.get("overallSpendCents") or 0), reverse=True)
    total_cents = sum(float(r.get("overallSpendCents") or 0) for r in rows)
    members = [
        {
            "email": r.get("email"),
            "spend_usd": round(float(r.get("overallSpendCents") or 0) / 100, 2),
            "fast_premium_requests": r.get("fastPremiumRequests"),
        }
        for r in rows[: max(1, top)]
    ]
    return {
        "members_count": len(rows),
        "total_spend_usd": round(total_cents / 100, 2),
        "top_members": members,
    }


@mcp.tool()
def cursor_token_usage(days: int = 30) -> dict[str, Any]:
    """Total AI tokens (input + output) consumed team-wide over a trailing window.

    Mirrors the ``AI Token Usage`` LeanDNA metric generator. Read-only; backed by the
    hourly-cached usage-events endpoint.
    """
    try:
        client = _require_client()
        from src.cursor_ai_usage_metrics import get_ai_token_usage_value

        return get_ai_token_usage_value(client, days=max(1, days))
    except CursorClientError as e:
        return _err(str(e))


@mcp.tool()
def cursor_efficiency(window_days: int = 30) -> dict[str, Any]:
    """AI coding efficiency: accepted AI-written lines vs. token cost over a window.

    Returns team totals (accepted lines, lines-kept ratio, accepted lines per 1K tokens,
    cost per accepted line), a daily accepted-lines-vs-cost series, and a per-engineer
    efficiency ranking (joined by email). This is an efficiency/ROI correlation — accepted
    lines span Tab and agent surfaces while cost is model-API usage, so cost-per-line is a
    blended proxy, not a clean unit cost. Read-only; the heavy reads are hourly-cached.
    """
    try:
        from src.cursor_usage_report import build_cursor_usage_report

        report = build_cursor_usage_report(window_days=max(1, window_days))
    except CursorClientError as e:
        return _err(str(e))
    if not report.get("configured"):
        return _err("; ".join(report.get("errors") or ["Cursor usage report unavailable"]))
    return {
        "window_days": report.get("window_days"),
        "efficiency": report.get("efficiency") or {},
        "warnings": report.get("warnings") or [],
        "errors": report.get("errors") or [],
    }


@mcp.tool()
def cursor_usage_report(window_days: int = 30, trend_months: int = 6) -> dict[str, Any]:
    """Full slide-ready Cursor usage report: adoption, spend, model mix, top users, trend.

    This is the same aggregate the engineering-portfolio Cursor slides render — including
    a ``warnings`` list for data-attribution gaps (e.g. tokens from removed accounts that
    cannot be mapped to a user). Read-only; the heavy reads are hourly-cached.
    """
    try:
        from src.cursor_usage_report import build_cursor_usage_report

        return build_cursor_usage_report(
            window_days=max(1, window_days), trend_months=max(1, trend_months)
        )
    except CursorClientError as e:
        return _err(str(e))


if __name__ == "__main__":
    mcp.run()
