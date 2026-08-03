"""LeanDNA metric generators backed by the Cursor Team Admin API.

``get_ai_token_usage`` — total AI tokens (input + output) consumed across the team
over a trailing window, from ``/teams/filtered-usage-events`` token usage.

``get_monthly_ai_spend`` — projected calendar-month Cursor spend (USD): real
month-to-date charged cost plus remaining days estimated from the average daily
spend over a trailing 90-day window that includes those MTD actuals.
"""

from __future__ import annotations

import calendar
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cursor_client import CursorClient

logger = logging.getLogger("cortex")

DEFAULT_TOKEN_USAGE_WINDOW_DAYS = 30
MONTHLY_AI_SPEND_LOOKBACK_DAYS = 90


def get_ai_token_usage_value(
    client: CursorClient,
    *,
    days: int = DEFAULT_TOKEN_USAGE_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - client carries its own timeout
) -> dict[str, Any]:
    """Return ``{"value": <input+output tokens>}`` over the trailing *days* window.

    Sums ``tokenUsage.inputTokens`` + ``tokenUsage.outputTokens`` across all team
    usage events. Fails loud (``{"error": ...}``) when the Cursor API is unavailable
    so ``metrics-upsert`` does not write a placeholder value.
    """
    window = max(1, int(days))
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=window)

    try:
        events = client.get_usage_events(start, end)
    except Exception as e:
        return {"error": f"Cursor token usage unavailable: {e}"}

    input_tokens = 0
    output_tokens = 0
    cache_read = 0
    cache_write = 0
    for event in events:
        tu = event.get("tokenUsage")
        if not isinstance(tu, dict):
            continue
        input_tokens += int(tu.get("inputTokens") or 0)
        output_tokens += int(tu.get("outputTokens") or 0)
        cache_read += int(tu.get("cacheReadTokens") or 0)
        cache_write += int(tu.get("cacheWriteTokens") or 0)

    total = input_tokens + output_tokens
    logger.info(
        "AI Token Usage: %s tokens (in=%s out=%s) over trailing %sd from %s event(s)",
        total, input_tokens, output_tokens, window, len(events),
    )
    return {
        "value": total,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "window_days": window,
        "event_count": len(events),
    }


def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _sum_event_cents(
    events: list[dict[str, Any]],
    *,
    month_key: str | None = None,
) -> float:
    """Sum charged cents; optionally restrict to one calendar month (UTC)."""
    from .cursor_usage_report import _event_cost_cents, _event_dt

    total = 0.0
    for event in events:
        dt = _event_dt(event)
        if dt is None:
            continue
        if month_key is not None and _month_key(dt.year, dt.month) != month_key:
            continue
        total += _event_cost_cents(event)
    return round(total, 2)


def project_monthly_ai_spend_cents(
    *,
    mtd_cents: float,
    window_cents: float,
    window_days: int,
    as_of: datetime,
) -> dict[str, Any]:
    """Project full-month spend from MTD real cents + trailing-window daily rate.

    Prefers real data: month-to-date charged cost is never replaced. Only remaining
    days are estimated from average daily spend over the trailing window (which
    includes MTD). When the window has no spend, falls back to linear pace from
    MTD. On the last day of the month, the value is purely MTD.
    """
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    else:
        as_of = as_of.astimezone(timezone.utc)

    cur_key = _month_key(as_of.year, as_of.month)
    days_in_month = calendar.monthrange(as_of.year, as_of.month)[1]
    days_elapsed = min(max(1, as_of.day), days_in_month)
    days_remaining = max(0, days_in_month - days_elapsed)

    mtd = float(mtd_cents)
    window = max(1, int(window_days))
    win_cents = float(window_cents)
    avg_daily = (win_cents / window) if win_cents > 0 else None

    method: str
    extrapolated_cents = 0.0
    if days_remaining == 0:
        projected_cents = mtd
        method = "actual_month_complete"
    elif avg_daily is not None:
        extrapolated_cents = avg_daily * days_remaining
        projected_cents = mtd + extrapolated_cents
        method = "mtd_plus_window_daily"
    elif mtd > 0 and days_elapsed > 0:
        projected_cents = mtd * (days_in_month / days_elapsed)
        extrapolated_cents = projected_cents - mtd
        method = "mtd_pace"
    else:
        projected_cents = mtd
        method = "mtd_only"

    return {
        "projected_cents": round(float(projected_cents), 2),
        "mtd_cents": round(mtd, 2),
        "extrapolated_cents": round(float(extrapolated_cents), 2),
        "window_cents": round(win_cents, 2),
        "window_days": window,
        "month": cur_key,
        "days_in_month": days_in_month,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "window_avg_daily_cents": round(avg_daily, 4) if avg_daily is not None else None,
        "method": method,
    }


def get_monthly_ai_spend(
    client: CursorClient,
    *,
    as_of: datetime | None = None,
    lookback_days: int = MONTHLY_AI_SPEND_LOOKBACK_DAYS,
    timeout: float = 60.0,  # noqa: ARG001 - client carries its own timeout
) -> dict[str, Any]:
    """Return ``{"value": <USD>}`` projected Cursor spend for the current calendar month.

    Real month-to-date cost comes from usage-event ``chargedCents``. Remaining days
    are estimated from the average daily spend over a trailing *lookback_days*
    window that includes those MTD actuals. Fails loud when the Cursor API is
    unavailable.
    """
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    window_days = max(1, int(lookback_days))
    start = now - timedelta(days=window_days)

    try:
        events = client.get_usage_events(start, now)
    except Exception as e:
        return {"error": f"Cursor monthly AI spend unavailable: {e}"}

    cur_key = _month_key(now.year, now.month)
    mtd_cents = _sum_event_cents(events, month_key=cur_key)
    window_cents = _sum_event_cents(events)
    proj = project_monthly_ai_spend_cents(
        mtd_cents=mtd_cents,
        window_cents=window_cents,
        window_days=window_days,
        as_of=now,
    )
    spend_usd = round(float(proj["projected_cents"]) / 100.0, 2)
    mtd_usd = round(float(proj["mtd_cents"]) / 100.0, 2)
    logger.info(
        "Monthly AI Spend: $%s projected for %s (mtd=$%s, method=%s, window=%sd, events=%s)",
        spend_usd,
        proj["month"],
        mtd_usd,
        proj["method"],
        window_days,
        len(events),
    )
    return {
        "value": spend_usd,
        "spend_usd": spend_usd,
        "mtd_usd": mtd_usd,
        "extrapolated_usd": round(float(proj["extrapolated_cents"]) / 100.0, 2),
        "window_usd": round(float(proj["window_cents"]) / 100.0, 2),
        "spend_cents": proj["projected_cents"],
        "mtd_cents": proj["mtd_cents"],
        "window_cents": proj["window_cents"],
        "window_days": proj["window_days"],
        "month": proj["month"],
        "days_elapsed": proj["days_elapsed"],
        "days_remaining": proj["days_remaining"],
        "days_in_month": proj["days_in_month"],
        "window_avg_daily_usd": (
            round(float(proj["window_avg_daily_cents"]) / 100.0, 4)
            if proj["window_avg_daily_cents"] is not None
            else None
        ),
        "method": proj["method"],
        "event_count": len(events),
    }
