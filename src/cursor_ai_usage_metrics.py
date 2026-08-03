"""LeanDNA metric generators backed by the Cursor Team Admin API.

``get_ai_token_usage`` — total AI tokens (input + output) consumed across the team
over a trailing window, from ``/teams/filtered-usage-events`` token usage.

``get_monthly_ai_spend`` — team-wide Cursor spend (USD) for the current billing
cycle, from ``/teams/spend``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cursor_client import CursorClient

logger = logging.getLogger("cortex")

DEFAULT_TOKEN_USAGE_WINDOW_DAYS = 30


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


def get_monthly_ai_spend(
    client: CursorClient,
    *,
    timeout: float = 60.0,  # noqa: ARG001 - client carries its own timeout
) -> dict[str, Any]:
    """Return ``{"value": <USD>}`` for Cursor team spend in the current billing cycle.

    Sums ``overallSpendCents`` across all ``/teams/spend`` members and converts to
    USD. Fails loud (``{"error": ...}``) when the Cursor API is unavailable so
    ``metrics-upsert`` does not write a placeholder value.
    """
    try:
        rows = client.get_spend()
    except Exception as e:
        return {"error": f"Cursor monthly AI spend unavailable: {e}"}

    total_cents = 0.0
    for row in rows or []:
        total_cents += float(row.get("overallSpendCents") or 0)
    spend_usd = round(total_cents / 100.0, 2)
    members = len(rows or [])
    logger.info(
        "Monthly AI Spend: $%s across %s member(s) (current Cursor billing cycle)",
        spend_usd,
        members,
    )
    return {
        "value": spend_usd,
        "spend_usd": spend_usd,
        "spend_cents": round(total_cents, 2),
        "members_count": members,
        "billing_cycle": "current",
    }
