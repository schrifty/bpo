"""LeanDNA metric generators backed by the Cursor Team Admin API.

``get_ai_token_usage`` — total AI tokens (input + output) consumed across the team
over a trailing window, from ``/teams/filtered-usage-events`` token usage.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cursor_client import CursorClient

logger = logging.getLogger("bpo")

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
