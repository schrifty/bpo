"""Drive-backed cache for engineering portfolio ``eng_portfolio`` payloads."""

from __future__ import annotations

from typing import Any

from .config import logger
from .integration_drive_cache import (
    KIND_ENGINEERING_PORTFOLIO,
    integration_drive_cache_reads_enabled,
    save_integration_payload,
    try_load_integration_payload,
)


def _cache_customer_key(days: int) -> str:
    return f"__engineering_portfolio__{max(1, int(days))}d"


def _days_match(cached: dict[str, Any], days: int) -> bool:
    try:
        return int(cached.get("days") or 0) == max(1, int(days))
    except (TypeError, ValueError):
        return False


def _refresh_live_sprint(portfolio: dict[str, Any]) -> dict[str, Any]:
    """Patch cached eng_portfolio with live Jira active sprint (cheap API call)."""
    try:
        from .jira_client import get_shared_jira_client

        live = get_shared_jira_client().fetch_active_board_sprint()
        if live:
            out = dict(portfolio)
            out["sprint"] = live
            return out
    except Exception as e:
        logger.debug("Engineering portfolio: sprint refresh skipped: %s", e)
    return portfolio


def load_or_fetch_engineering_portfolio(*, days: int = 30) -> dict[str, Any]:
    """Return ``eng_portfolio`` blob from Drive cache or Jira ``get_engineering_portfolio``."""
    window = max(1, int(days))
    cache_key = _cache_customer_key(window)

    if integration_drive_cache_reads_enabled():
        cached = try_load_integration_payload(KIND_ENGINEERING_PORTFOLIO, cache_key)
        if cached is not None and not cached.get("error") and _days_match(cached, window):
            logger.debug("Engineering portfolio: Drive cache hit (%dd)", window)
            return _refresh_live_sprint(dict(cached))

    from .jira_client import get_shared_jira_client

    logger.info("Engineering portfolio: fetching Jira snapshot (%dd)", window)
    payload = get_shared_jira_client().get_engineering_portfolio(days=window)
    if isinstance(payload, dict) and not payload.get("error"):
        save_integration_payload(KIND_ENGINEERING_PORTFOLIO, cache_key, payload)
    return dict(payload) if isinstance(payload, dict) else {"error": "invalid portfolio payload", "days": window}
