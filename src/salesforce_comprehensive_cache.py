"""Drive-backed cache for per-customer Salesforce comprehensive payloads."""

from __future__ import annotations

from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured
from .integration_drive_cache import (
    KIND_SALESFORCE_COMPREHENSIVE,
    integration_drive_cache_reads_enabled,
    save_integration_payload,
    try_load_integration_payload,
)


def _cached_row_limit_matches(cached: dict[str, Any], row_limit: int) -> bool:
    stored = cached.get("row_limit")
    if stored is None:
        return row_limit == 75
    try:
        return int(stored) == row_limit
    except (TypeError, ValueError):
        return False


def load_or_fetch_salesforce_comprehensive(
    customer: str,
    *,
    row_limit: int = 75,
    preferred_account_ids: list[str] | None = None,
    primary_account_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Return ``(payload, source)`` where *source* is ``drive_cache`` or ``salesforce``.

    Uses the same Drive JSON integration cache as ``salesforce_comprehensive`` decks
    (:data:`integration_drive_cache.KIND_SALESFORCE_COMPREHENSIVE`). Refetches when
    cached ``row_limit`` does not match the requested cap.
    """
    name = (customer or "").strip()
    cap = max(1, min(int(row_limit), 500))
    empty: dict[str, Any] = {
        "customer": name,
        "accounts": [],
        "account_ids": [],
        "matched": False,
        "opportunity_count_this_year": 0,
        "pipeline_arr": 0.0,
        "row_limit": cap,
        "categories": {},
        "category_errors": {},
    }
    if not name:
        return {**empty, "error": "customer name required"}, "salesforce"

    if not _salesforce_configured():
        return {**empty, "error": "Salesforce not configured"}, "salesforce"

    if integration_drive_cache_reads_enabled():
        cached = try_load_integration_payload(KIND_SALESFORCE_COMPREHENSIVE, name)
        if cached is not None and not cached.get("error"):
            if _cached_row_limit_matches(cached, cap):
                logger.info("Salesforce comprehensive: Drive cache hit for %r", name)
                return dict(cached), "drive_cache"
            logger.info(
                "Salesforce comprehensive: cache miss for %r (row_limit %s != requested %s)",
                name,
                cached.get("row_limit"),
                cap,
            )

    from .salesforce_client import SalesforceClient

    sf_kwargs: dict[str, Any] = {}
    if preferred_account_ids:
        sf_kwargs["preferred_account_ids"] = preferred_account_ids
    if primary_account_id:
        sf_kwargs["primary_account_id"] = primary_account_id
    try:
        payload = SalesforceClient().get_customer_salesforce_comprehensive(
            name,
            row_limit=cap,
            **sf_kwargs,
        )
    except Exception as e:
        logger.warning("Salesforce comprehensive fetch failed for %s: %s", name, e)
        return {**empty, "error": str(e)[:500]}, "salesforce"

    if isinstance(payload, dict) and not payload.get("error"):
        save_integration_payload(KIND_SALESFORCE_COMPREHENSIVE, name, payload)
    return dict(payload) if isinstance(payload, dict) else empty, "salesforce"
