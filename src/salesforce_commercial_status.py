"""Commercial contract status and ARR rollups for Salesforce portfolio aggregation."""

from __future__ import annotations

import datetime
from typing import Any

COMMERCIAL_STATUS_ACTIVE = "ACTIVE"
COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING = "OUT_OF_CONTRACT_RENEWING"
COMMERCIAL_STATUS_CHURNED = "CHURNED"
COMMERCIAL_STATUS_FUTURE = "FUTURE"

COMMERCIAL_STATUSES = frozenset(
    {
        COMMERCIAL_STATUS_ACTIVE,
        COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
        COMMERCIAL_STATUS_CHURNED,
        COMMERCIAL_STATUS_FUTURE,
    }
)

_CHURNED_CONTRACT_STATUS_LOWER = frozenset(
    {"churned", "cancelled", "terminated", "expired", "closed"}
)

_FUTURE_CONTRACT_STATUS_HINTS = frozenset(
    {
        "pending",
        "pending activation",
        "won",
        "future",
        "signed",
        "approved",
        "awaiting activation",
    }
)


def parse_sf_contract_date(raw: Any) -> datetime.date | None:
    if raw is None:
        return None
    s = str(raw).strip()[:10]
    if len(s) < 10:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def entity_has_active_contract(account: dict[str, Any]) -> bool:
    st = (account.get("Contract_Status__c") or "").strip().lower()
    return bool(st) and st not in _CHURNED_CONTRACT_STATUS_LOWER


def entity_has_future_contract(
    account: dict[str, Any],
    *,
    today: datetime.date | None = None,
) -> bool:
    """Entity has a won/signed contract that has not started yet (no active entitlement)."""
    if entity_has_active_contract(account):
        return False
    ref = today or datetime.date.today()
    start = parse_sf_contract_date(account.get("Contract_Contract_Start_Date__c"))
    if start and start > ref:
        return True
    st = (account.get("Contract_Status__c") or "").strip().lower()
    if st in _FUTURE_CONTRACT_STATUS_HINTS:
        if start is None or start > ref:
            return True
    return False


def _entity_arr(account: dict[str, Any]) -> float:
    try:
        return float(account.get("ARR__c") or 0)
    except (TypeError, ValueError):
        return 0.0


def rollup_arr_fields(
    matching: list[dict[str, Any]],
    *,
    commercial_status: str,
) -> dict[str, float]:
    """Derive per-label ARR components from entity rows and commercial status."""
    historical_arr = round(sum(_entity_arr(a) for a in matching), 2)
    active_arr = round(
        sum(_entity_arr(a) for a in matching if entity_has_active_contract(a)),
        2,
    )
    renewal_arr = historical_arr if commercial_status == COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING else 0.0
    current_arr = round(active_arr + renewal_arr, 2)
    return {
        "historical_arr": historical_arr,
        "active_arr": active_arr,
        "renewal_arr": renewal_arr,
        "current_arr": current_arr,
        "arr": historical_arr,
    }


def derive_commercial_status(
    matching: list[dict[str, Any]],
    *,
    renewal_in_flight: bool,
    today: datetime.date | None = None,
) -> str:
    """Classify a reporting group from entity contract rows and renewal pipeline signals."""
    if any(entity_has_active_contract(a) for a in matching):
        return COMMERCIAL_STATUS_ACTIVE
    if renewal_in_flight:
        return COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING
    if any(entity_has_future_contract(a, today=today) for a in matching):
        return COMMERCIAL_STATUS_FUTURE
    return COMMERCIAL_STATUS_CHURNED


def is_current_book_commercial_status(status: str | None) -> bool:
    """Included in executive current-ARR ranking (active installed base + renewal negotiation)."""
    return (status or "") in (
        COMMERCIAL_STATUS_ACTIVE,
        COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
    )


def is_active_installed_base_status(status: str | None) -> bool:
    return (status or "") == COMMERCIAL_STATUS_ACTIVE


def renewal_in_flight_from_status(status: str | None) -> bool:
    return (status or "") == COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING


def rollup_in_current_book(row: dict[str, Any]) -> bool:
    """True when a rollup row belongs in executive current-ARR / active export slices."""
    status = str(row.get("commercial_status") or "").strip()
    if status:
        return is_current_book_commercial_status(status)
    if row.get("renewal_in_flight") is True:
        return True
    return row.get("active") is not False


def rollup_current_arr(row: dict[str, Any]) -> float:
    try:
        if row.get("current_arr") is not None:
            return float(row["current_arr"])
    except (TypeError, ValueError):
        pass
    try:
        return float(row.get("arr") or 0)
    except (TypeError, ValueError):
        return 0.0
