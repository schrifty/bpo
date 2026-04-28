"""Optional mapping from health-report customer name to Salesforce Account Ids.

See ``customer_identity_map.yaml`` at the project root. Keys are matched case-insensitively
against ``customer_name`` passed to ``get_customer_health_report`` / deck generation.

When one or more Ids are present, ``SalesforceClient`` resolves the customer by Id first
(name-based matching is the fallback).
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import yaml

from .config import logger

_IDENTITY_FILE = Path(__file__).resolve().parent.parent / "customer_identity_map.yaml"
_map_lock = threading.Lock()
_map_cache: dict[str, Any] | None = None


def _normalize_sf_id(raw: str) -> str:
    s = (raw or "").strip()
    if len(s) in (15, 18) and s.isalnum():
        return s
    return ""


def _load_identity_map() -> dict[str, Any]:
    global _map_cache
    with _map_lock:
        if _map_cache is not None:
            return _map_cache
        out: dict[str, Any] = {}
        if _IDENTITY_FILE.is_file():
            try:
                raw = yaml.safe_load(_IDENTITY_FILE.read_text())
                if raw is None:
                    raw = {}
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if str(k).strip().startswith("#"):
                            continue
                        key = str(k).strip().lower()
                        if key:
                            out[key] = v
            except Exception as e:
                logger.warning("customer_identity_map: could not load %s: %s", _IDENTITY_FILE, e)
        _map_cache = out
        return out


def invalidate_customer_identity_cache() -> None:
    """Clear cached YAML (for tests)."""
    global _map_cache
    with _map_lock:
        _map_cache = None


def lookup_salesforce_identity(customer_name: str) -> tuple[list[str], str | None]:
    """Return ``(salesforce_account_ids, primary_account_id)`` for this customer label.

    Empty list and None when unmapped. Primary is set when the YAML entry specifies it,
    or when exactly one Id is listed.
    """
    raw = (customer_name or "").strip()
    if not raw:
        return [], None
    data = _load_identity_map().get(raw.lower())
    if data is None:
        return [], None

    ids: list[str] = []
    primary: str | None = None

    if isinstance(data, str):
        sid = _normalize_sf_id(data)
        return ([sid], sid) if sid else ([], None)

    if isinstance(data, dict):
        prim_raw = (
            data.get("salesforce_primary_account_id")
            or data.get("primary_account_id")
            or data.get("primary")
            or ""
        )
        primary = _normalize_sf_id(str(prim_raw)) or None

        raw_list = data.get("salesforce_account_ids")
        if isinstance(raw_list, list):
            for x in raw_list:
                sid = _normalize_sf_id(str(x))
                if sid:
                    ids.append(sid)
        one = data.get("salesforce_account_id") or data.get("account_id")
        if one and not ids:
            sid = _normalize_sf_id(str(one))
            if sid:
                ids.append(sid)

        if not primary and len(ids) == 1:
            primary = ids[0]
        elif primary and primary not in ids:
            logger.warning(
                "customer_identity_map: primary_account_id %r not in salesforce_account_ids for %r — ignoring primary",
                primary,
                raw,
            )
            primary = ids[0] if len(ids) == 1 else None

        return ids, primary

    return [], None
