"""Map secondary labels (CS Report export, Pendo prefix) to Salesforce corporate reporting groups.

Canonical customer identity for contracts and ARR lives in Salesforce — see
``src/salesforce_reporting.py`` and ``config/salesforce_reporting_rollups.yaml``.
``cohorts.yaml`` is for **usage benchmarking** only, not financial rollups.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import yaml

from .config import logger
from .salesforce_reporting import resolve_corporate_label

_CSR_ALIASES_FILE = Path(__file__).resolve().parent.parent / "cs_report_customer_aliases.yaml"
_map_lock = threading.Lock()
_cs_to_corporate_cache: dict[str, str] | None = None


def _add_mapping(out: dict[str, str], alias: str, group: str) -> None:
    key = (alias or "").strip().lower()
    group_clean = (group or "").strip()
    if not key or not group_clean:
        return
    existing = out.get(key)
    if existing and existing != group_clean:
        logger.warning(
            "customer_reporting: %r maps to both %r and %r — keeping %r",
            alias,
            existing,
            group_clean,
            existing,
        )
        return
    out[key] = group_clean


def _load_cs_to_corporate_map() -> dict[str, str]:
    """CS Report ``customer`` / alias string → Salesforce corporate rollup label."""
    global _cs_to_corporate_cache
    with _map_lock:
        if _cs_to_corporate_cache is not None:
            return _cs_to_corporate_cache

        out: dict[str, str] = {}
        if _CSR_ALIASES_FILE.is_file():
            try:
                raw = yaml.safe_load(_CSR_ALIASES_FILE.read_text()) or {}
                if isinstance(raw, dict):
                    for corporate_key, cs_names in raw.items():
                        if str(corporate_key).strip().startswith("#"):
                            continue
                        corp = resolve_corporate_label(str(corporate_key).strip())
                        if not corp:
                            continue
                        _add_mapping(out, corp, corp)
                        if isinstance(cs_names, str):
                            cs_list = [cs_names]
                        elif isinstance(cs_names, list):
                            cs_list = [str(x).strip() for x in cs_names if str(x).strip()]
                        else:
                            cs_list = []
                        for cs_name in cs_list:
                            _add_mapping(out, cs_name, corp)
            except Exception as e:
                logger.warning("customer_reporting: could not load %s: %s", _CSR_ALIASES_FILE, e)

        _cs_to_corporate_cache = out
        return out


def invalidate_customer_reporting_cache() -> None:
    """Clear cached maps (tests)."""
    global _cs_to_corporate_cache
    with _map_lock:
        _cs_to_corporate_cache = None
    from . import salesforce_reporting as _sf_rep

    _sf_rep.invalidate_salesforce_reporting_cache()


def reporting_group(customer_name: str) -> str:
    """Return the Salesforce corporate label for a CS Report or usage alias string."""
    raw = (customer_name or "").strip()
    if not raw:
        return raw
    mapped = _load_cs_to_corporate_map().get(raw.lower())
    if mapped:
        return mapped
    return resolve_corporate_label(raw)


def cs_report_name_to_reporting_group(cs_customer: str) -> str:
    """Alias for CS Report ``customer`` column values."""
    return reporting_group(cs_customer)


def build_reporting_group_index() -> dict[str, list[str]]:
    """``{corporate_label: [source alias strings]}`` for manifest / LLM context."""
    rev: dict[str, set[str]] = {}
    for alias_lower, group in _load_cs_to_corporate_map().items():
        rev.setdefault(group, set()).add(alias_lower)
    return {g: sorted(names, key=str.lower) for g, names in sorted(rev.items())}


def portfolio_rows_by_reporting_group(
    rows: list[dict[str, Any]],
    *,
    name_key: str = "customer",
) -> dict[str, list[dict[str, Any]]]:
    """Bucket dict rows by :func:`reporting_group` (SF corporate label)."""
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get(name_key) or "").strip()
        group = reporting_group(label)
        out.setdefault(group, []).append(row)
    return out
