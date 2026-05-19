"""Attach per-customer Salesforce comprehensive payloads to the all-customers LLM export."""

from __future__ import annotations

import os
from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured


def _env_truthy(name: str, *, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def llm_export_sf_comprehensive_enabled() -> bool:
    """When false, skip per-customer comprehensive SOQL (``BPO_LLM_EXPORT_SF_COMPREHENSIVE``)."""
    return _env_truthy("BPO_LLM_EXPORT_SF_COMPREHENSIVE", default=True)


def llm_export_sf_comprehensive_row_limit() -> int:
    raw = (os.environ.get("BPO_LLM_EXPORT_SF_COMPREHENSIVE_ROW_LIMIT") or "").strip()
    if not raw:
        return 8
    try:
        return max(1, min(int(raw), 500))
    except ValueError:
        return 8


def llm_export_sf_comprehensive_customer_cap() -> int | None:
    """Max customers to fetch. Default 12 (top active by ARR). ``0``/``all`` = no cap."""
    raw = (os.environ.get("BPO_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP") or "").strip()
    if not raw:
        return 12
    if raw.lower() in ("0", "all", "none", "unlimited"):
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return 12


def _labels_for_comprehensive_fetch(
    report: dict[str, Any],
    *,
    cap: int | None,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Choose Customer Entity labels for §3c fetch (CSM: top active book by ARR when capped)."""
    if cap is None:
        labels = _rollup_labels_with_segment(report)
        return labels, {"selection": "all_portfolio_labels", "top_n": None}

    from .llm_export_csr import top_active_customers_by_arr_for_csr

    ranked = top_active_customers_by_arr_for_csr(report, top_n=cap)
    labels = [
        (str(row.get("salesforce_label") or "").strip(), "active")
        for row in ranked
        if str(row.get("salesforce_label") or "").strip()
    ]
    return labels, {
        "selection": "top_active_by_arr",
        "top_n": cap,
        "selection_ranked": ranked,
    }


def _rollup_labels_with_segment(report: dict[str, Any]) -> list[tuple[str, str]]:
    """Unique portfolio labels as ``(label, 'active'|'churned')``."""
    ordered: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(rows: list[Any], segment: str) -> None:
        for r in rows:
            if not isinstance(r, dict):
                continue
            label = str(r.get("customer") or "").strip()
            if not label:
                continue
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append((label, segment))

    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")
    if isinstance(book, dict):
        for r in book.get("matched_customer_contract_rollups") or []:
            if not isinstance(r, dict):
                continue
            label = str(r.get("customer") or "").strip()
            if not label:
                continue
            seg = "churned" if r.get("active") is False else "active"
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append((label, seg))

    churn_seg = report.get("salesforce_churned_segment")
    if isinstance(churn_seg, dict):
        sf = churn_seg.get("salesforce")
        if isinstance(sf, dict):
            _add(sf.get("matched_customer_contract_rollups") or [], "churned")

    return ordered


def attach_salesforce_comprehensive_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Populate ``report['salesforce_comprehensive_portfolio']`` for LLM export §3c.

    Fetches the same multi-object slice as the ``salesforce_comprehensive`` deck
    (:meth:`SalesforceClient.get_customer_salesforce_comprehensive`) for each portfolio
    Customer Entity labels (by default top active accounts by ARR; churned only when uncapped).
    Also attaches all Customer Entity account
    rows and portfolio expansion KPIs when available.
    """
    summary: dict[str, Any] = {
        "enabled": llm_export_sf_comprehensive_enabled(),
        "salesforce_configured": _salesforce_configured(),
        "customers_requested": 0,
        "customers_fetched": 0,
        "customers_matched": 0,
        "customers_errors": 0,
        "customers_drive_cache_hit": 0,
        "customers_salesforce_fetch": 0,
        "row_limit": llm_export_sf_comprehensive_row_limit(),
        "customer_cap": llm_export_sf_comprehensive_customer_cap(),
        "integration_cache_kind": "salesforce_comprehensive",
    }
    if not summary["enabled"]:
        report["salesforce_comprehensive_portfolio"] = {
            "configured": False,
            "skipped": "disabled_via_BPO_LLM_EXPORT_SF_COMPREHENSIVE",
        }
        report["_llm_export_salesforce_comprehensive"] = summary
        return summary

    if not summary["salesforce_configured"]:
        report["salesforce_comprehensive_portfolio"] = {
            "configured": False,
            "skipped": "salesforce_not_configured",
        }
        report["_llm_export_salesforce_comprehensive"] = summary
        return summary

    cap = summary["customer_cap"]
    labels, selection_meta = _labels_for_comprehensive_fetch(report, cap=cap)
    summary.update(selection_meta)
    summary["customers_requested"] = len(labels)

    from src.customer_identity import lookup_salesforce_identity
    from src.salesforce_client import SalesforceClient
    from src.salesforce_comprehensive_cache import load_or_fetch_salesforce_comprehensive

    sf = SalesforceClient()
    row_limit = int(summary["row_limit"])
    by_customer: dict[str, Any] = {}
    total = len(labels)

    for idx, (label, segment) in enumerate(labels, 1):
        logger.info(
            "LLM export: SF comprehensive %d/%d — %s (%s)",
            idx,
            total,
            label,
            segment,
        )
        sf_ids, sf_prim = lookup_salesforce_identity(label)
        sf_kwargs: dict[str, Any] = {}
        if sf_ids:
            sf_kwargs["preferred_account_ids"] = sf_ids
            sf_kwargs["primary_account_id"] = sf_prim
        payload, source = load_or_fetch_salesforce_comprehensive(
            label,
            row_limit=row_limit,
            **sf_kwargs,
        )
        payload = dict(payload)
        payload["customer_segment"] = segment
        by_customer[label] = payload
        summary["customers_fetched"] += 1
        if source == "drive_cache":
            summary["customers_drive_cache_hit"] += 1
        else:
            summary["customers_salesforce_fetch"] += 1
        if payload.get("matched"):
            summary["customers_matched"] += 1
        if payload.get("error"):
            summary["customers_errors"] += 1

    entity_accounts: list[dict[str, Any]] = []
    try:
        entity_accounts = sf.get_entity_accounts()
    except Exception as e:
        logger.warning("LLM export: get_entity_accounts failed: %s", e)
        summary["entity_accounts_error"] = str(e)[:500]

    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")
    expansion = None
    if isinstance(book, dict):
        expansion = book.get("expansion_kpis")
    if expansion is None:
        expansion = report.get("portfolio_expansion_book")

    report["salesforce_comprehensive_portfolio"] = {
        "configured": True,
        "row_limit": row_limit,
        "customer_count": len(labels),
        "by_customer": by_customer,
        "entity_accounts": entity_accounts,
        "entity_accounts_count": len(entity_accounts),
        "portfolio_expansion_book": expansion,
        "note": (
            "Per-customer payloads mirror the salesforce_comprehensive deck (mainstream object "
            "categories scoped to matched Customer Entity accounts). When "
            "BPO_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP is set (default 12), only the top "
            "active Salesforce labels by ARR are fetched — same ranking as §4 CS Report top-N. "
            "Set CUSTOMER_CAP=0 or all to fetch every active+churned portfolio label."
        ),
    }
    report["_llm_export_salesforce_comprehensive"] = summary
    logger.info(
        "LLM export: attached Salesforce comprehensive for %d customer label(s) "
        "(%d matched, %d Drive cache hit(s), %d Salesforce fetch(es), row_limit=%d)",
        len(labels),
        summary["customers_matched"],
        summary["customers_drive_cache_hit"],
        summary["customers_salesforce_fetch"],
        row_limit,
    )
    return summary
