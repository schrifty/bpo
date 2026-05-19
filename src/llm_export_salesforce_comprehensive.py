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
        return 75
    try:
        return max(1, min(int(raw), 500))
    except ValueError:
        return 75


def llm_export_sf_comprehensive_customer_cap() -> int | None:
    """Optional max customers to fetch (active labels first, then churned). ``None`` = no cap."""
    raw = (os.environ.get("BPO_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP") or "").strip()
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return None


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
    Customer Entity label (active + churned). Also attaches all Customer Entity account
    rows and portfolio expansion KPIs when available.
    """
    summary: dict[str, Any] = {
        "enabled": llm_export_sf_comprehensive_enabled(),
        "salesforce_configured": _salesforce_configured(),
        "customers_requested": 0,
        "customers_fetched": 0,
        "customers_matched": 0,
        "customers_errors": 0,
        "row_limit": llm_export_sf_comprehensive_row_limit(),
        "customer_cap": llm_export_sf_comprehensive_customer_cap(),
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

    labels = _rollup_labels_with_segment(report)
    cap = summary["customer_cap"]
    if cap is not None and len(labels) > cap:
        labels = labels[:cap]
        summary["customers_truncated"] = True
    summary["customers_requested"] = len(labels)

    from src.salesforce_client import SalesforceClient

    sf = SalesforceClient()
    row_limit = int(summary["row_limit"])
    by_customer: dict[str, Any] = {}

    for label, segment in labels:
        try:
            payload = sf.get_customer_salesforce_comprehensive(label, row_limit=row_limit)
            if isinstance(payload, dict):
                payload = dict(payload)
                payload["customer_segment"] = segment
            by_customer[label] = payload
            summary["customers_fetched"] += 1
            if isinstance(payload, dict) and payload.get("matched"):
                summary["customers_matched"] += 1
            if isinstance(payload, dict) and payload.get("error"):
                summary["customers_errors"] += 1
        except Exception as e:
            logger.warning("LLM export Salesforce comprehensive failed for %s: %s", label, e)
            by_customer[label] = {
                "customer": label,
                "customer_segment": segment,
                "matched": False,
                "error": str(e)[:500],
            }
            summary["customers_fetched"] += 1
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
            "Per-customer payloads mirror the salesforce_comprehensive deck: mainstream object "
            "categories (contacts, opportunities, cases, tasks, events, contracts, orders, quotes, "
            "assets, campaigns, leads, products/pricebooks samples) scoped to matched Customer "
            "Entity accounts and ParentId hierarchy expansion."
        ),
    }
    report["_llm_export_salesforce_comprehensive"] = summary
    logger.info(
        "LLM export: attached Salesforce comprehensive for %d customer label(s) "
        "(%d matched, row_limit=%d)",
        len(labels),
        summary["customers_matched"],
        row_limit,
    )
    return summary
