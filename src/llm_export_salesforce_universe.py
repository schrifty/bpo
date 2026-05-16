"""Ensure LLM export includes active Salesforce Customer Entities without requiring Pendo."""

from __future__ import annotations

from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured
from .portfolio_salesforce_allowlist import (
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
)


def _customer_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    raw = report.get("customers")
    return [r for r in raw if isinstance(r, dict) and str(r.get("customer") or "").strip()]


def _pendo_prefixes_from_rows(rows: list[dict[str, Any]]) -> frozenset[str]:
    return frozenset(str(r.get("customer") or "").strip() for r in rows if str(r.get("customer") or "").strip())


def _row_has_pendo_metrics(row: dict[str, Any]) -> bool:
    if row.get("salesforce_only"):
        return False
    for key in ("total_users", "active_users", "login_pct"):
        if row.get(key) is not None:
            return True
    return False


def customer_matches_active_sf_label(
    customer: str,
    *,
    active_sf_labels_lower: frozenset[str],
    pendo_prefixes: frozenset[str],
    sf_portfolio_labels: list[str],
) -> bool:
    """True if *customer* is an active SF book label or a Pendo key mapped from one."""
    c = (customer or "").strip()
    if not c:
        return False
    cl = c.lower()
    if cl in active_sf_labels_lower:
        return True
    for label in sf_portfolio_labels:
        if label.lower() not in active_sf_labels_lower:
            continue
        mapped = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
        if mapped and mapped.lower() == cl:
            return True
    return False


def active_salesforce_portfolio_rollups() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Return (active rollups, portfolio labels, revenue book dict). Empty when SF unavailable."""
    if not _salesforce_configured():
        return [], [], {"configured": False}
    from src.salesforce_client import SalesforceClient

    sf = SalesforceClient()
    entity_accounts = sf.get_entity_accounts()
    labels = portfolio_labels_from_entity_accounts(entity_accounts)
    if not labels:
        return [], [], {"configured": True, "empty": True}
    book = sf.get_portfolio_revenue_book_metrics(labels)
    rollups = [
        r
        for r in (book.get("matched_customer_contract_rollups") or [])
        if isinstance(r, dict) and r.get("customer") and r.get("active") is not False
    ]
    return rollups, labels, book


def merge_active_salesforce_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Add §1 rows and refresh metadata so §3 includes non-churned SF entities without Pendo."""
    summary: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "salesforce_portfolio_labels": 0,
        "salesforce_active_entities": 0,
        "added_salesforce_only_rows": 0,
        "salesforce_labels_without_pendo": [],
    }
    if not summary["salesforce_configured"]:
        report["_llm_export_salesforce_universe"] = summary
        return summary

    rollups, sf_labels, book = active_salesforce_portfolio_rollups()
    summary["salesforce_portfolio_labels"] = len(sf_labels)
    summary["salesforce_active_entities"] = len(rollups)
    active_lower = frozenset(str(r["customer"]).strip().lower() for r in rollups)

    rows = _customer_rows(report)
    pendo_prefixes = _pendo_prefixes_from_rows(rows)
    by_lower: dict[str, dict[str, Any]] = {}
    for r in rows:
        c = str(r.get("customer") or "").strip()
        if c:
            by_lower[c.lower()] = r

    without_pendo: list[str] = []
    for r in rollups:
        sf_label = str(r["customer"]).strip()
        if not sf_label:
            continue
        mapped = resolve_sf_label_to_pendo_prefix(sf_label, pendo_prefixes)
        if mapped and mapped.lower() in by_lower:
            continue
        if sf_label.lower() in by_lower:
            row = by_lower[sf_label.lower()]
            if not _row_has_pendo_metrics(row):
                row.setdefault("salesforce_label", sf_label)
            continue
        by_lower[sf_label.lower()] = {
            "customer": sf_label,
            "salesforce_only": True,
            "pendo_metrics_available": False,
            "pendo_customer_key": mapped,
            "total_users": None,
            "active_users": None,
            "login_pct": None,
            "pendo_csm": None,
        }
        summary["added_salesforce_only_rows"] += 1
        if mapped is None:
            without_pendo.append(sf_label)

    ordered = sorted(by_lower.values(), key=lambda x: str(x.get("customer") or "").lower())
    report["customers"] = ordered
    report["customer_count"] = len(ordered)
    summary["salesforce_labels_without_pendo"] = without_pendo[:40]
    if without_pendo:
        logger.info(
            "LLM export: %d active Salesforce Customer Entity label(s) have no Pendo prefix match "
            "(included in export with Salesforce facts only): %s",
            len(without_pendo),
            without_pendo[:12],
        )
    report["_llm_export_salesforce_universe"] = summary
    report["_llm_export_salesforce_revenue_book"] = book
    return summary


def active_sf_allowlist_lower() -> tuple[frozenset[str], list[str], dict[str, Any]]:
    """Labels for non-churned SF Customer Entities (for ``--customers-sf-allowlist``)."""
    rollups, labels, meta = active_salesforce_portfolio_rollups()
    active_lower = frozenset(str(r["customer"]).strip().lower() for r in rollups if r.get("customer"))
    return active_lower, labels, meta
