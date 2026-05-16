"""Ensure LLM export includes Salesforce Customer Entities (active + churned segments)."""

from __future__ import annotations

from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured
from .portfolio_salesforce_allowlist import (
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
)

# (active rollups, churned rollups, portfolio labels, revenue book)
SfPortfolioSplit = tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], dict[str, Any]]


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


def salesforce_portfolio_rollups_split() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[str],
    dict[str, Any],
]:
    """Return (active rollups, churned rollups, portfolio labels, revenue book)."""
    if not _salesforce_configured():
        return [], [], [], {"configured": False}
    from src.salesforce_client import SalesforceClient

    sf = SalesforceClient()
    entity_accounts = sf.get_entity_accounts()
    labels = portfolio_labels_from_entity_accounts(entity_accounts)
    if not labels:
        return [], [], [], {"configured": True, "empty": True}
    book = sf.get_portfolio_revenue_book_metrics(labels)
    rollups = [
        r
        for r in (book.get("matched_customer_contract_rollups") or [])
        if isinstance(r, dict) and r.get("customer")
    ]
    active = [r for r in rollups if r.get("active") is not False]
    churned = [r for r in rollups if r.get("active") is False]
    return active, churned, labels, book


def active_salesforce_portfolio_rollups() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Return (active rollups, portfolio labels, revenue book dict). Empty when SF unavailable."""
    active, _churned, labels, book = salesforce_portfolio_rollups_split()
    return active, labels, book


def churned_sf_excluded_customer_keys(
    report: dict[str, Any],
    *,
    pendo_prefixes: frozenset[str] | None = None,
) -> frozenset[str]:
    """Lowercase names to omit from active §1/§5 (and §7): churned SF labels + mapped Pendo keys."""
    seg = report.get("salesforce_churned_segment")
    if not isinstance(seg, dict):
        return frozenset()
    sf_labels: list[str] = []
    excluded: set[str] = set()
    for r in seg.get("customers_headline") or []:
        if not isinstance(r, dict):
            continue
        c = str(r.get("customer") or "").strip()
        if c:
            sf_labels.append(c)
            excluded.add(c.lower())
    sf = seg.get("salesforce") if isinstance(seg, dict) else {}
    for r in sf.get("matched_customer_contract_rollups") or []:
        if not isinstance(r, dict):
            continue
        c = str(r.get("customer") or "").strip()
        if c:
            sf_labels.append(c)
            excluded.add(c.lower())
    prefixes = pendo_prefixes if pendo_prefixes is not None else _pendo_prefixes_from_rows(_customer_rows(report))
    for label in sf_labels:
        mapped = resolve_sf_label_to_pendo_prefix(label, prefixes)
        if mapped:
            excluded.add(mapped.strip().lower())
    return frozenset(excluded)


def strip_churned_customers_from_active_export(report: dict[str, Any]) -> dict[str, Any]:
    """Remove Salesforce-churned customers from active Pendo sections (§1, §5, §7 inputs).

    Churned accounts appear only in ``salesforce_churned_segment`` (§3b) with Salesforce facts —
    no Pendo headline metrics, usage signals, or per-customer Jira slices.
    """
    summary: dict[str, Any] = {
        "excluded_customer_keys": 0,
        "removed_customer_rows": 0,
        "removed_portfolio_signals": 0,
    }
    excluded = churned_sf_excluded_customer_keys(report)
    summary["excluded_customer_keys"] = len(excluded)
    if not excluded:
        return summary

    before_c = _customer_rows(report)
    kept = [r for r in before_c if str(r.get("customer") or "").strip().lower() not in excluded]
    summary["removed_customer_rows"] = len(before_c) - len(kept)
    report["customers"] = kept
    report["customer_count"] = len(kept)

    sigs = report.get("portfolio_signals")
    if isinstance(sigs, list):
        kept_sig = []
        removed_sig = 0
        for item in sigs:
            if not isinstance(item, dict):
                kept_sig.append(item)
                continue
            cust = str(item.get("customer") or "").strip().lower()
            if cust and cust in excluded:
                removed_sig += 1
                continue
            kept_sig.append(item)
        report["portfolio_signals"] = kept_sig
        summary["removed_portfolio_signals"] = removed_sig

    if summary["removed_customer_rows"] or summary["removed_portfolio_signals"]:
        logger.info(
            "LLM export: removed %d active customer row(s) and %d Pendo signal line(s) for "
            "Salesforce-churned accounts (see §3b only)",
            summary["removed_customer_rows"],
            summary["removed_portfolio_signals"],
        )
    report.setdefault("_llm_export_salesforce_universe", {})
    if isinstance(report["_llm_export_salesforce_universe"], dict):
        report["_llm_export_salesforce_universe"]["churn_exclusion_from_active"] = summary
    return summary


def _churned_headline_rows(churned_rollups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for r in sorted(churned_rollups, key=lambda x: str(x.get("customer") or "").lower()):
        label = str(r.get("customer") or "").strip()
        if not label:
            continue
        rows.append(
            {
                "customer": label,
                "customer_segment": "churned",
                "salesforce_only": True,
                "pendo_metrics_available": False,
                "active_in_salesforce": False,
                "arr": r.get("arr"),
                "contract_statuses_distinct": r.get("contract_statuses_distinct"),
                "contract_end_date_nearest": r.get("contract_end_date_nearest"),
                "total_users": None,
                "active_users": None,
                "login_pct": None,
                "pendo_csm": None,
            }
        )
    return rows


def attach_churned_salesforce_segment_for_llm_export(
    report: dict[str, Any],
    *,
    sf_split: SfPortfolioSplit | None = None,
) -> dict[str, Any]:
    """Populate ``report[\"salesforce_churned_segment\"]`` — separate from the active book (§1/§3)."""
    summary: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "salesforce_churned_entities": 0,
    }
    if not summary["salesforce_configured"]:
        report["salesforce_churned_segment"] = {
            "segment": "churned",
            "do_not_merge_with_active_book": True,
            "skipped": "salesforce_not_configured",
        }
        return summary

    if sf_split is None:
        _active, churned, _labels, book = salesforce_portfolio_rollups_split()
    else:
        _active, churned, _labels, book = sf_split
    summary["salesforce_churned_entities"] = len(churned)
    if not churned:
        report["salesforce_churned_segment"] = {
            "segment": "churned",
            "do_not_merge_with_active_book": True,
            "customer_count": 0,
            "customers_headline": [],
            "salesforce": {"matched": False, "resolution": "portfolio_aggregate", "customer_segment": "churned"},
        }
        report["_llm_export_salesforce_churned"] = summary
        return summary

    from src.data_sources.loaders.salesforce_portfolio_aggregate import salesforce_aggregate_from_rollups

    sf_churned = salesforce_aggregate_from_rollups(churned, book=book, segment="churned")
    report["salesforce_churned_segment"] = {
        "segment": "churned",
        "do_not_merge_with_active_book": True,
        "usage_note": (
            "Salesforce-only churn segment: inactive Customer Entity contract rollups. "
            "No Pendo product usage, no Jira/Atlassian HELP slices, and no §5 signals — "
            "do not sum with §1/§3 active installed-base metrics or pipeline ARR."
        ),
        "data_sources_included": ["salesforce"],
        "data_sources_excluded": ["pendo", "jira", "cs_report"],
        "customer_count": len(churned),
        "customers_headline": _churned_headline_rows(churned),
        "salesforce": sf_churned,
    }
    report["_llm_export_salesforce_churned"] = summary
    logger.info("LLM export: attached churned Salesforce segment with %d customer(s)", len(churned))
    return summary


def merge_active_salesforce_customers_for_llm_export(
    report: dict[str, Any],
    *,
    sf_split: SfPortfolioSplit | None = None,
) -> dict[str, Any]:
    """Add active §1 rows so §3 active book includes non-churned SF entities without Pendo."""
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

    if sf_split is None:
        rollups, sf_labels, book = active_salesforce_portfolio_rollups()
    else:
        rollups, _churned, sf_labels, book = sf_split
    summary["salesforce_portfolio_labels"] = len(sf_labels)
    summary["salesforce_active_entities"] = len(rollups)

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
            if row.get("customer_segment") is None:
                row["customer_segment"] = "active"
            continue
        by_lower[sf_label.lower()] = {
            "customer": sf_label,
            "customer_segment": "active",
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
            "(included in active export segment with Salesforce facts only): %s",
            len(without_pendo),
            without_pendo[:12],
        )
    report["_llm_export_salesforce_universe"] = summary
    report["_llm_export_salesforce_revenue_book"] = book
    return summary


def merge_salesforce_universe_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Merge active customers into §1, attach Salesforce-only §3b churn, strip churn from active Pendo."""
    sf_split: SfPortfolioSplit | None = None
    if _salesforce_configured():
        sf_split = salesforce_portfolio_rollups_split()
    active_summary = merge_active_salesforce_customers_for_llm_export(report, sf_split=sf_split)
    churn_summary = attach_churned_salesforce_segment_for_llm_export(report, sf_split=sf_split)
    exclusion_summary = strip_churned_customers_from_active_export(report)
    return {
        "active": active_summary,
        "churned": churn_summary,
        "churn_exclusion_from_active": exclusion_summary,
    }


def active_sf_allowlist_lower() -> tuple[frozenset[str], list[str], dict[str, Any]]:
    """Labels for non-churned SF Customer Entities (for ``--customers-sf-allowlist``)."""
    rollups, labels, meta = active_salesforce_portfolio_rollups()
    active_lower = frozenset(str(r["customer"]).strip().lower() for r in rollups if r.get("customer"))
    return active_lower, labels, meta
