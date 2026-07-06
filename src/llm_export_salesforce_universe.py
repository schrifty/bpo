"""Ensure LLM export includes Salesforce Customer Entities (active + churned segments)."""

from __future__ import annotations

from typing import Any

from .config import logger
from .data_source_health import _salesforce_configured
from .portfolio_salesforce_allowlist import (
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
)

# active rollups, churned-lost, portfolio labels, revenue book, renewal-negotiation, future-contract rollups
SfPortfolioSplit = tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[str],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]


def partition_inactive_sf_rollups(
    rollups: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Split non-current-book rollups into churned-lost, renewal-in-negotiation, and future-won."""
    from .salesforce_commercial_status import (
        COMMERCIAL_STATUS_CHURNED,
        COMMERCIAL_STATUS_FUTURE,
        COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
    )

    churned_lost: list[dict[str, Any]] = []
    renewal_negotiation: list[dict[str, Any]] = []
    future_contract: list[dict[str, Any]] = []
    for r in rollups:
        if not isinstance(r, dict):
            continue
        status = str(r.get("commercial_status") or "").strip()
        if status == COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING or r.get("renewal_in_flight") is True:
            renewal_negotiation.append(r)
        elif status == COMMERCIAL_STATUS_FUTURE:
            future_contract.append(r)
        elif status == COMMERCIAL_STATUS_CHURNED or (
            not status and r.get("active") is False and r.get("renewal_in_flight") is not True
        ):
            churned_lost.append(r)
    return churned_lost, renewal_negotiation, future_contract


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


def salesforce_portfolio_rollups_split() -> SfPortfolioSplit:
    """Return (current-book, churned-lost, labels, book, renewal-negotiation, future-won rollups)."""
    if not _salesforce_configured():
        return [], [], [], {"configured": False}, [], []
    from src.salesforce_client import SalesforceClient
    from src.salesforce_commercial_status import rollup_in_current_book

    sf = SalesforceClient()
    entity_accounts = sf.get_entity_accounts()
    labels = portfolio_labels_from_entity_accounts(entity_accounts)
    if not labels:
        return [], [], [], {"configured": True, "empty": True}, [], []
    book = sf.get_portfolio_revenue_book_metrics(labels)
    rollups = [
        r
        for r in (book.get("matched_customer_contract_rollups") or [])
        if isinstance(r, dict) and r.get("customer")
    ]
    active = [r for r in rollups if rollup_in_current_book(r)]
    churned_lost, renewal_neg, future_contract = partition_inactive_sf_rollups(rollups)
    return active, churned_lost, labels, book, renewal_neg, future_contract


def active_salesforce_portfolio_rollups() -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Return (current-book rollups, portfolio labels, revenue book dict). Empty when SF unavailable."""
    active, _churned, labels, book, _renewal, _future = salesforce_portfolio_rollups_split()
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
    def _skip_active_exclusion(row: dict[str, Any]) -> bool:
        from src.salesforce_commercial_status import (
            COMMERCIAL_STATUS_FUTURE,
            COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
        )

        status = str(row.get("commercial_status") or "").strip()
        if status in (COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING, COMMERCIAL_STATUS_FUTURE):
            return True
        return row.get("renewal_in_flight") is True

    for r in seg.get("customers_headline") or []:
        if not isinstance(r, dict):
            continue
        if _skip_active_exclusion(r):
            continue
        c = str(r.get("customer") or "").strip()
        if c:
            sf_labels.append(c)
            excluded.add(c.lower())
    sf = seg.get("salesforce") if isinstance(seg.get("salesforce"), dict) else {}
    for r in sf.get("matched_customer_contract_rollups") or []:
        if not isinstance(r, dict):
            continue
        if _skip_active_exclusion(r):
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

    True churn appears in ``salesforce_churned_segment`` (§3b); renewal negotiation in
    ``salesforce_renewal_negotiation_segment`` (§3b-renewal). Neither is stripped from §1
    when ``renewal_in_flight`` is true.
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


def _inactive_sf_headline_rows(
    rollups: list[dict[str, Any]],
    *,
    customer_segment: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for r in sorted(rollups, key=lambda x: str(x.get("customer") or "").lower()):
        label = str(r.get("customer") or "").strip()
        if not label:
            continue
        rows.append(
            {
                "customer": label,
                "customer_segment": customer_segment,
                "salesforce_only": True,
                "pendo_metrics_available": False,
                "active_in_salesforce": False,
                "churn_risk": r.get("churn_risk"),
                "arr": r.get("arr"),
                "contract_statuses_distinct": r.get("contract_statuses_distinct"),
                "contract_end_date_nearest": r.get("contract_end_date_nearest"),
                "renewal_in_flight": r.get("renewal_in_flight"),
                "pipeline_arr_including_parent_accounts": r.get(
                    "pipeline_arr_including_parent_accounts"
                ),
                "open_pipeline_opportunities_sample": r.get("open_pipeline_opportunities_sample"),
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
        _active, churned, _labels, book, _renewal, _future = salesforce_portfolio_rollups_split()
    else:
        _active, churned, _labels, book, _renewal, _future = sf_split
    summary["salesforce_churned_entities"] = len(churned)
    summary["salesforce_renewal_negotiation_entities"] = len(_renewal)
    if not churned:
        report["salesforce_churned_segment"] = {
            "segment": "churned",
            "do_not_merge_with_active_book": True,
            "customer_count": 0,
            "customers_headline": [],
            "salesforce": {"matched": False, "resolution": "portfolio_aggregate", "customer_segment": "churned"},
        }
    else:
        from src.data_sources.loaders.salesforce_portfolio_aggregate import (
            salesforce_aggregate_from_rollups,
        )

        sf_churned = salesforce_aggregate_from_rollups(churned, book=book, segment="churned")
        report["salesforce_churned_segment"] = {
            "segment": "churned",
            "do_not_merge_with_active_book": True,
            "usage_note": (
                "Salesforce-only **churned / lost** segment: inactive Customer Entity contracts with "
                "no open parent-account pipeline in stages 3–5. No Pendo, Jira, or §5 signals — "
                "do not sum with §1/§3 active installed-base metrics."
            ),
            "data_sources_included": ["salesforce"],
            "data_sources_excluded": ["pendo", "jira", "cs_report"],
            "customer_count": len(churned),
            "customers_headline": _inactive_sf_headline_rows(churned, customer_segment="churned"),
            "salesforce": sf_churned,
        }
        logger.info(
            "LLM export: attached churned-lost Salesforce segment with %d customer(s)",
            len(churned),
        )
    report["_llm_export_salesforce_churned"] = summary
    return summary


def attach_renewal_negotiation_segment_for_llm_export(
    report: dict[str, Any],
    *,
    sf_split: SfPortfolioSplit | None = None,
) -> dict[str, Any]:
    """Populate ``report['salesforce_renewal_negotiation_segment']`` (§3b-renewal)."""
    summary: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "salesforce_renewal_negotiation_entities": 0,
    }
    if not summary["salesforce_configured"]:
        report["salesforce_renewal_negotiation_segment"] = {
            "segment": "renewal_negotiation",
            "do_not_merge_with_active_book": True,
            "skipped": "salesforce_not_configured",
        }
        return summary

    if sf_split is None:
        _active, _churned, _labels, book, renewal, _future = salesforce_portfolio_rollups_split()
    else:
        _active, _churned, _labels, book, renewal, _future = sf_split
    summary["salesforce_renewal_negotiation_entities"] = len(renewal)
    if not renewal:
        report["salesforce_renewal_negotiation_segment"] = {
            "segment": "renewal_negotiation",
            "do_not_merge_with_active_book": True,
            "customer_count": 0,
            "customers_headline": [],
            "salesforce": {
                "matched": False,
                "resolution": "portfolio_aggregate",
                "customer_segment": "renewal_negotiation",
            },
        }
        return summary

    from src.data_sources.loaders.salesforce_portfolio_aggregate import salesforce_aggregate_from_rollups

    sf_renewal = salesforce_aggregate_from_rollups(
        renewal, book=book, segment="renewal_negotiation"
    )
    report["salesforce_renewal_negotiation_segment"] = {
        "segment": "renewal_negotiation",
        "do_not_merge_with_active_book": True,
        "usage_note": (
            "Expired/churned Customer Entity contracts with **open renewal pipeline** on parent "
            "accounts (stages 3–5, including Renewal type). Not churn risk — treat as contract "
            "negotiation. May still appear in §1 when Pendo data exists. Do not sum ARR into §3 "
            "active installed-base totals."
        ),
        "data_sources_included": ["salesforce"],
        "data_sources_excluded": ["pendo", "jira", "cs_report"],
        "customer_count": len(renewal),
        "customers_headline": _inactive_sf_headline_rows(
            renewal, customer_segment="renewal_negotiation"
        ),
        "salesforce": sf_renewal,
    }
    logger.info(
        "LLM export: attached renewal-negotiation segment with %d customer(s)",
        len(renewal),
    )
    return summary


def attach_future_contract_segment_for_llm_export(
    report: dict[str, Any],
    *,
    sf_split: SfPortfolioSplit | None = None,
) -> dict[str, Any]:
    """Populate ``report['salesforce_future_contract_segment']`` (won contracts not yet started)."""
    summary: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "salesforce_future_contract_entities": 0,
    }
    if not summary["salesforce_configured"]:
        report["salesforce_future_contract_segment"] = {
            "segment": "future_contract",
            "do_not_merge_with_active_book": True,
            "skipped": "salesforce_not_configured",
        }
        return summary

    if sf_split is None:
        _active, _churned, _labels, book, _renewal, future = salesforce_portfolio_rollups_split()
    else:
        _active, _churned, _labels, book, _renewal, future = sf_split
    summary["salesforce_future_contract_entities"] = len(future)
    if not future:
        report["salesforce_future_contract_segment"] = {
            "segment": "future_contract",
            "do_not_merge_with_active_book": True,
            "customer_count": 0,
            "customers_headline": [],
            "salesforce": {
                "matched": False,
                "resolution": "portfolio_aggregate",
                "customer_segment": "future_contract",
            },
        }
        return summary

    from src.data_sources.loaders.salesforce_portfolio_aggregate import salesforce_aggregate_from_rollups

    sf_future = salesforce_aggregate_from_rollups(future, book=book, segment="future_contract")
    report["salesforce_future_contract_segment"] = {
        "segment": "future_contract",
        "do_not_merge_with_active_book": True,
        "usage_note": (
            "Customer Entity groups with no active contract yet but a won/signed contract whose "
            "start date is in the future (or pending-activation status). Not churn and not "
            "renewal-in-flight — do not merge with §3 active installed-base totals."
        ),
        "data_sources_included": ["salesforce"],
        "data_sources_excluded": ["pendo", "jira", "cs_report"],
        "customer_count": len(future),
        "customers_headline": _inactive_sf_headline_rows(
            future, customer_segment="future_contract"
        ),
        "salesforce": sf_future,
    }
    logger.info(
        "LLM export: attached future-contract Salesforce segment with %d customer(s)",
        len(future),
    )
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
        rollups, _churned, sf_labels, book, _renewal, _future = sf_split
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
    """Merge active §1, attach §3b churn + §3b-renewal segments, strip true churn from active Pendo."""
    sf_split: SfPortfolioSplit | None = None
    if _salesforce_configured():
        sf_split = salesforce_portfolio_rollups_split()
    active_summary = merge_active_salesforce_customers_for_llm_export(report, sf_split=sf_split)
    churn_summary = attach_churned_salesforce_segment_for_llm_export(report, sf_split=sf_split)
    renewal_summary = attach_renewal_negotiation_segment_for_llm_export(report, sf_split=sf_split)
    future_summary = attach_future_contract_segment_for_llm_export(report, sf_split=sf_split)
    exclusion_summary = strip_churned_customers_from_active_export(report)
    return {
        "active": active_summary,
        "churned": churn_summary,
        "renewal_negotiation": renewal_summary,
        "future_contract": future_summary,
        "churn_exclusion_from_active": exclusion_summary,
    }


def active_sf_allowlist_lower() -> tuple[frozenset[str], list[str], dict[str, Any]]:
    """Labels for non-churned SF Customer Entities (for ``--customers-sf-allowlist``)."""
    rollups, labels, meta = active_salesforce_portfolio_rollups()
    active_lower = frozenset(str(r["customer"]).strip().lower() for r in rollups if r.get("customer"))
    return active_lower, labels, meta
