"""CS Report attachment for the all-customers LLM export (top ultimate parents by ARR)."""

from __future__ import annotations

import os
from typing import Any

from .config import logger
from .portfolio_salesforce_allowlist import resolve_sf_label_to_pendo_prefix
from .salesforce_reporting import entity_account_ultimate_parent_group

# Scope for §2 Jira, §4 CSR, Slack, and SF comprehensive top-N selection (aligned with
# ``salesforce_comprehensive_portfolio.arr_by_ultimate_parent``).
LLM_EXPORT_TOP_ARR_SCOPE = "top_ultimate_parents_by_arr"


def llm_export_csr_top_n() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_CSR_TOP_N") or "").strip()
    if not raw:
        return 20
    try:
        return max(1, min(int(raw), 100))
    except ValueError:
        return 20


def _active_contract_rollups(report: dict[str, Any]) -> list[dict[str, Any]]:
    from .salesforce_commercial_status import rollup_in_current_book

    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")
    if not isinstance(book, dict):
        return []
    return [
        r
        for r in (book.get("matched_customer_contract_rollups") or [])
        if isinstance(r, dict)
        and str(r.get("customer") or "").strip()
        and rollup_in_current_book(r)
    ]


def _rollup_as_synthetic_account(label: str) -> dict[str, Any]:
    return {
        "Name": label,
        "LeanDNA_Entity_Name__c": label,
        "parent_name": "",
        "ultimate_parent_name": "",
    }


def top_active_ultimate_parents_by_arr_for_llm_export(
    report: dict[str, Any],
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    """Rank active Salesforce contract ARR by ultimate parent (same grouping as §3c rollup)."""
    rollups = _active_contract_rollups(report)
    if not rollups:
        return []

    pendo_prefixes = frozenset(
        str(r.get("customer") or "").strip()
        for r in (report.get("customers") or [])
        if isinstance(r, dict) and str(r.get("customer") or "").strip()
    )
    grouped: dict[str, dict[str, Any]] = {}
    for r in rollups:
        label = str(r["customer"]).strip()
        try:
            from .salesforce_commercial_status import rollup_current_arr

            arr = rollup_current_arr(r)
        except (TypeError, ValueError):
            arr = 0.0
        ultimate = entity_account_ultimate_parent_group(_rollup_as_synthetic_account(label))
        bucket = grouped.setdefault(
            ultimate,
            {"ultimate_parent": ultimate, "arr": 0.0, "salesforce_labels": []},
        )
        bucket["arr"] = round(float(bucket["arr"]) + arr, 2)
        if label not in bucket["salesforce_labels"]:
            bucket["salesforce_labels"].append(label)

    rows: list[dict[str, Any]] = []
    for ultimate, bucket in grouped.items():
        mapped = resolve_sf_label_to_pendo_prefix(ultimate, pendo_prefixes)
        if not mapped:
            for label in bucket["salesforce_labels"]:
                mapped = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
                if mapped:
                    break
        lookup = (mapped or ultimate).strip()
        rows.append(
            {
                "ultimate_parent": ultimate,
                "salesforce_label": ultimate,
                "salesforce_labels": sorted(bucket["salesforce_labels"]),
                "arr": bucket["arr"],
                "current_arr": bucket["arr"],
                "pendo_customer_key": mapped,
                "csr_lookup_name": lookup,
            }
        )
    rows.sort(
        key=lambda x: (
            -float(x.get("arr") or 0),
            str(x.get("ultimate_parent") or "").lower(),
        )
    )
    return rows[: max(1, int(top_n))]


def top_active_customers_by_arr_for_csr(
    report: dict[str, Any],
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    """Build ranked selection rows from active contract rollups grouped by ultimate parent."""
    return top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=top_n)


def attach_csr_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['csr']`` to per-ultimate-parent CS Report slices for top ARR (not all-customers merge)."""
    top_n = llm_export_csr_top_n()
    summary: dict[str, Any] = {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": top_n,
        "customers_selected": 0,
        "customers_with_csr_data": 0,
        "customers_csr_errors": 0,
    }
    selection = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    if not selection:
        report["csr"] = {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
            "top_n": top_n,
            "note": (
                "No active Salesforce contract rollups on the report — CS Report per-customer "
                "slices were not loaded. Ensure Salesforce portfolio merge ran before CSR attachment."
            ),
            "selection_ranked": [],
            "customers": {},
        }
        report["_llm_export_csr"] = summary
        return summary

    from src.cs_report_client import load_csr_top_customers_by_arr

    report["csr"] = load_csr_top_customers_by_arr(selection)
    customers = report["csr"].get("customers") if isinstance(report["csr"], dict) else {}
    if isinstance(customers, dict):
        for _label, block in customers.items():
            if not isinstance(block, dict):
                continue
            errs = 0
            for key in ("platform_health", "supply_chain", "platform_value"):
                sec = block.get(key)
                if isinstance(sec, dict) and sec.get("error"):
                    errs += 1
            if errs == 3:
                summary["customers_csr_errors"] += 1
            elif errs < 3:
                summary["customers_with_csr_data"] += 1
    logger.info(
        "LLM export: CS Report for top %d customer(s) by ARR (%d with data, %d all-section errors)",
        len(selection),
        summary["customers_with_csr_data"],
        summary["customers_csr_errors"],
    )
    report["_llm_export_csr"] = summary
    return summary
