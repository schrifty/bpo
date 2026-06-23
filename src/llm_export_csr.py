"""CS Report attachment for the all-customers LLM export (top customers by ARR)."""

from __future__ import annotations

import os
from typing import Any

from .config import logger
from .portfolio_salesforce_allowlist import resolve_sf_label_to_pendo_prefix


def llm_export_csr_top_n() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_CSR_TOP_N") or "").strip()
    if not raw:
        return 20
    try:
        return max(1, min(int(raw), 100))
    except ValueError:
        return 20


def top_active_customers_by_arr_for_csr(
    report: dict[str, Any],
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    """Build ranked selection rows from Salesforce contract rollups (active book only)."""
    book = report.get("_llm_export_salesforce_revenue_book")
    if not isinstance(book, dict):
        book = report.get("portfolio_revenue_book")
    rollups: list[dict[str, Any]] = []
    if isinstance(book, dict):
        rollups = [
            r
            for r in (book.get("matched_customer_contract_rollups") or [])
            if isinstance(r, dict) and str(r.get("customer") or "").strip()
            and r.get("active") is not False
        ]
    if not rollups:
        return []

    pendo_prefixes = frozenset(
        str(r.get("customer") or "").strip()
        for r in (report.get("customers") or [])
        if isinstance(r, dict) and str(r.get("customer") or "").strip()
    )
    rows: list[dict[str, Any]] = []
    for r in rollups:
        label = str(r["customer"]).strip()
        try:
            arr = float(r.get("arr") or 0)
        except (TypeError, ValueError):
            arr = 0.0
        mapped = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
        lookup = (mapped or label).strip()
        rows.append(
            {
                "salesforce_label": label,
                "arr": round(arr, 2),
                "pendo_customer_key": mapped,
                "csr_lookup_name": lookup,
            }
        )
    rows.sort(key=lambda x: (-float(x.get("arr") or 0), str(x.get("salesforce_label") or "").lower()))
    return rows[: max(1, int(top_n))]


def attach_csr_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['csr']`` to per-customer CS Report slices for top ARR labels (not all-customers merge)."""
    top_n = llm_export_csr_top_n()
    summary: dict[str, Any] = {
        "scope": "top_customers_by_arr",
        "top_n": top_n,
        "customers_selected": 0,
        "customers_with_csr_data": 0,
        "customers_csr_errors": 0,
    }
    selection = top_active_customers_by_arr_for_csr(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    if not selection:
        report["csr"] = {
            "scope": "top_customers_by_arr",
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
