"""Jira HELP attachment for the all-customers LLM export (top customers by ARR)."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

from .config import logger
from .llm_export_csr import llm_export_csr_top_n, top_active_customers_by_arr_for_csr


def llm_export_jira_top_n() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_JIRA_TOP_N") or "").strip()
    if not raw:
        return llm_export_csr_top_n()
    try:
        return max(1, min(int(raw), 100))
    except ValueError:
        return llm_export_csr_top_n()


def llm_export_jira_workers() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_JIRA_WORKERS") or "").strip()
    if not raw:
        return 4
    try:
        return max(1, min(int(raw), 16))
    except ValueError:
        return 4


def llm_export_jira_customer_timeout_seconds() -> float:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_JIRA_CUSTOMER_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return 120.0
    try:
        return max(10.0, float(raw))
    except ValueError:
        return 120.0


def attach_jira_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['jira']`` to per-customer HELP slices for top ARR labels (not portfolio-only)."""
    days = min(int(report.get("days") or 90), 365)
    top_n = llm_export_jira_top_n()
    summary: dict[str, Any] = {
        "scope": "top_customers_by_arr",
        "top_n": top_n,
        "lookback_days": days,
        "customers_selected": 0,
        "customers_with_jira_data": 0,
        "customers_jira_errors": 0,
    }
    selection = top_active_customers_by_arr_for_csr(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    if not selection:
        report["jira"] = {
            "scope": "top_customers_by_arr",
            "top_n": top_n,
            "lookback_days": days,
            "note": (
                "No active Salesforce contract rollups — per-customer Jira HELP was not loaded. "
                "Ensure Salesforce portfolio merge ran before Jira attachment."
            ),
            "selection_ranked": [],
            "customers": {},
        }
        report["_llm_export_jira"] = summary
        return summary

    from src.jira_client import get_shared_jira_client

    jc = get_shared_jira_client()
    timeout_s = llm_export_jira_customer_timeout_seconds()
    workers = llm_export_jira_workers()

    from src.cs_report_client import cs_report_lookup_keys_for_account

    def _fetch_one(row: dict[str, Any]) -> tuple[str, dict[str, Any], list[str], str]:
        sf_label = str(row.get("salesforce_label") or "").strip()
        lookup_keys = cs_report_lookup_keys_for_account(
            salesforce_label=sf_label,
            pendo_customer_key=row.get("pendo_customer_key"),
        )
        if not lookup_keys:
            err = {"error": "empty jira lookup name", "customer": sf_label}
            return sf_label, err, [], ""
        lookup = lookup_keys[0]

        def _run() -> dict[str, Any]:
            last: dict[str, Any] = {"error": "no jira lookup keys", "customer": sf_label}
            for key in lookup_keys:
                last = jc.get_customer_jira(key, days=days)
                if isinstance(last, dict) and not last.get("error"):
                    return last
            return last

        try:
            with ThreadPoolExecutor(max_workers=1) as inner:
                fut = inner.submit(_run)
                payload = fut.result(timeout=timeout_s)
        except FuturesTimeoutError:
            payload = {"error": f"jira fetch timed out after {timeout_s:.0f}s", "customer": lookup}
        except Exception as e:
            payload = {"error": str(e)[:500], "customer": lookup}
        return sf_label, payload, lookup_keys, lookup

    by_customer: dict[str, Any] = {}

    logger.info(
        "LLM export: Jira HELP for top %d customer(s) by ARR (%d workers, %.0fs timeout each)",
        len(selection),
        workers,
        timeout_s,
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_fetch_one, row): row for row in selection}
        for fut in as_completed(futs):
            row = futs[fut]
            sf_label = str(row.get("salesforce_label") or "").strip()
            try:
                label, payload, lookup_keys, lookup = fut.result()
            except Exception as e:
                label, payload = sf_label, {"error": str(e)[:500], "customer": sf_label}
                lookup_keys, lookup = [], ""
            has_error = isinstance(payload, dict) and bool(payload.get("error"))
            if has_error:
                summary["customers_jira_errors"] += 1
            elif isinstance(payload, dict):
                summary["customers_with_jira_data"] += 1
            by_customer[label] = {
                "salesforce_label": sf_label,
                "arr": row.get("arr"),
                "pendo_customer_key": row.get("pendo_customer_key"),
                "jira_lookup_keys": lookup_keys,
                "jira_lookup_name": lookup,
                "jira": payload,
            }

    selection_ranked: list[dict[str, Any]] = []
    for row in selection:
        sf_label = str(row.get("salesforce_label") or "").strip()
        entry = by_customer.get(sf_label) or {}
        payload = entry.get("jira") if isinstance(entry, dict) else {}
        has_error = isinstance(payload, dict) and bool(payload.get("error"))
        selection_ranked.append(
            {
                "salesforce_label": sf_label,
                "arr": row.get("arr"),
                "jira_lookup_keys": entry.get("jira_lookup_keys") or [],
                "jira_lookup_name": entry.get("jira_lookup_name") or "",
                "jira_loaded": not has_error,
            }
        )

    report["jira"] = {
        "scope": "top_customers_by_arr",
        "top_n": top_n,
        "lookback_days": days,
        "selection_ranked": selection_ranked,
        "customers": by_customer,
        "note": (
            "Per-customer Jira HELP (JSM org scope, ticket metrics, engineering/enhancement slices) "
            "for the highest-ARR active Salesforce Customer Entity labels. Each entry under "
            "``customers`` is one account — not a single portfolio-wide HELP aggregate."
        ),
    }
    report["_llm_export_jira"] = summary
    logger.info(
        "LLM export: Jira for top %d customer(s) by ARR (%d with data, %d errors)",
        len(selection),
        summary["customers_with_jira_data"],
        summary["customers_jira_errors"],
    )
    return summary
