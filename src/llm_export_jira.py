"""Jira HELP attachment for the all-customers LLM export (top ultimate parents by ARR)."""

from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

from .config import logger
from .llm_export_csr import (
    LLM_EXPORT_TOP_ARR_SCOPE,
    llm_export_csr_top_n,
    top_active_ultimate_parents_by_arr_for_llm_export,
)


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


def _division_names_without_parenthetical(labels: list[Any]) -> list[str]:
    """JSM org labels often omit the parenthetical parent (e.g. ``Commercial HVAC`` not ``… (Carrier)``)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in labels:
        label = str(raw or "").strip()
        if not label:
            continue
        base = re.sub(r"\s*\([^)]+\)\s*$", "", label).strip()
        if not base or base == label:
            continue
        key = base.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(base)
    return out


def _jira_merged_lookup_bundle(row: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    """Primary JSM lookup + subsidiary match terms for one ultimate-parent export row."""
    from src.cs_report_client import selection_lookup_keys_for_llm_export

    lookup_keys = selection_lookup_keys_for_llm_export(row)
    if not lookup_keys:
        return "", [], []
    primary = lookup_keys[0]
    match_terms = list(lookup_keys[1:])
    seen = {t.lower() for t in lookup_keys}
    for division in _division_names_without_parenthetical(row.get("salesforce_labels") or []):
        if division.lower() not in seen:
            seen.add(division.lower())
            match_terms.append(division)
    return primary, match_terms, lookup_keys


def attach_jira_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['jira']`` to per-ultimate-parent HELP slices for top ARR (not portfolio-only)."""
    days = min(int(report.get("days") or 90), 365)
    top_n = llm_export_jira_top_n()
    summary: dict[str, Any] = {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": top_n,
        "lookback_days": days,
        "customers_selected": 0,
        "customers_with_jira_data": 0,
        "customers_jira_errors": 0,
    }
    selection = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    if not selection:
        report["jira"] = {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
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

    def _fetch_one(row: dict[str, Any]) -> tuple[str, dict[str, Any], list[str], str, list[str]]:
        customer_key = str(
            row.get("ultimate_parent") or row.get("salesforce_label") or ""
        ).strip()
        primary, match_terms, lookup_keys = _jira_merged_lookup_bundle(row)
        if not primary:
            err = {"error": "empty jira lookup name", "customer": customer_key}
            return customer_key, err, [], "", []

        def _run() -> dict[str, Any]:
            return jc.get_customer_jira(
                primary,
                days=days,
                match_terms=match_terms or None,
            )

        try:
            with ThreadPoolExecutor(max_workers=1) as inner:
                fut = inner.submit(_run)
                payload = fut.result(timeout=timeout_s)
        except FuturesTimeoutError:
            payload = {
                "error": f"jira fetch timed out after {timeout_s:.0f}s",
                "customer": primary,
            }
        except Exception as e:
            payload = {"error": str(e)[:500], "customer": primary}
        return customer_key, payload, lookup_keys, primary, match_terms

    by_customer: dict[str, Any] = {}

    logger.info(
        "LLM export: Jira HELP for top %d ultimate parent(s) by ARR (%d workers, %.0fs timeout each)",
        len(selection),
        workers,
        timeout_s,
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_fetch_one, row): row for row in selection}
        for fut in as_completed(futs):
            row = futs[fut]
            customer_key = str(
                row.get("ultimate_parent") or row.get("salesforce_label") or ""
            ).strip()
            try:
                label, payload, lookup_keys, lookup, match_terms = fut.result()
            except Exception as e:
                label, payload = customer_key, {"error": str(e)[:500], "customer": customer_key}
                lookup_keys, lookup, match_terms = [], "", []
            has_error = isinstance(payload, dict) and bool(payload.get("error"))
            if has_error:
                summary["customers_jira_errors"] += 1
            elif isinstance(payload, dict):
                summary["customers_with_jira_data"] += 1
            by_customer[label] = {
                "ultimate_parent": row.get("ultimate_parent") or label,
                "salesforce_label": label,
                "salesforce_labels": row.get("salesforce_labels") or [],
                "arr": row.get("arr"),
                "pendo_customer_key": row.get("pendo_customer_key"),
                "jira_lookup_keys": lookup_keys,
                "jira_lookup_name": lookup,
                "jira_match_terms": match_terms,
                "jira_merged_subsidiary_lookups": bool(match_terms),
                "jira": payload,
            }

    selection_ranked: list[dict[str, Any]] = []
    for row in selection:
        customer_key = str(
            row.get("ultimate_parent") or row.get("salesforce_label") or ""
        ).strip()
        entry = by_customer.get(customer_key) or {}
        payload = entry.get("jira") if isinstance(entry, dict) else {}
        has_error = isinstance(payload, dict) and bool(payload.get("error"))
        selection_ranked.append(
            {
                "ultimate_parent": row.get("ultimate_parent") or customer_key,
                "salesforce_label": customer_key,
                "salesforce_labels": row.get("salesforce_labels") or [],
                "arr": row.get("arr"),
                "jira_lookup_keys": entry.get("jira_lookup_keys") or [],
                "jira_lookup_name": entry.get("jira_lookup_name") or "",
                "jira_loaded": not has_error,
            }
        )

    report["jira"] = {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": top_n,
        "lookback_days": days,
        "selection_ranked": selection_ranked,
        "customers": by_customer,
        "note": (
            "Per-customer Jira HELP (JSM org scope, ticket metrics, engineering/enhancement slices) "
            "for the highest-ARR active Salesforce ultimate parents (contract rollups summed by "
            "ultimate parent — same grouping as ``arr_by_ultimate_parent`` in §3c). Each entry "
            "under ``customers`` is one ultimate parent. HELP tickets are fetched with a single "
            "merged JSM ``Organizations`` filter: the ultimate parent name plus every constituent "
            "Salesforce label and division name (e.g. ``Commercial HVAC`` from "
            "``Commercial HVAC (Carrier)``) so dirty or split JSM org naming still rolls up."
        ),
    }
    report["_llm_export_jira"] = summary
    logger.info(
        "LLM export: Jira for top %d ultimate parent(s) by ARR (%d with data, %d errors)",
        len(selection),
        summary["customers_with_jira_data"],
        summary["customers_jira_errors"],
    )
    return summary
