"""Slack conversation summaries for the all-customers LLM export (top customers by ARR)."""

from __future__ import annotations

import os
from typing import Any

from .config import logger
from .llm_export_csr import llm_export_csr_top_n, top_active_customers_by_arr_for_csr


def llm_export_slack_enabled() -> bool:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SLACK") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    from .slack_client import slack_configured

    return slack_configured()


def llm_export_slack_top_n() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SLACK_TOP_N") or "").strip()
    if not raw:
        return llm_export_csr_top_n()
    try:
        return max(1, min(int(raw), 100))
    except ValueError:
        return llm_export_csr_top_n()


def attach_slack_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['slack']`` to per-customer conversation digests for top ARR labels."""
    from .slack_client import get_customer_slack_conversations, slack_configured

    top_n = llm_export_slack_top_n()
    summary: dict[str, Any] = {
        "scope": "top_customers_by_arr",
        "top_n": top_n,
        "enabled": llm_export_slack_enabled(),
        "slack_configured": slack_configured(),
        "customers_selected": 0,
        "customers_with_slack_data": 0,
        "customers_slack_errors": 0,
    }
    if not summary["enabled"]:
        report["slack"] = {
            "scope": "top_customers_by_arr",
            "skipped": "CORTEX_LLM_EXPORT_SLACK disabled",
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        return summary
    if not slack_configured():
        report["slack"] = {
            "scope": "top_customers_by_arr",
            "skipped": "slack_not_configured",
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        return summary

    selection = top_active_customers_by_arr_for_csr(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    if not selection:
        report["slack"] = {
            "scope": "top_customers_by_arr",
            "top_n": top_n,
            "note": "No active Salesforce contract rollups — Slack slices were not loaded.",
            "selection_ranked": [],
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        return summary

    days = int(report.get("days") or 30)
    by_customer: dict[str, Any] = {}
    for row in selection:
        label = str(row.get("salesforce_label") or "").strip()
        lookup = str(row.get("csr_lookup_name") or label).strip()
        if not lookup:
            continue
        payload = get_customer_slack_conversations(lookup, days=days)
        by_customer[label] = {
            "salesforce_label": label,
            "lookup_name": lookup,
            "arr": row.get("arr"),
            "slack": payload,
        }
        if payload.get("error"):
            summary["customers_slack_errors"] += 1
        elif payload.get("conversation_summaries"):
            summary["customers_with_slack_data"] += 1

    report["slack"] = {
        "scope": "top_customers_by_arr",
        "top_n": top_n,
        "lookback_days": days,
        "selection_ranked": selection,
        "customers": by_customer,
        "note": (
            "Per-customer Slack channel digests for the highest-ARR active Salesforce labels. "
            "Channels are matched by name and config/slack_customer_aliases.yaml; messages are recent "
            "human posts (not Slack AI summaries)."
        ),
    }
    report["_llm_export_slack"] = summary
    logger.info(
        "LLM export: Slack for top %d customer(s) by ARR (%d with channel data, %d errors)",
        len(selection),
        summary["customers_with_slack_data"],
        summary["customers_slack_errors"],
    )
    return summary
