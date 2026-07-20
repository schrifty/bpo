"""Slack conversation summaries for the all-customers LLM export (top ultimate parents by ARR)."""

from __future__ import annotations

import os
import time
from typing import Any

from .config import (
    CORTEX_LLM_EXPORT_SLACK_LOOKBACK_DAYS,
    CORTEX_LLM_EXPORT_SLACK_MAX_MESSAGES_PER_CHANNEL,
    logger,
)
from .llm_export_csr import (
    LLM_EXPORT_TOP_ARR_SCOPE,
    top_active_ultimate_parents_by_arr_for_llm_export,
)
from .llm_export_slack_summarize import (
    SlackSummaryLlmError,
    llm_export_slack_llm_enabled,
    summarize_customer_slack_for_llm_export,
)

# Pilot scope: top-N by current ARR before expanding to the full current book.
_DEFAULT_SLACK_EXPORT_TOP_N = 10


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
        return _DEFAULT_SLACK_EXPORT_TOP_N
    try:
        return max(1, min(int(raw), 500))
    except ValueError:
        return _DEFAULT_SLACK_EXPORT_TOP_N


def attach_slack_top_customers_for_llm_export(report: dict[str, Any]) -> dict[str, Any]:
    """Set ``report['slack']`` to per-ultimate-parent Slack digests + LLM summaries (top ARR)."""
    from .slack_client import get_customer_slack_conversations, slack_configured

    top_n = llm_export_slack_top_n()
    lookback_days = CORTEX_LLM_EXPORT_SLACK_LOOKBACK_DAYS
    summary: dict[str, Any] = {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": top_n,
        "lookback_days": lookback_days,
        "enabled": llm_export_slack_enabled(),
        "slack_configured": slack_configured(),
        "llm_summary_enabled": llm_export_slack_llm_enabled(),
        "customers_selected": 0,
        "customers_with_slack_data": 0,
        "customers_slack_errors": 0,
        "customers_llm_summarized": 0,
        "customers_llm_errors": 0,
        "performance": {
            "wall_seconds_total": 0.0,
            "fetch_wall_seconds": 0.0,
            "llm_wall_seconds": 0.0,
            "per_customer": [],
        },
    }
    if not summary["enabled"]:
        report["slack"] = {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
            "skipped": "CORTEX_LLM_EXPORT_SLACK disabled",
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        logger.info("LLM export Slack: skipped (CORTEX_LLM_EXPORT_SLACK disabled)")
        return summary
    if not slack_configured():
        report["slack"] = {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
            "skipped": "slack_not_configured",
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        logger.info("LLM export Slack: skipped (SLACK_BOT_TOKEN not configured)")
        return summary

    selection = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=top_n)
    summary["customers_selected"] = len(selection)
    logger.info(
        "LLM export Slack: start top_n=%d lookback_days=%d max_messages_per_channel=%d "
        "llm_summary=%s selection=%d customers=%s",
        top_n,
        lookback_days,
        CORTEX_LLM_EXPORT_SLACK_MAX_MESSAGES_PER_CHANNEL,
        summary["llm_summary_enabled"],
        len(selection),
        [
            str(r.get("ultimate_parent") or r.get("salesforce_label") or "")
            for r in selection
        ],
    )
    if not selection:
        report["slack"] = {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
            "top_n": top_n,
            "lookback_days": lookback_days,
            "note": "No active Salesforce contract rollups — Slack slices were not loaded.",
            "selection_ranked": [],
            "customers": {},
        }
        report["_llm_export_slack"] = summary
        logger.warning("LLM export Slack: no ARR selection — nothing to fetch")
        return summary

    run_started = time.monotonic()
    fetch_started = time.monotonic()
    by_customer: dict[str, Any] = {}
    per_customer_perf: list[dict[str, Any]] = []

    for idx, row in enumerate(selection, start=1):
        customer_key = str(
            row.get("ultimate_parent") or row.get("salesforce_label") or ""
        ).strip()
        lookup = str(row.get("csr_lookup_name") or customer_key).strip()
        if not lookup:
            continue

        logger.info(
            "LLM export Slack: customer %d/%d %r lookup=%r arr=%s",
            idx,
            len(selection),
            customer_key,
            lookup,
            row.get("current_arr") or row.get("arr"),
        )
        cust_started = time.monotonic()
        payload = get_customer_slack_conversations(
            lookup,
            days=lookback_days,
            max_messages_per_channel=CORTEX_LLM_EXPORT_SLACK_MAX_MESSAGES_PER_CHANNEL,
            max_lookback_days=lookback_days,
        )
        fetch_seconds = round(time.monotonic() - cust_started, 3)

        llm_summary: dict[str, Any] | None = None
        llm_seconds = 0.0
        llm_error: str | None = None
        if payload.get("conversation_summaries") and summary["llm_summary_enabled"]:
            llm_started = time.monotonic()
            try:
                llm_summary = summarize_customer_slack_for_llm_export(
                    customer_key,
                    payload,
                    lookback_days=lookback_days,
                )
                llm_seconds = float(llm_summary.get("llm_seconds") or round(time.monotonic() - llm_started, 3))
                if llm_summary.get("status") == "ok":
                    summary["customers_llm_summarized"] += 1
                    logger.info(
                        "LLM export Slack: LLM summary ok for %r status=%s messages=%s "
                        "channels=%s llm_seconds=%.2f",
                        customer_key,
                        llm_summary.get("status"),
                        llm_summary.get("message_count_analyzed"),
                        llm_summary.get("channels_included"),
                        llm_seconds,
                    )
                else:
                    logger.info(
                        "LLM export Slack: LLM summary for %r status=%s detail=%s",
                        customer_key,
                        llm_summary.get("status"),
                        llm_summary.get("skipped") or llm_summary.get("error") or "-",
                    )
            except SlackSummaryLlmError as exc:
                llm_error = str(exc)[:400]
                llm_seconds = round(time.monotonic() - llm_started, 3)
                summary["customers_llm_errors"] += 1
                llm_summary = {
                    "customer": customer_key,
                    "status": "error",
                    "error": llm_error,
                    "lookback_days": lookback_days,
                }
                logger.warning(
                    "LLM export Slack: LLM summary failed for %r: %s (%.2fs)",
                    customer_key,
                    llm_error,
                    llm_seconds,
                )
        elif not payload.get("conversation_summaries"):
            logger.info(
                "LLM export Slack: no conversation_summaries for %r (error=%s note=%s)",
                customer_key,
                payload.get("error") or "-",
                (payload.get("note") or "-")[:160],
            )

        message_count = 0
        channel_count = 0
        for s in payload.get("conversation_summaries") or []:
            if isinstance(s, dict):
                channel_count += 1
                message_count += int(s.get("message_count") or 0)

        by_customer[customer_key] = {
            "ultimate_parent": row.get("ultimate_parent") or customer_key,
            "salesforce_label": customer_key,
            "salesforce_labels": row.get("salesforce_labels") or [],
            "lookup_name": lookup,
            "arr": row.get("arr"),
            "current_arr": row.get("current_arr"),
            "slack": payload,
            "llm_summary": llm_summary,
        }
        if payload.get("error"):
            summary["customers_slack_errors"] += 1
        elif payload.get("conversation_summaries"):
            summary["customers_with_slack_data"] += 1

        per_customer_perf.append(
            {
                "customer": customer_key,
                "rank_arr": row.get("arr"),
                "fetch_seconds": fetch_seconds,
                "llm_seconds": llm_seconds,
                "channels": channel_count,
                "messages": message_count,
                "llm_error": llm_error,
            }
        )
        logger.info(
            "LLM export Slack: customer %r done fetch=%.2fs llm=%.2fs channels=%d messages=%d",
            customer_key,
            fetch_seconds,
            llm_seconds,
            channel_count,
            message_count,
        )

    fetch_wall = round(time.monotonic() - fetch_started, 3)
    llm_wall = round(sum(float(p.get("llm_seconds") or 0) for p in per_customer_perf), 3)
    total_wall = round(time.monotonic() - run_started, 3)
    summary["performance"] = {
        "wall_seconds_total": total_wall,
        "fetch_wall_seconds": fetch_wall,
        "llm_wall_seconds": llm_wall,
        "per_customer": per_customer_perf,
    }

    report["slack"] = {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": top_n,
        "lookback_days": lookback_days,
        "selection_ranked": selection,
        "customers": by_customer,
        "note": (
            f"Pilot: top {top_n} Salesforce ultimate parents by current ARR. "
            f"{lookback_days}-day Slack history per customer (channels matched by name + "
            "config/slack_customer_aliases.yaml). "
            "``llm_summary`` is a Cortex LLM digest of human messages; raw lines remain under "
            "``conversation_summaries`` for audit. See ``_llm_export_slack.performance`` for timing."
        ),
    }
    report["_llm_export_slack"] = summary
    logger.info(
        "LLM export Slack: finished top %d by ARR — %d with channel data, %d fetch errors, "
        "%d LLM summaries, %d LLM errors, wall=%.1fs (fetch=%.1fs, llm=%.1fs)",
        len(selection),
        summary["customers_with_slack_data"],
        summary["customers_slack_errors"],
        summary["customers_llm_summarized"],
        summary["customers_llm_errors"],
        total_wall,
        fetch_wall,
        llm_wall,
    )
    return summary
