"""Assemble deck-scoped data governance metadata for the closing slide."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from .config import logger
from .data_governance_warnings import build_data_governance_warning_entries
from .slide_metadata import SLIDE_DATA_REQUIREMENTS
from .speaker_notes import (
    collect_declared_data_trace_entries,
    collect_jql_soql_trace_entries,
    dedupe_data_trace_entries,
)

_REGISTRY_PATH = Path(__file__).resolve().parent.parent / "config" / "data_source_registry.yaml"

_CURSOR_SLIDE_TYPES = frozenset({
    "cursor_cost", "cursor_cost_models", "cursor_usage", "cursor_usage_non_engineers",
    "cursor_model_usage", "cursor_efficiency", "cursor_efficiency_engineers",
    "cursor_users_volume", "cursor_users", "cursor_users_light",
    "cursor_users_non_engineers_volume", "cursor_users_non_engineers", "cursor_users_non_engineers_light",
})

_GITHUB_PRODUCTIVITY_SLIDE_TYPES = frozenset({
    "github_engineering_output",
    "github_engineer_contribution",
    "github_delivery_flow",
    "github_change_profile",
    "productivity_summary",
    "productivity_trend",
    "productivity_coaching",
    "ai_output_correlation",
    "ai_productivity_matrix",
})

_TEAMS_SLIDE_TYPES = frozenset({"eng_team_roster"}) | _CURSOR_SLIDE_TYPES

_LINEAGE_CAP = 10
_SCOPE_CAP = 6
_FRESHNESS_CAP = 4
_GOVERNANCE_FLAG_CAP = 12


@lru_cache(maxsize=1)
def load_data_source_registry() -> dict[str, Any]:
    with _REGISTRY_PATH.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _registry_sources() -> dict[str, dict[str, Any]]:
    reg = load_data_source_registry()
    return dict(reg.get("sources") or {})


def _registry_display_order() -> list[str]:
    reg = load_data_source_registry()
    return list(reg.get("source_display_order") or [])


def _registry_key_to_source_id() -> dict[str, str]:
    reg = load_data_source_registry()
    return dict(reg.get("report_key_to_source_id") or {})


def collect_report_keys_for_slide_plan(slide_plan: list[dict[str, Any]] | None) -> set[str]:
    keys: set[str] = set()
    for entry in slide_plan or ():
        slide_type = (entry.get("slide_type") or "").strip()
        if slide_type in ("", "data_quality", "qbr_divider", "eng_divider", "skip"):
            continue
        for req in SLIDE_DATA_REQUIREMENTS.get(slide_type) or ():
            keys.add(req)
    return keys


def collect_source_ids_for_slide_plan(slide_plan: list[dict[str, Any]] | None) -> list[str]:
    """Stable source ids used by this deck (from slide plan + Teams/Cursor rules)."""
    key_map = _registry_key_to_source_id()
    ids: set[str] = set()
    slide_types: set[str] = set()
    for entry in slide_plan or ():
        st = (entry.get("slide_type") or "").strip()
        if st and st not in ("data_quality", "qbr_divider", "eng_divider", "skip"):
            slide_types.add(st)
        for req in SLIDE_DATA_REQUIREMENTS.get(st) or ():
            sid = key_map.get(req)
            if sid:
                ids.add(sid)
    if slide_types & _TEAMS_SLIDE_TYPES:
        ids.add("atlassian_teams")
    order = _registry_display_order()
    return [sid for sid in order if sid in ids]


def ordered_dq_display_names_for_slide_plan(slide_plan: list[dict[str, Any]] | None) -> list[str] | None:
    """Human-readable pill labels for this deck, in registry order."""
    reg = _registry_sources()
    ids = collect_source_ids_for_slide_plan(slide_plan)
    if not ids:
        return None
    return [str((reg.get(sid) or {}).get("display_name") or sid) for sid in ids]


def ingest_report_integration_warnings(report: dict[str, Any], slide_plan: list[dict[str, Any]] | None) -> None:
    """Surface integration warnings/errors on the Data Quality slide via qa.flags."""
    from .qa import qa

    source_ids = set(collect_source_ids_for_slide_plan(slide_plan))

    if "cursor" in source_ids:
        cu = report.get("cursor_usage") or {}
        if isinstance(cu, dict):
            for err in cu.get("errors") or []:
                msg = str(err).strip()
                if msg:
                    qa.flag(msg, severity="error", sources=("Cursor",))
            for warn in cu.get("warnings") or []:
                msg = str(warn).strip()
                if msg:
                    qa.flag(msg, severity="warning", sources=("Cursor",))

    if "github" in source_ids:
        gp = report.get("github_productivity") or {}
        if isinstance(gp, dict):
            err = str(gp.get("error") or "").strip()
            if err:
                qa.flag(f"GitHub productivity: {err}", severity="error", sources=("GitHub",))
            for warn in gp.get("warnings") or []:
                msg = str(warn).strip()
                if msg:
                    qa.flag(msg, severity="warning", sources=("GitHub",))
        ai = report.get("ai_productivity") or {}
        if isinstance(ai, dict):
            for warn in ai.get("warnings") or []:
                msg = str(warn).strip()
                if msg:
                    qa.flag(msg, severity="warning", sources=("GitHub",))

    if "atlassian_teams" in source_ids:
        roster = (report.get("eng_portfolio") or {}).get("team_roster") or {}
        if isinstance(roster, dict):
            err = str(roster.get("error") or "").strip()
            if err:
                qa.flag(
                    f"Atlassian Teams roster: {err}",
                    severity="warning",
                    sources=("Atlassian Teams",),
                )

    if "atlassian_jira" in source_ids:
        jira = report.get("jira") or {}
        if isinstance(jira, dict) and str(jira.get("error") or "").strip():
            qa.flag(
                str(jira["error"]).strip(),
                severity="error",
                sources=("Atlassian Jira",),
            )
        ep = report.get("eng_portfolio") or {}
        if isinstance(ep, dict) and str(ep.get("error") or "").strip():
            qa.flag(
                str(ep["error"]).strip(),
                severity="error",
                sources=("Atlassian Jira",),
            )

    if "salesforce" in source_ids:
        sf = report.get("salesforce") or {}
        if isinstance(sf, dict) and str(sf.get("error") or "").strip():
            qa.flag(
                f"Salesforce data unavailable: {sf['error']}",
                severity="warning",
                sources=("Salesforce",),
            )

    for entry in build_data_governance_warning_entries(report):
        msg = str(entry.get("message") or "").strip()
        if not msg:
            continue
        cat = str(entry.get("category") or "governance")
        qa.flag(msg, severity="warning", sources=(f"Governance ({cat})",))

    for miss in report.get("_missing_slide_data") or []:
        if not isinstance(miss, dict):
            continue
        msg = str(miss.get("message") or miss.get("reason") or "").strip()
        if msg:
            qa.flag(msg, severity="warning", sources=("Slide builder",))


def _cursor_source_status(report: dict[str, Any] | None, slide_plan: list[dict[str, Any]] | None) -> str:
    if "cursor" not in collect_source_ids_for_slide_plan(slide_plan):
        return "omitted"
    cu = (report or {}).get("cursor_usage") or {}
    if not isinstance(cu, dict) or not cu:
        return "omitted"
    if not cu.get("configured"):
        if cu.get("errors"):
            return "error"
        return "unconfigured"
    if cu.get("errors"):
        return "error"
    return "ok"


def _atlassian_teams_source_status(report: dict[str, Any] | None, slide_plan: list[dict[str, Any]] | None) -> str:
    if "atlassian_teams" not in collect_source_ids_for_slide_plan(slide_plan):
        return "omitted"
    if not (os.environ.get("ATLASSIAN_ORG_ID") or "").strip():
        return "unconfigured"
    roster = ((report or {}).get("eng_portfolio") or {}).get("team_roster") or {}
    if isinstance(roster, dict) and roster.get("error"):
        return "error"
    if isinstance(roster, dict) and roster.get("teams"):
        return "ok"
    cu = (report or {}).get("cursor_usage") or {}
    if isinstance(cu, dict) and cu.get("configured"):
        if (cu.get("cost_engineers") or {}).get("configured") or (cu.get("usage_engineers") or {}).get("configured"):
            return "ok"
    return "unavailable"


def _atlassian_jira_source_status(report: dict[str, Any] | None, flags: list[Any]) -> str:
    for f in flags:
        msg = getattr(f, "message", "") or ""
        if "jira data unavailable" in msg.lower():
            return "unavailable"
    jira = (report or {}).get("jira") or {}
    if isinstance(jira, dict) and str(jira.get("error") or "").strip():
        return "unavailable"
    ep = (report or {}).get("eng_portfolio") or {}
    if isinstance(ep, dict) and str(ep.get("error") or "").strip():
        return "unavailable"
    if isinstance(jira, dict) and jira:
        return "ok"
    if isinstance(ep, dict) and ep and not ep.get("error"):
        return "ok"
    return "ok"


def _source_status_map(
    report: dict[str, Any] | None,
    slide_plan: list[dict[str, Any]] | None,
    *,
    flags: list[Any],
) -> dict[str, str]:
    from .qa import QARegistry

    reg = _registry_sources()
    ids = collect_source_ids_for_slide_plan(slide_plan)
    statuses: dict[str, str] = {}
    for sid in ids:
        display = str((reg.get(sid) or {}).get("display_name") or sid)
        if sid == "cursor":
            statuses[display] = _cursor_source_status(report, slide_plan)
        elif sid == "atlassian_teams":
            statuses[display] = _atlassian_teams_source_status(report, slide_plan)
        elif sid == "atlassian_jira":
            statuses[display] = _atlassian_jira_source_status(report, flags)
        elif sid == "salesforce":
            sf = (report or {}).get("salesforce") or {}
            if isinstance(sf, dict) and sf and "error" not in sf:
                statuses[display] = "ok"
            elif isinstance(sf, dict) and sf.get("error"):
                statuses[display] = "unavailable"
            else:
                statuses[display] = "unavailable"
        elif sid == "github":
            statuses[display] = QARegistry._github_source_status(report)
        elif sid == "leandna":
            statuses[display] = QARegistry._leandna_source_status(report)
        else:
            statuses[display] = "ok"
    return statuses


def _truncate_line(text: str, limit: int = 118) -> str:
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    return t[: limit - 3] + "..."


def _collect_deck_scoped_lineage(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]] | None,
    *,
    cap: int = _LINEAGE_CAP,
) -> list[dict[str, str]]:
    keys = collect_report_keys_for_slide_plan(slide_plan)
    pipeline: list[dict[str, str]] = []
    for key in sorted(keys):
        blob = report.get(key)
        if blob is None:
            continue
        pipeline.extend(collect_declared_data_trace_entries(blob))
        pipeline.extend(collect_jql_soql_trace_entries(blob))
    pipeline = dedupe_data_trace_entries(pipeline)

    cu = report.get("cursor_usage") or {}
    if isinstance(cu, dict) and cu.get("configured"):
        window = int(cu.get("window_days") or report.get("days") or 30)
        pipeline.insert(0, {
            "description": "Cursor usage events",
            "source": "Cursor",
            "query": f"POST /teams/filtered-usage-events ({window}d window, chargedCents per event)",
        })
        pipeline.insert(1, {
            "description": "Cursor accepted lines",
            "source": "Cursor",
            "query": f"GET /teams/daily-usage-data ({window}d, acceptedLinesAdded per user-day)",
        })

    return pipeline[:cap]


def _build_scope_lines(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]] | None,
    deck_id: str,
) -> list[str]:
    lines: list[str] = []
    customer = report.get("customer")
    if report.get("type") == "portfolio" or customer in (None, "", "Portfolio"):
        lines.append("Portfolio deck — customer inventory and churn from Salesforce when loaded; Pendo/Jira enrich.")
    elif customer:
        lines.append(f"Customer scope: {customer}")

    days = report.get("days")
    if days is not None:
        q = report.get("quarter")
        if q:
            lines.append(f"Window: {days}d lookback · quarter {q}")
        else:
            lines.append(f"Window: {days}-day lookback")

    slide_types = {(e.get("slide_type") or "") for e in (slide_plan or [])}
    if slide_types & _CURSOR_SLIDE_TYPES:
        lines.append(
            "Cursor engineers = Atlassian Teams Dev - * members (email join); "
            "non-engineers = other team members."
        )

    if deck_id in ("engineering-portfolio", "implementations_review"):
        lines.append("Engineering metrics: Jira LEAN + HELP projects; team field on LEAN board.")

    if deck_id in ("portfolio_review", "csm_book_of_business"):
        lines.append("Salesforce Customer Entity allowlist drives portfolio customer set.")

    return lines[:_SCOPE_CAP]


def _build_freshness_lines(report: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    generated = str(report.get("generated") or report.get("support_deck_generated_at") or "").strip()
    if generated:
        lines.append(f"Report assembled: {generated}")

    cu = report.get("cursor_usage") or {}
    if isinstance(cu, dict) and cu.get("generated_at"):
        lines.append(f"Cursor pull: {cu['generated_at']}")

    sf_ttl = (os.environ.get("BPO_SALESFORCE_CACHE_TTL_HOURS") or "48").strip()
    sf = report.get("salesforce") or {}
    if isinstance(sf, dict) and sf and "error" not in sf:
        lines.append(f"Salesforce: live or cached (TTL {sf_ttl}h via BPO_SALESFORCE_CACHE_TTL_HOURS)")

    cursor_ttl = (os.environ.get("BPO_CURSOR_CACHE_TTL_HOURS") or "1").strip()
    if isinstance(cu, dict) and cu.get("configured"):
        lines.append(f"Cursor cache TTL: {cursor_ttl}h (BPO_CURSOR_CACHE_TTL_HOURS)")

    return lines[:_FRESHNESS_CAP]


def _governance_discrepancy_entries(report: dict[str, Any], qa_summary: dict[str, Any]) -> list[dict[str, str]]:
    """Merge qa flags into slide-friendly rows (deduped by message)."""
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for flag in qa_summary.get("flags") or []:
        msg = str(flag.get("message") or "").strip()
        if not msg or msg in seen:
            continue
        seen.add(msg)
        sev = str(flag.get("severity") or "WARNING")
        rows.append({"severity": sev, "message": msg})
        if len(rows) >= _GOVERNANCE_FLAG_CAP:
            break
    return rows


def build_deck_governance(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]] | None,
    *,
    deck_id: str = "",
) -> dict[str, Any]:
    """Build governance metadata — provenance, scope, and discrepancy receipt for this run."""
    from .qa import qa

    ingest_report_integration_warnings(report, slide_plan)
    dq_order = ordered_dq_display_names_for_slide_plan(slide_plan)
    qa_snap = qa.summary(report=report, data_source_order=dq_order)
    flags = qa.flags

    reg = _registry_sources()
    source_ids = collect_source_ids_for_slide_plan(slide_plan)
    statuses = _source_status_map(report, slide_plan, flags=flags)

    sources_out: list[dict[str, Any]] = []
    for sid in source_ids:
        meta = reg.get(sid) or {}
        display = str(meta.get("display_name") or sid)
        sources_out.append({
            "id": sid,
            "display_name": display,
            "status": statuses.get(display, "ok"),
            "authority": meta.get("authority"),
        })

    return {
        "deck_id": deck_id or str(report.get("_deck_id") or ""),
        "assembled_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "customer": report.get("customer"),
        "window": {
            "days": report.get("days"),
            "quarter": report.get("quarter"),
        },
        "sources": sources_out,
        "source_status": statuses,
        "scope": _build_scope_lines(report, slide_plan, deck_id),
        "freshness": _build_freshness_lines(report),
        "lineage": _collect_deck_scoped_lineage(report, slide_plan),
        "discrepancies": _governance_discrepancy_entries(report, qa_snap),
        "cross_checks": {
            "total_checks": qa_snap.get("total_checks"),
            "total_flags": qa_snap.get("total_flags"),
            "errors": qa_snap.get("errors"),
            "warnings": qa_snap.get("warnings"),
        },
        "authority_footnote": (
            "Salesforce is system of record for customer inventory and contract status; "
            "Pendo, Jira, and Cursor enrich narrative. Single-source KPIs are not cross-verified."
        ),
    }


def attach_deck_governance(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]] | None,
    *,
    deck_id: str = "",
) -> dict[str, Any]:
    """Populate ``report['_governance']``; safe to call once per deck build."""
    try:
        report["_governance"] = build_deck_governance(report, slide_plan, deck_id=deck_id)
    except Exception as exc:
        logger.warning("build_deck_governance failed: %s", exc)
        report["_governance"] = {"error": str(exc)}
    return report
