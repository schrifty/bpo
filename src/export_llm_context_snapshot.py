#!/usr/bin/env python3
"""Export an all-customers LLM-oriented data snapshot to Google Drive under ``Output/``.

Datasource bundle: :mod:`src.data_sources` profile ``llm_export_all_customers`` — Pendo portfolio
rollup, CS Report (top customers by ARR), portfolio Salesforce revenue book, per-customer Salesforce comprehensive
(multi-object CRM categories), and Jira HELP (unscoped). The portfolio fetch does not read or sync
QBR slide YAML (cohort findings use built-in defaults).

**Pendo** detail payloads are stripped (sites, pages, features, …); **Jira** includes counts,
breakdowns, and SLA-style aggregates only — **no issue keys, summaries, or ticket rows.**

The markdown includes **Snapshot coverage & omission rationale** (profile sources, registry ids not in this export and why, caps, loader provenance, feedback prompt) plus **Planned integrations (not in this snapshot yet)** (e.g. Aha, GitHub).

Usage:
  cortex export-all [--days N] [--skip-risk-insights] [--customers-sf-allowlist] [--customers-exclude-sf-churned] [--exclude-customer NAME ...]
  python -m src.export_llm_context_snapshot --days 90

Optional portfolio row filters (after Pendo+Salesforce bundle, before markdown):

- ``--customers-sf-allowlist`` — keep headline customers/signals that match an **active** (non-churned)
  Salesforce Customer Entity label; Pendo metrics are included when present but not required.
  Requires Salesforce JWT env vars.
- ``--customers-exclude-sf-churned`` — drop rows that **matched** Salesforce rollups with ``active``
  false (contract-status churn rollup).
- ``--exclude-customer`` — repeat to drop explicit Pendo customer labels (case-insensitive), or see
  env ``CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS`` / ``CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS_FILE``.

Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (and optional ``GOOGLE_QBR_OUTPUT_PARENT_ID``) plus
Drive credentials. Each run uploads ``LLM-Context-Portfolio`` to **both**:

1. ``<generator>/Output/LLM-Context-Portfolio-persistent.md`` (bookmarkable current export)
2. ``<generator>/Output/Historical Data/{ISO-date}/LLM-Context-Portfolio.md`` (same-day snapshot, plain stem)

Every export appends **§7 Account & churn risk insights** (LLM). Failures are printed inside that section; the export still completes unless the core datasource report fails earlier.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.cli_warning_filters import apply_cli_warning_filters

apply_cli_warning_filters()

_DATA_SUMMARY_PATH = _ROOT / "config" / "comprehensive_data_element_list.json"

# HTTP surfaces wired in code (see ``src/data_sources/registry.SourceId`` docstring).
_LEANDNA_DATA_API_HTTP_SURFACES: tuple[str, ...] = (
    "GET /data/ItemMasterData",
    "GET /data/MaterialShortages/ShortagesByItem/Weekly",
    "GET /data/MaterialShortages/ShortagesByItem/Daily",
    "GET /data/MaterialShortages/ShortagesByOrder",
    "GET /data/MaterialShortages/ShortagesByItemWithScheduledDeliveries/Weekly",
    "GET /data/LeanProject",
    "GET /data/LeanProject/{projectIds}/Savings",
    "GET /data/Metric",
    "GET /data/MetricReport",
    "GET /data/Metric/{metricId}/MetricDataPoint",
    "POST /data/Metric/{metricId}/MetricDataPoint",
    "DELETE /data/Metric/{metricId}/MetricDataPoint",
    "POST /data/LeanProject",
    "PUT /data/LeanProject/{projectId}",
    "POST /data/LeanProject/{projectId}/Task",
    "PUT /data/LeanProject/{projectId}/Task/{taskId}",
    "POST /data/LeanProject/{projectId}/Issue",
    "PUT /data/LeanProject/{projectId}/Issue/{issueId}",
    "PUT /data/WriteBack/v1/TransitionActions",
)

# Typical ``report`` dotted paths when QBR LeanDNA enrichments run (no live values in this export).


def _is_llm_export_top_arr_scope(scope: Any) -> bool:
    return scope in ("top_customers_by_arr", "top_ultimate_parents_by_arr")
_LEANDNA_QBR_ENRICHMENT_PATHS: tuple[str, ...] = (
    "leandna_item_master",
    "leandna_item_master.abc_distribution",
    "leandna_item_master.doi_backwards",
    "leandna_item_master.enabled",
    "leandna_item_master.excess_breakdown",
    "leandna_item_master.high_risk_items",
    "leandna_item_master.item_count",
    "leandna_item_master.lead_time_variance",
    "leandna_item_master.sites_requested",
    "leandna_lean_projects",
    "leandna_lean_projects.active_projects",
    "leandna_lean_projects.all_projects",
    "leandna_lean_projects.best_practice_count",
    "leandna_lean_projects.data_fetched_at",
    "leandna_lean_projects.enabled",
    "leandna_lean_projects.monthly_savings",
    "leandna_lean_projects.project_savings",
    "leandna_lean_projects.project_savings_project_ids",
    "leandna_lean_projects.quarter_end",
    "leandna_lean_projects.quarter_start",
    "leandna_lean_projects.savings_achievement_pct",
    "leandna_lean_projects.stage_distribution",
    "leandna_lean_projects.state_distribution",
    "leandna_lean_projects.top_projects",
    "leandna_lean_projects.total_projects",
    "leandna_lean_projects.total_savings_actual",
    "leandna_lean_projects.total_savings_target",
    "leandna_lean_projects.validated_results_count",
    "leandna_shortage_trends",
    "leandna_shortage_trends.critical_items",
    "leandna_shortage_trends.critical_timeline",
    "leandna_shortage_trends.data_fetched_at",
    "leandna_shortage_trends.enabled",
    "leandna_shortage_trends.forecast",
    "leandna_shortage_trends.forecast.buckets",
    "leandna_shortage_trends.forecast.peak_week",
    "leandna_shortage_trends.forecast.total_shortage_value",
    "leandna_shortage_trends.scheduled_deliveries",
    "leandna_shortage_trends.total_items_in_shortage",
    "leandna_shortage_trends.weeks_forward",
)

# Representative Item Master API fields (list endpoint); rows can include additional keys.
_LEANDNA_TYPICAL_ITEM_MASTER_FIELDS: tuple[str, ...] = (
    "abcRank",
    "aggregateRiskScore",
    "criticalityLevel",
    "ctbShortageImpactedValue",
    "daysOfInventoryBackward",
    "daysOfInventoryForward",
    "excessOnHandValue",
    "itemCode",
    "itemDescription",
    "leadTime",
    "observedLeadTime",
    "riskLevel",
    "site",
)

# Product roadmap: named here so the export explicitly sets reader expectations.
_PLANNED_DATASOURCES_NOT_IN_EXPORT: tuple[str, ...] = ("Aha", "GitHub")

# Pendo §1: max rows in ``customers_headline`` when size caps are enabled (see ``_pendo_portfolio_topline``).
_PENDO_EXPORT_HEADLINE_CUSTOMER_CAP = 200

# ``--max-tokens`` / ``--max-bytes`` / compaction caps: 0 means no limit (full payloads).
_LLM_EXPORT_NO_CAP = 0
# The governing budget for this export is the LLM **token** window, not raw bytes. The byte
# cap is retained as an optional secondary guard (off by default). See ``do 1`` rationale:
# CSR JSON runs ~3.4 chars/token, so a byte cap wastes ~70% of a token budget.
_LLM_EXPORT_DEFAULT_MAX_BYTES = 0
_LLM_EXPORT_DEFAULT_MAX_TOKENS = 450_000
# Never shrink/truncate below this floor, regardless of an aggressive explicit cap.
_LLM_EXPORT_MIN_TOKEN_CAP = 20_000
_LLM_EXPORT_MIN_BYTE_CAP = 20_000
# Fallback chars-per-token when tiktoken is unavailable (conservative → over-counts tokens).
_LLM_EXPORT_FALLBACK_CHARS_PER_TOKEN = 3.2

_TOKEN_ENCODER: Any | None = None
_TOKEN_ENCODER_TRIED = False


def llm_export_default_max_bytes() -> int:
    """Default UTF-8 byte cap (``CORTEX_LLM_EXPORT_MAX_BYTES``; 0 = unlimited, the default).

    The primary export budget is token-based (:func:`llm_export_default_max_tokens`); the byte
    cap is an opt-in secondary guard.
    """
    import os

    raw = (os.environ.get("CORTEX_LLM_EXPORT_MAX_BYTES") or "").strip()
    if not raw:
        return _LLM_EXPORT_DEFAULT_MAX_BYTES
    try:
        return max(0, int(raw))
    except ValueError:
        return _LLM_EXPORT_DEFAULT_MAX_BYTES


def llm_export_default_max_tokens() -> int:
    """Default LLM token budget for the export (``CORTEX_LLM_EXPORT_MAX_TOKENS``; 0 = unlimited)."""
    import os

    raw = (os.environ.get("CORTEX_LLM_EXPORT_MAX_TOKENS") or "").strip()
    if not raw:
        return _LLM_EXPORT_DEFAULT_MAX_TOKENS
    try:
        return max(0, int(raw))
    except ValueError:
        return _LLM_EXPORT_DEFAULT_MAX_TOKENS


def _get_token_encoder() -> Any | None:
    """Return a cached ``tiktoken`` encoder, or ``None`` when the package is unavailable."""
    global _TOKEN_ENCODER, _TOKEN_ENCODER_TRIED
    if _TOKEN_ENCODER_TRIED:
        return _TOKEN_ENCODER
    _TOKEN_ENCODER_TRIED = True
    try:
        import tiktoken

        _TOKEN_ENCODER = tiktoken.get_encoding("cl100k_base")
    except Exception:
        _TOKEN_ENCODER = None
    return _TOKEN_ENCODER


def count_tokens(text: str) -> int:
    """Count LLM tokens in *text* via ``tiktoken`` (cl100k_base), or a conservative estimate.

    The fallback (no tiktoken) approximates from UTF-8 byte length so callers still get a
    monotonic, slightly high token estimate for cap enforcement.
    """
    if not text:
        return 0
    enc = _get_token_encoder()
    if enc is not None:
        return len(enc.encode(text, disallowed_special=()))
    import math

    return int(math.ceil(len(text.encode("utf-8")) / _LLM_EXPORT_FALLBACK_CHARS_PER_TOKEN))


def _truncate_to_tokens(text: str, max_tokens: int) -> str:
    """Return *text* clipped to at most *max_tokens* tokens (best-effort under fallback)."""
    if max_tokens <= 0 or not text:
        return text
    enc = _get_token_encoder()
    if enc is not None:
        tokens = enc.encode(text, disallowed_special=())
        if len(tokens) <= max_tokens:
            return text
        return enc.decode(tokens[:max_tokens])
    approx_bytes = int(max_tokens * _LLM_EXPORT_FALLBACK_CHARS_PER_TOKEN)
    raw = text.encode("utf-8")
    if len(raw) <= approx_bytes:
        return text
    return raw[:approx_bytes].decode("utf-8", errors="ignore")


def _format_tokens(n: int) -> str:
    n = max(0, int(n))
    if n >= 1000:
        return f"{n / 1000:.1f}K tokens ({n:,})"
    return f"{n:,} tokens"


def _over_size_caps(md: str, *, max_bytes: int, max_tokens: int) -> bool:
    """True when *md* exceeds an active token or byte cap (0 = that cap is disabled)."""
    if max_tokens and count_tokens(md) > max_tokens:
        return True
    if max_bytes and _utf8_byte_len(md) > max_bytes:
        return True
    return False


def _export_cap_active(cap: int | None) -> bool:
    return cap is not None and int(cap) > 0


def _integration_coverage_lines(*, salesforce: dict[str, Any], csr: dict[str, Any]) -> list[str]:
    """Human-readable bullets for whether Salesforce and CS Report succeeded (pre-compaction)."""
    lines: list[str] = []
    if not salesforce:
        lines.append(
            "- **Salesforce:** **Not loaded** — empty payload (unexpected). "
            "Regenerate after fixing credentials or report builder."
        )
    elif salesforce.get("error"):
        lines.append(f"- **Salesforce:** **Not loaded** — {salesforce['error']}")
    elif salesforce.get("resolution") == "portfolio_aggregate":
        lines.append(
            "- **Salesforce:** **Loaded** — portfolio revenue book (ARR, pipeline, opportunities, "
            "expansion KPIs) plus per-customer comprehensive CRM categories in §3c when configured."
        )
    else:
        lines.append("- **Salesforce:** **Loaded**.")

    if not csr:
        lines.append("- **CS Report:** **Not loaded** — no `csr` block on the merged report.")
        return lines
    if _is_llm_export_top_arr_scope(csr.get("scope")):
        customers = csr.get("customers") if isinstance(csr.get("customers"), dict) else {}
        n = len(customers)
        n_ok = sum(
            1
            for block in customers.values()
            if isinstance(block, dict)
            and not all(
                isinstance(block.get(k), dict) and block.get(k, {}).get("error")
                for k in ("platform_health", "supply_chain", "platform_value")
            )
        )
        if n and n_ok:
            lines.append(
                f"- **CS Report:** **Loaded** — per-customer week slices for top {n} ultimate parent(s) by ARR "
                f"({n_ok} with at least one section; see §4 ``customers``)."
            )
        elif n:
            lines.append(
                f"- **CS Report:** **Partial** — top {n} by ARR selected; all sections errored or missing "
                "for every customer (check CS Report export and name aliases)."
            )
        else:
            note = csr.get("note") or "no customers selected"
            lines.append(f"- **CS Report:** **Not loaded** — {note}")
        return lines
    errs: list[str] = []
    ok_any = False
    for key in ("platform_health", "supply_chain", "platform_value"):
        block = csr.get(key)
        if not isinstance(block, dict):
            errs.append(f"{key}: missing")
            continue
        if block.get("error"):
            errs.append(f"{key}: {block['error']}")
        else:
            ok_any = True
    if ok_any:
        lines.append(
            "- **CS Report:** **Loaded** — see §4 for platform_health / supply_chain / platform_value "
            "(sections with errors still list the error inline)."
        )
    else:
        detail = "; ".join(errs) if errs else "no row-level errors parsed"
        lines.append(f"- **CS Report:** **Not loaded** — {detail}")
    return lines


_REGISTRY_EXCLUDED_RATIONALE: dict[str, str] = {
    "pendo_customer_health": (
        "Single-customer Pendo health payloads (sites, features, guides, etc.) power deck/QBR flows. "
        "This snapshot uses **pendo_portfolio_rollup** in §1 only (headline counts per customer, no detail exports)."
    ),
    "cs_report_customer_week": (
        "Per-customer CS Report week slices attach to single-customer health reports. "
        "The all-customers LLM export uses the same per-customer APIs for the top N labels by ARR in §4."
    ),
    "leandna_item_master": (
        "LeanDNA Data API (item master) is wired into QBR enrichment paths, not into "
        "`build_llm_export_snapshot_report` / this markdown builder yet."
    ),
    "leandna_shortage_trends": (
        "LeanDNA shortage trends are QBR/deck enrichments; not merged into this all-customers export yet."
    ),
    "leandna_lean_projects": (
        "LeanDNA lean projects are QBR/deck enrichments; not merged into this all-customers export yet."
    ),
}


def _leandna_rows_from_data_summary(path: Path) -> list[dict[str, Any]]:
    """Return catalog rows for LeanDNA (paths + short terms) from the comprehensive data element list."""
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    entries = raw.get("entries")
    if not isinstance(entries, list):
        return []
    out: list[dict[str, Any]] = []
    for ent in entries:
        if not isinstance(ent, dict):
            continue
        p = ent.get("path")
        if not isinstance(p, str) or not p.strip():
            continue
        terms = ent.get("terms")
        tlist = [str(x) for x in terms] if isinstance(terms, list) else []
        lean_terms = any(
            ("LeanDNA Data API" in x or x.startswith("[LeanDNA") or "leandna" in x.lower())
            for x in tlist
        )
        if not (
            p.startswith("leandna_")
            or p.startswith("leandna_data_api.")
            or lean_terms
        ):
            continue
        out.append({"path": p, "terms": tlist[:5]})
    out.sort(key=lambda r: r["path"])
    return out


def build_leandna_data_api_reference() -> dict[str, Any]:
    """Reference material for LeanDNA Data API: catalog slice, QBR paths, HTTP surfaces."""
    catalog = _leandna_rows_from_data_summary(_DATA_SUMMARY_PATH)
    return {
        "note": (
            "LeanDNA Data API payloads attach to **single-customer** QBR/health reports when "
            "`LEANDNA_DATA_API_BEARER_TOKEN` is set and enrichment runs. This all-customers file still "
            "omits live §1–§6 values for those sources; use this block for **path and endpoint vocabulary**."
        ),
        "data_summary_catalog_path": str(_DATA_SUMMARY_PATH.relative_to(_ROOT)),
        "catalog_entries": catalog,
        "catalog_entry_count": len(catalog),
        "qbr_enrichment_dotted_paths": list(_LEANDNA_QBR_ENRICHMENT_PATHS),
        "http_surfaces": list(_LEANDNA_DATA_API_HTTP_SURFACES),
        "typical_item_master_api_fields": list(_LEANDNA_TYPICAL_ITEM_MASTER_FIELDS),
        "lean_project_row_notes": (
            "`leandna_lean_projects.all_projects` and `.top_projects` rows are shallow copies of "
            "`GET /data/LeanProject` objects plus aliases `savings_actual`, `savings_target`, "
            "`project_manager`, `sponsor_name`."
        ),
        "project_savings_notes": (
            "`project_savings` is the raw `GET /data/LeanProject/{ids}/Savings` list for the top "
            "projects by savings only; `monthly_savings` is a derived rollup."
        ),
    }


def _build_export_coverage(
    report: dict[str, Any],
    *,
    markdown_soft_cap_bytes: int,
    markdown_soft_cap_tokens: int = _LLM_EXPORT_NO_CAP,
    csr_site_limit: int,
    csr_string_cap: int,
    sf_accounts: int,
    signals_cap: int | None,
    signals_line_max_chars: int,
    size_caps_enabled: bool = False,
) -> dict[str, Any]:
    """Structured manifest for markdown 'what is in / out' (also drives the coverage section)."""
    from src.data_sources.profiles import PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS, PROFILE_LLM_EXPORT_ALL_CUSTOMERS
    from src.data_sources.registry import SourceId

    sources_in_profile = sorted(s.value for s in PROFILE_LLM_EXPORT_ALL_CUSTOMERS)
    registry_excluded: list[dict[str, str]] = []
    for sid in SourceId:
        if sid in PROFILE_LLM_EXPORT_ALL_CUSTOMERS:
            continue
        sid_s = sid.value
        registry_excluded.append(
            {
                "id": sid_s,
                "rationale": _REGISTRY_EXCLUDED_RATIONALE.get(
                    sid_s,
                    "Not loaded by `build_llm_export_snapshot_report` for this profile; other flows or roadmap only.",
                ),
            }
        )
    registry_excluded.sort(key=lambda r: r["id"])
    rollup_cap = (
        max(sf_accounts * 6, 72) if _export_cap_active(sf_accounts) else None
    )
    prov = report.get("_data_source_provenance")
    if not isinstance(prov, dict):
        prov = None
    out_cov: dict[str, Any] = {
        "profile_id": PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
        "sources_in_profile": sources_in_profile,
        "registry_excluded": registry_excluded,
        "markdown_soft_cap_bytes": int(markdown_soft_cap_bytes),
        "markdown_soft_cap_tokens": int(markdown_soft_cap_tokens),
        "size_caps_enabled": size_caps_enabled,
        "compaction": {
            "csr_site_limit": csr_site_limit,
            "csr_string_cap": csr_string_cap,
            "sf_accounts": sf_accounts,
            "signals_cap": signals_cap,
            "signals_line_max_chars": signals_line_max_chars,
            "rollup_cap": rollup_cap,
        },
        "loader_provenance": prov,
        "pendo_export_constants": {
            "customers_headline_max": _PENDO_EXPORT_HEADLINE_CUSTOMER_CAP,
            "portfolio_signals_top_max_items": 28,
            "portfolio_trends_max_list_items": 40,
            "portfolio_leaders_max_list_items": 40,
            "cohort_findings_bullets_max": 24,
            "csr_nested_max_list_items": 48,
            "csr_nested_max_dict_keys": 96,
            "signals_trend_context_max_string_if_present": 320,
        },
    }
    cf = report.get("_llm_export_customer_filter")
    if isinstance(cf, dict) and cf.get("enabled"):
        out_cov["customer_filter"] = cf
    sf_uni = report.get("_llm_export_salesforce_universe")
    if isinstance(sf_uni, dict):
        out_cov["salesforce_universe"] = sf_uni
    sf_churn = report.get("_llm_export_salesforce_churned")
    if isinstance(sf_churn, dict):
        out_cov["salesforce_churned"] = sf_churn
    csr_meta = report.get("_llm_export_csr")
    if isinstance(csr_meta, dict):
        out_cov["csr_top_by_arr"] = csr_meta
    sf_comp = report.get("_llm_export_salesforce_comprehensive")
    if isinstance(sf_comp, dict):
        out_cov["salesforce_comprehensive"] = sf_comp
    slack_meta = report.get("_llm_export_slack")
    if isinstance(slack_meta, dict):
        out_cov["slack_top_by_arr"] = slack_meta
    churn_seg_cov = report.get("salesforce_churned_segment")
    if isinstance(churn_seg_cov, dict):
        out_cov["salesforce_churned_segment"] = {
            "customer_count": churn_seg_cov.get("customer_count"),
            "do_not_merge_with_active_book": churn_seg_cov.get("do_not_merge_with_active_book"),
        }
    renewal_seg_cov = report.get("salesforce_renewal_negotiation_segment")
    if isinstance(renewal_seg_cov, dict):
        out_cov["salesforce_renewal_negotiation_segment"] = {
            "customer_count": renewal_seg_cov.get("customer_count"),
            "do_not_merge_with_active_book": renewal_seg_cov.get("do_not_merge_with_active_book"),
        }
    future_seg_cov = report.get("salesforce_future_contract_segment")
    if isinstance(future_seg_cov, dict):
        out_cov["salesforce_future_contract_segment"] = {
            "customer_count": future_seg_cov.get("customer_count"),
            "do_not_merge_with_active_book": future_seg_cov.get("do_not_merge_with_active_book"),
        }
    return out_cov


def _export_coverage_markdown_lines(cov: dict[str, Any]) -> list[str]:
    """Human-readable bullets for the snapshot coverage manifest."""
    if not cov:
        return ["- *(Coverage manifest missing.)*"]
    lines: list[str] = [
        "This block explains **what is in the snapshot**, **what is left out on purpose**, and **rough limits** "
        "(how many rows, how much text). If a number looks wrong in §1–§6, check here first, then **Loader provenance** "
        "below to see whether the upstream system returned an error.",
        "",
        "### Datasource profile",
        f"- **Profile id:** `{cov.get('profile_id', '')}`",
        f"- **Canonical sources in this profile:** {', '.join(f'`{s}`' for s in (cov.get('sources_in_profile') or []))}",
        "",
    ]
    sf_uni = cov.get("salesforce_universe")
    if isinstance(sf_uni, dict) and sf_uni.get("salesforce_configured"):
        lines.extend(
            [
                "### Salesforce customer universe (this run)",
                f"- **Current-book Customer Entity labels in Salesforce (ACTIVE + OUT_OF_CONTRACT_RENEWING):** "
                f"**{sf_uni.get('salesforce_active_entities', 0)}**",
                f"- **§1 rows added with Salesforce only (no Pendo headline metrics):** "
                f"**{sf_uni.get('added_salesforce_only_rows', 0)}**",
            ]
        )
        without = sf_uni.get("salesforce_labels_without_pendo") or []
        if without:
            preview = ", ".join(str(x) for x in without[:12])
            if len(without) > 12:
                preview += ", …"
            lines.append(
                f"- **Current-book SF labels with no Pendo prefix match (still in §1/§3):** {preview}"
            )
        lines.append("")
    sf_churn_cov = cov.get("salesforce_churned")
    if isinstance(sf_churn_cov, dict) and sf_churn_cov.get("salesforce_configured"):
        seg = cov.get("salesforce_churned_segment")
        n_churn = seg.get("customer_count") if isinstance(seg, dict) else sf_churn_cov.get("salesforce_churned_entities")
        lines.extend(
            [
                "### Salesforce churned segment (§3b — lost / no renewal pipeline)",
                f"- **Churned-lost Customer Entity count:** **{n_churn or 0}**",
                "- **Do not merge** §3b with §1/§3/§5. True churn is **Salesforce-only** here (no Pendo or Jira).",
                "",
            ]
        )
        ren_seg = cov.get("salesforce_renewal_negotiation_segment")
        n_renewal = (
            ren_seg.get("customer_count")
            if isinstance(ren_seg, dict)
            else sf_churn_cov.get("salesforce_renewal_negotiation_entities")
        )
        if n_renewal:
            lines.extend(
                [
                    "### Salesforce renewal negotiation (§3b-renewal)",
                    f"- **Expired contracts with open renewal pipeline:** **{n_renewal}**",
                    "- These accounts may still appear in §1 when Pendo matches; they are **not** counted in §3b churn.",
                    "",
                ]
            )
        fut_seg = cov.get("salesforce_future_contract_segment")
        n_future = fut_seg.get("customer_count") if isinstance(fut_seg, dict) else None
        if n_future:
            lines.extend(
                [
                    "### Salesforce future contracts (§3b-future)",
                    f"- **FUTURE commercial_status (won contracts not yet started):** **{n_future}**",
                    "- **Do not merge** with §3 current book, §3b churn, or §3b-renewal.",
                    "",
                ]
            )
    cf_cov = cov.get("customer_filter")
    if isinstance(cf_cov, dict) and cf_cov.get("enabled"):
        lines.extend(
            [
                "### Customer filter (portfolio rows)",
                "Per-customer headline rows (**§1**) and usage signal lines (**§5**) may have been **trimmed** before Salesforce §3 refresh.",
                "",
            ]
        )
        lines.append(
            f"- **Salesforce allowlist (active entities):** **`{bool(cf_cov.get('sf_allowlist'))}`** — dropped "
            f"**{cf_cov.get('dropped_sf_allowlist', 0)}** row(s) not on the active SF Customer Entity book."
        )
        lines.append(
            f"- **Exclude SF churn rollup (inactive matched):** **`{bool(cf_cov.get('exclude_sf_churned_matched'))}`** — dropped "
            f"**{cf_cov.get('dropped_sf_churned_matched', 0)}** row(s)."
        )
        lines.append(
            f"- **Explicit excludes:** dropped **{cf_cov.get('dropped_exclude_list', 0)}** row(s); "
            f"labels (lower): `{', '.join(cf_cov.get('explicit_excludes_loaded') or [])}`"
            if cf_cov.get("explicit_excludes_loaded")
            else f"- **Explicit excludes:** dropped **{cf_cov.get('dropped_exclude_list', 0)}** row(s)."
        )
        lines.append(
            f"- **Remainder:** **{cf_cov.get('before_customer_rows', '?')}** → **{cf_cov.get('after_customer_rows', '?')}** "
            f"Pendo headline rows; **§5 signals** `{cf_cov.get('portfolio_signals_before')}` → "
            f"`{cf_cov.get('portfolio_signals_after')}` lines."
        )
        for w in cf_cov.get("warnings") or []:
            if isinstance(w, str) and w.strip():
                lines.append(f"- **Warning:** {w.strip()}")
        lines.append("")
    lines.extend(
        [
            "",
            "### Registry sources not in this export (and why)",
        ]
    )
    for row in cov.get("registry_excluded") or []:
        if not isinstance(row, dict):
            continue
        rid = row.get("id", "")
        why = row.get("rationale", "")
        lines.append(f"- **`{rid}`** — {why}")
    lines.extend(
        [
            "",
            "### Section-by-section: what you are looking at",
            "The numbered sections are **JSON blocks** (machine-friendly). Here is what each one is meant to represent for CS conversations.",
            "",
            f"- **§1 — Pendo (all customers):** A **portfolio-level** snapshot: logins, adoption-style metrics, and short "
            f"per-customer headline rows. It is **not** a full Pendo analytics export (no page/feature catalogs, no "
            f"multi-gigabyte raw downloads). The headline customer list stops after **{cov.get('pendo_export_constants', {}).get('customers_headline_max', 200)}** rows; "
            "cohort and “who is leading / lagging” style details are shortened so the file stays shareable.",
            "",
            "- **§2 — Jira (HELP):** **Workload and health of the queue** — counts by status, type, and similar rollups, "
            "plus response-time style summaries. **Individual tickets are not listed** (no issue keys, titles, or customer notes in text).",
            "",
            "- **§3 — Salesforce (current book):** **Revenue and renewal-oriented facts** for ultimate parents with "
            "``commercial_status`` **ACTIVE** or **OUT_OF_CONTRACT_RENEWING**. **§3.1** is a markdown table of every "
            "account **pre-ranked by ``current_arr``** — the authoritative source for “top N by revenue” (read the "
            "``rank`` column); **§3.2** holds the contract rollups and portfolio aggregates as JSON. Pendo in §1 "
            "merges when a prefix matches, but **Pendo is not required** (see ``salesforce_only`` rows). "
            "**Do not combine** with §3b / §3b-renewal / §3b-future.",
            "- **§3b — Salesforce (churned / lost):** **Salesforce-only** ``CHURNED`` contracts **without** open "
            "parent-account renewal pipeline. **§3b-renewal** = ``OUT_OF_CONTRACT_RENEWING`` (negotiation, not churn). "
            "**§3b-future** = ``FUTURE`` (won contracts not yet started). **No Pendo/Jira** in inactive segments; "
            "true churn is stripped from §1/§5.",
            "",
            "- **§4 — CS Report (weekly export):** Per-customer **platform_health**, **supply_chain**, and "
            "**platform_value** for the **top Salesforce labels by ARR** (not an all-customer site merge). When size "
            "caps are enabled, site rows and long text fields may be truncated.",
            "",
            "- **§5 — Pendo usage signals:** A **ranked checklist** of product-side callouts (examples: Kei not used, "
            "high guide dismiss rate, very read-only usage). This export asks for a **long** list so you can scan the "
            "portfolio; the deck view still uses a shorter default. You can cap lines with `--signals-cap` if you need a smaller file.",
            "",
            "- **§6 — Signals trend context (optional):** **Extra timing / trend text** when the pipeline provides it "
            "**and** we have not aggressively shortened the CS Report section; otherwise it is skipped so the file "
            "fits the size budget.",
            "",
            "- **LeanDNA Data API (reference JSON):** The next section (**LeanDNA Data API — data elements**) "
            "lists catalog paths, typical QBR dotted paths, and HTTP surfaces for Item Master, shortages, and "
            "Lean Projects. **Not** live tenant data in this export — see single-customer QBR when enrichment runs.",
            "",
            "### File size budget (this run)",
        ]
    )
    if cov.get("size_caps_enabled"):
        cap_tok = cov.get("markdown_soft_cap_tokens")
        cap_b = cov.get("markdown_soft_cap_bytes")
        if cap_tok and int(cap_tok) > 0:
            budget = f"about **{int(cap_tok):,}** LLM tokens (`--max-tokens`, cl100k_base)"
            if cap_b and int(cap_b) > 0:
                budget += f" and **{int(cap_b):,}** UTF-8 bytes (`--max-bytes`)"
            raise_hint = "raise `--max-tokens`"
        else:
            budget = f"about **{int(cap_b or 0):,}** bytes of UTF-8 (`--max-bytes`)"
            raise_hint = "raise `--max-bytes`"
        lines.append(
            f"- **Target size:** {budget} for the whole markdown file. §3c Salesforce comprehensive is exported "
            "in **headline** form (per-customer KPIs + capped category samples, top customers by ARR). If the "
            "export is still too large, CSR and §3 rollup tighten further; the **end of the file may be cut "
            f"off** — {raise_hint} or set `CORTEX_LLM_EXPORT_SF_COMPREHENSIVE=false` for a smaller run."
        )
    else:
        lines.append(
            "- **Size caps:** **disabled** for this run (`--max-tokens 0 --max-bytes 0`). Full CS Report site "
            "rows, Salesforce rollups, Pendo headlines, and §3c comprehensive payloads are included without "
            "markdown truncation or tiered compaction."
        )
    c = cov.get("compaction") if isinstance(cov.get("compaction"), dict) else {}
    if c and cov.get("size_caps_enabled"):
        sig_n = c.get("signals_cap")
        sig_part = (
            "§5 shows the **full** ranked Pendo usage signal list"
            if sig_n is None
            else f"§5 shows the **first {sig_n}** Pendo usage lines"
        )
        lines.append(
            f"- **Effective compaction (numbers for this run):** CS Report — up to **{c.get('csr_site_limit', '')}** sites "
            f"per worksheet, text clipped to **{c.get('csr_string_cap', '')}** characters per field; Salesforce — "
            f"**{c.get('sf_accounts', '')}** accounts and **{c.get('rollup_cap', '')}** renewal / contract rollup rows; "
            f"{sig_part}, each line up to **{c.get('signals_line_max_chars', '')}** characters."
        )
    lines.extend(
        [
            "",
            "### Loader provenance (this run)",
            "- **How to read this:** each line is **one integration** (Pendo, CS Report, Salesforce, Jira). "
            "**ok** means we received data; **error** means that source failed — check the short message on the line. "
            "Empty or error-heavy §3 / §4 often lines up with an **error** here.",
        ]
    )
    prov = cov.get("loader_provenance") if isinstance(cov.get("loader_provenance"), dict) else {}
    src_rows = prov.get("sources") if isinstance(prov.get("sources"), list) else []
    if not src_rows:
        lines.append(
            "- *(Internal loader checklist is missing — treat the run as suspect and re-export after fixing credentials "
            "or connectivity.)*"
        )
    else:
        lines.append(f"- **Recorded profile id:** `{prov.get('profile_id', '')}`")
        for row in src_rows:
            if not isinstance(row, dict):
                continue
            src = row.get("source", "?")
            st = row.get("status", "?")
            det = row.get("detail")
            if det:
                lines.append(f"- **`{src}`** — **{st}** — {det}")
            else:
                lines.append(f"- **`{src}`** — **{st}**")
    slack_meta = cov.get("slack_top_by_arr")
    if isinstance(slack_meta, dict) and slack_meta.get("enabled"):
        perf = slack_meta.get("performance") if isinstance(slack_meta.get("performance"), dict) else {}
        lines.extend(
            [
                "",
                "### Slack pilot timing (top customers by ARR)",
                f"- **Scope:** top **{slack_meta.get('top_n', '')}** ultimate parents · "
                f"**{slack_meta.get('lookback_days', '')}**-day lookback",
                f"- **Customers:** {slack_meta.get('customers_selected', 0)} selected · "
                f"{slack_meta.get('customers_with_slack_data', 0)} with channel data · "
                f"{slack_meta.get('customers_llm_summarized', 0)} LLM summaries · "
                f"{slack_meta.get('customers_slack_errors', 0)} fetch errors · "
                f"{slack_meta.get('customers_llm_errors', 0)} LLM errors",
            ]
        )
        if perf:
            lines.append(
                f"- **Wall time:** **{perf.get('wall_seconds_total', '—')}s** total "
                f"(fetch **{perf.get('fetch_wall_seconds', '—')}s** · "
                f"LLM **{perf.get('llm_wall_seconds', '—')}s**)"
            )
            per_cust = perf.get("per_customer")
            if isinstance(per_cust, list) and per_cust:
                lines.append("- **Per customer:**")
                for row in per_cust:
                    if not isinstance(row, dict):
                        continue
                    name = row.get("customer") or "?"
                    lines.append(
                        f"  - **{name}** — fetch {row.get('fetch_seconds', '—')}s · "
                        f"LLM {row.get('llm_seconds', '—')}s · "
                        f"{row.get('channels', 0)} channels · {row.get('messages', 0)} messages"
                        + (f" · llm_error={row['llm_error']}" if row.get("llm_error") else "")
                    )
    lines.extend(
        [
            "",
            "### Feedback",
            "Tell us **which section or customer story** you still cannot tell from this file, **what decision** you "
            "need to make with it, and **how urgent** it is. We use that to tune what gets exported next.",
        ]
    )
    return lines


def _emit_integration_stderr_warnings(report: dict[str, Any]) -> None:
    """Record when SF or CSR did not produce usable data (recap at end of export)."""
    from .export_run_diagnostics import collect_export_warning

    sf = report.get("salesforce") if isinstance(report.get("salesforce"), dict) else {}
    if not sf or sf.get("error"):
        msg = sf.get("error") if sf else "Salesforce payload missing"
        collect_export_warning(f"Salesforce: {msg}", llm_export=True)

    csr = report.get("csr") if isinstance(report.get("csr"), dict) else {}
    errs: list[str] = []
    for key in ("platform_health", "supply_chain", "platform_value"):
        b = csr.get(key)
        if isinstance(b, dict) and b.get("error"):
            errs.append(f"{key}: {b['error']}")
    if len(errs) == 3:
        collect_export_warning(
            "CS Report: all sections failed — " + " | ".join(errs),
            llm_export=True,
        )

    jira = report.get("jira") if isinstance(report.get("jira"), dict) else {}
    jira_err = str(jira.get("error") or "").strip()
    if jira_err:
        collect_export_warning(f"Jira: {jira_err}", llm_export=True)
    else:
        nested: list[str] = []
        for key, val in jira.items():
            if key in ("base_url", "error", "scope", "customers"):
                continue
            if isinstance(val, dict) and val.get("error"):
                nested.append(f"{key}: {val['error']}")
        customers = jira.get("customers")
        if isinstance(customers, dict):
            for label, entry in customers.items():
                if not isinstance(entry, dict):
                    continue
                jb = entry.get("jira")
                if isinstance(jb, dict) and jb.get("error"):
                    nested.append(f"{label}: {jb['error']}")
        if nested:
            collect_export_warning(
                "Jira: " + " | ".join(nested[:5]) + (" …" if len(nested) > 5 else ""),
                llm_export=True,
            )


def _compact_eng_enh_counts_only(blob: dict[str, Any] | None) -> dict[str, Any]:
    """Engineering / enhancement rolls — counts only (no ticket keys or summaries)."""
    if not blob or not isinstance(blob, dict):
        return {}
    if blob.get("error"):
        return {"error": blob.get("error")}
    out: dict[str, Any] = {"total": blob.get("total")}
    for key in ("open", "recent_closed", "shipped"):
        rows = blob.get(key)
        if isinstance(rows, list):
            out[f"{key}_count"] = len(rows)
    return out


def _compact_jira(j: dict[str, Any], *, size_caps_enabled: bool = True) -> dict[str, Any]:
    if not j or not isinstance(j, dict):
        return {}
    if _is_llm_export_top_arr_scope(j.get("scope")):
        customers_in = j.get("customers")
        customers_out: dict[str, Any] = {}
        if isinstance(customers_in, dict):
            for label, entry in customers_in.items():
                if not isinstance(entry, dict):
                    continue
                slim: dict[str, Any] = {
                    k: entry.get(k)
                    for k in (
                        "ultimate_parent",
                        "salesforce_label",
                        "salesforce_labels",
                        "arr",
                        "pendo_customer_key",
                        "jira_lookup_name",
                        "jira_match_terms",
                        "jira_merged_subsidiary_lookups",
                    )
                    if k in entry
                }
                jb = entry.get("jira")
                if isinstance(jb, dict):
                    slim["jira"] = _compact_jira_block(
                        jb, size_caps_enabled=size_caps_enabled
                    )
                customers_out[label] = slim
        return {
            "scope": j.get("scope"),
            "top_n": j.get("top_n"),
            "lookback_days": j.get("lookback_days"),
            "note": j.get("note"),
            "selection_ranked": j.get("selection_ranked"),
            "customers": customers_out,
        }
    return _compact_jira_block(j, size_caps_enabled=size_caps_enabled)


def _compact_jira_block(j: dict[str, Any], *, size_caps_enabled: bool = True) -> dict[str, Any]:
    if not j or not isinstance(j, dict):
        return {}
    if j.get("error"):
        return {"error": j.get("error")}
    keys_keep = (
        "customer",
        "days",
        "help_scope",
        "total_issues",
        "open_issues",
        "resolved_issues",
        "escalated",
        "open_bugs",
        "by_status",
        "by_type",
        "by_priority",
        "by_sentiment",
        "by_request_type",
        "ttfr",
        "ttr",
        "jsm_organizations_resolved",
    )
    out = {k: j.get(k) for k in keys_keep if k in j}
    tick = j.get("customer_ticket_metrics")
    if isinstance(tick, dict) and tick:
        out["customer_ticket_metrics"] = {
            k: tick.get(k)
            for k in (
                "unresolved_count",
                "resolved_in_6mo_count",
                "ttfr_1y",
                "ttr_1y",
                "sla_adherence_1y",
                "error",
            )
            if k in tick
        }
        bto = tick.get("by_type_open")
        if isinstance(bto, dict):
            items = list(bto.items())
            out["customer_ticket_metrics"]["by_type_open"] = (
                dict(items[:12]) if size_caps_enabled else dict(items)
            )
        bso = tick.get("by_status_open")
        if isinstance(bso, dict):
            items = list(bso.items())
            out["customer_ticket_metrics"]["by_status_open"] = (
                dict(items[:12]) if size_caps_enabled else dict(items)
            )
    fsb = j.get("help_factory_start_day_buckets")
    if isinstance(fsb, dict) and fsb:
        out["help_factory_start_day_buckets"] = {
            k: fsb.get(k)
            for k in (
                "error",
                "customer",
                "factory_start_date_field",
                "bucket_labels",
                "counts",
                "entity_rows_matched",
                "entities_with_factory_and_org",
                "skipped_no_factory_start",
                "skipped_no_jsm_org",
                "jira_count_partial_failure",
                "portfolio_aggregate",
                "jql_queries",
            )
            if k in fsb
        }
    hom = j.get("help_monthly_operational_metrics")
    if isinstance(hom, dict) and hom:
        rows = hom.get("rows")
        out["help_monthly_operational_metrics"] = {
            k: hom.get(k)
            for k in (
                "error",
                "customer",
                "jira_count_partial_failure",
                "jql_queries",
            )
            if k in hom
        }
        if isinstance(rows, list) and rows:
            out["help_monthly_operational_metrics"]["rows"] = (
                rows[-6:] if size_caps_enabled and len(rows) > 6 else rows
            )
    out["engineering"] = _compact_eng_enh_counts_only(j.get("engineering"))
    out["enhancements"] = _compact_eng_enh_counts_only(j.get("enhancements"))
    tow = j.get("tickets_over_time")
    if isinstance(tow, list):
        out["tickets_over_time"] = (
            tow[-24:] if size_caps_enabled and len(tow) > 24 else tow
        )
    return out


_SF_ACCOUNT_EXPORT_KEYS = (
    "Id",
    "Name",
    "ARR__c",
    "Type",
    "commercial_status",
    "active_arr",
    "renewal_arr",
    "current_arr",
    "historical_arr",
    "renewal_in_flight",
    "contract_statuses_distinct",
    "contract_end_date_nearest",
    "contract_end_date_farthest",
    "days_until_contract_end_nearest",
    "contract_start_date_earliest_active",
    "contract_start_date_latest_active",
    "entity_row_count",
)


def _compact_salesforce(sf: dict[str, Any], *, account_cap: int = 0) -> dict[str, Any]:
    if not sf or not isinstance(sf, dict):
        return {}
    if sf.get("error") or "error" in sf:
        return {"error": sf.get("error", "salesforce error")}
    out: dict[str, Any] = {
        k: sf.get(k)
        for k in (
            "matched",
            "resolution",
            "primary_account_id",
            "pipeline_arr",
            "opportunity_count_this_year",
        )
        if k in sf
    }
    rollups = sf.get("matched_customer_contract_rollups")
    if isinstance(rollups, list):
        if _export_cap_active(account_cap):
            rollup_cap = max(account_cap * 6, 72)
            out["matched_customer_contract_rollups"] = rollups[:rollup_cap]
        else:
            out["matched_customer_contract_rollups"] = list(rollups)
        out["matched_customer_contract_rollups_total"] = len(rollups)
        seg = sf.get("customer_segment") or "active"
        if seg == "churned":
            out["salesforce_export_note"] = (
                "Churned / lost contracts only (``commercial_status = CHURNED``). "
                "Do not merge with §3 current book or §5 Pendo portfolio."
            )
        elif seg == "renewal_negotiation":
            out["salesforce_export_note"] = (
                "Renewal negotiation segment (``commercial_status = OUT_OF_CONTRACT_RENEWING``). "
                "Expired entity contracts with open parent-account renewal pipeline — not churn. "
                "Do not merge with §3b churned or add to §3 current-book ARR."
            )
        elif seg == "future_contract":
            out["salesforce_export_note"] = (
                "Future contract segment (``commercial_status = FUTURE``). "
                "Won/signed contracts not yet in the current book. "
                "Do not merge with §3, §3b, or §3b-renewal."
            )
        else:
            out["salesforce_export_note"] = (
                "Current book only — ACTIVE and OUT_OF_CONTRACT_RENEWING ultimate parents "
                "(``commercial_status``). Ranked by ``current_arr`` (= ``active_arr`` + ``renewal_arr``). "
                "Churned, renewal-only, and future segments are in §3b, §3b-renewal, and §3b-future."
            )
    accts = sf.get("accounts")
    if isinstance(accts, list):
        slim = []
        acct_iter = accts[:account_cap] if _export_cap_active(account_cap) else accts
        for a in acct_iter:
            if not isinstance(a, dict):
                continue
            slim.append({k: a.get(k) for k in _SF_ACCOUNT_EXPORT_KEYS})
        out["accounts"] = slim
        out["accounts_total"] = len(accts)
    for k in (
        "segment_customer_count",
        "segment_contract_arr",
        "segment_current_arr",
        "segment_note",
        "portfolio_book_note",
    ):
        if k in sf:
            out[k] = sf[k]
    if sf.get("resolution") == "portfolio_aggregate":
        for k in (
            "total_arr",
            "historical_arr",
            "active_installed_base_arr",
            "active_arr",
            "renewal_arr",
            "current_arr",
            "churned_contract_arr",
            "future_contract_arr",
            "pendo_customers",
            "salesforce_matched_customers",
            "salesforce_unmatched_customers",
            "active_customer_count",
            "churned_customer_count",
            "renewal_in_flight_customer_count",
            "future_customer_count",
            "expansion_kpis",
            "portfolio_expansion_book",
        ):
            if k in sf:
                out[k] = sf[k]
    return out


def _pendo_portfolio_topline(
    portfolio: dict[str, Any],
    *,
    max_customer_rows: int = _PENDO_EXPORT_HEADLINE_CUSTOMER_CAP,
    size_caps_enabled: bool = True,
) -> dict[str, Any]:
    """Portfolio rollup + capped per-customer headline rows (no Pendo detail payloads)."""
    from src.export_string_utils import truncate_strings_in_obj

    raw_customers = portfolio.get("customers") if isinstance(portfolio.get("customers"), list) else []
    cap_rows = (
        raw_customers[:max_customer_rows]
        if size_caps_enabled and _export_cap_active(max_customer_rows)
        else raw_customers
    )
    rows: list[dict[str, Any]] = []
    for row in cap_rows:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "customer": row.get("customer"),
                "total_users": row.get("total_users"),
                "active_users": row.get("active_users"),
                "login_pct": row.get("login_pct"),
                "pendo_csm": row.get("pendo_csm"),
            }
        )
    note_parts = [
        "Portfolio rollup: per-customer rows are headline engagement counts when Pendo data exists "
        "(``salesforce_only`` rows carry Salesforce identity without Pendo metrics)."
    ]
    raw_n = len(raw_customers)
    if size_caps_enabled and _export_cap_active(max_customer_rows) and raw_n > max_customer_rows:
        note_parts.append(f"customers_headline truncated to {max_customer_rows} of {raw_n}.")
    sig_items = portfolio.get("portfolio_signals") or []
    if size_caps_enabled:
        sig_items = sig_items[:28]
    trunc_kw = (
        dict(max_str=240, max_list_items=28, max_dict_keys=16)
        if size_caps_enabled
        else dict(max_str=50_000, max_list_items=100_000, max_dict_keys=10_000)
    )
    cohort_bullets = portfolio.get("cohort_findings_bullets") or []
    if size_caps_enabled:
        cohort_bullets = cohort_bullets[:24]
    digest_kw = (
        dict(max_str=400, max_list_items=36, max_dict_keys=48)
        if size_caps_enabled
        else dict(max_str=50_000, max_list_items=100_000, max_dict_keys=10_000)
    )
    return {
        "scope": "portfolio_all_customers",
        "note": " ".join(note_parts),
        "customer_count": portfolio.get("customer_count"),
        "days": portfolio.get("days"),
        "generated": portfolio.get("generated"),
        "customers_headline": rows,
        "portfolio_signals_top": truncate_strings_in_obj(
            sig_items,
            **trunc_kw,
        ),
        "portfolio_trends": truncate_strings_in_obj(
            portfolio.get("portfolio_trends") or {},
            **(
                dict(max_str=400, max_list_items=40, max_dict_keys=48)
                if size_caps_enabled
                else dict(max_str=50_000, max_list_items=100_000, max_dict_keys=10_000)
            ),
        ),
        "portfolio_leaders": truncate_strings_in_obj(
            portfolio.get("portfolio_leaders") or {},
            **(
                dict(max_str=400, max_list_items=40, max_dict_keys=48)
                if size_caps_enabled
                else dict(max_str=50_000, max_list_items=100_000, max_dict_keys=10_000)
            ),
        ),
        "cohort_findings_bullets": list(cohort_bullets),
        "cohort_digest": truncate_strings_in_obj(
            portfolio.get("cohort_digest") or {},
            **digest_kw,
        ),
    }


def _sample_csr_sites_for_export(sites: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Prefer a health mix when capping site rows (avoid all high-shortage NONE factories)."""
    if limit <= 0 or len(sites) <= limit:
        return list(sites)
    order = ("GREEN", "YELLOW", "RED", "NONE")
    buckets: dict[str, list[dict[str, Any]]] = {k: [] for k in order}
    other: list[dict[str, Any]] = []
    for s in sites:
        hs = str(s.get("health_score") or "NONE").strip().upper()
        if hs in buckets:
            buckets[hs].append(s)
        else:
            other.append(s)
    per_bucket = max(1, limit // len(order))
    out: list[dict[str, Any]] = []
    for k in order:
        out.extend(buckets[k][:per_bucket])
    if len(out) < limit:
        remainder = [s for s in sites if s not in out]
        out.extend(remainder[: max(0, limit - len(out))])
    return out[:limit]


_CSR_SECTION_KEYS = ("platform_health", "supply_chain", "platform_value")
_CSR_SITE_JOIN_FIELDS = ("factory", "site", "entity")

# Per-factory `sites` rows repeat their field names hundreds of times across the export, so we
# emit short, stable keys in each row and publish a single `field_legend` (short -> long) at the
# top of §4. This keeps the structure fully key-value (self-describing per row, nulls omitted,
# chunk-safe) while removing the repeated long-field-name token cost (~19% off §4). LLMs decode
# each row via the legend; do NOT reuse a short key for two different long names.
_CSR_SITE_FIELD_ABBR: dict[str, str] = {
    # identity
    "factory": "fac",
    "site": "st",
    "entity": "ent",
    # platform health
    "health_score": "hs",
    "clear_to_build_pct": "ctb",
    "clear_to_commit_pct": "ctc",
    "component_availability_pct": "ca",
    "component_availability_projected_pct": "cap",
    "shortages": "sh",
    "critical_shortages": "csh",
    "weekly_active_buyers_pct": "wab",
    "buyer_mapping_quality": "bmq",
    "high_risk_items": "hri",
    # supply chain
    "on_hand_value": "ohv",
    "on_order_value": "oov",
    "excess_on_hand": "eoh",
    "doi_days": "doi",
    "days_coverage": "dcov",
    "turns_of_inventory": "toi",
    "late_pos": "lpo",
    "late_prs": "lpr",
    # platform value
    "savings_current_period": "scp",
    "open_ia_value": "oia",
    "recs_created_30d": "rc30",
    "pos_placed_30d": "pp30",
    "overdue_tasks": "odt",
    "current_fy_spend": "cfs",
    "previous_fy_spend": "pfs",
}
# short -> long, published once per §4 so any LLM can decode the abbreviated site rows.
_CSR_SITE_FIELD_LEGEND: dict[str, str] = {v: k for k, v in _CSR_SITE_FIELD_ABBR.items()}


def _abbreviate_csr_site(site: dict[str, Any]) -> dict[str, Any]:
    """Rename site-row keys to their short forms (order preserved; unknown keys kept as-is)."""
    return {_CSR_SITE_FIELD_ABBR.get(k, k): v for k, v in site.items()}

# One-line, self-describing schema hint so any LLM reading the export understands the
# de-duplicated §4 shape without external docs.
_CSR_SCHEMA_NOTE = (
    "Each `sites` row is ONE factory with all CS Report metrics merged inline. Row keys are "
    "ABBREVIATED to save tokens — decode them with the `field_legend` map at the top of this "
    "section (short -> long, e.g. `hs`=health_score, `ctb`=clear_to_build_pct, `doi`=doi_days, "
    "`ohv`=on_hand_value, `scp`=savings_current_period). A key is present only when that factory "
    "has a value (nulls are omitted). Merged metrics span platform-health, supply-chain, and "
    "platform-value worksheets. Per-customer section rollups (health_distribution, total_shortages, "
    "inventory_totals, total_savings, …) are in the §4.1 markdown table (one row per customer), not "
    "in this JSON. When `sites_total` > len(sites), rows were sampled (see `sites_sample_strategy`); "
    "the §4.1 summary still reflects all factories."
)

_CSR_SUMMARY_HEALTH_KEYS = (
    "factory_count",
    "health_distribution",
    "total_shortages",
    "total_critical_shortages",
)
_CSR_SUMMARY_VALUE_KEYS = (
    "total_savings",
    "total_open_ia_value",
    "total_potential_savings",
    "total_potential_to_sell",
    "total_recs_created_30d",
    "total_pos_placed_30d",
    "total_overdue_tasks",
)


def _csr_site_join_key(site: dict[str, Any]) -> tuple[str, str, str]:
    return tuple(str(site.get(f) or "").strip().lower() for f in _CSR_SITE_JOIN_FIELDS)  # type: ignore[return-value]


def _merge_customer_csr_site_rows(block: dict[str, Any]) -> list[dict[str, Any]]:
    """Union the three CSR worksheet ``sites`` lists into one row per factory (metrics inline).

    Removes the ~3× duplication of factory name + wrapper keys across the health / supply-chain /
    value worksheets. Metric field names are disjoint across worksheets, so a factory row gathers
    every metric present for it. Errored sections contribute nothing.
    """
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    order: list[tuple[str, str, str]] = []
    for sec_name in _CSR_SECTION_KEYS:
        sec = block.get(sec_name)
        if not isinstance(sec, dict) or sec.get("error"):
            continue
        for site in sec.get("sites") or []:
            if not isinstance(site, dict):
                continue
            key = _csr_site_join_key(site)
            if key not in merged:
                merged[key] = {}
                order.append(key)
            merged[key].update(site)
    return [merged[k] for k in order]


def _csr_customer_summary(block: dict[str, Any], *, factory_count: int) -> dict[str, Any]:
    """Per-customer section rollups (no site rows) across the three CSR worksheets."""
    summary: dict[str, Any] = {"factory_count": factory_count}
    ph = block.get("platform_health")
    if isinstance(ph, dict) and not ph.get("error"):
        for k in _CSR_SUMMARY_HEALTH_KEYS:
            if k in ph and k != "factory_count":
                summary[k] = ph[k]
    sc = block.get("supply_chain")
    if isinstance(sc, dict) and not sc.get("error") and isinstance(sc.get("totals"), dict):
        summary["inventory_totals"] = sc["totals"]
    pv = block.get("platform_value")
    if isinstance(pv, dict) and not pv.get("error"):
        for k in _CSR_SUMMARY_VALUE_KEYS:
            if k in pv:
                summary[k] = pv[k]
    return summary


def _compact_csr_customer_block(
    block: dict[str, Any], *, site_limit: int, string_cap: int, size_caps_enabled: bool = True
) -> dict[str, Any]:
    """Merged, LLM-friendly per-customer CSR: one row per factory + section rollups."""
    from src.export_string_utils import truncate_strings_in_obj

    merged_sites = _merge_customer_csr_site_rows(block)
    out: dict[str, Any] = {"summary": _csr_customer_summary(block, factory_count=len(merged_sites))}

    section_errors = {
        name: block[name]["error"]
        for name in _CSR_SECTION_KEYS
        if isinstance(block.get(name), dict) and block[name].get("error")
    }
    if section_errors:
        out["section_errors"] = section_errors

    if merged_sites:
        total = len(merged_sites)
        if size_caps_enabled and _export_cap_active(site_limit):
            sampled = _sample_csr_sites_for_export(merged_sites, site_limit)
            out["sites"] = [_abbreviate_csr_site(s) for s in sampled]
            if len(sampled) < total:
                out["sites_sample_strategy"] = "health_mix_then_shortage_bias"
        else:
            out["sites"] = [_abbreviate_csr_site(s) for s in merged_sites]
        out["sites_total"] = total
    elif not section_errors:
        out["sites"] = []

    if size_caps_enabled and _export_cap_active(string_cap):
        return truncate_strings_in_obj(out, max_str=string_cap, max_list_items=200, max_dict_keys=96)
    return truncate_strings_in_obj(out, max_str=50_000, max_list_items=100_000, max_dict_keys=10_000)


def _compact_csr(
    csr: dict[str, Any], *, site_limit: int, string_cap: int, size_caps_enabled: bool = True
) -> dict[str, Any]:
    if not csr:
        return {
            "note": "CS Report was not attached (empty csr). Check Drive CS Report export and openpyxl.",
        }
    out: dict[str, Any] = {}
    if isinstance(csr.get("note"), str):
        out["note"] = csr["note"]
    out["schema_note"] = _CSR_SCHEMA_NOTE
    out["field_legend"] = _CSR_SITE_FIELD_LEGEND
    if _is_llm_export_top_arr_scope(csr.get("scope")):
        out["scope"] = csr["scope"]
        if csr.get("top_n") is not None:
            out["top_n"] = csr["top_n"]
        if isinstance(csr.get("selection_ranked"), list):
            out["selection_ranked"] = csr["selection_ranked"]
        customers = csr.get("customers")
        if isinstance(customers, dict):
            out["customers"] = {}
            for label, block in customers.items():
                if not isinstance(block, dict):
                    continue
                slim: dict[str, Any] = {
                    k: block[k]
                    for k in (
                        "ultimate_parent",
                        "salesforce_label",
                        "salesforce_labels",
                        "arr",
                        "pendo_customer_key",
                        "csr_lookup_name",
                    )
                    if k in block
                }
                slim.update(
                    _compact_csr_customer_block(
                        block,
                        site_limit=site_limit,
                        string_cap=string_cap,
                        size_caps_enabled=size_caps_enabled,
                    )
                )
                out["customers"][label] = slim
        return out
    # Legacy / all-customers aggregate shape: three sections at the top level.
    if any(isinstance(csr.get(k), dict) for k in _CSR_SECTION_KEYS):
        out.update(
            _compact_csr_customer_block(
                csr,
                site_limit=site_limit,
                string_cap=string_cap,
                size_caps_enabled=size_caps_enabled,
            )
        )
    return out


# Column order for the per-customer CS Report summary table (§4). Known keys are placed first in
# this order; any extra keys found at runtime are appended so nothing is silently dropped.
_CSR_SUMMARY_TABLE_SCALARS = (
    "factory_count",
    "total_shortages",
    "total_critical_shortages",
    "total_savings",
    "total_open_ia_value",
    "total_potential_savings",
    "total_potential_to_sell",
    "total_recs_created_30d",
    "total_pos_placed_30d",
    "total_overdue_tasks",
)
_CSR_SUMMARY_HEALTH_ORDER = ("GREEN", "YELLOW", "RED", "NONE")
_CSR_SUMMARY_INV_ORDER = (
    "on_hand",
    "on_order",
    "excess_on_hand",
    "excess_on_order",
    "past_due_po",
    "past_due_req",
)


def _md_table_cell(value: Any) -> str:
    """Render one markdown-table cell: None/empty -> blank; escape pipes and collapse newlines."""
    if value is None:
        return ""
    text = str(value).replace("|", r"\|").replace("\n", " ").strip()
    return text


def _ordered_keys(seen: set[str], preferred: tuple[str, ...]) -> list[str]:
    ordered = [k for k in preferred if k in seen]
    ordered.extend(sorted(k for k in seen if k not in preferred))
    return ordered


def _csr_summary_markdown_table(cs: dict[str, Any]) -> str | None:
    """Render all per-customer CS Report ``summary`` rollups as ONE markdown table.

    A single cross-customer table costs far fewer tokens than 98 repeated JSON objects (long keys
    are written once as headers, not per row) and is the shape an LLM reads most reliably for
    "how is every customer doing" questions. Nested ``health_distribution`` / ``inventory_totals``
    dicts are flattened into ``health_*`` / ``inv_*`` columns. Returns None when there are no
    per-customer summaries (e.g. empty or legacy single-block CSR), so the caller falls back to JSON.
    """
    customers = cs.get("customers")
    if not isinstance(customers, dict):
        return None
    rows: list[tuple[str, dict[str, Any], Any]] = []
    scalar_seen: set[str] = set()
    health_seen: set[str] = set()
    inv_seen: set[str] = set()
    for label, block in customers.items():
        if not isinstance(block, dict):
            continue
        summary = block.get("summary")
        if not isinstance(summary, dict):
            continue
        rows.append((str(label), summary, block.get("arr")))
        for k, v in summary.items():
            if k == "health_distribution" and isinstance(v, dict):
                health_seen |= set(v.keys())
            elif k == "inventory_totals" and isinstance(v, dict):
                inv_seen |= set(v.keys())
            elif not isinstance(v, (dict, list)):
                scalar_seen.add(k)
    if not rows:
        return None

    scalar_cols = _ordered_keys(scalar_seen, _CSR_SUMMARY_TABLE_SCALARS)
    health_cols = _ordered_keys(health_seen, _CSR_SUMMARY_HEALTH_ORDER)
    inv_cols = _ordered_keys(inv_seen, _CSR_SUMMARY_INV_ORDER)
    has_arr = any(arr is not None for _, _, arr in rows)

    header: list[str] = ["customer"]
    if has_arr:
        header.append("arr")
    header += scalar_cols
    header += [f"health_{k}" for k in health_cols]
    header += [f"inv_{k}" for k in inv_cols]

    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(["---"] * len(header)) + " |"]
    for label, summary, arr in rows:
        hd = summary.get("health_distribution") if isinstance(summary.get("health_distribution"), dict) else {}
        inv = summary.get("inventory_totals") if isinstance(summary.get("inventory_totals"), dict) else {}
        cells = [_md_table_cell(label)]
        if has_arr:
            cells.append(_md_table_cell(arr))
        cells += [_md_table_cell(summary.get(k)) for k in scalar_cols]
        cells += [_md_table_cell(hd.get(k)) for k in health_cols]
        cells += [_md_table_cell(inv.get(k)) for k in inv_cols]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _cs_report_detail_without_summary(cs: dict[str, Any]) -> dict[str, Any]:
    """Copy of the CSR block with per-customer ``summary`` removed (it now lives in the table)."""
    detail = {k: v for k, v in cs.items() if k != "customers"}
    customers = cs.get("customers")
    if isinstance(customers, dict):
        detail["customers"] = {
            label: ({k: v for k, v in block.items() if k != "summary"} if isinstance(block, dict) else block)
            for label, block in customers.items()
        }
    return detail


def _render_cs_report_section(cs: Any) -> list[str]:
    """§4 body: a per-customer summary markdown table + factory-detail JSON (summary removed).

    Falls back to a single JSON blob when there are no per-customer summaries to tabulate.
    """
    if not isinstance(cs, dict):
        return [_json_compact(cs)]
    table = _csr_summary_markdown_table(cs)
    if table is None:
        return [_json_compact(cs)]
    return [
        "### 4.1 Per-customer summary (all customers) — one row per customer",
        "",
        table,
        "",
        "### 4.2 Per-customer factory detail (JSON) — one `sites` row per factory",
        "",
        "> Summaries are in the §4.1 table above and are omitted here to avoid duplication. "
        "Site-row keys are abbreviated; decode with ``field_legend``.",
        "",
        _json_compact(_cs_report_detail_without_summary(cs)),
    ]


# §3 current-book table columns: (account json key, table header). Rendered in this order,
# but a column is dropped when every row is empty for it (keeps the table narrow per segment).
_SF_ACCOUNTS_TABLE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("Name", "customer"),
    ("commercial_status", "commercial_status"),
    ("current_arr", "current_arr"),
    ("active_arr", "active_arr"),
    ("renewal_arr", "renewal_arr"),
    ("historical_arr", "historical_arr"),
    ("renewal_in_flight", "renewal_in_flight"),
    ("days_until_contract_end_nearest", "days_to_renewal"),
    ("contract_end_date_nearest", "contract_end_nearest"),
    ("contract_end_date_farthest", "contract_end_farthest"),
    ("contract_start_date_earliest_active", "contract_start_earliest"),
    ("contract_start_date_latest_active", "contract_start_latest"),
    ("contract_statuses_distinct", "contract_statuses"),
    ("entity_row_count", "entity_rows"),
    ("ARR__c", "arr_field_c"),
)


def _sf_arr_num(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def _rollup_rows_from_sf_accounts(accounts: Any) -> list[dict[str, Any]]:
    """Map §3 ``accounts`` rows back to portfolio contract-rollup shape for grouping."""
    rows: list[dict[str, Any]] = []
    if not isinstance(accounts, list):
        return rows
    for account in accounts:
        if not isinstance(account, dict):
            continue
        label = str(account.get("Name") or "").strip()
        if not label:
            continue
        row = dict(account)
        row["customer"] = label
        rows.append(row)
    return rows


def _grouped_sf_accounts_for_table(accounts: Any) -> list[dict[str, Any]]:
    """Roll up current-book labels to ultimate parent (same math as ``selection_ranked`` / §3c)."""
    from .llm_export_csr import group_contract_rollups_by_ultimate_parent

    rollups = _rollup_rows_from_sf_accounts(accounts)
    if not rollups:
        return []
    grouped = group_contract_rollups_by_ultimate_parent(rollups, current_book_only=False)
    out: list[dict[str, Any]] = []
    for bucket in grouped:
        out.append(
            {
                "Name": bucket["ultimate_parent"],
                "commercial_status": bucket.get("commercial_status"),
                "current_arr": bucket.get("current_arr"),
                "active_arr": bucket.get("active_arr"),
                "renewal_arr": bucket.get("renewal_arr"),
                "historical_arr": bucket.get("historical_arr"),
                "entity_row_count": bucket.get("entity_count"),
            }
        )
    return out


def _salesforce_accounts_markdown_table(accounts: Any) -> str | None:
    """Render the Salesforce ``accounts`` list as ONE markdown table, ranked by ``current_arr``.

    LLMs rank a single minified-JSON line unreliably and tend to confabulate a plausible-looking
    "top N by ARR" (dropping/renumbering rows, inventing figures). A pre-sorted table with a
    ``rank`` column makes "top N customers by revenue" a lookup, not a computation. Columns that
    are empty across every row are dropped so churned/future segments stay narrow. Returns None
    when there are no account rows, so the caller falls back to JSON.
    """
    rows = [a for a in accounts if isinstance(a, dict)] if isinstance(accounts, list) else []
    if not rows:
        return None
    rows = sorted(
        rows,
        key=lambda a: (-_sf_arr_num(a.get("current_arr")), -_sf_arr_num(a.get("historical_arr"))),
    )
    present = [
        (key, label)
        for key, label in _SF_ACCOUNTS_TABLE_COLUMNS
        if any(a.get(key) not in (None, "", []) for a in rows)
    ]
    if not present:
        return None
    header = ["rank"] + [label for _, label in present]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(["---"] * len(header)) + " |",
    ]
    for i, a in enumerate(rows, 1):
        cells = [str(i)]
        for key, _label in present:
            val = a.get(key)
            if isinstance(val, list):
                val = ", ".join(str(x) for x in val)
            cells.append(_md_table_cell(val))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _render_salesforce_current_book_section(block: Any) -> list[str]:
    """§3 body: a pre-sorted per-account markdown table (§3.1) + rollup/aggregate JSON (§3.2).

    The per-account list is removed from the JSON to avoid duplicating it under the table.
    Falls back to a single JSON blob when there are no account rows to tabulate.
    """
    if not isinstance(block, dict):
        return [_json_compact(block)]
    table = _salesforce_accounts_markdown_table(_grouped_sf_accounts_for_table(block.get("accounts")))
    if table is None:
        return [_json_compact(block)]
    detail = {k: v for k, v in block.items() if k != "accounts"}
    return [
        "### 3.1 Current book by ARR (one row per ultimate parent, ranked)",
        "",
        "> Ranked by ``current_arr`` desc (= ``active_arr`` + ``renewal_arr``). This table is the "
        "authoritative source for \u201ctop N customers by revenue\u201d \u2014 read the ``rank`` column "
        "directly; do not re-derive the ranking from other sections.",
        "",
        table,
        "",
        "### 3.2 Contract rollups & portfolio aggregates (JSON)",
        "",
        "> Per-account rows are in the §3.1 table above and omitted here to avoid duplication.",
        "",
        _json_compact(detail),
    ]


def _compact_slack(slack: dict[str, Any], *, size_caps_enabled: bool = True) -> dict[str, Any]:
    if not slack:
        return {
            "note": "Slack was not attached (empty slack). Set SLACK_BOT_TOKEN and CORTEX_LLM_EXPORT_SLACK.",
        }
    max_lines = 30 if size_caps_enabled else 500
    out: dict[str, Any] = {}
    for key in ("scope", "top_n", "lookback_days", "note", "skipped", "error"):
        if key in slack and slack[key] is not None:
            out[key] = slack[key]
    if isinstance(slack.get("selection_ranked"), list):
        out["selection_ranked"] = slack["selection_ranked"]
    customers = slack.get("customers")
    if isinstance(customers, dict):
        out["customers"] = {}
        for label, block in customers.items():
            if not isinstance(block, dict):
                continue
            slim: dict[str, Any] = {
                k: block[k]
                for k in ("salesforce_label", "lookup_name", "arr")
                if k in block
            }
            payload = block.get("slack") if isinstance(block.get("slack"), dict) else {}
            summaries = payload.get("conversation_summaries") if isinstance(payload.get("conversation_summaries"), list) else []
            slim_summaries: list[dict[str, Any]] = []
            for s in summaries:
                if not isinstance(s, dict):
                    continue
                lines = s.get("summary_lines") if isinstance(s.get("summary_lines"), list) else []
                llm_ok = isinstance(block.get("llm_summary"), dict) and block["llm_summary"].get("status") == "ok"
                slim_summaries.append(
                    {
                        "channel_id": s.get("channel_id"),
                        "channel_name": s.get("channel_name"),
                        "message_count": s.get("message_count"),
                        "summary_lines": [] if llm_ok else (lines[-max_lines:] if max_lines else lines),
                        "error": s.get("error"),
                    }
                )
            slim["slack"] = {
                "source": payload.get("source"),
                "days": payload.get("days"),
                "channels_matched": payload.get("channels_matched"),
                "conversation_summaries": slim_summaries,
                "combined_summary_markdown": payload.get("combined_summary_markdown"),
                "note": payload.get("note"),
                "error": payload.get("error"),
            }
            if isinstance(block.get("llm_summary"), dict):
                slim["llm_summary"] = block["llm_summary"]
            out["customers"][label] = slim
        return out
    payload = slack
    summaries = payload.get("conversation_summaries") if isinstance(payload.get("conversation_summaries"), list) else []
    slim_summaries = []
    for s in summaries:
        if not isinstance(s, dict):
            continue
        lines = s.get("summary_lines") if isinstance(s.get("summary_lines"), list) else []
        slim_summaries.append(
            {
                "channel_id": s.get("channel_id"),
                "channel_name": s.get("channel_name"),
                "message_count": s.get("message_count"),
                "summary_lines": lines[-max_lines:] if max_lines else lines,
                "error": s.get("error"),
            }
        )
    return {
        "source": payload.get("source"),
        "customer": payload.get("customer"),
        "days": payload.get("days"),
        "channels_matched": payload.get("channels_matched"),
        "conversation_summaries": slim_summaries,
        "combined_summary_markdown": payload.get("combined_summary_markdown"),
        "note": payload.get("note"),
        "error": payload.get("error"),
        "skipped": payload.get("skipped"),
    }


def _compact_salesforce_comprehensive_portfolio(
    block: dict[str, Any],
    *,
    report: dict[str, Any] | None = None,
    top_customers: int = 25,
    rows_per_category: int = 5,
    entity_account_cap: int = 48,
) -> dict[str, Any]:
    """Shrink §3c for LLM export: top ARR customers, KPIs, and capped category row samples."""
    if not block or not isinstance(block, dict):
        return {}
    out: dict[str, Any] = {
        k: block[k]
        for k in (
            "configured",
            "row_limit",
            "customer_count",
            "portfolio_expansion_book",
            "note",
            "error",
            "skipped",
        )
        if k in block
    }
    out["export_compaction"] = {
        "mode": "headline",
        "top_customers": top_customers,
        "rows_per_category": rows_per_category,
        "entity_account_cap": entity_account_cap,
    }
    by_customer = block.get("by_customer") if isinstance(block.get("by_customer"), dict) else {}
    priority: list[str] = []
    if report is not None:
        try:
            from .llm_export_csr import top_active_ultimate_parents_by_arr_for_llm_export

            for row in top_active_ultimate_parents_by_arr_for_llm_export(
                report, top_n=max(1, top_customers)
            ):
                label = str(
                    row.get("ultimate_parent") or row.get("salesforce_label") or ""
                ).strip()
                if label and label not in priority:
                    priority.append(label)
        except Exception:
            pass
    for label in sorted(by_customer.keys()):
        if label not in priority:
            priority.append(label)
    chosen = priority[: max(0, int(top_customers))]
    slim_by: dict[str, Any] = {}
    rows_cap = max(0, int(rows_per_category))
    for label in chosen:
        payload = by_customer.get(label)
        if not isinstance(payload, dict):
            continue
        slim: dict[str, Any] = {
            k: payload[k]
            for k in (
                "customer",
                "matched",
                "resolution",
                "primary_account_id",
                "pipeline_arr",
                "opportunity_count_this_year",
                "row_limit",
                "customer_segment",
                "error",
            )
            if k in payload
        }
        aids = payload.get("account_ids")
        if isinstance(aids, list):
            slim["account_ids_count"] = len(aids)
        cats = payload.get("categories") if isinstance(payload.get("categories"), dict) else {}
        slim_cats: dict[str, Any] = {}
        cat_errors = payload.get("category_errors") if isinstance(payload.get("category_errors"), dict) else {}
        for cat, rows in cats.items():
            if not isinstance(rows, list):
                continue
            slim_cats[str(cat)] = {
                "row_count": len(rows),
                "sample": rows[:rows_cap] if rows_cap else [],
            }
        slim["categories"] = slim_cats
        if cat_errors:
            slim["category_errors"] = cat_errors
        slim_by[label] = slim
    out["by_customer"] = slim_by
    out["by_customer_exported"] = len(slim_by)
    out["by_customer_total"] = len(by_customer)
    entities = block.get("entity_accounts") if isinstance(block.get("entity_accounts"), list) else []
    cap_e = max(0, int(entity_account_cap))
    if cap_e and len(entities) > cap_e:
        out["entity_accounts"] = entities[:cap_e]
        out["entity_accounts_count"] = len(entities)
        out["entity_accounts_truncated"] = True
    else:
        out["entity_accounts"] = entities
        out["entity_accounts_count"] = len(entities)
    # Ultimate-parent ARR rollup is small and pre-aggregated across the *full* book, so keep
    # it even when per-entity rows are truncated — it answers "top N by Ultimate Parent ARR".
    arr_by_up = block.get("arr_by_ultimate_parent")
    if isinstance(arr_by_up, list):
        out["arr_by_ultimate_parent"] = arr_by_up[:100]
        out["arr_by_ultimate_parent_count"] = len(arr_by_up)
    return out


def _portfolio_signal_lines(
    portfolio: dict[str, Any], *, cap: int | None, line_max: int, size_caps_enabled: bool = True
) -> list[str]:
    """``cap`` ``None`` includes every ``portfolio_signals`` row (subject only to ``line_max``)."""
    items = portfolio.get("portfolio_signals") if isinstance(portfolio.get("portfolio_signals"), list) else []
    chosen = items if cap is None else items[: max(0, int(cap))]
    out: list[str] = []
    for item in chosen:
        if isinstance(item, dict):
            cust = str(item.get("customer") or "").strip()
            sig = str(item.get("signal") or "").strip()
            line = f"{cust}: {sig}" if cust else sig
        else:
            line = str(item)
        line = " ".join(line.split())
        if size_caps_enabled and _export_cap_active(line_max) and len(line) > line_max:
            line = line[: line_max - 1] + "…"
        out.append(line)
    return out


def build_snapshot_document(
    report: dict[str, Any],
    *,
    markdown_soft_cap_bytes: int = _LLM_EXPORT_NO_CAP,
    markdown_soft_cap_tokens: int = _LLM_EXPORT_NO_CAP,
    csr_site_limit: int = _LLM_EXPORT_NO_CAP,
    csr_string_cap: int = _LLM_EXPORT_NO_CAP,
    sf_accounts: int = _LLM_EXPORT_NO_CAP,
    signals_cap: int | None = None,
    signal_line_max: int = _LLM_EXPORT_NO_CAP,
    size_caps_enabled: bool = False,
    export_diag: Any | None = None,
) -> dict[str, Any]:
    csr = report.get("csr") if isinstance(report.get("csr"), dict) else {}
    pendo_sec = _pendo_portfolio_topline(report, size_caps_enabled=size_caps_enabled)
    sig_lines = _portfolio_signal_lines(
        report,
        cap=signals_cap,
        line_max=signal_line_max,
        size_caps_enabled=size_caps_enabled,
    )
    churn_seg = report.get("salesforce_churned_segment")
    churn_sf = {}
    churn_headline: list[dict[str, Any]] = []
    if isinstance(churn_seg, dict):
        raw_sf = churn_seg.get("salesforce")
        if isinstance(raw_sf, dict):
            churn_sf = _compact_salesforce(
                raw_sf, account_cap=sf_accounts if size_caps_enabled else _LLM_EXPORT_NO_CAP
            )
        raw_rows = churn_seg.get("customers_headline")
        if isinstance(raw_rows, list):
            churn_rows = [r for r in raw_rows if isinstance(r, dict)]
            if size_caps_enabled and _export_cap_active(sf_accounts):
                churn_headline = churn_rows[: max(sf_accounts, 1)]
            else:
                churn_headline = churn_rows

    renewal_seg = report.get("salesforce_renewal_negotiation_segment")
    renewal_sf = {}
    renewal_headline: list[dict[str, Any]] = []
    if isinstance(renewal_seg, dict):
        raw_sf = renewal_seg.get("salesforce")
        if isinstance(raw_sf, dict):
            renewal_sf = _compact_salesforce(
                raw_sf, account_cap=sf_accounts if size_caps_enabled else _LLM_EXPORT_NO_CAP
            )
        raw_rows = renewal_seg.get("customers_headline")
        if isinstance(raw_rows, list):
            renewal_rows = [r for r in raw_rows if isinstance(r, dict)]
            if size_caps_enabled and _export_cap_active(sf_accounts):
                renewal_headline = renewal_rows[: max(sf_accounts, 1)]
            else:
                renewal_headline = renewal_rows

    future_seg = report.get("salesforce_future_contract_segment")
    future_sf = {}
    future_headline: list[dict[str, Any]] = []
    if isinstance(future_seg, dict):
        raw_sf = future_seg.get("salesforce")
        if isinstance(raw_sf, dict):
            future_sf = _compact_salesforce(
                raw_sf, account_cap=sf_accounts if size_caps_enabled else _LLM_EXPORT_NO_CAP
            )
        raw_rows = future_seg.get("customers_headline")
        if isinstance(raw_rows, list):
            future_rows = [r for r in raw_rows if isinstance(r, dict)]
            if size_caps_enabled and _export_cap_active(sf_accounts):
                future_headline = future_rows[: max(sf_accounts, 1)]
            else:
                future_headline = future_rows

    doc: dict[str, Any] = {
        "document_purpose": (
            "Structured facts from Cortex integrations for LLM Q&A. Figures are snapshots from vendor APIs "
            "and internal exports; verify in source systems before contractual or financial use. "
            "Current-book customers (§1, §3, §5) use ``commercial_status`` ACTIVE + OUT_OF_CONTRACT_RENEWING; "
            "inactive SF segments are §3b churned-lost, §3b-renewal negotiation, and §3b-future contracts."
        ),
        "customer": report.get("customer"),
        "generated_report_timestamp": report.get("generated"),
        "lookback_days": report.get("days"),
        "pendo": pendo_sec,
        "jira_help": _compact_jira(
            report.get("jira") or {}, size_caps_enabled=size_caps_enabled
        ),
        "salesforce": _compact_salesforce(
            report.get("salesforce") or {},
            account_cap=sf_accounts if size_caps_enabled else _LLM_EXPORT_NO_CAP,
        ),
        "salesforce_churned_segment": {
            "segment": "churned",
            "do_not_merge_with_active_book": True,
            "usage_note": (churn_seg or {}).get("usage_note") if isinstance(churn_seg, dict) else None,
            "customer_count": (churn_seg or {}).get("customer_count") if isinstance(churn_seg, dict) else 0,
            "customers_headline": churn_headline,
            "salesforce": churn_sf,
        },
        "salesforce_renewal_negotiation_segment": {
            "segment": "renewal_negotiation",
            "do_not_merge_with_active_book": True,
            "usage_note": (renewal_seg or {}).get("usage_note") if isinstance(renewal_seg, dict) else None,
            "customer_count": (renewal_seg or {}).get("customer_count") if isinstance(renewal_seg, dict) else 0,
            "customers_headline": renewal_headline,
            "salesforce": renewal_sf,
        },
        "salesforce_future_contract_segment": {
            "segment": "future_contract",
            "do_not_merge_with_active_book": True,
            "usage_note": (future_seg or {}).get("usage_note") if isinstance(future_seg, dict) else None,
            "customer_count": (future_seg or {}).get("customer_count") if isinstance(future_seg, dict) else 0,
            "customers_headline": future_headline,
            "salesforce": future_sf,
        },
        "salesforce_comprehensive_portfolio": (
            _compact_salesforce_comprehensive_portfolio(
                report.get("salesforce_comprehensive_portfolio")
                if isinstance(report.get("salesforce_comprehensive_portfolio"), dict)
                else {},
                report=report,
            )
            if size_caps_enabled
            else (
                report.get("salesforce_comprehensive_portfolio")
                if isinstance(report.get("salesforce_comprehensive_portfolio"), dict)
                else {}
            )
        ),
        "cs_report": _compact_csr(
            csr,
            site_limit=csr_site_limit,
            string_cap=csr_string_cap,
            size_caps_enabled=size_caps_enabled,
        ),
        "slack": _compact_slack(
            report.get("slack") if isinstance(report.get("slack"), dict) else {},
            size_caps_enabled=size_caps_enabled,
        ),
        "notable_signals_lines": sig_lines,
        "planned_data_sources": {
            "not_in_snapshot_yet": list(_PLANNED_DATASOURCES_NOT_IN_EXPORT),
            "note": (
                "These vendor/product integrations are not included in this export yet; "
                "snapshot coverage is planned."
            ),
        },
        "integration_coverage_lines": _integration_coverage_lines(
            salesforce=report.get("salesforce") if isinstance(report.get("salesforce"), dict) else {},
            csr=csr,
        ),
        "export_coverage": _build_export_coverage(
            report,
            markdown_soft_cap_bytes=markdown_soft_cap_bytes,
            markdown_soft_cap_tokens=markdown_soft_cap_tokens,
            csr_site_limit=csr_site_limit,
            csr_string_cap=csr_string_cap,
            sf_accounts=sf_accounts,
            signals_cap=signals_cap,
            signals_line_max_chars=signal_line_max,
            size_caps_enabled=size_caps_enabled,
        ),
        "leandna_data_api_reference": build_leandna_data_api_reference(),
        "data_governance_warnings": [],
    }
    stc = report.get("signals_trend_context")
    if stc:
        from src.export_string_utils import truncate_strings_in_obj

        if size_caps_enabled:
            doc["signals_trend_context"] = truncate_strings_in_obj(
                stc, max_str=320, max_list_items=24, max_dict_keys=48
            )
        else:
            doc["signals_trend_context"] = truncate_strings_in_obj(
                stc, max_str=50_000, max_list_items=100_000, max_dict_keys=10_000
            )
    from .data_governance_warnings import build_data_governance_warning_entries

    doc["data_governance_warnings"] = build_data_governance_warning_entries(
        report,
        export_diag,
    )
    return doc


def _json_compact(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)


def _utf8_byte_len(text: str) -> int:
    return len(text.encode("utf-8"))


def _format_utf8_bytes(n: int) -> str:
    n = max(0, int(n))
    if n >= 1_048_576:
        return f"{n / 1_048_576:.2f} MiB ({n:,} B)"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB ({n:,} B)"
    return f"{n:,} B"


def _markdown_section_byte_breakdown(md: str) -> list[tuple[str, int]]:
    """UTF-8 size per ``## …`` section in the uploaded markdown."""
    text = md or ""
    if not text.strip():
        return [("(empty)", 0)]
    parts = re.split(r"\n(?=## )", text.lstrip("\n"))
    out: list[tuple[str, int]] = []
    for part in parts:
        block = part.strip("\n")
        if not block:
            continue
        first = block.split("\n", 1)[0]
        if first.startswith("## "):
            label = first[3:].strip()
        else:
            label = "header / preamble"
        out.append((label, _utf8_byte_len(block)))
    return out


def _doc_payload_component_bytes(doc: dict[str, Any]) -> list[tuple[str, int]]:
    """Compact-JSON byte sizes for major export document fields (pre-upload doc)."""
    rows: list[tuple[str, int]] = []

    def add(label: str, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            rows.append((label, _utf8_byte_len(value)))
        elif isinstance(value, list) and label == "notable_signals_lines":
            body = "\n".join(f"- {ln}" for ln in value if ln is not None)
            rows.append((label, _utf8_byte_len(body)))
        else:
            rows.append((label, _utf8_byte_len(_json_compact(value))))

    add("pendo", doc.get("pendo"))
    add("jira_help", doc.get("jira_help"))
    add("salesforce", doc.get("salesforce"))
    add("salesforce_churned_segment", doc.get("salesforce_churned_segment"))
    add("salesforce_renewal_negotiation_segment", doc.get("salesforce_renewal_negotiation_segment"))
    add("salesforce_future_contract_segment", doc.get("salesforce_future_contract_segment"))
    add("salesforce_comprehensive_portfolio", doc.get("salesforce_comprehensive_portfolio"))
    add("cs_report", doc.get("cs_report"))
    add("slack", doc.get("slack"))
    add("notable_signals_lines", doc.get("notable_signals_lines"))
    add("signals_trend_context", doc.get("signals_trend_context"))
    add("leandna_data_api_reference", doc.get("leandna_data_api_reference"))
    add("export_coverage", doc.get("export_coverage"))
    add("data_governance_warnings", doc.get("data_governance_warnings"))
    add("planned_data_sources", doc.get("planned_data_sources"))
    add("integration_coverage_lines", doc.get("integration_coverage_lines"))
    return sorted(rows, key=lambda x: (-x[1], x[0]))


def _export_summary_rule() -> str:
    return "─" * 60


def _export_status_label(diag: Any | None) -> str:
    if diag is None:
        return "completed"
    failures = getattr(diag, "failures", None) or []
    warnings = getattr(diag, "warnings", None) or []
    if failures:
        n = len(failures)
        return f"FAILED ({n} failure{'s' if n != 1 else ''})"
    if warnings:
        n = len(warnings)
        return f"completed with {n} warning{'s' if n != 1 else ''}"
    return "completed"


def emit_export_size_breakdown_stderr(
    md: str,
    doc: dict[str, Any],
    diag: Any | None = None,
    *,
    max_bytes_cap: int | None = None,
    max_tokens_cap: int | None = None,
    truncated: bool = False,
    pre_truncation_bytes: int | None = None,
    body_before_section7_bytes: int | None = None,
) -> None:
    """Print a single consolidated export summary (size, timing, cache, warnings)."""
    from .export_run_diagnostics import format_elapsed_hms

    total = _utf8_byte_len(md)
    total_tokens = count_tokens(md)
    status = _export_status_label(diag)
    wall = format_elapsed_hms(diag.total_elapsed_s()) if diag is not None else "—"

    print("", file=sys.stderr)
    print(_export_summary_rule(), file=sys.stderr)
    print(
        f"Export {status} · {_format_tokens(total_tokens)} · {_format_utf8_bytes(total)} uploaded · {wall} total",
        file=sys.stderr,
    )

    size_lines: list[str] = [f"uploaded {_format_tokens(total_tokens)} · {_format_utf8_bytes(total)}"]
    if max_tokens_cap:
        pct = (100.0 * total_tokens / max_tokens_cap) if max_tokens_cap else 0.0
        size_lines.append(f"token budget {int(max_tokens_cap):,} (using {pct:.0f}%)")
    if truncated and pre_truncation_bytes is not None:
        cap_note = (
            f"--max-tokens {int(max_tokens_cap):,}"
            if max_tokens_cap
            else f"--max-bytes {_format_utf8_bytes(max_bytes_cap or 0)}"
        )
        size_lines.append(
            f"before {cap_note} cut {_format_utf8_bytes(pre_truncation_bytes)}"
        )
    if body_before_section7_bytes is not None and total > body_before_section7_bytes:
        s7 = total - body_before_section7_bytes
        size_lines.append(f"§7 risk insights {_format_utf8_bytes(s7)} (after cap)")
    print("", file=sys.stderr)
    print("Size", file=sys.stderr)
    for line in size_lines:
        print(f"  {line}", file=sys.stderr)

    sections = _markdown_section_byte_breakdown(md)
    if sections:
        print("", file=sys.stderr)
        print("Markdown sections", file=sys.stderr)
        for label, size in sorted(sections, key=lambda x: (-x[1], x[0])):
            pct = (100.0 * size / total) if total else 0.0
            print(f"  {pct:5.1f}%  {_format_utf8_bytes(size):>10}  {label}", file=sys.stderr)

    components = _doc_payload_component_bytes(doc)
    comp_total = sum(n for _, n in components)
    if components:
        print("", file=sys.stderr)
        print("Payload components", file=sys.stderr)
        for label, size in components:
            pct = (100.0 * size / comp_total) if comp_total else 0.0
            print(f"  {pct:5.1f}%  {_format_utf8_bytes(size):>10}  {label}", file=sys.stderr)
        print(f"  subtotal {_format_utf8_bytes(comp_total)} (JSON only, excl. markdown framing)", file=sys.stderr)

    if diag is not None and getattr(diag, "timings", None):
        timings = list(diag.timings)
        if timings:
            print("", file=sys.stderr)
            print("Timing", file=sys.stderr)
            for label, secs in timings:
                print(f"  {format_elapsed_hms(secs):>8}  {label}", file=sys.stderr)
            measured = sum(secs for _, secs in timings)
            overhead = max(0.0, diag.total_elapsed_s() - measured)
            if overhead >= 0.5:
                print(f"  {format_elapsed_hms(overhead):>8}  unphased overhead", file=sys.stderr)
            print(f"  {format_elapsed_hms(diag.total_elapsed_s()):>8}  total", file=sys.stderr)

    from .drive_cache_stats import drive_cache_breakdown_lines

    portfolio_raw = doc.get("_portfolio_raw")
    sf_comp_summary = None
    if isinstance(portfolio_raw, dict):
        raw_sf = portfolio_raw.get("_llm_export_salesforce_comprehensive")
        if isinstance(raw_sf, dict):
            sf_comp_summary = raw_sf
    cache_lines = drive_cache_breakdown_lines(sf_comprehensive_summary=sf_comp_summary)
    if cache_lines:
        print("", file=sys.stderr)
        print("Cache", file=sys.stderr)
        for line in cache_lines:
            print(f"  {line.strip()}", file=sys.stderr)

    if diag is not None:
        failures = list(getattr(diag, "failures", None) or [])
        warnings = list(getattr(diag, "warnings", None) or [])
        if failures:
            print("", file=sys.stderr)
            print(f"Failures ({len(failures)})", file=sys.stderr)
            for i, msg in enumerate(failures, 1):
                print(f"  {i}. {msg}", file=sys.stderr)
        if warnings:
            print("", file=sys.stderr)
            print(f"Warnings ({len(warnings)})", file=sys.stderr)
            for i, msg in enumerate(warnings, 1):
                print(f"  {i}. {msg}", file=sys.stderr)

    print(_export_summary_rule(), file=sys.stderr)


def render_markdown(doc: dict[str, Any], *, exported_at_utc: str) -> str:
    parts: list[str] = [
        "# Cortex data snapshot (LLM context)",
        "",
        "Use this file as **reference material** only. Prefer citing numbers exactly as shown; "
        "when a field is missing, say it was not in the snapshot.",
        "",
        f"- **Exported (UTC):** {exported_at_utc}",
        f"- **Customer:** {doc.get('customer')}",
        f"- **Report `days`:** {doc.get('lookback_days')}",
        f"- **Underlying `generated` stamp:** {doc.get('generated_report_timestamp')}",
    ]
    ec = doc.get("export_coverage") if isinstance(doc.get("export_coverage"), dict) else {}
    cap_b = ec.get("markdown_soft_cap_tokens")
    cap_bytes = ec.get("markdown_soft_cap_bytes")
    if ec.get("size_caps_enabled") and cap_b is not None and int(cap_b) > 0:
        line = f"- **Token budget (this run):** {int(cap_b):,} tokens (`--max-tokens`, cl100k_base)"
        if cap_bytes is not None and int(cap_bytes) > 0:
            line += f"; byte cap {int(cap_bytes):,} (`--max-bytes`)"
        parts.append(line)
    elif ec.get("size_caps_enabled") and cap_bytes is not None and int(cap_bytes) > 0:
        parts.append(f"- **Markdown soft cap (this run):** {cap_bytes} bytes (`--max-bytes`)")
    elif not ec.get("size_caps_enabled"):
        parts.append("- **Markdown soft cap (this run):** none (`--max-tokens 0 --max-bytes 0`)")
    parts.extend(
        [
        "",
        "## Purpose",
        "",
        doc.get("document_purpose", ""),
        "",
        "## Snapshot coverage & omission rationale",
        "",
        ]
    )
    for ln in _export_coverage_markdown_lines(doc.get("export_coverage") or {}):
        parts.append(ln)
    parts.extend(
        [
            "",
            "## Data Governance",
            "",
        ]
    )
    from .data_governance_warnings import render_data_governance_markdown_lines

    for ln in render_data_governance_markdown_lines(doc.get("data_governance_warnings") or []):
        parts.append(ln)
    parts.extend(
        [
            "",
            "## LeanDNA Data API — data elements (reference)",
            "",
            "Structured list of **catalog paths** (from `config/comprehensive_data_element_list.json` when present), **typical QBR "
            "report paths**, and **HTTP surfaces** used by Cortex. No live LeanDNA values are included here.",
            "",
            _json_compact(doc.get("leandna_data_api_reference") or {}),
            "",
            "## Integration coverage",
            "",
            "Whether Salesforce and CS Report produced usable data for this run (check §3 and §4 for payloads):",
            "",
        ]
    )
    for ln in doc.get("integration_coverage_lines") or []:
        parts.append(ln)
    parts.extend(
        [
            "",
            "## Planned integrations (not in this snapshot yet)",
            "",
            "The following data sources are **not** represented in the numbered sections below; "
            "including them in this export is **planned**:",
            "",
        ]
    )
    planned = doc.get("planned_data_sources") if isinstance(doc.get("planned_data_sources"), dict) else {}
    names = planned.get("not_in_snapshot_yet")
    if isinstance(names, list) and names:
        for name in names:
            parts.append(f"- **{name}**")
    else:
        for name in _PLANNED_DATASOURCES_NOT_IN_EXPORT:
            parts.append(f"- **{name}**")
    parts.extend(
        [
            "",
            "## 1. Pendo (headline metrics only)",
            "",
            _json_compact(doc.get("pendo")),
            "",
            "## 2. Jira (HELP — per customer, top by ARR)",
            "",
            _json_compact(doc.get("jira_help")),
            "",
            "## 3. Salesforce (current book — ACTIVE + OUT_OF_CONTRACT_RENEWING)",
            "",
            *_render_salesforce_current_book_section(doc.get("salesforce")),
            "",
            "## 3b. Salesforce (churned / lost — do not merge with §1–§3 current book)",
            "",
            "> **Segment boundary:** ``commercial_status = CHURNED`` — inactive contracts **without** open "
            "parent-account renewal pipeline. No Pendo or Jira. Do not add ARR to §3 current-book totals or §5.",
            "",
            _json_compact(doc.get("salesforce_churned_segment")),
            "",
            "## 3b-renewal. Salesforce (renewal negotiation — not churn)",
            "",
            "> **Segment boundary:** ``commercial_status = OUT_OF_CONTRACT_RENEWING`` — expired entity contracts "
            "with **open renewal pipeline** on parent accounts. Not churn risk; may still appear in §1 when Pendo matches.",
            "",
            _json_compact(doc.get("salesforce_renewal_negotiation_segment")),
            "",
            "## 3b-future. Salesforce (future contracts — not current book)",
            "",
            "> **Segment boundary:** ``commercial_status = FUTURE`` — won/signed contracts whose start date is "
            "in the future. Not churn and not renewal-in-flight; do not merge with §3 current-book ARR.",
            "",
            _json_compact(doc.get("salesforce_future_contract_segment")),
            "",
            "## 3c. Salesforce comprehensive (per customer + entity accounts)",
            "",
            "Full mainstream-object categories (contacts, opportunities, cases, tasks, events, "
            "contracts, orders, quotes, assets, campaigns, leads, product/pricebook samples) per "
            "portfolio Customer Entity label, plus all Customer Entity account rows and portfolio "
            "expansion KPIs. Row counts per category respect ``row_limit`` on each payload.",
            "",
            _json_compact(doc.get("salesforce_comprehensive_portfolio") or {}),
            "",
            "## 4. CS Report (top customers by ARR — per-customer week)",
            "",
            "Per-customer CS Report for the highest-ARR active Salesforce labels. **§4.1** is a single "
            "markdown table of every customer's section rollups (health mix, shortages, inventory "
            "totals, savings) — the fastest layer for cross-customer questions. **§4.2** is the "
            "factory-level detail as JSON: one ``sites`` row per factory with platform-health, "
            "supply-chain, and platform-value metrics merged inline (no 3× worksheet duplication). "
            "Site-row keys are abbreviated to save tokens — decode them with the ``field_legend`` "
            "(short → long) in §4.2; see ``schema_note`` for how to read rows.",
            "",
            *_render_cs_report_section(doc.get("cs_report")),
            "",
            "## 4b. Slack (top customers by ARR — 6-month conversations + LLM summaries)",
            "",
            "Pilot scope: top ultimate parents by current ARR (default 10). Per customer: Slack "
            "channels matched by name/aliases, up to 180 days of human messages, and a Cortex "
            "``llm_summary`` digest. Raw lines remain under ``conversation_summaries``; timing "
            "is in ``_llm_export_slack.performance`` on the coverage manifest.",
            "",
            _json_compact(doc.get("slack") or {}),
            "",
            "## 5. Pendo usage signals",
            "",
        ]
    )
    for line in doc.get("notable_signals_lines") or []:
        parts.append(f"- {line}")
    parts.append("")
    if doc.get("signals_trend_context"):
        parts.extend(
            [
                "## 6. Signals trend context (truncated)",
                "",
                _json_compact(doc["signals_trend_context"]),
                "",
            ]
        )
    return "\n".join(parts).rstrip() + "\n"


def _shrink_snapshot_params(
    doc: dict[str, Any],
    *,
    csr_site_limit: int,
    csr_string_cap: int,
    sf_accounts: int,
    signals_cap: int | None = None,
    size_caps_enabled: bool = True,
    sf_comp_top_customers: int = 25,
    sf_comp_rows_per_category: int = 5,
) -> None:
    """Mutate ``doc`` in place for smaller serialization.

    ``signals_cap`` is not reduced by tiered shrink (only CSR/SF/§3c tighten under ``--max-bytes``).
    """
    doc["jira_help"] = _compact_jira(
        doc.get("_full_jira") or {}, size_caps_enabled=size_caps_enabled
    )
    csr = doc.get("_full_csr") or {}
    doc["cs_report"] = _compact_csr(
        csr,
        site_limit=csr_site_limit,
        string_cap=csr_string_cap,
        size_caps_enabled=size_caps_enabled,
    )
    doc["salesforce"] = _compact_salesforce(
        doc.get("_full_sf") or {},
        account_cap=sf_accounts,
    )
    pr = doc.get("_portfolio_raw")
    doc["salesforce_comprehensive_portfolio"] = _compact_salesforce_comprehensive_portfolio(
        doc.get("_full_sf_comprehensive") or {},
        report=pr if isinstance(pr, dict) else None,
        top_customers=sf_comp_top_customers,
        rows_per_category=sf_comp_rows_per_category,
    )
    line_mx = 280
    cov0 = doc.get("export_coverage")
    if isinstance(cov0, dict):
        c0 = cov0.get("compaction")
        if isinstance(c0, dict) and c0.get("signals_line_max_chars") is not None:
            line_mx = int(c0["signals_line_max_chars"])
    if isinstance(pr, dict):
        doc["notable_signals_lines"] = _portfolio_signal_lines(
            pr, cap=signals_cap, line_max=line_mx
        )
    if doc.get("signals_trend_context") and csr_string_cap < 280:
        doc.pop("signals_trend_context", None)
    doc["integration_coverage_lines"] = _integration_coverage_lines(
        salesforce=doc.get("_full_sf") or {},
        csr=doc.get("_full_csr") or {},
    )
    cov = doc.get("export_coverage")
    if isinstance(cov, dict):
        comp = cov.setdefault("compaction", {})
        rollup_cap = max(sf_accounts * 6, 72)
        comp.update(
            {
                "csr_site_limit": csr_site_limit,
                "csr_string_cap": csr_string_cap,
                "sf_accounts": sf_accounts,
                "signals_cap": signals_cap,
                "rollup_cap": rollup_cap,
                "sf_comp_top_customers": sf_comp_top_customers,
                "sf_comp_rows_per_category": sf_comp_rows_per_category,
            }
        )
        comp.setdefault("signals_line_max_chars", line_mx)


def _build_export_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Export all-customers LLM data snapshot to Drive Output/ (stable filename).",
        prog=prog or "python -m src.export_llm_context_snapshot",
    )
    ap.add_argument("--days", type=int, default=90, help="Lookback days for portfolio window (default 90)")
    ap.add_argument(
        "--max-tokens",
        type=int,
        default=llm_export_default_max_tokens(),
        help=(
            f"Primary budget: LLM tokens (cl100k_base) for the whole file (default "
            f"{llm_export_default_max_tokens():,} from CORTEX_LLM_EXPORT_MAX_TOKENS). 0 = no token cap. "
            "When N>0, compacts §3c/CSR/SF and may truncate markdown to fit the token budget."
        ),
    )
    ap.add_argument(
        "--max-bytes",
        type=int,
        default=llm_export_default_max_bytes(),
        help=(
            f"Optional secondary UTF-8 byte guard (default {llm_export_default_max_bytes():,} from "
            "CORTEX_LLM_EXPORT_MAX_BYTES; 0 = no byte cap). Applied in addition to --max-tokens; the "
            "tighter of the two governs."
        ),
    )
    ap.add_argument(
        "--signals-cap",
        type=int,
        default=None,
        metavar="N",
        help="Max §5 Pendo usage signal lines from portfolio_signals (default: no cap — all signals).",
    )
    ap.add_argument(
        "--customers-sf-allowlist",
        action="store_true",
        help=(
            "After the merge, keep §1/§5 rows for active (non-churned) Salesforce Customer Entity "
            "labels; Pendo metrics are optional (requires Salesforce JWT env vars)."
        ),
    )
    ap.add_argument(
        "--customers-exclude-sf-churned",
        action="store_true",
        help="Drop headline/signal rows for customers SF matched as inactive churn (contract rollup).",
    )
    ap.add_argument(
        "--exclude-customer",
        action="append",
        default=None,
        metavar="NAME",
        help="Drop this Pendo customer label (repeatable). Also see CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS (+ _FILE env).",
    )
    ap.add_argument(
        "--skip-risk-insights",
        action="store_true",
        help="Omit §7 Account & churn risk insights (LLM + per-customer Jira prefetch).",
    )
    return ap


def export_main(cli_args: list[str] | None = None, *, prog: str | None = None) -> None:
    from .export_run_diagnostics import collect_export_warning, export_diagnostics_scope, export_phase

    args = _build_export_parser(prog=prog).parse_args(cli_args)

    exported_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with export_diagnostics_scope() as diag:
        from src.data_sources import build_llm_export_snapshot_report
        from src.pendo_client import PendoClient

        with export_phase(diag, "portfolio snapshot (Pendo preload + summaries)"):
            pc = PendoClient()
            report = build_llm_export_snapshot_report(pc, days=args.days)
        if report.get("error"):
            print(f"error: report failed: {report.get('error')}", file=sys.stderr)
            sys.exit(1)

        from src.llm_export_customer_filter import (
            LlmExportCustomerFilterConfig,
            apply_llm_export_customer_filters,
        )

        with export_phase(diag, "customer filters"):
            fcfg = LlmExportCustomerFilterConfig.from_cli_and_env(
                customers_sf_allowlist=bool(args.customers_sf_allowlist),
                customers_exclude_sf_churned=bool(args.customers_exclude_sf_churned),
                exclude_customer=list(args.exclude_customer) if args.exclude_customer else [],
            )
            if fcfg.any_enabled():
                try:
                    filt = apply_llm_export_customer_filters(report, fcfg)
                    wrs = filt.get("warnings") or []
                    for w in wrs:
                        if isinstance(w, str) and w.strip():
                            collect_export_warning(
                                f"customer filter: {w.strip()}",
                                llm_export=True,
                            )
                except Exception as exc:
                    print(f"error: customer filter failed: {exc}", file=sys.stderr)
                    sys.exit(1)

        _emit_integration_stderr_warnings(report)

        token_cap = int(args.max_tokens)
        byte_cap = int(args.max_bytes)
        size_caps_enabled = token_cap > 0 or byte_cap > 0
        csr_lim = 15 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        csr_str = 400 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        sf_acct = 24 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        pre_truncation_bytes = None
        markdown_truncated = False
        md_body_before_section7_bytes: int | None = None
        with export_phase(diag, "markdown build"):
            doc = build_snapshot_document(
                report,
                markdown_soft_cap_bytes=byte_cap,
                markdown_soft_cap_tokens=token_cap,
                csr_site_limit=csr_lim,
                csr_string_cap=csr_str,
                sf_accounts=sf_acct,
                signals_cap=args.signals_cap,
                size_caps_enabled=size_caps_enabled,
                export_diag=diag,
            )
            # Keep refs for iterative shrinking
            doc["_full_jira"] = report.get("jira") or {}
            doc["_full_csr"] = report.get("csr") or {}
            doc["_full_sf"] = report.get("salesforce") or {}
            doc["_full_sf_comprehensive"] = report.get("salesforce_comprehensive_portfolio") or {}
            doc["_portfolio_raw"] = report

            md = render_markdown(doc, exported_at_utc=exported_at)
            max_tok = max(_LLM_EXPORT_MIN_TOKEN_CAP, token_cap) if token_cap > 0 else 0
            max_b = max(_LLM_EXPORT_MIN_BYTE_CAP, byte_cap) if byte_cap > 0 else 0

            if size_caps_enabled:
                tiers = [
                    (10, 320, 16, 20, 4),
                    (8, 260, 12, 15, 3),
                    (6, 220, 8, 10, 2),
                    (4, 180, 4, 6, 1),
                ]
                while _over_size_caps(md, max_bytes=max_b, max_tokens=max_tok) and tiers:
                    csr_lim, csr_str, sf_acct, sf_top, sf_rows = tiers.pop(0)
                    _shrink_snapshot_params(
                        doc,
                        csr_site_limit=csr_lim,
                        csr_string_cap=csr_str,
                        sf_accounts=sf_acct,
                        signals_cap=args.signals_cap,
                        size_caps_enabled=True,
                        sf_comp_top_customers=sf_top,
                        sf_comp_rows_per_category=sf_rows,
                    )
                    md = render_markdown(
                        {k: v for k, v in doc.items() if not str(k).startswith("_")},
                        exported_at_utc=exported_at,
                    )

                if max_tok and count_tokens(md) > max_tok:
                    pre_truncation_bytes = _utf8_byte_len(md)
                    markdown_truncated = True
                    collect_export_warning(
                        f"markdown truncated to --max-tokens ({max_tok:,}); raise limit if needed",
                        llm_export=True,
                    )
                    md = _truncate_to_tokens(md, max_tok).rstrip() + (
                        "\n\n<!-- Document truncated to --max-tokens; re-run with a higher limit "
                        "or narrow integrations if needed. -->\n"
                    )

                raw = md.encode("utf-8")
                if max_b and len(raw) > max_b:
                    if pre_truncation_bytes is None:
                        pre_truncation_bytes = len(raw)
                    markdown_truncated = True
                    collect_export_warning(
                        f"markdown truncated to --max-bytes ({max_b}); raise limit if needed",
                        llm_export=True,
                    )
                    md = raw[:max_b].decode("utf-8", errors="ignore").rstrip() + (
                        "\n\n<!-- Document truncated to --max-bytes; re-run with a higher limit "
                        "or narrow integrations if needed. -->\n"
                    )
            md_body_before_section7_bytes = _utf8_byte_len(md)

        if args.skip_risk_insights:
            import logging

            logging.getLogger("cortex").info(
                "LLM export: skipping §7 risk insights (--skip-risk-insights)"
            )
        else:
            from src.export_llm_risk_insights import render_risk_insights_section

            with export_phase(diag, "risk insights (LLM §7)"):
                try:
                    md = md.rstrip() + "\n" + render_risk_insights_section(
                        report,
                        jira_days=min(int(args.days), 365),
                    )
                except Exception as exc:
                    collect_export_warning(
                        f"risk insights section failed: {exc}",
                        llm_export=True,
                    )
                    md = (
                        md.rstrip()
                        + "\n\n## 7. Account & churn risk insights (LLM)\n\n### Error\n\n"
                        f"Section generation raised an unexpected error: {exc}\n\n"
                        + "*Export body above is unchanged; core snapshot completed.*\n"
                    )

        for k in list(doc.keys()):
            if str(k).startswith("_"):
                doc.pop(k, None)

        nbytes = len(md.encode("utf-8"))

        from .export_drive_layout import ensure_portfolio_output_folders, upload_text_persistent_and_historical

        folders = ensure_portfolio_output_folders()
        with export_phase(diag, "Drive upload"):
            urls = upload_text_persistent_and_historical(
                stem="LLM-Context-Portfolio",
                content=md,
                ext=".md",
                persistent_folder_id=folders["persistent_folder_id"],
                historical_folder_id=folders["historical_folder_id"],
                base_label=folders["base_label"],
                mime_type="text/markdown",
            )

        print(
            f"Uploaded {_format_utf8_bytes(nbytes)} → Output/{urls['persistent_filename']} "
            f"and Historical Data/{urls['historical_day_folder']}/{urls['historical_filename']}",
            file=sys.stderr,
        )
        print(f"Output/ (persistent): https://drive.google.com/file/d/{urls['persistent_file_id']}/view")
        print(
            f"Historical Data/{urls['historical_day_folder']}/: "
            f"https://drive.google.com/file/d/{urls['historical_file_id']}/view"
        )

        emit_export_size_breakdown_stderr(
            md,
            doc,
            diag,
            max_bytes_cap=max_b,
            max_tokens_cap=max_tok,
            truncated=markdown_truncated,
            pre_truncation_bytes=pre_truncation_bytes,
            body_before_section7_bytes=md_body_before_section7_bytes,
        )
        from src.data_source_health import integration_freshness_metadata

        diag.set_integration_meta(integration_freshness_metadata())
        # Integration join warnings (SF↔Pendo, Jira HELP fallbacks, etc.) belong in the
        # export markdown and stderr recap — not as a failed exit code.
        summary = diag.emit_run_summary(job_name="export", fail_on_warnings=False)
        if diag.failures:
            sys.exit(1)


def main() -> None:
    export_main(None)


if __name__ == "__main__":
    main()
