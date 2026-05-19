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
  decks --export [--days N] [--skip-risk-insights] [--customers-sf-allowlist] [--customers-exclude-sf-churned] [--exclude-customer NAME ...]
  python -m src.export_llm_context_snapshot --days 90

Optional portfolio row filters (after Pendo+Salesforce bundle, before markdown):

- ``--customers-sf-allowlist`` — keep headline customers/signals that match an **active** (non-churned)
  Salesforce Customer Entity label; Pendo metrics are included when present but not required.
  Requires Salesforce JWT env vars.
- ``--customers-exclude-sf-churned`` — drop rows that **matched** Salesforce rollups with ``active``
  false (contract-status churn rollup).
- ``--exclude-customer`` — repeat to drop explicit Pendo customer labels (case-insensitive), or see
  env ``BPO_LLM_EXPORT_EXCLUDE_CUSTOMERS`` / ``BPO_LLM_EXPORT_EXCLUDE_CUSTOMERS_FILE``.

Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (and optional ``GOOGLE_QBR_OUTPUT_PARENT_ID``) plus
Drive credentials. Each run uploads ``LLM-Context-All_Customers.md`` to **both**:

1. ``<generator>/Output/`` (stable path for bookmarks)
2. ``<generator>/Output/{ISO-date} - Output/`` (same-day bundle folder; filename replaced if present)

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

# ``--max-bytes`` / compaction caps: 0 means no limit (full payloads in export).
_LLM_EXPORT_NO_CAP = 0


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
    if csr.get("scope") == "top_customers_by_arr":
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
                f"- **CS Report:** **Loaded** — per-customer week slices for top {n} label(s) by ARR "
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
    churn_seg_cov = report.get("salesforce_churned_segment")
    if isinstance(churn_seg_cov, dict):
        out_cov["salesforce_churned_segment"] = {
            "customer_count": churn_seg_cov.get("customer_count"),
            "do_not_merge_with_active_book": churn_seg_cov.get("do_not_merge_with_active_book"),
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
                f"- **Active Customer Entity labels in Salesforce:** **{sf_uni.get('salesforce_active_entities', 0)}**",
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
                f"- **Active SF labels with no Pendo prefix match (still in §1/§3):** {preview}"
            )
        lines.append("")
    sf_churn_cov = cov.get("salesforce_churned")
    if isinstance(sf_churn_cov, dict) and sf_churn_cov.get("salesforce_configured"):
        seg = cov.get("salesforce_churned_segment")
        n_churn = seg.get("customer_count") if isinstance(seg, dict) else sf_churn_cov.get("salesforce_churned_entities")
        lines.extend(
            [
                "### Salesforce churned segment (§3b — separate from active book)",
                f"- **Churned Customer Entity count:** **{n_churn or 0}**",
                "- **Do not merge** §3b with §1/§3/§5. Churned SF accounts are **Salesforce-only** here (no Pendo or Jira).",
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
            "- **§3 — Salesforce (active installed base):** **Revenue and renewal-oriented facts** for **active "
            "(non-churned) Customer Entity** labels. Pendo usage in §1 is merged when a prefix matches, but **Pendo is "
            "not required** for a customer to appear (see ``salesforce_only`` rows in §1). **Do not combine** with §3b.",
            "- **§3b — Salesforce (churned):** **Salesforce-only** inactive / churned contract rollups — useful for "
            "retention post-mortems and historical ARR. **No Pendo**, **no Jira/Atlassian**, and **no §5** "
            "signals for these accounts (they are removed from the active segment when SF marks them churned).",
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
        lines.append(
            f"- **Target size (`--max-bytes`):** about **{cov.get('markdown_soft_cap_bytes', '')}** bytes of UTF-8 for "
            "the whole markdown file. If the export is still too large, it first **tightens CS Report and "
            "Salesforce** (fewer sites, shorter text, fewer accounts). If it is **still** too large, the **end of the file "
            "may be cut off** — raise `--max-bytes` if you need every section intact (especially with a long §5)."
        )
    else:
        lines.append(
            "- **Size caps:** **disabled** for this run (`--max-bytes 0`, default). Full CS Report site rows, "
            "Salesforce rollups, Pendo headlines, and §3c comprehensive payloads are included without markdown "
            "truncation or tiered compaction. Pass `--max-bytes N` with **N > 0** to re-enable limits."
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
    "active_in_salesforce",
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
                "Churned Customer Entity rollups (inactive contract status). "
                "Do not combine with §3 active installed-base totals or §5 Pendo signals."
            )
        else:
            out["salesforce_export_note"] = (
                "Active installed-base rollups: ARR sum, distinct Contract_Status__c, "
                "nearest/farthest Contract_Contract_End_Date__c among non-churned rows "
                "(fallback: all rows if every matched row is churned). "
                "Churned customers are in §3b only."
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
    if sf.get("resolution") == "portfolio_aggregate":
        for k in (
            "total_arr",
            "active_installed_base_arr",
            "churned_contract_arr",
            "pendo_customers",
            "salesforce_matched_customers",
            "salesforce_unmatched_customers",
            "active_customer_count",
            "churned_customer_count",
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
    from src.hydrate_data_summary import truncate_strings_in_obj

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


def _compact_csr_section_block(
    block: dict[str, Any], *, site_limit: int, string_cap: int, size_caps_enabled: bool = True
) -> dict[str, Any]:
    from src.hydrate_data_summary import truncate_strings_in_obj

    if block.get("error"):
        return {"error": block.get("error")}
    pruned = dict(block)
    sites = pruned.get("sites")
    if isinstance(sites, list):
        if size_caps_enabled and _export_cap_active(site_limit):
            pruned["sites"] = sites[:site_limit]
        else:
            pruned["sites"] = list(sites)
        pruned["sites_total"] = len(sites)
    if size_caps_enabled and _export_cap_active(string_cap):
        return truncate_strings_in_obj(
            pruned, max_str=string_cap, max_list_items=48, max_dict_keys=96
        )
    return truncate_strings_in_obj(
        pruned, max_str=50_000, max_list_items=100_000, max_dict_keys=10_000
    )


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
    if csr.get("scope") == "top_customers_by_arr":
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
                    for k in ("salesforce_label", "arr", "pendo_customer_key", "csr_lookup_name")
                    if k in block
                }
                for key in ("platform_health", "supply_chain", "platform_value"):
                    sec = block.get(key)
                    if isinstance(sec, dict):
                        slim[key] = _compact_csr_section_block(
                            sec,
                            site_limit=site_limit,
                            string_cap=string_cap,
                            size_caps_enabled=size_caps_enabled,
                        )
                out["customers"][label] = slim
        return out
    for key in ("platform_health", "supply_chain", "platform_value"):
        block = csr.get(key)
        if isinstance(block, dict):
            out[key] = _compact_csr_section_block(
                block,
                site_limit=site_limit,
                string_cap=string_cap,
                size_caps_enabled=size_caps_enabled,
            )
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

    doc: dict[str, Any] = {
        "document_purpose": (
            "Structured facts from BPO integrations for LLM Q&A. Figures are snapshots from vendor APIs "
            "and internal exports; verify in source systems before contractual or financial use. "
            "Active installed-base customers (§1, §3, §5) are separate from churned Salesforce-only facts (§3b)."
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
        "salesforce_comprehensive_portfolio": (
            report.get("salesforce_comprehensive_portfolio")
            if isinstance(report.get("salesforce_comprehensive_portfolio"), dict)
            else {}
        ),
        "cs_report": _compact_csr(
            csr,
            site_limit=csr_site_limit,
            string_cap=csr_string_cap,
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
        from src.hydrate_data_summary import truncate_strings_in_obj

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
    add("salesforce_comprehensive_portfolio", doc.get("salesforce_comprehensive_portfolio"))
    add("cs_report", doc.get("cs_report"))
    add("notable_signals_lines", doc.get("notable_signals_lines"))
    add("signals_trend_context", doc.get("signals_trend_context"))
    add("leandna_data_api_reference", doc.get("leandna_data_api_reference"))
    add("export_coverage", doc.get("export_coverage"))
    add("data_governance_warnings", doc.get("data_governance_warnings"))
    add("planned_data_sources", doc.get("planned_data_sources"))
    add("integration_coverage_lines", doc.get("integration_coverage_lines"))
    return sorted(rows, key=lambda x: (-x[1], x[0]))


def emit_export_size_breakdown_stderr(
    md: str,
    doc: dict[str, Any],
    *,
    max_bytes_cap: int | None = None,
    truncated: bool = False,
    pre_truncation_bytes: int | None = None,
    body_before_section7_bytes: int | None = None,
) -> None:
    """Print UTF-8 size totals and per-section / per-component contribution (stderr)."""
    total = _utf8_byte_len(md)
    print("", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print("Export size breakdown (UTF-8):", file=sys.stderr)
    print(f"  total uploaded: {_format_utf8_bytes(total)}", file=sys.stderr)
    if truncated and pre_truncation_bytes is not None:
        print(
            f"  body before --max-bytes cut: {_format_utf8_bytes(pre_truncation_bytes)} "
            f"(cap {_format_utf8_bytes(max_bytes_cap or 0)})",
            file=sys.stderr,
        )
    if body_before_section7_bytes is not None and total > body_before_section7_bytes:
        s7 = total - body_before_section7_bytes
        print(
            f"  §7 risk insights (appended after cap): {_format_utf8_bytes(s7)}",
            file=sys.stderr,
        )

    sections = _markdown_section_byte_breakdown(md)
    if sections:
        print("  --- markdown sections (uploaded file) ---", file=sys.stderr)
        for label, size in sorted(sections, key=lambda x: (-x[1], x[0])):
            pct = (100.0 * size / total) if total else 0.0
            print(
                f"    {pct:5.1f}%  {_format_utf8_bytes(size):>18}  {label}",
                file=sys.stderr,
            )

    components = _doc_payload_component_bytes(doc)
    comp_total = sum(n for _, n in components)
    if components:
        print("  --- document payloads (compact JSON in snapshot) ---", file=sys.stderr)
        for label, size in components:
            pct = (100.0 * size / comp_total) if comp_total else 0.0
            print(
                f"    {pct:5.1f}%  {_format_utf8_bytes(size):>18}  {label}",
                file=sys.stderr,
            )
        print(
            f"  payload subtotal (excludes markdown framing): {_format_utf8_bytes(comp_total)}",
            file=sys.stderr,
        )
    print("=" * 60, file=sys.stderr)


def render_markdown(doc: dict[str, Any], *, exported_at_utc: str) -> str:
    parts: list[str] = [
        "# BPO data snapshot (LLM context)",
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
    cap_b = ec.get("markdown_soft_cap_bytes")
    if ec.get("size_caps_enabled") and cap_b is not None and int(cap_b) > 0:
        parts.append(f"- **Markdown soft cap (this run):** {cap_b} bytes (`--max-bytes`)")
    elif not ec.get("size_caps_enabled"):
        parts.append("- **Markdown soft cap (this run):** none (`--max-bytes 0`, default)")
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
            "report paths**, and **HTTP surfaces** used by BPO. No live LeanDNA values are included here.",
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
            "## 2. Jira (HELP — scoped per deck rules)",
            "",
            _json_compact(doc.get("jira_help")),
            "",
            "## 3. Salesforce (active installed base)",
            "",
            _json_compact(doc.get("salesforce")),
            "",
            "## 3b. Salesforce (churned — do not merge with §1–§3 active book)",
            "",
            "> **Segment boundary:** Salesforce churn / inactive contract customers only. "
            "No Pendo or Jira/Atlassian data for these accounts. Do not add these ARR figures to §3 totals, "
            "pipeline ARR, or §5.",
            "",
            _json_compact(doc.get("salesforce_churned_segment")),
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
            "Per-customer ``platform_health``, ``supply_chain``, and ``platform_value`` for the "
            "highest-ARR active Salesforce labels (not an all-customers site merge).",
            "",
            _json_compact(doc.get("cs_report")),
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
) -> None:
    """Mutate ``doc`` in place for smaller serialization.

    ``signals_cap`` is not reduced by tiered shrink (only CSR/SF tighten under ``--max-bytes``).
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
    doc["salesforce_comprehensive_portfolio"] = doc.get("_full_sf_comprehensive") or {}
    pr = doc.get("_portfolio_raw")
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
        "--max-bytes",
        type=int,
        default=_LLM_EXPORT_NO_CAP,
        help=(
            "Soft cap on UTF-8 body size; 0 = no cap (default). When N>0, trims CSR/SF and may truncate markdown."
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
        help="Drop this Pendo customer label (repeatable). Also see BPO_LLM_EXPORT_EXCLUDE_CUSTOMERS (+ _FILE env).",
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

        size_caps_enabled = int(args.max_bytes) > 0
        csr_lim = 15 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        csr_str = 400 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        sf_acct = 24 if size_caps_enabled else _LLM_EXPORT_NO_CAP
        pre_truncation_bytes = None
        markdown_truncated = False
        md_body_before_section7_bytes: int | None = None
        with export_phase(diag, "markdown build"):
            doc = build_snapshot_document(
                report,
                markdown_soft_cap_bytes=int(args.max_bytes),
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
            max_b = max(20_000, int(args.max_bytes)) if size_caps_enabled else 0

            if size_caps_enabled:
                tiers = [
                    (10, 320, 16),
                    (8, 260, 12),
                    (6, 220, 8),
                    (4, 180, 4),
                ]
                while len(md.encode("utf-8")) > max_b and tiers:
                    csr_lim, csr_str, sf_acct = tiers.pop(0)
                    _shrink_snapshot_params(
                        doc,
                        csr_site_limit=csr_lim,
                        csr_string_cap=csr_str,
                        sf_accounts=sf_acct,
                        signals_cap=args.signals_cap,
                        size_caps_enabled=True,
                    )
                    md = render_markdown(
                        {k: v for k, v in doc.items() if not str(k).startswith("_")},
                        exported_at_utc=exported_at,
                    )

                raw = md.encode("utf-8")
                if len(raw) > max_b:
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

            logging.getLogger("bpo").info(
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

        fname = "LLM-Context-All_Customers.md"
        nbytes = len(md.encode("utf-8"))

        from src.drive_config import (
            get_qbr_output_folder_id,
            get_qbr_output_root_folder_id,
            upload_text_file_to_drive_folder,
        )

        root_id = get_qbr_output_root_folder_id()
        dated_id = get_qbr_output_folder_id()
        if not root_id or not dated_id:
            print(
                "error: could not resolve Drive Output folders (set GOOGLE_QBR_GENERATOR_FOLDER_ID "
                "and verify Drive access).",
                file=sys.stderr,
            )
            sys.exit(1)

        dated_label = f"{dt.date.today().isoformat()} - Output"
        with export_phase(diag, "Drive upload"):
            fid_root = upload_text_file_to_drive_folder(fname, md, root_id, mime_type="text/markdown")
            fid_dated = upload_text_file_to_drive_folder(fname, md, dated_id, mime_type="text/markdown")

        print(f"Exported {nbytes} bytes → Drive Output/{fname} (id={fid_root})", file=sys.stderr)
        print(f"Exported {nbytes} bytes → Drive Output/{dated_label}/{fname} (id={fid_dated})", file=sys.stderr)
        print(f"Output/ (stable): https://drive.google.com/file/d/{fid_root}/view")
        print(f"Output/{dated_label}/: https://drive.google.com/file/d/{fid_dated}/view")

        emit_export_size_breakdown_stderr(
            md,
            doc,
            max_bytes_cap=max_b,
            truncated=markdown_truncated,
            pre_truncation_bytes=pre_truncation_bytes,
            body_before_section7_bytes=md_body_before_section7_bytes,
        )
        diag.emit_stderr_summary()


def main() -> None:
    export_main(None)


if __name__ == "__main__":
    main()
