#!/usr/bin/env python3
"""Export an all-customers LLM-oriented data snapshot to Google Drive (QBR Generator).

Builds :meth:`PendoClient.get_portfolio_report` plus aggregated CS Report (week), portfolio
Salesforce revenue book, and Jira HELP (unscoped). The portfolio fetch does not read or sync
QBR slide YAML (cohort findings use built-in defaults).

**Pendo** detail payloads are stripped (sites, pages, features, …); **Jira** includes counts,
breakdowns, and SLA-style aggregates only — **no issue keys, summaries, or ticket rows.**

The markdown names integrations planned for future export coverage (e.g. Aha, GitHub) under
**Planned integrations (not in this snapshot yet)**.

Usage:
  python scripts/export_llm_context_snapshot.py --days 90
  python scripts/export_llm_context_snapshot.py --out ./snapshot.md --skip-drive

Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` and credentials for Drive upload unless ``--skip-drive``.
Drive upload **replaces** an existing file with the same name in the target folder by default.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Product roadmap: named here so the export explicitly sets reader expectations.
_PLANNED_DATASOURCES_NOT_IN_EXPORT: tuple[str, ...] = ("Aha", "GitHub")

_SF_NOT_CONFIGURED_MSG = (
    "Salesforce not configured: set SF_LOGIN_URL, SF_CONSUMER_KEY, SF_USERNAME, "
    "and SF_PRIVATE_KEY or SF_PRIVATE_KEY_PATH (JWT integration)."
)


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
            "- **Salesforce:** **Loaded** — portfolio revenue book (ARR, pipeline, opportunities)."
        )
    else:
        lines.append("- **Salesforce:** **Loaded**.")

    if not csr:
        lines.append("- **CS Report:** **Not loaded** — no `csr` block on the merged report.")
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


def _emit_integration_stderr_warnings(report: dict[str, Any]) -> None:
    """Fail loud on stderr when SF or CSR did not produce usable data."""
    sf = report.get("salesforce") if isinstance(report.get("salesforce"), dict) else {}
    if not sf or sf.get("error"):
        msg = sf.get("error") if sf else "Salesforce payload missing"
        print(f"warning: LLM export — Salesforce: {msg}", file=sys.stderr)

    csr = report.get("csr") if isinstance(report.get("csr"), dict) else {}
    errs: list[str] = []
    for key in ("platform_health", "supply_chain", "platform_value"):
        b = csr.get(key)
        if isinstance(b, dict) and b.get("error"):
            errs.append(f"{key}: {b['error']}")
    if len(errs) == 3:
        print(
            "warning: LLM export — CS Report: all sections failed — " + " | ".join(errs),
            file=sys.stderr,
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


def _compact_jira(j: dict[str, Any]) -> dict[str, Any]:
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
        # Small pies only — cap categories
        bto = tick.get("by_type_open")
        if isinstance(bto, dict):
            out["customer_ticket_metrics"]["by_type_open"] = dict(list(bto.items())[:12])
        bso = tick.get("by_status_open")
        if isinstance(bso, dict):
            out["customer_ticket_metrics"]["by_status_open"] = dict(list(bso.items())[:12])
    out["engineering"] = _compact_eng_enh_counts_only(j.get("engineering"))
    out["enhancements"] = _compact_eng_enh_counts_only(j.get("enhancements"))
    tow = j.get("tickets_over_time")
    if isinstance(tow, list):
        out["tickets_over_time"] = tow[-24:] if len(tow) > 24 else tow
    return out


def _compact_salesforce(sf: dict[str, Any], *, account_cap: int) -> dict[str, Any]:
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
    accts = sf.get("accounts")
    if isinstance(accts, list):
        slim = []
        for a in accts[:account_cap]:
            if not isinstance(a, dict):
                continue
            slim.append({k: a.get(k) for k in ("Id", "Name", "ARR__c", "Type") if k in a})
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
        ):
            if k in sf:
                out[k] = sf[k]
    return out


def _pendo_portfolio_topline(portfolio: dict[str, Any], *, max_customer_rows: int = 200) -> dict[str, Any]:
    """Portfolio rollup + capped per-customer headline rows (no Pendo detail payloads)."""
    from src.hydrate_data_summary import truncate_strings_in_obj

    raw_customers = portfolio.get("customers") if isinstance(portfolio.get("customers"), list) else []
    rows: list[dict[str, Any]] = []
    for row in raw_customers[:max_customer_rows]:
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
        "Portfolio rollup: per-customer rows are headline engagement counts only "
        "(no sites/pages/features/tickets)."
    ]
    raw_n = len(raw_customers)
    if raw_n > max_customer_rows:
        note_parts.append(f"customers_headline truncated to {max_customer_rows} of {raw_n}.")
    return {
        "scope": "portfolio_all_customers",
        "note": " ".join(note_parts),
        "customer_count": portfolio.get("customer_count"),
        "days": portfolio.get("days"),
        "generated": portfolio.get("generated"),
        "customers_headline": rows,
        "portfolio_signals_top": truncate_strings_in_obj(
            (portfolio.get("portfolio_signals") or [])[:28],
            max_str=240,
            max_list_items=28,
            max_dict_keys=16,
        ),
        "portfolio_trends": truncate_strings_in_obj(
            portfolio.get("portfolio_trends") or {},
            max_str=400,
            max_list_items=40,
            max_dict_keys=48,
        ),
        "portfolio_leaders": truncate_strings_in_obj(
            portfolio.get("portfolio_leaders") or {},
            max_str=400,
            max_list_items=40,
            max_dict_keys=48,
        ),
        "cohort_findings_bullets": list((portfolio.get("cohort_findings_bullets") or [])[:24]),
        "cohort_digest": truncate_strings_in_obj(
            portfolio.get("cohort_digest") or {},
            max_str=400,
            max_list_items=36,
            max_dict_keys=48,
        ),
    }


def _compact_csr(csr: dict[str, Any], *, site_limit: int, string_cap: int) -> dict[str, Any]:
    from src.hydrate_data_summary import truncate_strings_in_obj

    if not csr:
        return {
            "note": "CS Report was not attached (empty csr). Check Drive CS Report export and openpyxl.",
        }
    out: dict[str, Any] = {}
    if isinstance(csr.get("note"), str):
        out["note"] = csr["note"]
    for key in ("platform_health", "supply_chain", "platform_value"):
        block = csr.get(key)
        if isinstance(block, dict) and block.get("error"):
            out[key] = {"error": block.get("error")}
        elif isinstance(block, dict):
            pruned = dict(block)
            sites = pruned.get("sites")
            if isinstance(sites, list):
                pruned["sites"] = sites[:site_limit]
                pruned["sites_total"] = len(sites)
            out[key] = truncate_strings_in_obj(
                pruned, max_str=string_cap, max_list_items=48, max_dict_keys=96
            )
    return out


def _portfolio_signal_lines(portfolio: dict[str, Any], *, cap: int, line_max: int) -> list[str]:
    items = portfolio.get("portfolio_signals") if isinstance(portfolio.get("portfolio_signals"), list) else []
    out: list[str] = []
    for item in items[:cap]:
        if isinstance(item, dict):
            cust = str(item.get("customer") or "").strip()
            sig = str(item.get("signal") or "").strip()
            line = f"{cust}: {sig}" if cust else sig
        else:
            line = str(item)
        line = " ".join(line.split())
        if len(line) > line_max:
            line = line[: line_max - 1] + "…"
        out.append(line)
    return out


def build_snapshot_document(
    report: dict[str, Any],
    *,
    csr_site_limit: int = 15,
    csr_string_cap: int = 400,
    sf_accounts: int = 6,
    signals_cap: int = 22,
    signal_line_max: int = 280,
) -> dict[str, Any]:
    csr = report.get("csr") if isinstance(report.get("csr"), dict) else {}
    pendo_sec = _pendo_portfolio_topline(report)
    sig_lines = _portfolio_signal_lines(report, cap=signals_cap, line_max=signal_line_max)
    doc: dict[str, Any] = {
        "document_purpose": (
            "Structured facts from BPO integrations for LLM Q&A. Figures are snapshots from vendor APIs "
            "and internal exports; verify in source systems before contractual or financial use."
        ),
        "customer": report.get("customer"),
        "generated_report_timestamp": report.get("generated"),
        "lookback_days": report.get("days"),
        "pendo": pendo_sec,
        "jira_help": _compact_jira(report.get("jira") or {}),
        "salesforce": _compact_salesforce(report.get("salesforce") or {}, account_cap=sf_accounts),
        "cs_report": _compact_csr(csr, site_limit=csr_site_limit, string_cap=csr_string_cap),
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
    }
    stc = report.get("signals_trend_context")
    if stc:
        from src.hydrate_data_summary import truncate_strings_in_obj

        doc["signals_trend_context"] = truncate_strings_in_obj(
            stc, max_str=320, max_list_items=24, max_dict_keys=48
        )
    return doc


def _json_compact(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)


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
        "",
        "## Purpose",
        "",
        doc.get("document_purpose", ""),
        "",
        "## Integration coverage",
        "",
        "Whether Salesforce and CS Report produced usable data for this run (check §3 and §4 for payloads):",
        "",
    ]
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
            "## 3. Salesforce",
            "",
            _json_compact(doc.get("salesforce")),
            "",
            "## 4. CS Report (Data Exports Drive)",
            "",
            _json_compact(doc.get("cs_report")),
            "",
            "## 5. Notable signals (heuristic lines)",
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
    signals_cap: int,
) -> None:
    """Mutate ``doc`` in place for smaller serialization."""
    doc["jira_help"] = _compact_jira(doc.get("_full_jira") or {})
    csr = doc.get("_full_csr") or {}
    doc["cs_report"] = _compact_csr(
        csr,
        site_limit=csr_site_limit,
        string_cap=csr_string_cap,
    )
    doc["salesforce"] = _compact_salesforce(
        doc.get("_full_sf") or {},
        account_cap=sf_accounts,
    )
    pr = doc.get("_portfolio_raw")
    if isinstance(pr, dict):
        doc["notable_signals_lines"] = _portfolio_signal_lines(
            pr, cap=signals_cap, line_max=240
        )
    if doc.get("signals_trend_context") and csr_string_cap < 280:
        doc.pop("signals_trend_context", None)
    doc["integration_coverage_lines"] = _integration_coverage_lines(
        salesforce=doc.get("_full_sf") or {},
        csr=doc.get("_full_csr") or {},
    )


def _salesforce_for_all_customers_report(report: dict[str, Any]) -> dict[str, Any]:
    """Attach ``portfolio_revenue_book`` via :func:`enrich_portfolio_report_with_revenue_book` and map to ``salesforce`` shape."""
    from src.data_source_health import _salesforce_configured
    from src.deck_variants import enrich_portfolio_report_with_revenue_book

    if not _salesforce_configured():
        return {
            "error": _SF_NOT_CONFIGURED_MSG,
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }
    enrich_portfolio_report_with_revenue_book(report)
    prb = report.get("portfolio_revenue_book") or {}
    if prb.get("error"):
        return {
            "error": str(prb.get("error")),
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }
    if prb.get("configured") is False:
        return {
            "error": _SF_NOT_CONFIGURED_MSG,
            "matched": False,
            "resolution": "none",
            "source": "salesforce",
        }

    matched_n = int(prb.get("salesforce_matched_customers") or 0)
    accounts: list[dict[str, Any]] = []
    for row in prb.get("top_customers_by_arr") or []:
        cust = row.get("customer")
        if not cust:
            continue
        accounts.append({"Name": cust, "ARR__c": row.get("arr"), "Type": "Customer Entity"})

    return {
        "customer": "All Customers",
        "matched": matched_n > 0,
        "resolution": "portfolio_aggregate",
        "primary_account_id": None,
        "accounts": accounts,
        "account_ids": [],
        "pipeline_arr": float(prb.get("pipeline_arr") or 0),
        "opportunity_count_this_year": int(prb.get("opportunity_count_this_year") or 0),
        "total_arr": prb.get("total_arr"),
        "active_installed_base_arr": prb.get("active_installed_base_arr"),
        "churned_contract_arr": prb.get("churned_contract_arr"),
        "pendo_customers": prb.get("pendo_customers"),
        "salesforce_matched_customers": matched_n,
        "salesforce_unmatched_customers": prb.get("salesforce_unmatched_customers"),
        "active_customer_count": prb.get("active_customer_count"),
        "churned_customer_count": prb.get("churned_customer_count"),
    }


def _build_snapshot_report(pc: Any, *, days: int) -> dict[str, Any]:
    """Pendo portfolio rollup + CS Report (week) + portfolio Salesforce + Jira HELP."""
    portfolio = pc.get_portfolio_report(days=days, cohort_rollup_from_slide_yaml=False)
    if not isinstance(portfolio, dict):
        return {"error": "portfolio report returned non-dict"}
    if portfolio.get("error"):
        return dict(portfolio)
    report = dict(portfolio)
    report["customer"] = "All Customers"
    try:
        from src.cs_report_client import load_csr_all_customers_week

        report["csr"] = load_csr_all_customers_week()
    except Exception as e:
        err = {"error": str(e), "source": "cs_report"}
        report["csr"] = {
            "platform_health": dict(err),
            "supply_chain": dict(err),
            "platform_value": dict(err),
        }
    report["salesforce"] = _salesforce_for_all_customers_report(report)
    report["signals"] = []
    try:
        from src.jira_client import get_shared_jira_client

        report["jira"] = get_shared_jira_client().get_customer_jira(None, days=min(int(days), 365))
    except Exception as e:
        report["jira"] = {"error": str(e)}
    return report


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export all-customers LLM data snapshot to QBR Generator Drive folder."
    )
    ap.add_argument("--days", type=int, default=90, help="Lookback days for portfolio window (default 90)")
    ap.add_argument(
        "--max-bytes",
        type=int,
        default=140_000,
        help="Soft cap on UTF-8 body size; trims CSR/Jira samples if exceeded (default 140000)",
    )
    ap.add_argument("--out", "-o", metavar="FILE", help="Also write markdown locally")
    ap.add_argument("--skip-drive", action="store_true", help="Do not upload to Drive")
    ap.add_argument(
        "--drive-subfolder",
        default="",
        metavar="NAME",
        help="Optional subfolder under QBR Generator (created if missing). Empty = generator root.",
    )
    args = ap.parse_args()

    exported_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    from src.pendo_client import PendoClient

    pc = PendoClient()
    report = _build_snapshot_report(pc, days=args.days)
    if report.get("error"):
        print(f"error: report failed: {report.get('error')}", file=sys.stderr)
        sys.exit(1)

    _emit_integration_stderr_warnings(report)

    csr_lim, csr_str, sf_acct, sig_cap = 15, 400, 6, 22
    doc = build_snapshot_document(
        report,
        csr_site_limit=csr_lim,
        csr_string_cap=csr_str,
        sf_accounts=sf_acct,
        signals_cap=sig_cap,
    )
    # Keep refs for iterative shrinking
    doc["_full_jira"] = report.get("jira") or {}
    doc["_full_csr"] = report.get("csr") or {}
    doc["_full_sf"] = report.get("salesforce") or {}
    doc["_portfolio_raw"] = report

    md = render_markdown(doc, exported_at_utc=exported_at)
    max_b = max(20_000, int(args.max_bytes))

    tiers = [
        (10, 320, 4, 18),
        (8, 260, 3, 14),
        (6, 220, 2, 10),
        (4, 180, 2, 8),
    ]
    while len(md.encode("utf-8")) > max_b and tiers:
        csr_lim, csr_str, sf_acct, sig_cap = tiers.pop(0)
        _shrink_snapshot_params(
            doc,
            csr_site_limit=csr_lim,
            csr_string_cap=csr_str,
            sf_accounts=sf_acct,
            signals_cap=sig_cap,
        )
        md = render_markdown(
            {k: v for k, v in doc.items() if not str(k).startswith("_")},
            exported_at_utc=exported_at,
        )

    raw = md.encode("utf-8")
    if len(raw) > max_b:
        md = raw[:max_b].decode("utf-8", errors="ignore").rstrip() + (
            "\n\n<!-- Document truncated to --max-bytes; re-run with a higher limit "
            "or narrow integrations if needed. -->\n"
        )

    for k in list(doc.keys()):
        if str(k).startswith("_"):
            doc.pop(k, None)

    fname = f"LLM-Context-All_Customers-{dt.date.today().isoformat()}.md"

    if args.out:
        path = args.out
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"Wrote local {path} ({len(md.encode('utf-8'))} bytes)")

    if args.skip_drive:
        if not args.out:
            sys.stdout.write(md)
        print(f"Skipped Drive upload ({len(md.encode('utf-8'))} bytes)", file=sys.stderr)
        return

    from src.drive_config import (
        _find_or_create_folder,
        get_qbr_generator_folder_id_for_drive_config,
        upload_text_file_to_drive_folder,
    )

    root = get_qbr_generator_folder_id_for_drive_config()
    folder_id = root
    sub = (args.drive_subfolder or "").strip()
    if sub:
        folder_id = _find_or_create_folder(sub, root)
    fid = upload_text_file_to_drive_folder(fname, md, folder_id, mime_type="text/markdown")
    url = f"https://drive.google.com/file/d/{fid}/view"
    print(f"Uploaded Drive file id={fid} ({len(md.encode('utf-8'))} bytes)")
    print(url)


if __name__ == "__main__":
    main()
