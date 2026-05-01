#!/usr/bin/env python3
"""Export a single LLM-oriented data snapshot to Google Drive (QBR Generator).

Pulls the same merged report as deck generation (:meth:`PendoClient.get_customer_health_report`)
for one customer, or :meth:`PendoClient.get_portfolio_report` plus aggregated CS Report (week) and
Jira HELP (all customers) when ``--all-customers`` is set.

**Pendo** detail payloads are stripped (sites, pages, features, …); **Jira** includes counts,
breakdowns, and SLA-style aggregates only — **no issue keys, summaries, or ticket rows.**

Usage:
  python scripts/export_llm_context_snapshot.py "Customer Name"
  python scripts/export_llm_context_snapshot.py --all-customers --days 90
  python scripts/export_llm_context_snapshot.py "Customer Name" --out ./snapshot.md --skip-drive

Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` and credentials for Drive upload unless ``--skip-drive``.
Drive upload **replaces** an existing file with the same name in the target folder by default.
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


def _slug_customer(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", (name or "").strip()).strip("_")
    return (s[:96] if s else "customer")


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
    return out


def _pendo_topline(report: dict[str, Any]) -> dict[str, Any]:
    """Headline Pendo fields only — omit lists used on definition / appendix slides."""
    acct = report.get("account") if isinstance(report.get("account"), dict) else {}
    eng = report.get("engagement") if isinstance(report.get("engagement"), dict) else {}
    bench = report.get("benchmarks") if isinstance(report.get("benchmarks"), dict) else {}

    slim_eng = {
        k: eng.get(k)
        for k in ("active_7d", "active_30d", "dormant", "active_rate_7d")
        if k in eng
    }

    slim_acct = {
        k: acct.get(k)
        for k in (
            "total_visitors",
            "total_sites",
            "active_visitors",
            "active_sites",
            "health_score",
            "csm",
        )
        if k in acct
    }

    depth = report.get("depth") if isinstance(report.get("depth"), dict) else {}
    slim_depth: dict[str, Any] = {}
    if depth and not depth.get("error"):
        slim_depth = {
            k: depth.get(k)
            for k in (
                "total_feature_events",
                "active_users",
                "read_events",
                "write_events",
                "collab_events",
                "write_ratio",
                "error",
            )
            if k in depth
        }

    exports = report.get("exports") if isinstance(report.get("exports"), dict) else {}
    slim_exports: dict[str, Any] = {}
    if exports and not exports.get("error"):
        slim_exports = {
            k: exports.get(k)
            for k in ("total_exports", "exports_per_active_user", "active_users", "error")
            if k in exports
        }

    kei = report.get("kei") if isinstance(report.get("kei"), dict) else {}
    slim_kei: dict[str, Any] = {}
    if kei and not kei.get("error"):
        slim_kei = {
            k: kei.get(k)
            for k in (
                "total_queries",
                "unique_users",
                "active_users",
                "adoption_rate",
                "executive_users",
                "executive_queries",
                "error",
            )
            if k in kei
        }

    guides = report.get("guides") if isinstance(report.get("guides"), dict) else {}
    slim_guides: dict[str, Any] = {}
    if guides and not guides.get("error"):
        slim_guides = {
            k: guides.get(k)
            for k in (
                "total_guide_events",
                "users_who_saw_guides",
                "total_visitors",
                "guide_reach",
                "seen",
                "advanced",
                "dismissed",
                "dismiss_rate",
                "advance_rate",
                "error",
            )
            if k in guides
        }

    pe = report.get("poll_events") if isinstance(report.get("poll_events"), dict) else {}
    slim_poll: dict[str, Any] = {}
    if pe and not pe.get("error"):
        slim_poll["response_count"] = pe.get("response_count")
        nps = pe.get("nps")
        if isinstance(nps, dict):
            slim_poll["nps"] = {k: nps.get(k) for k in ("count", "median", "avg") if k in nps}

    fr = report.get("frustration") if isinstance(report.get("frustration"), dict) else {}
    slim_fr: dict[str, Any] = {}
    if fr and not fr.get("error"):
        slim_fr["total_frustration_signals"] = fr.get("total_frustration_signals")
        tot = fr.get("totals")
        if isinstance(tot, dict):
            slim_fr["totals"] = {
                k: tot.get(k)
                for k in ("rageClickCount", "deadClickCount", "errorClickCount", "uTurnCount")
                if k in tot
            }

    te = report.get("track_events_breakdown") if isinstance(report.get("track_events_breakdown"), dict) else {}
    slim_track: dict[str, Any] = {}
    if te and not te.get("error"):
        slim_track["distinct_track_types"] = te.get("distinct_track_types")
        bd = te.get("breakdown")
        ev_total = 0
        users_total = 0
        if isinstance(bd, list):
            for row in bd:
                if not isinstance(row, dict):
                    continue
                ev_total += int(row.get("events") or 0)
                users_total += int(row.get("unique_users") or 0)
        slim_track["custom_track_events_total"] = ev_total
        slim_track["custom_track_unique_users_sum"] = users_total

    sites = report.get("sites")
    site_count = len(sites) if isinstance(sites, list) else None

    # People: minimal, low PII — role + recency only
    champs = report.get("champions") if isinstance(report.get("champions"), list) else []
    at_risk = report.get("at_risk_users") if isinstance(report.get("at_risk_users"), list) else []
    people_preview = []
    for row in list(champs)[:4]:
        if isinstance(row, dict):
            people_preview.append(
                {"role": row.get("role"), "days_inactive": row.get("days_inactive")}
            )

    return {
        "note": (
            "Pendo detail omitted by design: no per-site tables, page/feature rankings, "
            "localization rows, poll transcripts, friction top-pages, track event names, or catalog appendix."
        ),
        "account": slim_acct,
        "site_rows_in_report_excluded": site_count,
        "engagement": slim_eng,
        "benchmarks": bench,
        "depth": slim_depth or None,
        "exports": slim_exports or None,
        "kei": slim_kei or None,
        "guides": slim_guides or None,
        "poll_events": slim_poll or None,
        "frustration": slim_fr or None,
        "track_events": slim_track or None,
        "champions_preview_roles": people_preview,
        "at_risk_users_count": len(at_risk) if at_risk else 0,
    }


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
        return {}
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


def _signals_lines(report: dict[str, Any], *, cap: int, line_max: int) -> list[str]:
    sigs = report.get("signals")
    if not isinstance(sigs, list):
        return []
    out: list[str] = []
    for s in sigs[:cap]:
        line = s if isinstance(s, str) else json.dumps(s, default=str)
        line = " ".join(line.split())
        if len(line) > line_max:
            line = line[: line_max - 1] + "…"
        out.append(line)
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
    is_portfolio = report.get("type") == "portfolio"
    pendo_sec = _pendo_portfolio_topline(report) if is_portfolio else _pendo_topline(report)
    sig_lines = (
        _portfolio_signal_lines(report, cap=signals_cap, line_max=signal_line_max)
        if is_portfolio
        else _signals_lines(report, cap=signals_cap, line_max=signal_line_max)
    )
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
    if isinstance(pr, dict) and pr.get("type") == "portfolio":
        doc["notable_signals_lines"] = _portfolio_signal_lines(
            pr, cap=signals_cap, line_max=240
        )
    else:
        doc["notable_signals_lines"] = _signals_lines(
            {"signals": doc.get("_full_signals") or []},
            cap=signals_cap,
            line_max=240,
        )
    if doc.get("signals_trend_context") and csr_string_cap < 280:
        doc.pop("signals_trend_context", None)


def _build_all_customers_report(pc: Any, *, days: int) -> dict[str, Any]:
    """Pendo portfolio rollup + CS Report (all CSR customers, week) + Jira HELP; no Salesforce."""
    portfolio = pc.get_portfolio_report(days=days)
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
    report["salesforce"] = {}
    report["signals"] = []
    try:
        from src.jira_client import get_shared_jira_client

        report["jira"] = get_shared_jira_client().get_customer_jira(None, days=min(int(days), 365))
    except Exception as e:
        report["jira"] = {"error": str(e)}
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Export LLM-oriented data snapshot to QBR Generator Drive folder.")
    ap.add_argument(
        "customer",
        nargs="?",
        default=None,
        help="Customer name (same as deck / Pendo match). Omit when using --all-customers.",
    )
    ap.add_argument(
        "--all-customers",
        action="store_true",
        help="Portfolio Pendo rollup + aggregated CS Report (week) + Jira HELP; Salesforce omitted.",
    )
    ap.add_argument("--days", type=int, default=90, help="Lookback days for health report (default 90)")
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

    if args.all_customers and args.customer:
        ap.error("Pass either --all-customers or a customer name, not both.")
    if not args.all_customers and not args.customer:
        ap.error("Pass a customer name or use --all-customers.")

    exported_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    from src.pendo_client import PendoClient

    pc = PendoClient()
    if args.all_customers:
        report = _build_all_customers_report(pc, days=args.days)
    else:
        report = pc.get_customer_health_report(str(args.customer).strip(), days=args.days)
    if report.get("error"):
        print(f"error: report failed: {report.get('error')}", file=sys.stderr)
        sys.exit(1)

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
    doc["_full_signals"] = report.get("signals") or []
    doc["_portfolio_raw"] = report if report.get("type") == "portfolio" else None

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

    fname = (
        f"LLM-Context-All_Customers-{dt.date.today().isoformat()}.md"
        if args.all_customers
        else f"LLM-Context-{_slug_customer(str(args.customer))}-{dt.date.today().isoformat()}.md"
    )

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
