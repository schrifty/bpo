#!/usr/bin/env python3
"""Site- and user-level Pendo usage exports (extends account-level Ford-style reports).

Usage:
  cortex --export-pendo-detailed --customer Ford [--days 30] [--compare-days 30]
  cortex --export-pendo-top-arr [--top-n 5] [--days 30] [--compare-days 30]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .config import logger
from .export_customer_pendo_snapshot import (
    _fetch_activity_day_buckets,
    _md_section,
    _pct_change,
    _sum_activity_in_window,
    _write_local,
    build_customer_pendo_export_report,
    ensure_customer_pendo_export_folders,
    render_customer_pendo_markdown,
)
from .export_run_diagnostics import export_diagnostics_scope, export_phase
from .pendo_client import PendoClient, _name_matches

_PROFILE_ID = "customer_pendo_detailed_export"
_MS_PER_DAY = 86_400_000
_DEFAULT_TOP_N = 5
_DEFAULT_USER_ROSTER_MD_CAP = 250


def _pendo_detailed_export_file_stem(customer: str, days: int) -> str:
    label = (customer or "").strip() or "customer"
    return f"Pendo Detailed Export  ({label}, {days}d)"


def _pendo_top_arr_manifest_stem(days: int, top_n: int) -> str:
    return f"Pendo Detailed Export Top {top_n} by ARR ({days}d)"


def _visitors_for_sitename(
    customer_visitors: list[dict[str, Any]],
    sitename: str,
) -> list[dict[str, Any]]:
    target = (sitename or "").strip()
    if not target:
        return []
    out: list[dict[str, Any]] = []
    for v in customer_visitors:
        agent = (v.get("metadata") or {}).get("agent") or {}
        names = list(agent.get("sitenames") or [])
        if agent.get("sitename"):
            names.append(agent.get("sitename"))
        if any(str(n or "").strip() == target for n in names):
            out.append(v)
    return out


def _visitor_sitenames(agent: dict[str, Any], customer_name: str) -> list[str]:
    names: list[str] = []
    for raw in list(agent.get("sitenames") or []):
        sn = str(raw or "").strip()
        if sn and _name_matches(customer_name, sn) and sn not in names:
            names.append(sn)
    single = str(agent.get("sitename") or "").strip()
    if single and _name_matches(customer_name, single) and single not in names:
        names.append(single)
    return names


def _engagement_bucket(days_inactive: float) -> str:
    if days_inactive <= 7:
        return "active_7d"
    if days_inactive <= 30:
        return "active_30d"
    return "dormant"


def _top_pages_and_features_for_visitors(
    pc: PendoClient,
    visitor_ids: set[str],
    days: int,
    *,
    limit: int = 8,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    page_catalog = pc._get_page_catalog_cached()
    feature_catalog = pc.get_feature_catalog()
    page_counts: dict[str, dict[str, int]] = {}
    feat_counts: dict[str, int] = {}
    try:
        for ev in pc._get_page_events_cached(days):
            if str(ev.get("visitorId")) not in visitor_ids:
                continue
            pid = str(ev.get("pageId") or "")
            if not pid:
                continue
            bucket = page_counts.setdefault(pid, {"events": 0, "minutes": 0})
            bucket["events"] += int(ev.get("numEvents") or 0)
            bucket["minutes"] += int(ev.get("numMinutes") or 0)
    except Exception as e:
        logger.debug("Site detail top pages skipped: %s", e)
    try:
        for ev in pc._get_feature_events_cached(days):
            if str(ev.get("visitorId")) not in visitor_ids:
                continue
            fid = str(ev.get("featureId") or "")
            if not fid:
                continue
            feat_counts[fid] = feat_counts.get(fid, 0) + int(ev.get("numEvents") or 0)
    except Exception as e:
        logger.debug("Site detail top features skipped: %s", e)

    top_pages = [
        {"name": page_catalog.get(pid, pid), "events": c["events"], "minutes": c["minutes"]}
        for pid, c in sorted(page_counts.items(), key=lambda x: -x[1]["events"])[:limit]
    ]
    top_features = [
        {"name": feature_catalog.get(fid, fid), "events": count}
        for fid, count in sorted(feat_counts.items(), key=lambda x: -x[1])[:limit]
    ]
    return top_pages, top_features


def build_site_detail_slices(
    pc: PendoClient,
    pendo_prefix: str,
    *,
    days: int,
    compare_days: int,
    customer_visitors: list[dict[str, Any]],
    page_rows: list[dict[str, Any]],
    feat_rows: list[dict[str, Any]],
    now_ms: int,
) -> list[dict[str, Any]]:
    """Per-sitename mini-reports for one Pendo customer prefix."""
    window_days = max(1, int(days))
    compare = max(1, int(compare_days))
    current_start_ms = now_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY

    site_names: list[str] = []
    seen: set[str] = set()
    for v in customer_visitors:
        agent = (v.get("metadata") or {}).get("agent") or {}
        for sn in _visitor_sitenames(agent, pendo_prefix):
            if sn.lower() not in seen:
                seen.add(sn.lower())
                site_names.append(sn)
    site_names.sort(key=str.lower)

    slices: list[dict[str, Any]] = []
    for sitename in site_names:
        site_visitors = _visitors_for_sitename(customer_visitors, sitename)
        visitor_ids = {str(v.get("visitorId")) for v in site_visitors if v.get("visitorId")}
        current = _sum_activity_in_window(page_rows, feat_rows, visitor_ids, current_start_ms, now_ms)
        prior = _sum_activity_in_window(page_rows, feat_rows, visitor_ids, prior_start_ms, prior_end_ms)
        users = pc._build_user_activity(site_visitors, now_ms)
        engagement = {"active_7d": 0, "active_30d": 0, "dormant": 0}
        for u in users:
            engagement[_engagement_bucket(float(u.get("days_inactive") or 999))] += 1
        top_pages, top_features = _top_pages_and_features_for_visitors(pc, visitor_ids, window_days)
        slices.append(
            {
                "sitename": sitename,
                "visitors": len(site_visitors),
                "engagement": engagement,
                "activity_current": current,
                "activity_prior": prior,
                "activity_pct_change": {
                    "total_events": _pct_change(current.get("total_events"), prior.get("total_events")),
                    "page_minutes": _pct_change(current.get("page_minutes"), prior.get("page_minutes")),
                    "feature_events": _pct_change(current.get("feature_events"), prior.get("feature_events")),
                },
                "top_pages": top_pages,
                "top_features": top_features,
                "users": sorted(users, key=lambda u: (u.get("days_inactive", 999), u.get("email") or "")),
            }
        )
    slices.sort(key=lambda s: (-int((s.get("activity_current") or {}).get("total_events") or 0), s.get("sitename") or ""))
    return slices


def build_full_user_roster(
    pc: PendoClient,
    pendo_prefix: str,
    *,
    days: int,
    compare_days: int,
    customer_visitors: list[dict[str, Any]],
    page_rows: list[dict[str, Any]],
    feat_rows: list[dict[str, Any]],
    now_ms: int,
) -> list[dict[str, Any]]:
    """All customer visitors with window activity and site assignments."""
    window_days = max(1, int(days))
    compare = max(1, int(compare_days))
    current_start_ms = now_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY

    roster: list[dict[str, Any]] = []
    for v in customer_visitors:
        vid = str(v.get("visitorId") or "")
        if not vid:
            continue
        agent = (v.get("metadata") or {}).get("agent") or {}
        auto = (v.get("metadata") or {}).get("auto") or {}
        lv = auto.get("lastvisit", 0)
        days_inactive = (now_ms - lv) / (86400 * 1000) if lv else 999.0
        current = _sum_activity_in_window(page_rows, feat_rows, {vid}, current_start_ms, now_ms)
        prior = _sum_activity_in_window(page_rows, feat_rows, {vid}, prior_start_ms, prior_end_ms)
        roster.append(
            {
                "visitor_id": vid,
                "email": agent.get("emailaddress", ""),
                "role": agent.get("role", "Unknown"),
                "language": (agent.get("language") or "").strip(),
                "sites": _visitor_sitenames(agent, pendo_prefix),
                "last_visit": pc._build_user_activity([v], now_ms)[0]["last_visit"] if v else "Never",
                "days_inactive": round(days_inactive, 1),
                "engagement_status": _engagement_bucket(days_inactive),
                "events_current": int(current.get("total_events") or 0),
                "page_minutes_current": int(current.get("page_minutes") or 0),
                "feature_events_current": int(current.get("feature_events") or 0),
                "events_prior": int(prior.get("total_events") or 0),
                "events_pct_change": _pct_change(current.get("total_events"), prior.get("total_events")),
            }
        )
    roster.sort(
        key=lambda r: (
            r.get("engagement_status") != "active_7d",
            r.get("days_inactive", 999),
            r.get("email") or "",
        )
    )
    return roster


def build_customer_pendo_detailed_report(
    pc: PendoClient,
    customer_query: str,
    *,
    days: int = 30,
    compare_days: int | None = None,
) -> dict[str, Any]:
    """Account-level Ford-style report plus site slices and full user roster."""
    account = build_customer_pendo_export_report(
        pc,
        customer_query,
        days=days,
        compare_days=compare_days,
    )
    if account.get("error"):
        return account

    pendo_prefix = (account.get("meta") or {}).get("pendo_prefix") or customer_query
    window_days = max(1, int(days))
    compare = max(1, int(compare_days if compare_days is not None else window_days))
    total_lookback = window_days + compare

    partition = pc._get_visitor_partition(window_days)
    now_ms = int(partition["now_ms"])
    customer_visitors, _ = pc._filter_customer_visitors(pendo_prefix, partition)
    if not customer_visitors:
        return {
            **account,
            "error": f"No visitors found matching {pendo_prefix!r}",
        }

    page_rows, feat_rows = _fetch_activity_day_buckets(pc, total_lookback)
    site_detail = build_site_detail_slices(
        pc,
        pendo_prefix,
        days=window_days,
        compare_days=compare,
        customer_visitors=customer_visitors,
        page_rows=page_rows,
        feat_rows=feat_rows,
        now_ms=now_ms,
    )
    user_roster = build_full_user_roster(
        pc,
        pendo_prefix,
        days=window_days,
        compare_days=compare,
        customer_visitors=customer_visitors,
        page_rows=page_rows,
        feat_rows=feat_rows,
        now_ms=now_ms,
    )

    meta = dict(account.get("meta") or {})
    meta["profile_id"] = _PROFILE_ID
    meta["granularity"] = "account_site_user"
    meta["site_count"] = len(site_detail)
    meta["user_roster_count"] = len(user_roster)

    return {
        **account,
        "meta": meta,
        "site_detail": site_detail,
        "user_roster": user_roster,
    }


def render_site_detail_markdown(site_detail: list[dict[str, Any]], *, compare_days: int) -> str:
    if not site_detail:
        return _md_section("13. Site detail", "*(No sites with matching visitor metadata.)*")

    blocks: list[str] = []
    for idx, site in enumerate(site_detail, 1):
        eng = site.get("engagement") or {}
        cur = site.get("activity_current") or {}
        cmp_ = site.get("activity_pct_change") or {}
        lines = [
            f"### {idx}. {site.get('sitename')}",
            f"- Visitors: **{site.get('visitors', 0)}** "
            f"(7d active **{eng.get('active_7d', 0)}** · 30d **{eng.get('active_30d', 0)}** · dormant **{eng.get('dormant', 0)}**)",
            f"- Events: **{int(cur.get('total_events') or 0):,}** · minutes: **{int(cur.get('page_minutes') or 0):,}** · "
            f"feature clicks: **{int(cur.get('feature_events') or 0):,}**",
        ]
        if cmp_.get("total_events") is not None:
            lines.append(
                f"- vs prior {compare_days}d: events **{cmp_.get('total_events')}%** · "
                f"minutes **{cmp_.get('page_minutes')}%** · features **{cmp_.get('feature_events')}%**"
            )
        if site.get("top_pages"):
            lines.append("")
            lines.append("**Top pages:**")
            for row in site.get("top_pages") or []:
                lines.append(
                    f"- {row.get('name')}: {int(row.get('events') or 0):,} events, "
                    f"{int(row.get('minutes') or 0):,} min"
                )
        if site.get("top_features"):
            lines.append("")
            lines.append("**Top features:**")
            for row in site.get("top_features") or []:
                lines.append(f"- {row.get('name')}: {int(row.get('events') or 0):,} events")
        site_users = site.get("users") or []
        if site_users:
            lines.append("")
            lines.append("| User | Role | Last visit | Days inactive |")
            lines.append("| --- | --- | --- | ---: |")
            for u in site_users[:15]:
                lines.append(
                    f"| {u.get('email', '')} | {u.get('role', '')} | {u.get('last_visit', '')} | "
                    f"{u.get('days_inactive', '')} |"
                )
            if len(site_users) > 15:
                lines.append(f"\n*Showing 15 of {len(site_users)} users at this site.*")
        blocks.append("\n".join(lines))

    body = "\n\n".join(blocks)
    if len(site_detail) > 40:
        body += f"\n\n*Site detail includes all {len(site_detail)} sites.*"
    return _md_section("13. Site detail", body)


def render_user_roster_markdown(
    user_roster: list[dict[str, Any]],
    *,
    cap: int = _DEFAULT_USER_ROSTER_MD_CAP,
) -> str:
    if not user_roster:
        return _md_section("14. User roster", "*(No users in roster.)*")

    lines = [
        f"- Total users: **{len(user_roster)}**",
        "",
        "| Email | Role | Sites | Status | Last visit | Days inactive | Events | Minutes | Feature clicks | Δ events % |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    shown = user_roster[:cap]
    for u in shown:
        sites = ", ".join(u.get("sites") or []) or "—"
        if len(sites) > 48:
            sites = sites[:45] + "…"
        pct = u.get("events_pct_change")
        pct_s = f"{pct}" if pct is not None else "n/a"
        lines.append(
            f"| {u.get('email', '')} | {u.get('role', '')} | {sites} | {u.get('engagement_status', '')} | "
            f"{u.get('last_visit', '')} | {u.get('days_inactive', '')} | "
            f"{int(u.get('events_current') or 0):,} | {int(u.get('page_minutes_current') or 0):,} | "
            f"{int(u.get('feature_events_current') or 0):,} | {pct_s} |"
        )
    if len(user_roster) > cap:
        lines.append(f"\n*Showing {cap} of {len(user_roster)} users (full roster in spreadsheet export).*")
    return _md_section("14. User roster", "\n".join(lines))


def render_customer_pendo_detailed_markdown(report: dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    compare_days = int(meta.get("compare_days") or meta.get("days") or 30)
    base = render_customer_pendo_markdown(report).rstrip()
    extra = render_site_detail_markdown(report.get("site_detail") or [], compare_days=compare_days)
    extra += render_user_roster_markdown(report.get("user_roster") or [])
    return base + "\n\n" + extra.strip() + "\n"


def load_top_ultimate_parents_by_arr_for_pendo(top_n: int = _DEFAULT_TOP_N) -> list[dict[str, Any]]:
    """Rank current-book ultimate parents by ARR with Pendo prefix mapping."""
    from .llm_export_csr import top_active_ultimate_parents_by_arr_for_llm_export
    from .pendo_client import PendoClient, customer_is_excluded_from_portfolio
    from .salesforce_client import SalesforceClient

    pc = PendoClient()
    by_customer = pc.get_sites_by_customer()
    pendo_prefixes = [
        c
        for c in (by_customer.get("customer_list") or [])
        if c and c != "(unknown)" and not customer_is_excluded_from_portfolio(c)
    ]
    sf = SalesforceClient()
    book = sf.get_portfolio_revenue_book_metrics(pendo_prefixes)
    report = {
        "_llm_export_salesforce_revenue_book": book,
        "customers": [{"customer": p} for p in pendo_prefixes],
    }
    return top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=max(1, int(top_n)))


def _customer_query_for_pendo_row(row: dict[str, Any]) -> str:
    pendo = str(row.get("pendo_customer_key") or "").strip()
    if pendo:
        return pendo
    return str(row.get("ultimate_parent") or row.get("salesforce_label") or "").strip()


def export_pendo_detailed_for_customer(
    pc: PendoClient,
    customer_query: str,
    *,
    days: int,
    compare_days: int | None,
) -> dict[str, Any]:
    return build_customer_pendo_detailed_report(
        pc,
        customer_query,
        days=days,
        compare_days=compare_days,
    )


def _upload_detailed_export(
    report: dict[str, Any],
    *,
    days: int,
    stem: str,
    no_drive: bool,
    out: Path | None,
) -> None:
    meta = report.get("meta") or {}
    pendo_prefix = str(meta.get("pendo_prefix") or meta.get("customer_query") or "customer")
    md = render_customer_pendo_detailed_markdown(report)

    if no_drive or out is not None:
        base = out
        if base is None:
            base = Path("output") / stem
        elif base.suffix.lower() in (".md", ".xlsx"):
            base = base.with_suffix("")
        _write_local(base.with_suffix(".md"), md)
        print(f"Wrote {base.with_suffix('.md')}")
        from .export_pendo_spreadsheet import write_pendo_export_xlsx

        write_pendo_export_xlsx(base.with_suffix(".xlsx"), report)
        print(f"Wrote {base.with_suffix('.xlsx')}")

    if not no_drive:
        from .drive_config import upload_text_file_to_drive_folder
        from .export_pendo_spreadsheet import spreadsheet_url, upload_pendo_export_spreadsheet

        folders = ensure_customer_pendo_export_folders(pendo_prefix)
        stable_id = folders["stable_folder_id"]
        dated_id = folders["dated_folder_id"]
        dated_label = folders["dated_label"]
        fid_stable = upload_text_file_to_drive_folder(
            f"{stem}.md", md, stable_id, mime_type="text/markdown"
        )
        fid_dated = upload_text_file_to_drive_folder(
            f"{stem}.md", md, dated_id, mime_type="text/markdown"
        )
        print(
            f"Uploaded markdown → customer-exports/{pendo_prefix}/{stem}.md "
            f"and {dated_label}/{stem}.md",
            file=sys.stderr,
        )
        print(f"Stable: https://drive.google.com/file/d/{fid_stable}/view")
        print(f"Dated:  https://drive.google.com/file/d/{fid_dated}/view")
        ss_stable = upload_pendo_export_spreadsheet(report, stem, stable_id)
        ss_dated = upload_pendo_export_spreadsheet(report, stem, dated_id)
        print(f"Spreadsheet (stable): {spreadsheet_url(ss_stable)}", file=sys.stderr)
        print(f"Spreadsheet (dated):  {spreadsheet_url(ss_dated)}", file=sys.stderr)


def export_pendo_detailed_main(cli_args: list[str] | None = None, *, prog: str | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Export site- and user-level Pendo usage (markdown + Sheet) for one customer.",
        prog=prog or "cortex --export-pendo-detailed",
    )
    ap.add_argument("--customer", required=True, help="Pendo customer prefix or alias (e.g. Ford)")
    ap.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    ap.add_argument("--compare-days", type=int, default=None, help="Prior comparison window (default: --days)")
    ap.add_argument("--no-drive", action="store_true", help="Skip Drive upload")
    ap.add_argument("-o", "--out", metavar="PATH", help="Local output path prefix (.md and .xlsx)")
    args = ap.parse_args(cli_args)

    with export_diagnostics_scope() as diag:
        with export_phase(diag, "Pendo detailed export"):
            pc = PendoClient()
            report = export_pendo_detailed_for_customer(
                pc,
                args.customer,
                days=args.days,
                compare_days=args.compare_days,
            )
        if report.get("error"):
            print(f"error: {report['error']}", file=sys.stderr)
            sys.exit(1)
        pendo_prefix = (report.get("meta") or {}).get("pendo_prefix") or args.customer
        stem = _pendo_detailed_export_file_stem(pendo_prefix, args.days)
        out_path = Path(args.out) if args.out else None
        with export_phase(diag, "Write detailed export"):
            _upload_detailed_export(
                report,
                days=args.days,
                stem=stem,
                no_drive=args.no_drive,
                out=out_path,
            )
        from .data_source_health import integration_freshness_metadata

        diag.set_integration_meta(integration_freshness_metadata())
        diag.emit_run_summary(job_name="export-pendo-detailed", fail_on_warnings=False)


def render_top_arr_batch_manifest(
    *,
    days: int,
    top_n: int,
    results: list[dict[str, Any]],
) -> str:
    lines = [
        f"# Pendo detailed exports — top {top_n} by ARR ({days}d)",
        "",
        "| Rank | Ultimate parent | Current ARR | Pendo prefix | Status | Output stem |",
        "| ---: | --- | ---: | --- | --- | --- |",
    ]
    for idx, row in enumerate(results, 1):
        sel = row.get("selection") or {}
        lines.append(
            f"| {idx} | {sel.get('ultimate_parent', '')} | "
            f"${float(sel.get('current_arr') or sel.get('arr') or 0):,.0f} | "
            f"{sel.get('pendo_customer_key') or '—'} | {row.get('status', '')} | "
            f"{row.get('stem') or '—'} |"
        )
        err = row.get("error")
        if err:
            lines.append(f"| | | | | *{err}* | |")
    return "\n".join(lines) + "\n"


def export_pendo_top_arr_main(cli_args: list[str] | None = None, *, prog: str | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Run site/user Pendo detailed export for top Salesforce ultimate parents by ARR.",
        prog=prog or "cortex --export-pendo-top-arr",
    )
    ap.add_argument("--top-n", type=int, default=_DEFAULT_TOP_N, help="Number of customers (default 5)")
    ap.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    ap.add_argument("--compare-days", type=int, default=None, help="Prior comparison window (default: --days)")
    ap.add_argument("--no-drive", action="store_true", help="Skip Drive upload")
    ap.add_argument(
        "-o",
        "--out-dir",
        metavar="DIR",
        help="Local output directory (default output/pendo-top-arr when --no-drive)",
    )
    args = ap.parse_args(cli_args)

    selection = load_top_ultimate_parents_by_arr_for_pendo(args.top_n)
    if not selection:
        print("error: no active Salesforce ultimate parents found for Pendo export", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.out_dir) if args.out_dir else Path("output") / "pendo-top-arr"
    batch_results: list[dict[str, Any]] = []
    errors = 0

    with export_diagnostics_scope() as diag:
        pc = PendoClient()
        for row in selection:
            customer_query = _customer_query_for_pendo_row(row)
            ultimate = str(row.get("ultimate_parent") or customer_query)
            entry: dict[str, Any] = {"selection": row, "ultimate_parent": ultimate}
            if not customer_query:
                entry.update({"status": "skipped", "error": "empty customer query"})
                errors += 1
                batch_results.append(entry)
                continue
            try:
                with export_phase(diag, f"Pendo detailed {ultimate}"):
                    report = export_pendo_detailed_for_customer(
                        pc,
                        customer_query,
                        days=args.days,
                        compare_days=args.compare_days,
                    )
                if report.get("error"):
                    entry.update({"status": "error", "error": report["error"]})
                    errors += 1
                    batch_results.append(entry)
                    continue
                pendo_prefix = (report.get("meta") or {}).get("pendo_prefix") or customer_query
                stem = _pendo_detailed_export_file_stem(pendo_prefix, args.days)
                entry["stem"] = stem
                entry["status"] = "ok"
                entry["pendo_prefix"] = pendo_prefix
                entry["site_count"] = len(report.get("site_detail") or [])
                entry["user_count"] = len(report.get("user_roster") or [])
                out_prefix = out_dir / stem if (args.no_drive or args.out_dir) else None
                _upload_detailed_export(
                    report,
                    days=args.days,
                    stem=stem,
                    no_drive=args.no_drive,
                    out=out_prefix,
                )
                batch_results.append(entry)
            except Exception as e:
                logger.warning("Pendo detailed export failed for %s: %s", ultimate, e)
                entry.update({"status": "error", "error": str(e)[:500]})
                errors += 1
                batch_results.append(entry)

        manifest = render_top_arr_batch_manifest(days=args.days, top_n=args.top_n, results=batch_results)
        manifest_stem = _pendo_top_arr_manifest_stem(args.days, args.top_n)
        if args.no_drive or args.out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = out_dir / f"{manifest_stem}.md"
            _write_local(manifest_path, manifest)
            print(f"Wrote {manifest_path}")
        if not args.no_drive:
            from .drive_config import upload_text_file_to_drive_folder, get_qbr_output_root_folder_id
            from .export_customer_pendo_snapshot import _CUSTOMER_EXPORTS_FOLDER
            from .drive_config import _find_or_create_folder

            root = get_qbr_output_root_folder_id()
            if root:
                batch_folder = _find_or_create_folder(_CUSTOMER_EXPORTS_FOLDER, root)
                fid = upload_text_file_to_drive_folder(
                    f"{manifest_stem}.md",
                    manifest,
                    batch_folder,
                    mime_type="text/markdown",
                )
                print(f"Batch manifest: https://drive.google.com/file/d/{fid}/view", file=sys.stderr)

        from .data_source_health import integration_freshness_metadata

        diag.set_integration_meta(integration_freshness_metadata())
        diag.emit_run_summary(job_name="export-pendo-top-arr", fail_on_warnings=False)

    if errors:
        print(f"Completed with {errors} error(s) of {len(selection)} customer(s)", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    export_pendo_detailed_main(None)


if __name__ == "__main__":
    main()
