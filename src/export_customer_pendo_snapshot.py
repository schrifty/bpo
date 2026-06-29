#!/usr/bin/env python3
"""Export single-customer Pendo usage snapshots to Google Drive (JSON + markdown).

Designed for strategic accounts that track product usage across sites (e.g. Ford daily).

Usage:
  cortex --export-pendo --customer Ford [--days 30] [--format both|json|markdown] [--no-drive] [-o PATH]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any

from .config import logger
from .export_run_diagnostics import export_diagnostics_scope, export_phase
from .pendo_client import PendoClient, _name_matches
from .signals_trends import _snapshot_metrics

_PROFILE_ID = "customer_pendo_export"
_CUSTOMER_EXPORTS_FOLDER = "customer-exports"
_MS_PER_DAY = 86_400_000


def _safe_export_stem(customer: str) -> str:
    stem = re.sub(r"[^\w\-]+", "-", (customer or "").strip()).strip("-")
    return stem or "customer"


def resolve_pendo_customer_prefix(query: str, pc: PendoClient) -> str:
    """Map a user label to the canonical Pendo customer prefix."""
    q = (query or "").strip()
    if not q:
        raise ValueError("customer name is required")

    customers = list((pc.get_sites_by_customer() or {}).get("customer_list") or [])
    if not customers:
        return q

    for c in customers:
        if c.lower() == q.lower():
            return c

    matches = [c for c in customers if _name_matches(q, c)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        names = ", ".join(sorted(matches)[:8])
        raise ValueError(
            f"customer {q!r} is ambiguous ({len(matches)} Pendo prefixes: {names}); "
            "pass the exact prefix from `cortex --list` or cohorts.yaml"
        )
    return q


def _optional_salesforce_context(pendo_prefix: str) -> dict[str, Any]:
    """One-line SF commercial context when JWT credentials are configured."""
    try:
        from .portfolio_salesforce_allowlist import _load_sf_portfolio_pendo_alias_map
        from .salesforce_client import SalesforceClient

        alias_map = _load_sf_portfolio_pendo_alias_map()
        sf_labels: list[str] = []
        for label, prefixes in alias_map.items():
            targets = prefixes if isinstance(prefixes, list) else [prefixes]
            if any(str(p).strip().lower() == pendo_prefix.lower() for p in targets if p):
                sf_labels.append(label)
        if not sf_labels:
            sf_labels = [pendo_prefix]

        sf = SalesforceClient()
        metrics = sf.get_portfolio_revenue_book_metrics([sf_labels[0]])
        rows = metrics.get("matched_customer_contract_rollups") or []
        if not rows:
            return {"salesforce_label": sf_labels[0], "note": "no Customer Entity rollup matched"}
        row = rows[0]
        return {
            "salesforce_label": row.get("customer") or sf_labels[0],
            "entity_count": row.get("entity_count"),
            "active_arr_usd": row.get("active_arr_usd"),
            "total_arr_usd": row.get("total_arr_usd"),
            "active": row.get("active"),
        }
    except Exception as exc:
        logger.debug("customer pendo export: Salesforce context skipped: %s", exc)
        return {"note": "Salesforce context unavailable (credentials or lookup failed)"}


def _pct_change(current: float | int | None, prior: float | int | None) -> float | None:
    if current is None or prior is None:
        return None
    try:
        cur = float(current)
        prev = float(prior)
    except (TypeError, ValueError):
        return None
    if prev == 0:
        return None if cur == 0 else 100.0
    return round((cur - prev) / prev * 100.0, 1)


def build_usage_trends(pc: PendoClient, customer: str, days: int) -> dict[str, Any]:
    """Prior-period comparison and weekly active-user series."""
    partition = pc._get_visitor_partition(days)
    end_ms = int(partition["now_ms"])
    start_ms = end_ms - int(days) * _MS_PER_DAY
    prior_end = start_ms
    prior_start = prior_end - int(days) * _MS_PER_DAY

    current = _snapshot_metrics(pc, customer, start_ms, end_ms) or {}
    prior = _snapshot_metrics(pc, customer, prior_start, prior_end) or {}

    weekly: list[dict[str, Any]] = []
    num_weeks = max(1, min(13, (int(days) + 6) // 7))
    for i in range(num_weeks):
        w_end = end_ms - i * 7 * _MS_PER_DAY
        w_start = w_end - 7 * _MS_PER_DAY
        snap = _snapshot_metrics(pc, customer, w_start, w_end) or {}
        weekly.append(
            {
                "week_index": num_weeks - i,
                "window_start": dt.datetime.fromtimestamp(w_start / 1000, tz=dt.timezone.utc).date().isoformat(),
                "window_end": dt.datetime.fromtimestamp(w_end / 1000, tz=dt.timezone.utc).date().isoformat(),
                "active_users_7d": snap.get("active_7d"),
                "total_users": snap.get("total_users"),
                "weekly_active_rate_pct": snap.get("weekly_active_rate_pct"),
            }
        )
    weekly.sort(key=lambda r: r["week_index"])

    cur_rate = current.get("weekly_active_rate_pct")
    prev_rate = prior.get("weekly_active_rate_pct")
    return {
        "prior_period_days": int(days),
        "current_period": current,
        "prior_period": prior,
        "comparison": {
            "active_users_7d_pct_change": _pct_change(current.get("active_7d"), prior.get("active_7d")),
            "total_users_pct_change": _pct_change(current.get("total_users"), prior.get("total_users")),
            "weekly_active_rate_pp_change": (
                round(float(cur_rate) - float(prev_rate), 1)
                if cur_rate is not None and prev_rate is not None
                else None
            ),
        },
        "weekly_active_users": weekly,
    }


def build_headline(
    *,
    health: dict[str, Any],
    depth: dict[str, Any],
    sites: dict[str, Any],
    features: dict[str, Any],
    trends: dict[str, Any],
) -> dict[str, Any]:
    site_rows = sites.get("sites") or []
    total_events = sum(int(s.get("total_events") or 0) for s in site_rows)
    total_minutes = sum(int(s.get("total_minutes") or 0) for s in site_rows)
    engagement = health.get("engagement") or {}
    account = health.get("account") or {}
    adoption = features.get("feature_adoption_insights") or {}
    distinct_features_used = len(features.get("top_features") or [])
    return {
        "active_users_7d": engagement.get("active_7d"),
        "active_users_30d": engagement.get("active_30d"),
        "dormant_users": engagement.get("dormant"),
        "total_visitors": account.get("total_visitors"),
        "total_sites": account.get("total_sites"),
        "weekly_active_rate_pct": engagement.get("active_rate_7d"),
        "total_events": total_events,
        "total_minutes": round(total_minutes, 1),
        "feature_events": depth.get("total_feature_events"),
        "distinct_features_used_top10": distinct_features_used,
        "write_ratio_pct": depth.get("write_ratio"),
        "feature_clicks_total": adoption.get("feature_clicks_total"),
        "vs_prior_period": trends.get("comparison") or {},
    }


def build_customer_pendo_export_report(
    pc: PendoClient,
    customer_query: str,
    *,
    days: int = 30,
) -> dict[str, Any]:
    """Fetch Pendo usage slices for one customer (product adoption focus)."""
    pendo_prefix = resolve_pendo_customer_prefix(customer_query, pc)
    exported_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = dt.date.today()
    window_start = window_end - dt.timedelta(days=max(1, int(days)) - 1)

    pc.preload(days)

    health = pc.get_customer_health(pendo_prefix, days=days)
    if health.get("error"):
        return {"error": health["error"], "customer_query": customer_query, "pendo_prefix": pendo_prefix}

    sites = pc.get_customer_sites(pendo_prefix, days=days)
    if sites.get("error"):
        return {"error": sites["error"], "customer_query": customer_query, "pendo_prefix": pendo_prefix}

    features = pc.get_customer_features(pendo_prefix, days=days)
    depth = pc.get_customer_depth(pendo_prefix, days=days)
    kei = pc.get_customer_kei(pendo_prefix, days=days)
    trends = build_usage_trends(pc, pendo_prefix, days)

    report: dict[str, Any] = {
        "meta": {
            "profile_id": _PROFILE_ID,
            "exported_at_utc": exported_at,
            "customer_query": customer_query.strip(),
            "pendo_prefix": pendo_prefix,
            "days": int(days),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "salesforce": _optional_salesforce_context(pendo_prefix),
        },
        "headline": build_headline(
            health=health,
            depth=depth if not depth.get("error") else {},
            sites=sites,
            features=features if not features.get("error") else {},
            trends=trends,
        ),
        "engagement": {
            "account": health.get("account") or {},
            "engagement": health.get("engagement") or {},
            "benchmarks": health.get("benchmarks") or {},
            "signals": health.get("signals") or [],
        },
        "sites": sites,
        "features": features,
        "depth": depth,
        "kei": kei,
        "trends": trends,
    }
    return report


def _md_section(title: str, body: str) -> str:
    return f"## {title}\n\n{body.strip()}\n\n"


def render_customer_pendo_markdown(report: dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    headline = report.get("headline") or {}
    sf = meta.get("salesforce") or {}
    lines = [
        f"# Pendo usage — {meta.get('pendo_prefix') or meta.get('customer_query')}",
        "",
        f"- **Exported:** {meta.get('exported_at_utc')}",
        f"- **Window:** {meta.get('window_start')} → {meta.get('window_end')} ({meta.get('days')} days)",
        f"- **Pendo prefix:** `{meta.get('pendo_prefix')}`",
    ]
    if sf.get("salesforce_label"):
        arr = sf.get("active_arr_usd")
        arr_s = f"${arr:,.0f}" if isinstance(arr, (int, float)) else "n/a"
        lines.append(
            f"- **Salesforce:** {sf.get('salesforce_label')} · active ARR {arr_s} · "
            f"{sf.get('entity_count', 'n/a')} entities"
        )
    elif sf.get("note"):
        lines.append(f"- **Salesforce:** {sf['note']}")

    md = "\n".join(lines) + "\n\n"

    cmp_ = headline.get("vs_prior_period") or {}
    md += _md_section(
        "1. Headline",
        "\n".join(
            [
                f"- Active users (7d): **{headline.get('active_users_7d')}** "
                f"({cmp_.get('active_users_7d_pct_change')}% vs prior {meta.get('days')}d)"
                if cmp_.get("active_users_7d_pct_change") is not None
                else f"- Active users (7d): **{headline.get('active_users_7d')}**",
                f"- Total visitors: **{headline.get('total_visitors')}** · sites: **{headline.get('total_sites')}**",
                f"- Weekly active rate: **{headline.get('weekly_active_rate_pct')}%**",
                f"- Events: **{headline.get('total_events'):,}** · minutes: **{headline.get('total_minutes'):,}**",
                f"- Feature events: **{headline.get('feature_events'):,}** · write ratio: **{headline.get('write_ratio_pct')}%**",
            ]
        ),
    )

    site_rows = (report.get("sites") or {}).get("sites") or []
    site_lines = ["| Site | Visitors | Events | Minutes | Last active |", "| --- | ---: | ---: | ---: | --- |"]
    for s in site_rows[:40]:
        site_lines.append(
            f"| {s.get('sitename', '')} | {s.get('visitors', 0)} | "
            f"{s.get('total_events', 0):,} | {s.get('total_minutes', 0):,} | {s.get('last_active', '')} |"
        )
    if len(site_rows) > 40:
        site_lines.append(f"\n*Showing 40 of {len(site_rows)} sites.*")
    md += _md_section("2. Sites", "\n".join(site_lines))

    feat = report.get("features") or {}
    feat_lines: list[str] = []
    for label, key in (("Top pages", "top_pages"), ("Top features", "top_features")):
        rows = feat.get(key) or []
        if not rows:
            continue
        feat_lines.append(f"### {label}")
        for row in rows[:20]:
            if key == "top_pages":
                feat_lines.append(
                    f"- {row.get('name')}: {row.get('events', 0):,} events, "
                    f"{row.get('minutes', 0):,} min"
                )
            else:
                feat_lines.append(f"- {row.get('name')}: {row.get('events', 0):,} events")
        feat_lines.append("")
    insights = feat.get("feature_adoption_insights") or {}
    if insights.get("narrative"):
        feat_lines.append(f"**Adoption note:** {insights['narrative']}")
    md += _md_section("3. Feature & page adoption", "\n".join(feat_lines) or "*(no feature data)*")

    depth = report.get("depth") or {}
    breakdown = depth.get("breakdown") or []
    depth_lines = [
        f"- Total feature events: **{depth.get('total_feature_events', 0):,}**",
        f"- Active users: **{depth.get('active_users', 0)}**",
        f"- Write ratio: **{depth.get('write_ratio', 0)}%** "
        f"(read {depth.get('read_events', 0):,} · write {depth.get('write_events', 0):,} · "
        f"collab {depth.get('collab_events', 0):,})",
    ]
    if breakdown:
        depth_lines.append("")
        depth_lines.append("**By category:**")
        for row in breakdown[:15]:
            depth_lines.append(
                f"- {row.get('category')}: {row.get('events', 0):,} events "
                f"({row.get('users', 0)} users)"
            )
    md += _md_section("4. Behavioral depth", "\n".join(depth_lines))

    kei = report.get("kei") or {}
    kei_lines = [
        f"- Total queries: **{kei.get('total_queries', 0):,}**",
        f"- Unique users: **{kei.get('unique_users', 0)}** · adoption: **{kei.get('adoption_rate', 0)}%**",
        f"- Executive users: **{kei.get('executive_users', 0)}** "
        f"({kei.get('executive_queries', 0):,} queries)",
    ]
    md += _md_section("5. Kei AI", "\n".join(kei_lines))

    trends = report.get("trends") or {}
    trend_lines = ["| Week | Start | End | Active (7d) | Total users | WAU % |", "| ---: | --- | --- | ---: | ---: | ---: |"]
    for row in trends.get("weekly_active_users") or []:
        trend_lines.append(
            f"| {row.get('week_index')} | {row.get('window_start')} | {row.get('window_end')} | "
            f"{row.get('active_users_7d', 0)} | {row.get('total_users', 0)} | "
            f"{row.get('weekly_active_rate_pct', 0)} |"
        )
    cmp_ = trends.get("comparison") or {}
    if cmp_:
        trend_lines.append("")
        trend_lines.append(
            f"Prior {meta.get('days')}d comparison: active users "
            f"{cmp_.get('active_users_7d_pct_change')}% · total users "
            f"{cmp_.get('total_users_pct_change')}% · WAU "
            f"{cmp_.get('weekly_active_rate_pp_change')} pp"
        )
    md += _md_section("6. Usage trends", "\n".join(trend_lines))

    eng = report.get("engagement") or {}
    bench = eng.get("benchmarks") or {}
    secondary = [
        f"- Cohort: **{bench.get('cohort_name') or bench.get('cohort') or 'n/a'}**",
        f"- Cohort median WAU: **{bench.get('cohort_median_rate')}%** · portfolio median: **{bench.get('peer_median_rate')}%**",
    ]
    signals = eng.get("signals") or []
    if signals:
        secondary.append("")
        secondary.append("**Auto-detected signals:**")
        secondary.extend(f"- {s}" for s in signals[:12])
    md += _md_section("7. Engagement context", "\n".join(secondary))

    return md.rstrip() + "\n"


def ensure_customer_pendo_export_folders(customer: str) -> dict[str, str]:
    """Return stable and dated folder ids under Output/customer-exports/{customer}/."""
    from .drive_config import _find_or_create_folder, get_qbr_output_root_folder_id

    root = get_qbr_output_root_folder_id()
    if not root:
        raise RuntimeError(
            "Could not resolve Drive Output folder (set GOOGLE_QBR_GENERATOR_FOLDER_ID)."
        )
    customer_folder = _find_or_create_folder(_CUSTOMER_EXPORTS_FOLDER, root)
    account_folder = _find_or_create_folder(customer, customer_folder)
    dated_name = f"{dt.date.today().isoformat()} - Output"
    dated_folder = _find_or_create_folder(dated_name, account_folder)
    return {
        "stable_folder_id": account_folder,
        "dated_folder_id": dated_folder,
        "dated_label": dated_name,
    }


def _write_local(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def export_pendo_main(cli_args: list[str] | None = None, *, prog: str | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Export single-customer Pendo usage snapshot (JSON + markdown) to Drive.",
        prog=prog or "cortex --export-pendo",
    )
    ap.add_argument("--customer", required=True, help="Pendo customer prefix or alias (e.g. Ford)")
    ap.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    ap.add_argument(
        "--format",
        choices=("json", "markdown", "both"),
        default="both",
        help="Output format (default both)",
    )
    ap.add_argument("--no-drive", action="store_true", help="Skip Drive upload; write locally only")
    ap.add_argument(
        "-o",
        "--out",
        metavar="PATH",
        help="Local output path prefix (writes PATH.json and/or PATH.md); default output/ when --no-drive",
    )
    args = ap.parse_args(cli_args)

    with export_diagnostics_scope() as diag:
        with export_phase(diag, "Pendo preload + customer export"):
            pc = PendoClient()
            report = build_customer_pendo_export_report(pc, args.customer, days=args.days)

        if report.get("error"):
            print(f"error: {report['error']}", file=sys.stderr)
            sys.exit(1)

        pendo_prefix = (report.get("meta") or {}).get("pendo_prefix") or args.customer
        stem = f"Pendo-Usage-{_safe_export_stem(pendo_prefix)}"
        md = render_customer_pendo_markdown(report) if args.format in ("markdown", "both") else ""
        json_text = json.dumps(report, indent=2, default=str) if args.format in ("json", "both") else ""

        if args.no_drive or args.out:
            if args.out:
                base = Path(args.out)
                if base.suffix.lower() in (".json", ".md"):
                    base = base.with_suffix("")
            else:
                base = Path("output") / stem
            if json_text:
                _write_local(base.with_suffix(".json"), json_text)
                print(f"Wrote {base.with_suffix('.json')}")
            if md:
                _write_local(base.with_suffix(".md"), md)
                print(f"Wrote {base.with_suffix('.md')}")

        if not args.no_drive:
            from .drive_config import upload_text_file_to_drive_folder

            folders = ensure_customer_pendo_export_folders(pendo_prefix)
            stable_id = folders["stable_folder_id"]
            dated_id = folders["dated_folder_id"]
            dated_label = folders["dated_label"]

            with export_phase(diag, "Drive upload"):
                if md:
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
                if json_text:
                    upload_text_file_to_drive_folder(
                        f"{stem}.json", json_text, dated_id, mime_type="application/json"
                    )
                    upload_text_file_to_drive_folder(
                        f"{stem}.json", json_text, stable_id, mime_type="application/json"
                    )

        from .data_source_health import integration_freshness_metadata

        diag.set_integration_meta(integration_freshness_metadata())
        diag.emit_run_summary(job_name="export-pendo", fail_on_warnings=False)


def main() -> None:
    export_pendo_main(None)


# Back-compat alias (internal callers).
export_customer_main = export_pendo_main


if __name__ == "__main__":
    main()
