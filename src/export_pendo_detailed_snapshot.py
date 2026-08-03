#!/usr/bin/env python3
"""Site- and user-level Pendo usage exports (extends account-level Ford-style reports).

Usage:
  cortex --export-pendo-detailed --customer Ford [--days 30] [--compare-days 30]
  cortex --export-pendo-top-arr [--top-n 5] [--days 30] [--compare-days 30]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from .config import logger
from .export_customer_pendo_snapshot import (
    _day_in_window,
    _fetch_activity_day_buckets,
    _md_section,
    _pct_change,
    _sum_activity_in_window,
    _write_local,
    build_customer_pendo_export_report,
    render_customer_pendo_markdown,
    render_csr_markdown,
    resolve_site_business_unit,
)
from .export_run_diagnostics import export_diagnostics_scope, export_phase
from .pendo_client import PendoClient, _name_matches

_PROFILE_ID = "customer_pendo_detailed_export"
_MS_PER_DAY = 86_400_000
_DEFAULT_TOP_N = 5
_DEFAULT_USER_ROSTER_MD_CAP = 250
_DEFAULT_SITE_USERS_CAP = 50
_DEFAULT_SITE_DETAIL_USER_SITES = 20
_ROSTER_SCOPE = "active_30d_or_window_events"


def _site_detail_user_sites_cap() -> int:
    """How many top sites (by events) get a per-site user table in §13.2."""
    raw = os.environ.get(
        "CORTEX_PENDO_SITE_DETAIL_USER_SITES", str(_DEFAULT_SITE_DETAIL_USER_SITES)
    ).strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_SITE_DETAIL_USER_SITES


def _md_cell(value: Any) -> str:
    """Render a markdown table cell: collapse newlines and escape pipes."""
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _customer_is_bu_mapped(customer_prefix: str) -> bool:
    """True when the customer has a business-unit mapping (empty name yields the default BU)."""
    return resolve_site_business_unit(customer_prefix, "") is not None


def _primary_business_unit(customer_prefix: str, sites: list[str] | None) -> str | None:
    """Most common business unit across a user's sites (mode; first-seen tie-break)."""
    counts: dict[str, int] = {}
    order: list[str] = []
    for site in sites or []:
        bu = resolve_site_business_unit(customer_prefix, str(site or ""))
        if not bu:
            continue
        if bu not in counts:
            order.append(bu)
        counts[bu] = counts.get(bu, 0) + 1
    if not counts:
        return None
    return max(order, key=lambda bu: (counts[bu], -order.index(bu)))


def _site_users_cap() -> int:
    raw = os.environ.get("CORTEX_PENDO_SITE_USERS_CAP", str(_DEFAULT_SITE_USERS_CAP)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_SITE_USERS_CAP


def _roster_max_users() -> int:
    raw = os.environ.get("CORTEX_PENDO_ROSTER_MAX_USERS", "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _roster_user_relevant(row: dict[str, Any]) -> bool:
    """Keep users active in the last 30d or with any events in the export window."""
    if int(row.get("events_current") or 0) > 0:
        return True
    return float(row.get("days_inactive") or 999) <= 30


def _cap_site_users(users: list[dict[str, Any]], *, cap: int) -> tuple[list[dict[str, Any]], int]:
    total = len(users)
    if total <= cap:
        return users, total
    ranked = sorted(users, key=lambda u: (float(u.get("days_inactive") or 999), str(u.get("email") or "")))
    return ranked[:cap], total


def _pendo_detailed_export_file_stem(customer: str, days: int) -> str:
    label = (customer or "").strip() or "customer"
    return f"Pendo Detailed Export  ({label}, {days}d)"


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


def _index_rows_by_visitor(
    page_rows: list[dict[str, Any]],
    feat_rows: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    page_by_visitor: dict[str, list[dict[str, Any]]] = {}
    feat_by_visitor: dict[str, list[dict[str, Any]]] = {}
    for ev in page_rows:
        vid = str(ev.get("visitorId") or "")
        if vid:
            page_by_visitor.setdefault(vid, []).append(ev)
    for ev in feat_rows:
        vid = str(ev.get("visitorId") or "")
        if vid:
            feat_by_visitor.setdefault(vid, []).append(ev)
    return page_by_visitor, feat_by_visitor


def _sum_activity_indexed(
    page_by_visitor: dict[str, list[dict[str, Any]]],
    feat_by_visitor: dict[str, list[dict[str, Any]]],
    visitor_ids: set[str],
    start_ms: int,
    end_ms: int,
) -> dict[str, int | float]:
    page_events = page_minutes = feature_events = 0
    for vid in visitor_ids:
        for ev in page_by_visitor.get(vid, ()):
            if not _day_in_window(ev.get("day"), start_ms, end_ms):
                continue
            page_events += int(ev.get("numEvents") or 0)
            page_minutes += int(ev.get("numMinutes") or 0)
        for ev in feat_by_visitor.get(vid, ()):
            if not _day_in_window(ev.get("day"), start_ms, end_ms):
                continue
            feature_events += int(ev.get("numEvents") or 0)
    total_events = page_events + feature_events
    return {
        "total_events": total_events,
        "page_events": page_events,
        "page_minutes": page_minutes,
        "feature_events": feature_events,
    }


def _index_merged_events_by_visitor(
    events: list[dict[str, Any]],
    visitor_ids: set[str],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for ev in events:
        vid = str(ev.get("visitorId") or "")
        if vid in visitor_ids:
            out.setdefault(vid, []).append(ev)
    return out


def _top_pages_and_features_from_merged(
    page_by_visitor: dict[str, list[dict[str, Any]]],
    feat_by_visitor: dict[str, list[dict[str, Any]]],
    visitor_ids: set[str],
    *,
    page_catalog: dict[str, str],
    feature_catalog: dict[str, str],
    limit: int = 8,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    page_counts: dict[str, dict[str, int]] = {}
    feat_counts: dict[str, int] = {}
    for vid in visitor_ids:
        for ev in page_by_visitor.get(vid, ()):
            pid = str(ev.get("pageId") or "")
            if not pid:
                continue
            bucket = page_counts.setdefault(pid, {"events": 0, "minutes": 0})
            bucket["events"] += int(ev.get("numEvents") or 0)
            bucket["minutes"] += int(ev.get("numMinutes") or 0)
        for ev in feat_by_visitor.get(vid, ()):
            fid = str(ev.get("featureId") or "")
            if not fid:
                continue
            feat_counts[fid] = feat_counts.get(fid, 0) + int(ev.get("numEvents") or 0)
    top_pages = [
        {"name": page_catalog.get(pid, pid), "events": c["events"], "minutes": c["minutes"]}
        for pid, c in sorted(page_counts.items(), key=lambda x: -x[1]["events"])[:limit]
    ]
    top_features = [
        {"name": feature_catalog.get(fid, fid), "events": count}
        for fid, count in sorted(feat_counts.items(), key=lambda x: -x[1])[:limit]
    ]
    return top_pages, top_features


def _top_pages_and_features_in_window(
    page_by_visitor: dict[str, list[dict[str, Any]]],
    feat_by_visitor: dict[str, list[dict[str, Any]]],
    visitor_ids: set[str],
    *,
    start_ms: int,
    end_ms: int,
    page_catalog: dict[str, str],
    feature_catalog: dict[str, str],
    limit: int = 8,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    page_counts: dict[str, dict[str, int]] = {}
    feat_counts: dict[str, int] = {}
    for vid in visitor_ids:
        for ev in page_by_visitor.get(vid, ()):
            if not _day_in_window(ev.get("day"), start_ms, end_ms):
                continue
            pid = str(ev.get("pageId") or "")
            if not pid:
                continue
            bucket = page_counts.setdefault(pid, {"events": 0, "minutes": 0})
            bucket["events"] += int(ev.get("numEvents") or 0)
            bucket["minutes"] += int(ev.get("numMinutes") or 0)
        for ev in feat_by_visitor.get(vid, ()):
            if not _day_in_window(ev.get("day"), start_ms, end_ms):
                continue
            fid = str(ev.get("featureId") or "")
            if not fid:
                continue
            feat_counts[fid] = feat_counts.get(fid, 0) + int(ev.get("numEvents") or 0)
    top_pages = [
        {"name": page_catalog.get(pid, pid), "events": c["events"], "minutes": c["minutes"]}
        for pid, c in sorted(page_counts.items(), key=lambda x: -x[1]["events"])[:limit]
    ]
    top_features = [
        {"name": feature_catalog.get(fid, fid), "events": count}
        for fid, count in sorted(feat_counts.items(), key=lambda x: -x[1])[:limit]
    ]
    return top_pages, top_features


def _site_names_from_account_sites(sites_payload: dict[str, Any]) -> list[str]:
    """Unique sitenames from the account export Sites table (section 2).

    Merges entity-level rows that share a sitename and keeps only sites with
    activity in the export window (``total_events > 0``), so ``site_detail`` is not
    inflated by historical sitename strings on visitor profiles.
    """
    by_name: dict[str, dict[str, Any]] = {}
    for row in sites_payload.get("sites") or []:
        sn = str(row.get("sitename") or "").strip()
        if not sn:
            continue
        key = sn.lower()
        bucket = by_name.setdefault(
            key,
            {"sitename": sn, "visitors": 0, "total_events": 0},
        )
        bucket["visitors"] += int(row.get("visitors") or 0)
        bucket["total_events"] += int(row.get("total_events") or 0)
    active = [v for v in by_name.values() if int(v.get("total_events") or 0) > 0]
    active.sort(
        key=lambda v: (
            -int(v.get("total_events") or 0),
            -int(v.get("visitors") or 0),
            str(v.get("sitename") or "").lower(),
        )
    )
    return [str(v["sitename"]) for v in active]


def _filter_rows_for_visitors(
    rows: list[dict[str, Any]],
    visitor_ids: set[str],
) -> list[dict[str, Any]]:
    if not visitor_ids:
        return []
    return [ev for ev in rows if str(ev.get("visitorId") or "") in visitor_ids]


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
    canonical_site_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Per-sitename mini-reports for one Pendo customer prefix."""
    window_days = max(1, int(days))
    compare = max(1, int(compare_days))
    current_start_ms = now_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY

    customer_visitor_ids = {
        str(v.get("visitorId"))
        for v in customer_visitors
        if v.get("visitorId")
    }
    customer_page_rows = _filter_rows_for_visitors(page_rows, customer_visitor_ids)
    customer_feat_rows = _filter_rows_for_visitors(feat_rows, customer_visitor_ids)
    page_by_visitor, feat_by_visitor = _index_rows_by_visitor(customer_page_rows, customer_feat_rows)
    merged_page_by_visitor = _index_merged_events_by_visitor(
        pc._get_page_events_cached(window_days),
        customer_visitor_ids,
    )
    merged_feat_by_visitor = _index_merged_events_by_visitor(
        pc._get_feature_events_cached(window_days),
        customer_visitor_ids,
    )
    page_catalog = pc._get_page_catalog_cached()
    feature_catalog = pc.get_feature_catalog()

    site_names = list(canonical_site_names or [])
    logger.info(
        "Pendo detailed: building %d site slice(s) for %r (%d visitors; active account sites)",
        len(site_names),
        pendo_prefix,
        len(customer_visitor_ids),
    )

    slices: list[dict[str, Any]] = []
    for sitename in site_names:
        site_visitors = _visitors_for_sitename(customer_visitors, sitename)
        visitor_ids = {str(v.get("visitorId")) for v in site_visitors if v.get("visitorId")}
        current = _sum_activity_indexed(
            page_by_visitor, feat_by_visitor, visitor_ids, current_start_ms, now_ms
        )
        prior = _sum_activity_indexed(
            page_by_visitor, feat_by_visitor, visitor_ids, prior_start_ms, prior_end_ms
        )
        users = pc._build_user_activity(site_visitors, now_ms)
        engagement = {"active_7d": 0, "active_30d": 0, "dormant": 0}
        for u in users:
            engagement[_engagement_bucket(float(u.get("days_inactive") or 999))] += 1
        top_pages, top_features = _top_pages_and_features_from_merged(
            merged_page_by_visitor,
            merged_feat_by_visitor,
            visitor_ids,
            page_catalog=page_catalog,
            feature_catalog=feature_catalog,
        )
        site_users, users_total = _cap_site_users(users, cap=_site_users_cap())
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
                "users": site_users,
                "users_total": users_total,
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
    """Customer visitors active in 30d or with window activity (see ``_ROSTER_SCOPE``)."""
    window_days = max(1, int(days))
    compare = max(1, int(compare_days))
    current_start_ms = now_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY

    customer_visitor_ids = {
        str(v.get("visitorId"))
        for v in customer_visitors
        if v.get("visitorId")
    }
    customer_page_rows = _filter_rows_for_visitors(page_rows, customer_visitor_ids)
    customer_feat_rows = _filter_rows_for_visitors(feat_rows, customer_visitor_ids)
    page_by_visitor, feat_by_visitor = _index_rows_by_visitor(customer_page_rows, customer_feat_rows)

    roster: list[dict[str, Any]] = []
    for v in customer_visitors:
        vid = str(v.get("visitorId") or "")
        if not vid:
            continue
        agent = (v.get("metadata") or {}).get("agent") or {}
        auto = (v.get("metadata") or {}).get("auto") or {}
        lv = auto.get("lastvisit", 0)
        days_inactive = (now_ms - lv) / (86400 * 1000) if lv else 999.0
        current = _sum_activity_indexed(
            page_by_visitor, feat_by_visitor, {vid}, current_start_ms, now_ms
        )
        if days_inactive > 30 and int(current.get("total_events") or 0) <= 0:
            continue
        prior = _sum_activity_indexed(
            page_by_visitor, feat_by_visitor, {vid}, prior_start_ms, prior_end_ms
        )
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
            -int(r.get("events_current") or 0),
            r.get("engagement_status") != "active_7d",
            r.get("days_inactive", 999),
            r.get("email") or "",
        )
    )
    max_users = _roster_max_users()
    if max_users and len(roster) > max_users:
        roster = roster[:max_users]
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
    logger.info(
        "Pendo detailed: enriching %r with site/user slices (%d visitors)",
        pendo_prefix,
        len(customer_visitors),
    )
    site_detail = build_site_detail_slices(
        pc,
        pendo_prefix,
        days=window_days,
        compare_days=compare,
        customer_visitors=customer_visitors,
        page_rows=page_rows,
        feat_rows=feat_rows,
        now_ms=now_ms,
        canonical_site_names=_site_names_from_account_sites(account.get("sites") or {}),
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
    logger.info(
        "Pendo detailed: roster %d of %d visitors for %r (%s)",
        len(user_roster),
        len(customer_visitors),
        pendo_prefix,
        _ROSTER_SCOPE,
    )

    meta = dict(account.get("meta") or {})
    meta["profile_id"] = _PROFILE_ID
    meta["granularity"] = "account_site_user"
    meta["site_count"] = len(site_detail)
    meta["user_roster_total_visitors"] = len(customer_visitors)
    meta["user_roster_count"] = len(user_roster)
    meta["user_roster_scope"] = _ROSTER_SCOPE

    return {
        **account,
        "meta": meta,
        "site_detail": site_detail,
        "user_roster": user_roster,
    }


def render_site_detail_markdown(
    site_detail: list[dict[str, Any]],
    *,
    compare_days: int,
    customer_prefix: str = "",
) -> str:
    """Render §13 as a site-activity table (§13.1) plus per-site user drill-down (§13.2).

    The old prose-block-per-site layout produced thousands of near-duplicate lines for
    large accounts. A single table is far more LLM-queryable ("which E&D sites are
    declining?" is one filter) and much cheaper in tokens. Per-site user tables are kept
    only for the busiest sites (by events); the full user list lives in §14.
    """
    if not site_detail:
        return _md_section("13. Site detail", "*(No sites with matching visitor metadata.)*")

    show_bu = resolve_site_business_unit(customer_prefix, str(site_detail[0].get("sitename") or "")) is not None

    header = ["Site"]
    if show_bu:
        header.append("Business unit")
    header += [
        "Visitors", "7d", "30d", "Dormant", "Events", "Minutes",
        "Feature clicks", "Δ events %", "Top page", "Top feature",
    ]
    align = ["---"] + (["---"] if show_bu else []) + [
        "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---:", "---", "---",
    ]
    table = [f"| {' | '.join(header)} |", f"| {' | '.join(align)} |"]
    for site in site_detail:
        eng = site.get("engagement") or {}
        cur = site.get("activity_current") or {}
        cmp_ = site.get("activity_pct_change") or {}
        top_pages = site.get("top_pages") or []
        top_features = site.get("top_features") or []
        top_page = top_pages[0].get("name") if top_pages else ""
        top_feature = top_features[0].get("name") if top_features else ""
        delta = cmp_.get("total_events")
        row = [_md_cell(site.get("sitename"))]
        if show_bu:
            row.append(_md_cell(resolve_site_business_unit(customer_prefix, str(site.get("sitename") or "")) or "Unclassified"))
        row += [
            f"{int(site.get('visitors') or 0):,}",
            f"{int(eng.get('active_7d') or 0):,}",
            f"{int(eng.get('active_30d') or 0):,}",
            f"{int(eng.get('dormant') or 0):,}",
            f"{int(cur.get('total_events') or 0):,}",
            f"{int(cur.get('page_minutes') or 0):,}",
            f"{int(cur.get('feature_events') or 0):,}",
            (f"{delta}" if delta is not None else "n/a"),
            _md_cell(top_page),
            _md_cell(top_feature),
        ]
        table.append(f"| {' | '.join(row)} |")

    activity = "### 13.1 Site activity\n\n" + "\n".join(table) + f"\n\n*All {len(site_detail)} active sites (sorted by events). Per-site top page/feature shown; deeper page/feature detail is in §3.*"

    user_sites_cap = _site_detail_user_sites_cap()
    detail_blocks: list[str] = []
    for site in site_detail[:user_sites_cap]:
        site_users = site.get("users") or []
        if not site_users:
            continue
        users_total = int(site.get("users_total") or len(site_users))
        block = [
            f"#### {site.get('sitename')}",
            "",
            "| User | Role | Last visit | Days inactive |",
            "| --- | --- | --- | ---: |",
        ]
        for u in site_users[:15]:
            block.append(
                f"| {_md_cell(u.get('email'))} | {_md_cell(u.get('role'))} | "
                f"{_md_cell(u.get('last_visit'))} | {u.get('days_inactive', '')} |"
            )
        shown = min(15, len(site_users))
        if users_total > shown:
            block.append(f"\n*Showing {shown} of {users_total} users at this site.*")
        detail_blocks.append("\n".join(block))

    body = activity
    if detail_blocks:
        capped = min(user_sites_cap, len(site_detail))
        body += (
            f"\n\n### 13.2 Site user detail (top {capped} sites by events)\n\n"
            "Per-site user samples for the busiest sites. The complete user list is in "
            "§14 (roster).\n\n" + "\n\n".join(detail_blocks)
        )
    return _md_section("13. Site detail", body)


def render_user_roster_markdown(
    user_roster: list[dict[str, Any]],
    *,
    cap: int = _DEFAULT_USER_ROSTER_MD_CAP,
    total_visitors: int | None = None,
    roster_scope: str | None = None,
    customer_prefix: str = "",
) -> str:
    if not user_roster:
        return _md_section("14. User roster", "*(No users in roster.)*")

    show_bu = _customer_is_bu_mapped(customer_prefix)
    lines = [
        f"- Roster users: **{len(user_roster)}**"
        + (f" of **{total_visitors}** total visitors" if total_visitors else ""),
    ]
    if roster_scope:
        lines[0] += f" ({roster_scope})"
    if show_bu:
        lines.extend(["", "| Email | Role | Primary BU | Sites | Status | Last visit | Days inactive | Events | Minutes | Feature clicks | Δ events % |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ])
    else:
        lines.extend(["", "| Email | Role | Sites | Status | Last visit | Days inactive | Events | Minutes | Feature clicks | Δ events % |",
            "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ])
    shown = user_roster[:cap]
    for u in shown:
        sites = ", ".join(u.get("sites") or []) or "—"
        if len(sites) > 48:
            sites = sites[:45] + "…"
        pct = u.get("events_pct_change")
        pct_s = f"{pct}" if pct is not None else "n/a"
        bu_cell = ""
        if show_bu:
            bu_cell = f" {_primary_business_unit(customer_prefix, u.get('sites')) or '—'} |"
        lines.append(
            f"| {_md_cell(u.get('email'))} | {_md_cell(u.get('role'))} |{bu_cell} {_md_cell(sites)} | "
            f"{u.get('engagement_status', '')} | "
            f"{u.get('last_visit', '')} | {u.get('days_inactive', '')} | "
            f"{int(u.get('events_current') or 0):,} | {int(u.get('page_minutes_current') or 0):,} | "
            f"{int(u.get('feature_events_current') or 0):,} | {pct_s} |"
        )
    if len(user_roster) > cap:
        lines.append(f"\n*Showing {cap} of {len(user_roster)} roster users (spreadsheet export).*")
    return _md_section("14. User roster", "\n".join(lines))


def render_customer_pendo_detailed_markdown(report: dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    compare_days = int(meta.get("compare_days") or meta.get("days") or 30)
    base = render_customer_pendo_markdown(report).rstrip()
    customer_prefix = str(meta.get("pendo_prefix") or meta.get("customer_query") or "")
    extra = render_site_detail_markdown(
        report.get("site_detail") or [],
        compare_days=compare_days,
        customer_prefix=customer_prefix,
    )
    meta = report.get("meta") or {}
    extra += render_user_roster_markdown(
        report.get("user_roster") or [],
        total_visitors=meta.get("user_roster_total_visitors"),
        roster_scope=meta.get("user_roster_scope"),
        customer_prefix=customer_prefix,
    )
    csr_md = render_csr_markdown(report, section_number=15)
    if csr_md:
        extra += csr_md
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
        from .export_drive_layout import ensure_customer_export_folders, upload_pendo_markdown_and_spreadsheet

        folders = ensure_customer_export_folders(pendo_prefix)
        urls = upload_pendo_markdown_and_spreadsheet(
            stem=stem,
            md=md,
            report=report,
            persistent_folder_id=folders["persistent_folder_id"],
            historical_folder_id=folders["historical_folder_id"],
            base_label=folders["base_label"],
        )
        print(
            f"Persistent markdown: https://drive.google.com/file/d/{urls['persistent_md_id']}/view",
            file=sys.stderr,
        )
        print(
            f"Historical markdown: https://drive.google.com/file/d/{urls['historical_md_id']}/view",
            file=sys.stderr,
        )
        print(f"Spreadsheet (persistent): {urls['persistent_spreadsheet_url']}", file=sys.stderr)
        print(f"Spreadsheet (historical):  {urls['historical_spreadsheet_url']}", file=sys.stderr)


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
