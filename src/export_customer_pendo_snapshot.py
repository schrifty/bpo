#!/usr/bin/env python3
"""Export single-customer Pendo usage snapshots to Google Drive (markdown + Sheet).

Designed for strategic accounts that track product usage across sites (e.g. Ford daily).

Usage:
  cortex --export-pendo --customer Ford [--days 30] [--compare-days 30] [--no-drive] [-o PATH]
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import re
import sys
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
import yaml

from .config import logger
from .config_paths import PENDO_CORE_FEATURES_FILE, PENDO_SITE_BU_MAP_FILE
from .export_run_diagnostics import export_diagnostics_scope, export_phase
from .pendo_client import PendoClient, _name_matches, _time_series
from .signals_trends import _snapshot_metrics

_PROFILE_ID = "customer_pendo_export"
_MS_PER_DAY = 86_400_000


def merge_active_site_rows(
    site_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    """Collapse raw ``(sitename, entity)`` rows into one row per sitename.

    Pendo's ``get_customer_sites`` returns a row per site-*entity* pair, so a single
    physical site can appear many times (Safran: ~5k rows for ~400 sitenames). For an
    LLM-queryable export we want one row per site, and only sites that were actually
    used in the window (events > 0).

    Aggregation across a sitename's entity rows is deliberately asymmetric:

    * **visitors** are summed — Pendo counts visitors per (sitename, entity) and a
      visitor carries a single entity, so entity rows partition the visitor set.
    * **events / minutes** take the **max**, not the sum. When entity-level usage is
      not populated, Pendo repeats the *site-level* total on every entity row (observed
      for Safran: ~12 identical rows). Summing would inflate a site's events several-fold.
      Max returns the true site total in that (dominant) case and merely under-counts in
      the rare case where usage really is entity-specific — far safer than gross inflation.

    Returns ``(active_rows_sorted, active_count, provisioned_count)`` where
    ``provisioned_count`` is the number of distinct sitenames before the active filter
    (i.e. everything provisioned, active or idle).
    """
    by_name: dict[str, dict[str, Any]] = {}
    for row in site_rows or []:
        sitename = str(row.get("sitename") or "").strip()
        if not sitename:
            continue
        bucket = by_name.setdefault(
            sitename.lower(),
            {
                "sitename": sitename,
                "visitors": 0,
                "total_events": 0,
                "total_minutes": 0,
                "last_active": "",
            },
        )
        bucket["visitors"] += int(row.get("visitors") or 0)
        bucket["total_events"] = max(bucket["total_events"], int(row.get("total_events") or 0))
        bucket["total_minutes"] = max(bucket["total_minutes"], int(row.get("total_minutes") or 0))
        last_active = str(row.get("last_active") or "").strip()
        if last_active and last_active != "N/A" and last_active > bucket["last_active"]:
            bucket["last_active"] = last_active
    provisioned_count = len(by_name)
    active_rows = [r for r in by_name.values() if int(r.get("total_events") or 0) > 0]
    active_rows.sort(
        key=lambda r: (
            -int(r.get("total_events") or 0),
            -int(r.get("visitors") or 0),
            str(r.get("sitename") or "").lower(),
        )
    )
    return active_rows, len(active_rows), provisioned_count


@lru_cache(maxsize=1)
def _load_site_bu_map() -> dict[str, Any]:
    if not PENDO_SITE_BU_MAP_FILE.is_file():
        return {"customers": {}}
    data = yaml.safe_load(PENDO_SITE_BU_MAP_FILE.read_text(encoding="utf-8")) or {}
    return {"customers": dict(data.get("customers") or {})}


_BU_UNMAPPED = "Unmapped — needs review"


def _bu_rules_for_customer(customer_prefix: str) -> tuple[list[dict[str, Any]], str] | None:
    """Return ``(rules, default_business_unit)`` for a customer, or ``None`` if unmapped.

    Each rule carries a ``confidence`` of ``"high"`` (sitename self-labels its division)
    or ``"inferred"`` (mapped by a location/brand guess that needs confirmation).
    """
    customers = _load_site_bu_map().get("customers") or {}
    target = str(customer_prefix or "").strip().lower()
    entry = None
    for key, val in customers.items():
        if str(key).strip().lower() == target:
            entry = val
            break
    if not isinstance(entry, dict):
        return None
    rules: list[dict[str, Any]] = []
    for rule in entry.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        bu = str(rule.get("business_unit") or "").strip()
        patterns = [str(p) for p in (rule.get("patterns") or []) if str(p).strip()]
        confidence = str(rule.get("confidence") or "high").strip().lower()
        if confidence not in ("high", "inferred"):
            confidence = "high"
        if bu and patterns:
            rules.append({"business_unit": bu, "patterns": patterns, "confidence": confidence})
    default_bu = str(entry.get("default_business_unit") or _BU_UNMAPPED).strip()
    return rules, default_bu


def resolve_site_business_unit_detail(customer_prefix: str, sitename: str) -> tuple[str | None, str]:
    """Map a sitename to ``(business_unit, confidence)``.

    ``confidence`` is one of:
      * ``"high"``     — matched a self-labeling rule
      * ``"inferred"`` — matched a location/brand guess (needs CS confirmation)
      * ``"none"``     — no rule matched; fell back to the default (needs review)
      * ``"unmapped_customer"`` — the customer has no mapping (``business_unit`` is ``None``)
    """
    resolved = _bu_rules_for_customer(customer_prefix)
    if resolved is None:
        return None, "unmapped_customer"
    rules, default_bu = resolved
    lowered = str(sitename or "").lower()
    for rule in rules:
        if any(pat.lower() in lowered for pat in rule["patterns"]):
            return rule["business_unit"], rule["confidence"]
    return default_bu, "none"


def resolve_site_business_unit(customer_prefix: str, sitename: str) -> str | None:
    """Map a sitename to a business unit via ``config/pendo_site_bu_map.yaml``.

    Returns ``None`` when the customer has no BU mapping configured, so callers can
    omit the BU column / §2.1 section rather than emit an all-unmapped view.
    """
    bu, _ = resolve_site_business_unit_detail(customer_prefix, sitename)
    return bu


def business_unit_review_sites(
    customer_prefix: str,
    active_sites: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Active sites whose business unit is *inferred* or *unmapped* — i.e. need review.

    Returns ``[]`` for unmapped customers. Each item is
    ``{"sitename", "business_unit", "confidence"}`` with confidence in
    ``{"inferred", "none"}``.
    """
    if _bu_rules_for_customer(customer_prefix) is None:
        return []
    out: list[dict[str, Any]] = []
    for site in active_sites:
        sitename = str(site.get("sitename") or "")
        bu, confidence = resolve_site_business_unit_detail(customer_prefix, sitename)
        if confidence in ("inferred", "none"):
            out.append({"sitename": sitename, "business_unit": bu, "confidence": confidence})
    return out


def build_business_unit_summary(
    customer_prefix: str,
    active_sites: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate deduped active sites by business unit.

    Returns ``[]`` when the customer is unmapped. ``total_events`` / ``total_minutes``
    sum cleanly here because ``active_sites`` are already one-row-per-site (entity rows
    merged), so there is no cross-entity double-count.
    """
    if _bu_rules_for_customer(customer_prefix) is None:
        return []
    groups: dict[str, dict[str, Any]] = {}
    for site in active_sites:
        bu = resolve_site_business_unit(customer_prefix, site.get("sitename", "")) or "Unclassified"
        events = int(site.get("total_events") or 0)
        group = groups.setdefault(
            bu,
            {
                "business_unit": bu,
                "site_count": 0,
                "visitors": 0,
                "total_events": 0,
                "total_minutes": 0,
                "top_site": "",
                "_top_site_events": -1,
            },
        )
        group["site_count"] += 1
        group["visitors"] += int(site.get("visitors") or 0)
        group["total_events"] += events
        group["total_minutes"] += int(site.get("total_minutes") or 0)
        if events > group["_top_site_events"]:
            group["_top_site_events"] = events
            group["top_site"] = str(site.get("sitename") or "")
    rows = list(groups.values())
    for row in rows:
        row.pop("_top_site_events", None)
    rows.sort(
        key=lambda g: (-int(g["total_events"]), -int(g["site_count"]), str(g["business_unit"]))
    )
    return rows


def _pendo_export_file_stem(customer: str, days: int) -> str:
    """Return filename stem (no extension), e.g. ``Pendo Export  (Ford, 30d)``."""
    label = (customer or "").strip() or "customer"
    return f"Pendo Export  ({label}, {days}d)"


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


def _customer_visitor_ids(pc: PendoClient, customer: str, days: int) -> set[str]:
    partition = pc._get_visitor_partition(days)
    customer_visitors, _ = pc._filter_customer_visitors(customer, partition)
    return {str(v.get("visitorId")) for v in customer_visitors if v.get("visitorId")}


def _activity_aggregate_read_timeout(total_days: int) -> float:
    """Scale Pendo read timeout for timeSeries aggregates (more days → longer)."""
    return min(300.0, max(90.0, 90.0 + (total_days - 14) * 3.0))


# Pendo responses worth retrying: rate limiting (429) and transient server errors.
_RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
# Random backoff jitter (seconds) added to the base wait so parallel workers don't
# retry in lockstep after a shared 429/5xx.
_RETRY_JITTER_S = 2.0
# Upper bound on a single backoff (incl. a server Retry-After) so a pathological
# hint can't stall a batch run.
_RETRY_MAX_WAIT_S = 60.0


def _retry_after_seconds(exc: requests.exceptions.RequestException) -> float | None:
    """Return the ``Retry-After`` header (seconds form) from a response, if present."""
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None)
    raw = headers.get("Retry-After") if headers else None
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        # HTTP-date form is not honored; fall back to computed backoff.
        return None


def _aggregate_with_retry(
    pc: PendoClient,
    pipeline: list[dict[str, Any]],
    *,
    total_days: int,
    label: str,
    max_attempts: int = 3,
) -> dict[str, Any]:
    read_timeout = _activity_aggregate_read_timeout(total_days)
    timeout = (10, read_timeout)
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        retry_after: float | None = None
        try:
            return pc.aggregate(pipeline, timeout=timeout)
        except requests.exceptions.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in _RETRYABLE_HTTP_STATUS:
                raise
            last_exc = exc
            reason = f"HTTP {status}"
            retry_after = _retry_after_seconds(exc)
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            reason = "timed out"
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            reason = "connection error"

        if attempt >= max_attempts:
            raise last_exc  # type: ignore[misc]
        if retry_after is not None:
            wait = min(_RETRY_MAX_WAIT_S, retry_after)
        else:
            wait = min(_RETRY_MAX_WAIT_S, 5.0 * attempt + random.uniform(0.0, _RETRY_JITTER_S))
        logger.warning(
            "Pendo %s aggregate failed (%s, attempt %d/%d, read_timeout=%.0fs); retry in %.1fs",
            label,
            reason,
            attempt,
            max_attempts,
            read_timeout,
            wait,
        )
        time.sleep(wait)
    raise RuntimeError(f"Pendo {label} aggregate failed after {max_attempts} attempts") from last_exc


def _fetch_activity_day_buckets(pc: PendoClient, total_days: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Page and feature event rows with ``day`` buckets (not merged across days)."""
    ts = _time_series(total_days)
    page_raw = _aggregate_with_retry(
        pc,
        [{"source": {"pageEvents": None, "timeSeries": ts}}],
        total_days=total_days,
        label="pageEvents",
    ).get("results") or []
    feat_raw = _aggregate_with_retry(
        pc,
        [{"source": {"featureEvents": None, "timeSeries": ts}}],
        total_days=total_days,
        label="featureEvents",
    ).get("results") or []
    return (
        [ev for ev in page_raw if isinstance(ev, dict)],
        [ev for ev in feat_raw if isinstance(ev, dict)],
    )


def _day_in_window(day_ms: int | None, start_ms: int, end_ms: int) -> bool:
    if day_ms is None:
        return False
    try:
        day = int(day_ms)
    except (TypeError, ValueError):
        return False
    return start_ms <= day < end_ms


def _sum_activity_in_window(
    page_rows: list[dict[str, Any]],
    feat_rows: list[dict[str, Any]],
    visitor_ids: set[str],
    start_ms: int,
    end_ms: int,
) -> dict[str, int | float]:
    page_events = page_minutes = feature_events = 0
    for ev in page_rows:
        if str(ev.get("visitorId")) not in visitor_ids:
            continue
        if not _day_in_window(ev.get("day"), start_ms, end_ms):
            continue
        page_events += int(ev.get("numEvents") or 0)
        page_minutes += int(ev.get("numMinutes") or 0)
    for ev in feat_rows:
        if str(ev.get("visitorId")) not in visitor_ids:
            continue
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


def _feature_counts_in_window(
    feat_rows: list[dict[str, Any]],
    visitor_ids: set[str],
    start_ms: int,
    end_ms: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for ev in feat_rows:
        if str(ev.get("visitorId")) not in visitor_ids:
            continue
        if not _day_in_window(ev.get("day"), start_ms, end_ms):
            continue
        fid = str(ev.get("featureId") or "")
        if not fid:
            continue
        counts[fid] = counts.get(fid, 0) + int(ev.get("numEvents") or 0)
    return counts


def _rolling_average(values: list[int | float | None], window: int) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(values)):
        chunk = values[max(0, i - window + 1) : i + 1]
        nums = [float(v) for v in chunk if v is not None]
        out.append(round(sum(nums) / len(nums), 1) if nums else None)
    return out


@lru_cache(maxsize=1)
def _load_core_feature_specs() -> dict[str, Any]:
    if not PENDO_CORE_FEATURES_FILE.is_file():
        return {"defaults": [], "customers": {}}
    data = yaml.safe_load(PENDO_CORE_FEATURES_FILE.read_text(encoding="utf-8")) or {}
    return {
        "defaults": list(data.get("defaults") or []),
        "customers": dict(data.get("customers") or {}),
    }


def _core_feature_entries_for_customer(customer: str) -> list[dict[str, str]]:
    cfg = _load_core_feature_specs()
    customer_specs = (cfg.get("customers") or {}).get(customer) or []
    specs = customer_specs if customer_specs else (cfg.get("defaults") or [])
    out: list[dict[str, str]] = []
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        label = str(spec.get("label") or "").strip()
        match = str(spec.get("match") or "").strip()
        if label and match:
            out.append({"label": label, "match": match})
    return out


def _catalog_features_matching_pattern(feature_catalog: dict[str, str], pattern: str) -> list[dict[str, str]]:
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error:
        return []
    rows: list[dict[str, str]] = []
    for fid, name in feature_catalog.items():
        if rx.search(str(name or "")):
            rows.append({"feature_id": str(fid), "name": str(name)})
    rows.sort(key=lambda r: r["name"].lower())
    return rows


def build_core_feature_checklist(
    *,
    customer: str,
    feature_catalog: dict[str, str],
    feat_current: dict[str, int],
    feat_prior: dict[str, int],
) -> dict[str, Any]:
    """Adopted / not adopted / declining checklist from config/pendo_core_features.yaml."""
    entries: list[dict[str, Any]] = []
    for spec in _core_feature_entries_for_customer(customer):
        matched = _catalog_features_matching_pattern(feature_catalog, spec["match"])
        matched_ids = {row["feature_id"] for row in matched}
        current_events = sum(feat_current.get(fid, 0) for fid in matched_ids)
        prior_events = sum(feat_prior.get(fid, 0) for fid in matched_ids)
        if current_events <= 0:
            status = "not_adopted"
        elif prior_events >= 5 and current_events < prior_events * 0.72:
            status = "declining"
        else:
            status = "adopted"
        entries.append(
            {
                "label": spec["label"],
                "match": spec["match"],
                "status": status,
                "matched_features": matched,
                "events_current": current_events,
                "events_prior": prior_events,
                "events_pct_change": _pct_change(current_events, prior_events),
            }
        )
    summary = {
        "adopted": sum(1 for e in entries if e["status"] == "adopted"),
        "not_adopted": sum(1 for e in entries if e["status"] == "not_adopted"),
        "declining": sum(1 for e in entries if e["status"] == "declining"),
        "total_tracked": len(entries),
    }
    return {"summary": summary, "entries": entries}


def build_unused_features(
    feature_catalog: dict[str, str],
    feat_current: dict[str, int],
    *,
    limit: int = 100,
) -> dict[str, Any]:
    """Catalog features with zero customer usage in the current window."""
    unused: list[dict[str, Any]] = []
    for fid, name in feature_catalog.items():
        if feat_current.get(str(fid), 0) > 0:
            continue
        unused.append({"feature_id": str(fid), "name": str(name)})
    unused.sort(key=lambda r: r["name"].lower())
    return {
        "catalog_total": len(feature_catalog),
        "unused_count": len(unused),
        "unused_features": unused[:limit],
        "truncated": len(unused) > limit,
    }


def build_usage_trends(
    pc: PendoClient,
    customer: str,
    days: int,
    *,
    compare_days: int | None = None,
    visitor_ids: set[str] | None = None,
    day_buckets: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Prior-period comparison and weekly active-user / activity series."""
    compare = max(1, int(compare_days if compare_days is not None else days))
    window_days = max(1, int(days))
    total_lookback = window_days + compare

    partition = pc._get_visitor_partition(window_days)
    end_ms = int(partition["now_ms"])
    current_start_ms = end_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY

    vids = visitor_ids or _customer_visitor_ids(pc, customer, window_days)
    page_rows, feat_rows = day_buckets if day_buckets is not None else _fetch_activity_day_buckets(pc, total_lookback)

    current = _snapshot_metrics(pc, customer, current_start_ms, end_ms) or {}
    prior = _snapshot_metrics(pc, customer, prior_start_ms, prior_end_ms) or {}
    current_activity = _sum_activity_in_window(page_rows, feat_rows, vids, current_start_ms, end_ms)
    prior_activity = _sum_activity_in_window(page_rows, feat_rows, vids, prior_start_ms, prior_end_ms)

    weekly: list[dict[str, Any]] = []
    num_weeks = max(1, min(13, (window_days + 6) // 7))
    for i in range(num_weeks):
        w_end = end_ms - i * 7 * _MS_PER_DAY
        w_start = w_end - 7 * _MS_PER_DAY
        snap = _snapshot_metrics(pc, customer, w_start, w_end) or {}
        activity = _sum_activity_in_window(page_rows, feat_rows, vids, w_start, w_end)
        weekly.append(
            {
                "week_index": num_weeks - i,
                "window_start": dt.datetime.fromtimestamp(w_start / 1000, tz=dt.timezone.utc).date().isoformat(),
                "window_end": dt.datetime.fromtimestamp(w_end / 1000, tz=dt.timezone.utc).date().isoformat(),
                "active_users_7d": snap.get("active_7d"),
                "total_users": snap.get("total_users"),
                "weekly_active_rate_pct": snap.get("weekly_active_rate_pct"),
                "total_events": activity.get("total_events"),
                "page_events": activity.get("page_events"),
                "page_minutes": activity.get("page_minutes"),
                "feature_events": activity.get("feature_events"),
            }
        )
    weekly.sort(key=lambda r: r["week_index"])

    rolling_active = _rolling_average([row.get("active_users_7d") for row in weekly], 4)
    rolling_events = _rolling_average([row.get("total_events") for row in weekly], 4)
    for idx, row in enumerate(weekly):
        row["rolling_4w_avg_active_users"] = rolling_active[idx]
        row["rolling_4w_avg_total_events"] = rolling_events[idx]

    cur_rate = current.get("weekly_active_rate_pct")
    prev_rate = prior.get("weekly_active_rate_pct")
    return {
        "window_days": window_days,
        "compare_days": compare,
        "current_period": {**current, **current_activity},
        "prior_period": {**prior, **prior_activity},
        "comparison": {
            "active_users_7d_pct_change": _pct_change(current.get("active_7d"), prior.get("active_7d")),
            "total_users_pct_change": _pct_change(current.get("total_users"), prior.get("total_users")),
            "weekly_active_rate_pp_change": (
                round(float(cur_rate) - float(prev_rate), 1)
                if cur_rate is not None and prev_rate is not None
                else None
            ),
            "total_events_pct_change": _pct_change(current_activity.get("total_events"), prior_activity.get("total_events")),
            "page_minutes_pct_change": _pct_change(current_activity.get("page_minutes"), prior_activity.get("page_minutes")),
            "feature_events_pct_change": _pct_change(
                current_activity.get("feature_events"), prior_activity.get("feature_events")
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
    compare_days: int | None = None,
) -> dict[str, Any]:
    """Fetch Pendo usage slices for one customer (product adoption focus)."""
    pendo_prefix = resolve_pendo_customer_prefix(customer_query, pc)
    window_days = max(1, int(days))
    compare = max(1, int(compare_days if compare_days is not None else window_days))
    total_lookback = window_days + compare
    exported_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = dt.date.today()
    window_start = window_end - dt.timedelta(days=window_days - 1)

    pc.preload(max(window_days, total_lookback))

    health = pc.get_customer_health(pendo_prefix, days=window_days)
    if health.get("error"):
        return {"error": health["error"], "customer_query": customer_query, "pendo_prefix": pendo_prefix}

    sites = pc.get_customer_sites(pendo_prefix, days=window_days)
    if sites.get("error"):
        return {"error": sites["error"], "customer_query": customer_query, "pendo_prefix": pendo_prefix}

    features = pc.get_customer_features(pendo_prefix, days=window_days)
    depth = pc.get_customer_depth(pendo_prefix, days=window_days)
    kei = pc.get_customer_kei(pendo_prefix, days=window_days)
    people = pc.get_customer_people(pendo_prefix, days=window_days)
    exports = pc.get_customer_exports(pendo_prefix, days=window_days)
    frustration = pc.get_customer_frustration_signals(pendo_prefix, days=window_days)

    visitor_ids = _customer_visitor_ids(pc, pendo_prefix, window_days)
    day_buckets = _fetch_activity_day_buckets(pc, total_lookback)
    partition = pc._get_visitor_partition(window_days)
    end_ms = int(partition["now_ms"])
    current_start_ms = end_ms - window_days * _MS_PER_DAY
    prior_end_ms = current_start_ms
    prior_start_ms = prior_end_ms - compare * _MS_PER_DAY
    _, feat_rows = day_buckets
    feat_current = _feature_counts_in_window(feat_rows, visitor_ids, current_start_ms, end_ms)
    feat_prior = _feature_counts_in_window(feat_rows, visitor_ids, prior_start_ms, prior_end_ms)
    feature_catalog = pc.get_feature_catalog()
    core_checklist = build_core_feature_checklist(
        customer=pendo_prefix,
        feature_catalog=feature_catalog,
        feat_current=feat_current,
        feat_prior=feat_prior,
    )
    unused_features = build_unused_features(feature_catalog, feat_current)
    trends = build_usage_trends(
        pc,
        pendo_prefix,
        window_days,
        compare_days=compare,
        visitor_ids=visitor_ids,
        day_buckets=day_buckets,
    )

    report: dict[str, Any] = {
        "meta": {
            "profile_id": _PROFILE_ID,
            "exported_at_utc": exported_at,
            "customer_query": customer_query.strip(),
            "pendo_prefix": pendo_prefix,
            "days": window_days,
            "compare_days": compare,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
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
        "core_feature_checklist": core_checklist,
        "unused_features": unused_features,
        "depth": depth,
        "people": people,
        "exports": exports,
        "frustration": frustration,
        "kei": kei,
        "trends": trends,
    }
    return report


def _md_section(title: str, body: str) -> str:
    return f"## {title}\n\n{body.strip()}\n\n"


def render_customer_pendo_markdown(report: dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    headline = report.get("headline") or {}
    lines = [
        f"# Pendo usage — {meta.get('pendo_prefix') or meta.get('customer_query')}",
        "",
        f"- **Exported:** {meta.get('exported_at_utc')}",
        f"- **Window:** {meta.get('window_start')} → {meta.get('window_end')} ({meta.get('days')} days)",
        f"- **Compare window:** prior {meta.get('compare_days', meta.get('days'))} days",
        f"- **Pendo prefix:** `{meta.get('pendo_prefix')}`",
        "",
        "**How to read this export**",
        "- This file is **Pendo usage only** — no contract status, ARR, or churn "
        "(use the portfolio LLM-context export for those).",
        "- **Sites** = one row per *active* site (had events in the window); Pendo's "
        "internal per-entity duplicates are merged. The count of every provisioned site "
        "(active + idle) is in §1 only.",
        "- **Visitor counts can overlap across sites** — a user assigned to several sites "
        "is counted on each — so do **not** sum per-site or per-business-unit visitors to "
        "get unique headcount; use §1 total visitors for that.",
        "- When a **business unit** mapping exists for this customer, §2.1 rolls sites up "
        "by unit and §2 / §13.1 / §14 carry a business-unit column.",
        "- Sections are numbered (§1 headline; §2 sites, §2.1 business units; §3–§12 "
        "adoption, people, trends). The **detailed** export adds §13 site detail and §14 "
        "user roster; the §14 roster is complete in the companion spreadsheet even when "
        "the markdown table is capped.",
    ]

    md = "\n".join(lines) + "\n\n"

    pendo_prefix = str(meta.get("pendo_prefix") or meta.get("customer_query") or "")
    raw_site_rows = (report.get("sites") or {}).get("sites") or []
    active_sites, active_count, provisioned_count = merge_active_site_rows(raw_site_rows)
    provisioned_display = int(headline.get("total_sites") or provisioned_count or 0)
    idle_count = max(0, provisioned_display - active_count)

    cmp_ = headline.get("vs_prior_period") or {}
    compare_days = meta.get("compare_days", meta.get("days"))
    headline_lines = [
        f"- Active users (7d): **{headline.get('active_users_7d')}** "
        f"({cmp_.get('active_users_7d_pct_change')}% vs prior {compare_days}d)"
        if cmp_.get("active_users_7d_pct_change") is not None
        else f"- Active users (7d): **{headline.get('active_users_7d')}**",
        f"- Total visitors: **{headline.get('total_visitors')}** · "
        f"sites: **{active_count}** active of **{provisioned_display}** provisioned",
        f"- Weekly active rate: **{headline.get('weekly_active_rate_pct')}%**",
        f"- Events: **{headline.get('total_events'):,}** · minutes: **{headline.get('total_minutes'):,}**",
        f"- Feature events: **{headline.get('feature_events'):,}** · write ratio: **{headline.get('write_ratio_pct')}%**",
    ]
    if cmp_.get("total_events_pct_change") is not None:
        headline_lines.append(
            f"- Activity vs prior {compare_days}d: events **{cmp_.get('total_events_pct_change')}%** · "
            f"minutes **{cmp_.get('page_minutes_pct_change')}%** · "
            f"feature clicks **{cmp_.get('feature_events_pct_change')}%**"
        )
    md += _md_section("1. Headline", "\n".join(headline_lines))

    bu_summary = build_business_unit_summary(pendo_prefix, active_sites)
    show_bu = bool(bu_summary)
    site_lines = [
        f"- Active sites: **{active_count}** of **{provisioned_display}** provisioned "
        f"({idle_count} idle in window). One row per site; Pendo entity rows merged; "
        "only sites with events in the window are listed.",
        "",
    ]
    if show_bu:
        site_lines.append("| Site | Business unit | Visitors | Events | Minutes | Last active |")
        site_lines.append("| --- | --- | ---: | ---: | ---: | --- |")
        for s in active_sites:
            bu = resolve_site_business_unit(pendo_prefix, s.get("sitename", "")) or "Unclassified"
            site_lines.append(
                f"| {s.get('sitename', '')} | {bu} | {s.get('visitors', 0):,} | "
                f"{s.get('total_events', 0):,} | {s.get('total_minutes', 0):,} | {s.get('last_active', '')} |"
            )
        if not active_sites:
            site_lines.append("| _(no sites with activity in window)_ |  |  |  |  |  |")
    else:
        site_lines.append("| Site | Visitors | Events | Minutes | Last active |")
        site_lines.append("| --- | ---: | ---: | ---: | --- |")
        for s in active_sites:
            site_lines.append(
                f"| {s.get('sitename', '')} | {s.get('visitors', 0):,} | "
                f"{s.get('total_events', 0):,} | {s.get('total_minutes', 0):,} | {s.get('last_active', '')} |"
            )
        if not active_sites:
            site_lines.append("| _(no sites with activity in window)_ |  |  |  |  |")
    md += _md_section("2. Sites", "\n".join(site_lines))

    if show_bu:
        bu_lines = [
            "Active sites rolled up to business unit "
            "(mapping: `config/pendo_site_bu_map.yaml`). Visitors may overlap across "
            "sites, so per-BU visitor counts are associations, not unique headcount.",
            "",
            "| Business unit | Active sites | Visitors | Events | Minutes | Top site (by events) |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
        for row in bu_summary:
            bu_lines.append(
                f"| {row['business_unit']} | {row['site_count']:,} | {row['visitors']:,} | "
                f"{row['total_events']:,} | {row['total_minutes']:,} | {row['top_site']} |"
            )
        review = business_unit_review_sites(pendo_prefix, active_sites)
        if review:
            inferred = sum(1 for r in review if r["confidence"] == "inferred")
            unmapped = sum(1 for r in review if r["confidence"] == "none")
            parts: list[str] = []
            if inferred:
                parts.append(f"**{inferred}** inferred (location/brand guess, not confirmed)")
            if unmapped:
                parts.append(f"**{unmapped}** unmapped")
            bu_lines.append("")
            bu_lines.append(
                f"> **Confidence:** {' · '.join(parts)} of {len(active_sites)} active sites "
                "need Customer Success confirmation. See "
                "`docs/DATA-GOVERNANCE/BUSINESS_UNIT_MAPPING_REVIEW.md`."
            )
            logger.warning(
                "Pendo export %s: %d active site(s) have inferred/unmapped business units "
                "(need CS review): %s",
                pendo_prefix,
                len(review),
                ", ".join(r["sitename"] for r in review[:25]),
            )
        md += _md_section("2.1 Business unit summary", "\n".join(bu_lines))

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

    checklist = report.get("core_feature_checklist") or {}
    checklist_lines = [
        f"- Tracked: **{checklist.get('summary', {}).get('total_tracked', 0)}** · "
        f"adopted **{checklist.get('summary', {}).get('adopted', 0)}** · "
        f"not adopted **{checklist.get('summary', {}).get('not_adopted', 0)}** · "
        f"declining **{checklist.get('summary', {}).get('declining', 0)}**",
        "",
        "| Capability | Status | Events (current) | Events (prior) | Δ % |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for entry in checklist.get("entries") or []:
        checklist_lines.append(
            f"| {entry.get('label', '')} | {entry.get('status', '')} | "
            f"{entry.get('events_current', 0):,} | {entry.get('events_prior', 0):,} | "
            f"{entry.get('events_pct_change') if entry.get('events_pct_change') is not None else 'n/a'} |"
        )
    md += _md_section("4. Core feature checklist", "\n".join(checklist_lines))

    unused = report.get("unused_features") or {}
    unused_rows = unused.get("unused_features") or []
    unused_lines = [
        f"- Catalog features: **{unused.get('catalog_total', 0)}** · "
        f"unused in window: **{unused.get('unused_count', 0)}**",
        "",
    ]
    if unused_rows:
        unused_lines.extend(f"- {row.get('name')}" for row in unused_rows[:40])
        if unused.get("truncated"):
            unused_lines.append(f"\n*Showing 40 of {unused.get('unused_count')} unused features.*")
    else:
        unused_lines.append("*(All catalog features had at least one event in the window.)*")
    md += _md_section("5. Unused product features", "\n".join(unused_lines))

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
    md += _md_section("6. Behavioral depth", "\n".join(depth_lines))

    people = report.get("people") or {}
    people_lines: list[str] = []
    if people.get("error"):
        people_lines.append(f"*(unavailable: {people['error']})*")
    else:
        champions = people.get("champions") or []
        at_risk = people.get("at_risk_users") or []
        if champions:
            people_lines.append("### Champions (most recently active)")
            people_lines.append("| Email | Role | Last visit | Days inactive |")
            people_lines.append("| --- | --- | --- | ---: |")
            for u in champions[:5]:
                people_lines.append(
                    f"| {u.get('email', '')} | {u.get('role', '')} | "
                    f"{u.get('last_visit', '')} | {u.get('days_inactive', '')} |"
                )
            people_lines.append("")
        if at_risk:
            people_lines.append("### At-risk users (2 wk – ~6 mo inactive)")
            people_lines.append("| Email | Role | Last visit | Days inactive |")
            people_lines.append("| --- | --- | --- | ---: |")
            for u in at_risk[:5]:
                people_lines.append(
                    f"| {u.get('email', '')} | {u.get('role', '')} | "
                    f"{u.get('last_visit', '')} | {u.get('days_inactive', '')} |"
                )
        if not champions and not at_risk:
            people_lines.append("*(No champion or at-risk users in this window.)*")
    md += _md_section("7. People", "\n".join(people_lines))

    exports = report.get("exports") or {}
    export_lines: list[str] = []
    if exports.get("error"):
        export_lines.append(f"*(unavailable: {exports['error']})*")
    else:
        export_lines.extend(
            [
                f"- Total exports: **{exports.get('total_exports', 0):,}**",
                f"- Exports per active user: **{exports.get('exports_per_active_user', 0)}** "
                f"(active users: **{exports.get('active_users', 0)}**)",
            ]
        )
        by_feature = exports.get("by_feature") or []
        if by_feature:
            export_lines.append("")
            export_lines.append("**By feature:**")
            for row in by_feature[:15]:
                export_lines.append(f"- {row.get('feature', '')}: {row.get('exports', 0):,}")
        top_exporters = exports.get("top_exporters") or []
        if top_exporters:
            export_lines.append("")
            export_lines.append("**Top exporters:**")
            for row in top_exporters[:5]:
                export_lines.append(
                    f"- {row.get('email', '')} ({row.get('role', '')}): {row.get('exports', 0):,}"
                )
        if exports.get("note"):
            export_lines.append("")
            export_lines.append(f"*{exports['note']}*")
    md += _md_section("8. Export behavior", "\n".join(export_lines) or "*(no export data)*")

    frustration = report.get("frustration") or {}
    frustration_lines: list[str] = []
    if frustration.get("error"):
        frustration_lines.append(f"*(unavailable: {frustration['error']})*")
    else:
        totals = frustration.get("totals") or {}
        frustration_lines.append(
            f"- Total frustration signals: **{frustration.get('total_frustration_signals', 0):,}** "
            f"(rage {totals.get('rageClickCount', 0):,} · dead {totals.get('deadClickCount', 0):,} · "
            f"error {totals.get('errorClickCount', 0):,} · U-turn {totals.get('uTurnCount', 0):,})"
        )
        top_pages = frustration.get("top_pages") or []
        if top_pages:
            frustration_lines.append("")
            frustration_lines.append("**Top pages:**")
            frustration_lines.append("| Page | Rage | Dead | Error | U-turn |")
            frustration_lines.append("| --- | ---: | ---: | ---: | ---: |")
            for row in top_pages[:10]:
                frustration_lines.append(
                    f"| {row.get('page', '')} | {row.get('rageClickCount', 0)} | "
                    f"{row.get('deadClickCount', 0)} | {row.get('errorClickCount', 0)} | "
                    f"{row.get('uTurnCount', 0)} |"
                )
        top_features = frustration.get("top_features") or []
        if top_features:
            frustration_lines.append("")
            frustration_lines.append("**Top features:**")
            frustration_lines.append("| Feature | Rage | Dead | Error | U-turn |")
            frustration_lines.append("| --- | ---: | ---: | ---: | ---: |")
            for row in top_features[:10]:
                frustration_lines.append(
                    f"| {row.get('feature', '')} | {row.get('rageClickCount', 0)} | "
                    f"{row.get('deadClickCount', 0)} | {row.get('errorClickCount', 0)} | "
                    f"{row.get('uTurnCount', 0)} |"
                )
        if frustration.get("total_frustration_signals", 0) == 0:
            frustration_lines.append("")
            frustration_lines.append("*(No frustration signals in this window.)*")
    md += _md_section("9. Frustration signals", "\n".join(frustration_lines) or "*(no frustration data)*")

    kei = report.get("kei") or {}
    kei_lines = [
        f"- Total queries: **{kei.get('total_queries', 0):,}**",
        f"- Unique users: **{kei.get('unique_users', 0)}** · adoption: **{kei.get('adoption_rate', 0)}%**",
        f"- Executive users: **{kei.get('executive_users', 0)}** "
        f"({kei.get('executive_queries', 0):,} queries)",
    ]
    md += _md_section("10. Kei AI", "\n".join(kei_lines))

    trends = report.get("trends") or {}
    trend_lines = [
        "| Week | Start | End | Active (7d) | Events | Minutes | Feature clicks | 4w avg users | 4w avg events |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in trends.get("weekly_active_users") or []:
        trend_lines.append(
            f"| {row.get('week_index')} | {row.get('window_start')} | {row.get('window_end')} | "
            f"{row.get('active_users_7d', 0)} | {row.get('total_events', 0):,} | "
            f"{row.get('page_minutes', 0):,} | {row.get('feature_events', 0):,} | "
            f"{row.get('rolling_4w_avg_active_users', 'n/a')} | {row.get('rolling_4w_avg_total_events', 'n/a')} |"
        )
    cmp_ = trends.get("comparison") or {}
    if cmp_:
        trend_lines.append("")
        trend_lines.append(
            f"Prior {compare_days}d comparison: active users "
            f"{cmp_.get('active_users_7d_pct_change')}% · total users "
            f"{cmp_.get('total_users_pct_change')}% · WAU "
            f"{cmp_.get('weekly_active_rate_pp_change')} pp · events "
            f"{cmp_.get('total_events_pct_change')}% · minutes "
            f"{cmp_.get('page_minutes_pct_change')}%"
        )
    md += _md_section("11. Usage trends", "\n".join(trend_lines))

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
    md += _md_section("12. Engagement context", "\n".join(secondary))

    return md.rstrip() + "\n"


def ensure_customer_pendo_export_folders(customer: str) -> dict[str, str]:
    """Return persistent and historical folder ids under Output/Customer Exports/{customer}/."""
    from .export_drive_layout import ensure_customer_export_folders

    return ensure_customer_export_folders(customer)


def _write_local(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def export_pendo_main(cli_args: list[str] | None = None, *, prog: str | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Export single-customer Pendo usage snapshot (markdown + Google Sheet) to Drive.",
        prog=prog or "cortex --export-pendo",
    )
    ap.add_argument("--customer", required=True, help="Pendo customer prefix or alias (e.g. Ford)")
    ap.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    ap.add_argument(
        "--compare-days",
        type=int,
        default=None,
        help="Prior comparison window in days (default: same as --days)",
    )
    ap.add_argument("--no-drive", action="store_true", help="Skip Drive upload; write locally only")
    ap.add_argument(
        "-o",
        "--out",
        metavar="PATH",
        help="Local output path prefix (writes PATH.md and PATH.xlsx); default output/ when --no-drive",
    )
    args = ap.parse_args(cli_args)

    with export_diagnostics_scope() as diag:
        with export_phase(diag, "Pendo preload + customer export"):
            pc = PendoClient()
            report = build_customer_pendo_export_report(
                pc,
                args.customer,
                days=args.days,
                compare_days=args.compare_days,
            )

        if report.get("error"):
            print(f"error: {report['error']}", file=sys.stderr)
            sys.exit(1)

        pendo_prefix = (report.get("meta") or {}).get("pendo_prefix") or args.customer
        stem = _pendo_export_file_stem(pendo_prefix, args.days)
        md = render_customer_pendo_markdown(report)

        if args.no_drive or args.out:
            if args.out:
                base = Path(args.out)
                if base.suffix.lower() in (".md", ".xlsx"):
                    base = base.with_suffix("")
            else:
                base = Path("output") / stem
            _write_local(base.with_suffix(".md"), md)
            print(f"Wrote {base.with_suffix('.md')}")
            from .export_pendo_spreadsheet import write_pendo_export_xlsx

            write_pendo_export_xlsx(base.with_suffix(".xlsx"), report)
            print(f"Wrote {base.with_suffix('.xlsx')}")

        if not args.no_drive:
            from .export_drive_layout import ensure_customer_export_folders, upload_pendo_markdown_and_spreadsheet

            folders = ensure_customer_export_folders(pendo_prefix)
            with export_phase(diag, "Drive upload"):
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

        from .data_source_health import integration_freshness_metadata

        diag.set_integration_meta(integration_freshness_metadata())
        diag.emit_run_summary(job_name="export-pendo", fail_on_warnings=False)


def main() -> None:
    export_pendo_main(None)


# Back-compat alias (internal callers).
export_customer_main = export_pendo_main


if __name__ == "__main__":
    main()
