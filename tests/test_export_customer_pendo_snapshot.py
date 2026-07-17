"""Tests for single-customer Pendo usage export."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.export_customer_pendo_snapshot import (
    build_core_feature_checklist,
    build_customer_pendo_export_report,
    build_headline,
    business_unit_review_sites,
    build_business_unit_summary,
    build_unused_features,
    build_usage_trends,
    merge_active_site_rows,
    render_customer_pendo_markdown,
    resolve_site_business_unit,
    resolve_site_business_unit_detail,
    resolve_pendo_customer_prefix,
    _activity_aggregate_read_timeout,
    _aggregate_with_retry,
    _pendo_export_file_stem,
)
from src.job_runner import build_step_argv, load_job_spec


def test_pendo_export_file_stem_includes_granularity() -> None:
    assert _pendo_export_file_stem("Ford", 7) == "Pendo Export  (Ford, 7d)"
    assert _pendo_export_file_stem("Ford", 30) == "Pendo Export  (Ford, 30d)"


def test_activity_aggregate_read_timeout_scales_with_window() -> None:
    assert _activity_aggregate_read_timeout(14) == 90.0
    assert _activity_aggregate_read_timeout(7) == 90.0
    assert _activity_aggregate_read_timeout(60) == 228.0
    assert _activity_aggregate_read_timeout(120) == 300.0


def _http_error(status: int, *, retry_after: str | None = None) -> requests.exceptions.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    if retry_after is not None:
        resp.headers["Retry-After"] = retry_after
    return requests.exceptions.HTTPError(f"{status}", response=resp)


@patch("src.export_customer_pendo_snapshot.random.uniform", return_value=0.0)
@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_succeeds_after_timeout(mock_sleep, _mock_jitter) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = [
        requests.exceptions.ReadTimeout("timed out"),
        {"results": [{"visitorId": "v1"}]},
    ]
    out = _aggregate_with_retry(
        pc,
        [{"source": {"pageEvents": None}}],
        total_days=60,
        label="pageEvents",
    )
    assert out["results"][0]["visitorId"] == "v1"
    assert pc.aggregate.call_count == 2
    pc.aggregate.assert_called_with(
        [{"source": {"pageEvents": None}}],
        timeout=(10, 228.0),
    )
    mock_sleep.assert_called_once_with(5.0)


@patch("src.export_customer_pendo_snapshot.random.uniform", return_value=0.0)
@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_raises_after_max_attempts(mock_sleep, _mock_jitter) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = requests.exceptions.ReadTimeout("timed out")
    with pytest.raises(requests.exceptions.ReadTimeout):
        _aggregate_with_retry(
            pc,
            [{"source": {"featureEvents": None}}],
            total_days=30,
            label="featureEvents",
            max_attempts=3,
        )
    assert pc.aggregate.call_count == 3
    assert mock_sleep.call_count == 2


@patch("src.export_customer_pendo_snapshot.random.uniform", return_value=0.0)
@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_retries_on_429(mock_sleep, _mock_jitter) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = [_http_error(429), {"results": []}]
    out = _aggregate_with_retry(
        pc,
        [{"source": {"pageEvents": None}}],
        total_days=30,
        label="pageEvents",
    )
    assert out == {"results": []}
    assert pc.aggregate.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_honors_retry_after_header(mock_sleep) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = [_http_error(503, retry_after="12"), {"results": []}]
    _aggregate_with_retry(
        pc,
        [{"source": {"featureEvents": None}}],
        total_days=30,
        label="featureEvents",
    )
    mock_sleep.assert_called_once_with(12.0)


@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_caps_retry_after(mock_sleep) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = [_http_error(429, retry_after="9999"), {"results": []}]
    _aggregate_with_retry(
        pc,
        [{"source": {"pageEvents": None}}],
        total_days=30,
        label="pageEvents",
    )
    mock_sleep.assert_called_once_with(60.0)


@patch("src.export_customer_pendo_snapshot.random.uniform", return_value=0.0)
@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_retries_on_connection_error(mock_sleep, _mock_jitter) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = [
        requests.exceptions.ConnectionError("reset"),
        {"results": []},
    ]
    _aggregate_with_retry(
        pc,
        [{"source": {"pageEvents": None}}],
        total_days=30,
        label="pageEvents",
    )
    assert pc.aggregate.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_does_not_retry_non_retryable_http(mock_sleep) -> None:
    pc = MagicMock()
    pc.aggregate.side_effect = _http_error(400)
    with pytest.raises(requests.exceptions.HTTPError):
        _aggregate_with_retry(
            pc,
            [{"source": {"pageEvents": None}}],
            total_days=30,
            label="pageEvents",
        )
    assert pc.aggregate.call_count == 1
    mock_sleep.assert_not_called()


def test_resolve_pendo_customer_prefix_exact() -> None:
    pc = MagicMock()
    pc.get_sites_by_customer.return_value = {"customer_list": ["Ford", "Ford Training"]}
    assert resolve_pendo_customer_prefix("ford", pc) == "Ford"


def test_resolve_pendo_customer_prefix_ambiguous_raises() -> None:
    pc = MagicMock()
    pc.get_sites_by_customer.return_value = {"customer_list": ["Ford A", "Ford B"]}
    with pytest.raises(ValueError, match="ambiguous"):
        resolve_pendo_customer_prefix("Ford", pc)


def test_build_headline_aggregates_site_totals() -> None:
    headline = build_headline(
        health={
            "engagement": {"active_7d": 10, "active_30d": 20, "dormant": 5, "active_rate_7d": 40.0},
            "account": {"total_visitors": 25, "total_sites": 3},
        },
        depth={"total_feature_events": 500, "write_ratio": 22.5},
        sites={
            "sites": [
                {"total_events": 100, "total_minutes": 50},
                {"total_events": 200, "total_minutes": 75},
            ]
        },
        features={"top_features": [{}, {}], "feature_adoption_insights": {"feature_clicks_total": 500}},
        trends={"comparison": {"active_users_7d_pct_change": 5.0}},
    )
    assert headline["total_events"] == 300
    assert headline["total_minutes"] == 125.0
    assert headline["distinct_features_used_top10"] == 2


@patch("src.export_customer_pendo_snapshot._fetch_activity_day_buckets")
@patch("src.export_customer_pendo_snapshot.build_usage_trends")
def test_build_customer_pendo_export_report(mock_trends, mock_buckets) -> None:
    mock_trends.return_value = {
        "comparison": {"active_users_7d_pct_change": 2.0},
        "weekly_active_users": [],
        "compare_days": 30,
    }
    mock_buckets.return_value = ([], [])
    pc = MagicMock()
    pc.get_sites_by_customer.return_value = {"customer_list": ["Ford"]}
    pc.get_customer_health.return_value = {
        "account": {"total_visitors": 10, "total_sites": 2},
        "engagement": {"active_7d": 4, "active_rate_7d": 40.0},
        "benchmarks": {},
        "signals": [],
    }
    pc.get_customer_sites.return_value = {
        "sites": [{"sitename": "Essex", "visitors": 5, "total_events": 10, "total_minutes": 3, "last_active": "2026-06-01"}]
    }
    pc.get_customer_features.return_value = {"top_features": [{"name": "CTB", "events": 9}]}
    pc.get_customer_depth.return_value = {"total_feature_events": 9, "write_ratio": 10, "breakdown": []}
    pc.get_customer_kei.return_value = {"total_queries": 1, "unique_users": 1, "adoption_rate": 10}
    pc.get_customer_people.return_value = {
        "champions": [{"email": "a@ford.com", "role": "Buyer", "last_visit": "2026-06-28", "days_inactive": 1.0}],
        "at_risk_users": [],
    }
    pc.get_customer_exports.return_value = {
        "total_exports": 12,
        "exports_per_active_user": 3.0,
        "active_users": 4,
        "by_feature": [{"feature": "CTB: Export to Excel", "exports": 12}],
        "top_exporters": [{"email": "a@ford.com", "role": "Buyer", "exports": 12}],
    }
    pc.get_customer_frustration_signals.return_value = {
        "total_frustration_signals": 2,
        "totals": {"rageClickCount": 1, "deadClickCount": 1, "errorClickCount": 0, "uTurnCount": 0},
        "top_pages": [],
        "top_features": [],
    }
    pc.get_feature_catalog.return_value = {"f1": "Clear to Build", "f2": "Unused Widget"}
    pc._get_visitor_partition.return_value = {"now_ms": 1_700_000_000_000}
    pc._filter_customer_visitors.return_value = ([{"visitorId": "v1"}], None)

    report = build_customer_pendo_export_report(pc, "Ford", days=30, compare_days=14)

    assert report["meta"]["pendo_prefix"] == "Ford"
    assert report["meta"]["compare_days"] == 14
    assert "salesforce" not in report["meta"]
    assert report["sites"]["sites"][0]["sitename"] == "Essex"
    assert "core_feature_checklist" in report
    assert "unused_features" in report
    assert report["people"]["champions"][0]["email"] == "a@ford.com"
    assert report["exports"]["total_exports"] == 12
    assert report["frustration"]["total_frustration_signals"] == 2
    pc.get_customer_people.assert_called_once_with("Ford", days=30)
    pc.get_customer_exports.assert_called_once_with("Ford", days=30)
    pc.get_customer_frustration_signals.assert_called_once_with("Ford", days=30)
    pc.preload.assert_called_once_with(44)
    mock_trends.assert_called_once()


def test_render_customer_pendo_markdown_includes_sections() -> None:
    md = render_customer_pendo_markdown(
        {
            "meta": {
                "exported_at_utc": "2026-06-29T12:00:00Z",
                "pendo_prefix": "Ford",
                "customer_query": "Ford",
                "days": 30,
                "window_start": "2026-05-30",
                "window_end": "2026-06-29",
            },
            "headline": {
                "active_users_7d": 4,
                "total_visitors": 10,
                "total_sites": 2,
                "weekly_active_rate_pct": 40,
                "total_events": 100,
                "total_minutes": 50,
                "feature_events": 20,
                "write_ratio_pct": 12,
                "vs_prior_period": {},
            },
            "sites": {"sites": []},
            "features": {},
            "depth": {},
            "people": {
                "champions": [{"email": "a@ford.com", "role": "Buyer", "last_visit": "2026-06-28", "days_inactive": 1.0}],
                "at_risk_users": [{"email": "b@ford.com", "role": "Planner", "last_visit": "2026-05-01", "days_inactive": 30.0}],
            },
            "exports": {
                "total_exports": 5,
                "exports_per_active_user": 2.5,
                "active_users": 2,
                "by_feature": [{"feature": "CTB: Export to Excel", "exports": 5}],
                "top_exporters": [{"email": "a@ford.com", "role": "Buyer", "exports": 5}],
            },
            "frustration": {
                "total_frustration_signals": 3,
                "totals": {"rageClickCount": 2, "deadClickCount": 1, "errorClickCount": 0, "uTurnCount": 0},
                "top_pages": [{"page": "Shortages", "rageClickCount": 2, "deadClickCount": 0, "errorClickCount": 0, "uTurnCount": 0}],
                "top_features": [],
            },
            "kei": {},
            "trends": {"weekly_active_users": [], "comparison": {}},
            "core_feature_checklist": {"summary": {"total_tracked": 1, "adopted": 1, "not_adopted": 0, "declining": 0}, "entries": []},
            "unused_features": {"catalog_total": 2, "unused_count": 1, "unused_features": [{"name": "Unused Widget"}], "truncated": False},
            "engagement": {"benchmarks": {}, "signals": []},
        }
    )
    assert "# Pendo usage — Ford" in md
    assert "Salesforce" not in md
    assert "**How to read this export**" in md
    assert "Visitor counts can overlap across sites" in md
    assert "## 1. Headline" in md
    assert "## 4. Core feature checklist" in md
    assert "## 5. Unused product features" in md
    assert "## 7. People" in md
    assert "## 8. Export behavior" in md
    assert "## 9. Frustration signals" in md
    assert "## 10. Kei AI" in md
    assert "## 11. Usage trends" in md


def test_merge_active_site_rows_merges_entities_and_filters_idle() -> None:
    # Two entity rows for Montreal carry the SAME site-level events/minutes (Pendo
    # fallback) but different visitors — the real Safran pattern.
    rows = [
        {"sitename": "Safran Montreal CG1", "entity": "A", "visitors": 10, "total_events": 100, "total_minutes": 60, "last_active": "2026-07-01"},
        {"sitename": "Safran Montreal CG1", "entity": "B", "visitors": 5, "total_events": 100, "total_minutes": 60, "last_active": "2026-07-08"},
        {"sitename": "Safran Issoudun 36P", "visitors": 3, "total_events": 20, "total_minutes": 10, "last_active": "2026-07-05"},
        {"sitename": "Safran Idle Plant", "visitors": 2, "total_events": 0, "total_minutes": 0, "last_active": "2025-01-01"},
    ]
    active, active_count, provisioned = merge_active_site_rows(rows)
    assert provisioned == 3
    assert active_count == 2
    # Montreal merged across entities: visitors summed, events/minutes NOT double-counted
    # (max, not sum), newest last_active kept.
    montreal = active[0]
    assert montreal["sitename"] == "Safran Montreal CG1"
    assert montreal["visitors"] == 15
    assert montreal["total_events"] == 100
    assert montreal["total_minutes"] == 60
    assert montreal["last_active"] == "2026-07-08"
    # Idle site excluded
    assert all(r["sitename"] != "Safran Idle Plant" for r in active)
    # Sorted by events desc
    assert [r["sitename"] for r in active] == ["Safran Montreal CG1", "Safran Issoudun 36P"]


def test_merge_active_site_rows_handles_empty() -> None:
    active, active_count, provisioned = merge_active_site_rows([])
    assert active == []
    assert active_count == 0
    assert provisioned == 0


def test_render_pendo_markdown_section2_lists_active_sites_deduped() -> None:
    # Unmapped customer (no BU config) keeps §2 focused on the dedupe behavior.
    md = render_customer_pendo_markdown(
        {
            "meta": {
                "exported_at_utc": "2026-06-29T12:00:00Z",
                "pendo_prefix": "Ford",
                "customer_query": "Ford",
                "days": 30,
                "window_start": "2026-05-30",
                "window_end": "2026-06-29",
            },
            "headline": {
                "active_users_7d": 4,
                "total_visitors": 10,
                "total_sites": 2,
                "weekly_active_rate_pct": 40,
                "total_events": 100,
                "total_minutes": 50,
                "feature_events": 20,
                "write_ratio_pct": 12,
                "vs_prior_period": {},
            },
            "sites": {
                "sites": [
                    {"sitename": "Ford Dearborn Engine", "entity": "A", "visitors": 10, "total_events": 100, "total_minutes": 60, "last_active": "2026-06-01"},
                    {"sitename": "Ford Dearborn Engine", "entity": "B", "visitors": 5, "total_events": 100, "total_minutes": 60, "last_active": "2026-06-29"},
                    {"sitename": "Ford Idle Plant", "visitors": 2, "total_events": 0, "total_minutes": 0, "last_active": "N/A"},
                ]
            },
            "features": {},
            "depth": {},
            "people": {"champions": [], "at_risk_users": []},
            "exports": {"total_exports": 0, "exports_per_active_user": 0, "active_users": 0, "by_feature": [], "top_exporters": []},
            "frustration": {"total_frustration_signals": 0, "totals": {}, "top_pages": [], "top_features": []},
            "kei": {},
            "trends": {"weekly_active_users": [], "comparison": {}},
            "core_feature_checklist": {"summary": {"total_tracked": 0, "adopted": 0, "not_adopted": 0, "declining": 0}, "entries": []},
            "unused_features": {"catalog_total": 0, "unused_count": 0, "unused_features": [], "truncated": False},
            "engagement": {"benchmarks": {}, "signals": []},
        }
    )
    assert "## 2. Sites" in md
    assert "Active sites: **1** of **2** provisioned" in md
    # Site appears once (entities merged), idle plant excluded
    assert md.count("Ford Dearborn Engine |") == 1
    assert "Ford Idle Plant" not in md
    # No stale "Showing 40 of N" footer
    assert "Showing 40 of" not in md


def test_resolve_site_business_unit_safran_divisions() -> None:
    # Division-named rules win over shared location catch-alls (ordering matters).
    assert resolve_site_business_unit("Safran", "Safran Electrical and Power Soliman") == "Electrical & Power"
    assert resolve_site_business_unit("Safran", "Safran Seats Soliman 33P") == "Cabin & Seats"
    assert resolve_site_business_unit("Safran", "Safran Electronics and Defense Auxerre") == "Electronics & Defense"
    assert resolve_site_business_unit("Safran", "Safran Aerosystems A1P Chateaudun Production") == "Aerosystems"
    assert resolve_site_business_unit("Safran", "Safran Montreal CG1") == "Cabin & Seats"
    # CSR-confirmed corrections: Astronautics is Cabin & Seats; AMX is Electrical & Power
    assert resolve_site_business_unit("Safran", "Safran Astronautics") == "Cabin & Seats"
    assert resolve_site_business_unit("Safran", "Safran AMX SM1") == "Electrical & Power"
    assert resolve_site_business_unit("Safran", "Safran SA Lean Projects") == "Other / Corporate"
    # Case-insensitive prefix match
    assert resolve_site_business_unit("safran", "Safran Montreal CG1") == "Cabin & Seats"


def test_resolve_site_business_unit_unmapped_customer_returns_none() -> None:
    assert resolve_site_business_unit("SomeUnmappedCo", "SomeUnmappedCo Plant 1") is None


def test_resolve_site_business_unit_detail_reports_confidence() -> None:
    # Self-labeling division name -> high confidence
    bu, conf = resolve_site_business_unit_detail("Safran", "Safran Electronics and Defense Auxerre")
    assert bu == "Electronics & Defense" and conf == "high"
    # Location-only guess -> inferred
    bu, conf = resolve_site_business_unit_detail("Safran", "Safran Montreal CG1")
    assert bu == "Cabin & Seats" and conf == "inferred"
    # No rule match -> default bucket, confidence "none"
    bu, conf = resolve_site_business_unit_detail("Safran", "Safran Mystery Plant XYZ")
    assert bu == "Unmapped — needs review" and conf == "none"
    # Unmapped customer
    bu, conf = resolve_site_business_unit_detail("Ford", "Ford Dearborn")
    assert bu is None and conf == "unmapped_customer"


def test_business_unit_review_sites_lists_inferred_and_unmapped() -> None:
    active = [
        {"sitename": "Safran Electronics and Defense Auxerre", "total_events": 100},  # high -> excluded
        {"sitename": "Safran Montreal CG1", "total_events": 90},                       # inferred
        {"sitename": "Safran Mystery Plant XYZ", "total_events": 5},                   # none
    ]
    review = business_unit_review_sites("Safran", active)
    names = {r["sitename"]: r["confidence"] for r in review}
    assert names == {
        "Safran Montreal CG1": "inferred",
        "Safran Mystery Plant XYZ": "none",
    }
    # Unmapped customer -> no review items
    assert business_unit_review_sites("Ford", active) == []


def test_build_business_unit_summary_aggregates_and_sorts() -> None:
    active = [
        {"sitename": "Safran Montreal CG1", "visitors": 100, "total_events": 90_000, "total_minutes": 80_000},
        {"sitename": "Safran Tijuana C44", "visitors": 80, "total_events": 60_000, "total_minutes": 50_000},
        {"sitename": "Safran Electrical and Power Niort", "visitors": 40, "total_events": 120_000, "total_minutes": 90_000},
    ]
    summary = build_business_unit_summary("Safran", active)
    by_bu = {r["business_unit"]: r for r in summary}
    assert by_bu["Cabin & Seats"]["site_count"] == 2
    assert by_bu["Cabin & Seats"]["visitors"] == 180
    assert by_bu["Cabin & Seats"]["total_events"] == 150_000
    assert by_bu["Cabin & Seats"]["top_site"] == "Safran Montreal CG1"
    assert by_bu["Electrical & Power"]["site_count"] == 1
    # Sorted by total events desc: Cabin & Seats (150k) before Electrical & Power (120k)
    assert [r["business_unit"] for r in summary] == ["Cabin & Seats", "Electrical & Power"]
    # Internal sort key removed from output
    assert "_top_site_events" not in summary[0]


def test_build_business_unit_summary_empty_for_unmapped_customer() -> None:
    assert build_business_unit_summary("SomeUnmappedCo", [{"sitename": "X", "total_events": 5}]) == []


def test_render_pendo_markdown_emits_business_unit_section_for_mapped_customer() -> None:
    md = render_customer_pendo_markdown(
        {
            "meta": {
                "exported_at_utc": "2026-06-29T12:00:00Z",
                "pendo_prefix": "Safran",
                "customer_query": "Safran",
                "days": 30,
                "window_start": "2026-05-30",
                "window_end": "2026-06-29",
            },
            "headline": {
                "active_users_7d": 4,
                "total_visitors": 500,
                "total_sites": 400,
                "weekly_active_rate_pct": 40,
                "total_events": 100,
                "total_minutes": 50,
                "feature_events": 20,
                "write_ratio_pct": 12,
                "vs_prior_period": {},
            },
            "sites": {
                "sites": [
                    {"sitename": "Safran Montreal CG1", "visitors": 100, "total_events": 90_000, "total_minutes": 80_000, "last_active": "2026-06-29"},
                    {"sitename": "Safran Electrical and Power Niort", "visitors": 40, "total_events": 120_000, "total_minutes": 90_000, "last_active": "2026-06-29"},
                ]
            },
            "features": {},
            "depth": {},
            "people": {"champions": [], "at_risk_users": []},
            "exports": {"total_exports": 0, "exports_per_active_user": 0, "active_users": 0, "by_feature": [], "top_exporters": []},
            "frustration": {"total_frustration_signals": 0, "totals": {}, "top_pages": [], "top_features": []},
            "kei": {},
            "trends": {"weekly_active_users": [], "comparison": {}},
            "core_feature_checklist": {"summary": {"total_tracked": 0, "adopted": 0, "not_adopted": 0, "declining": 0}, "entries": []},
            "unused_features": {"catalog_total": 0, "unused_count": 0, "unused_features": [], "truncated": False},
            "engagement": {"benchmarks": {}, "signals": []},
        }
    )
    assert "## 2.1 Business unit summary" in md
    assert "Electrical & Power" in md
    assert "Cabin & Seats" in md
    # Montreal is a location guess -> confidence footnote surfaces it for CS review
    assert "**Confidence:**" in md
    assert "inferred" in md
    assert "BUSINESS_UNIT_MAPPING_REVIEW.md" in md
    # §2 gains a Business unit column for mapped customers
    assert "| Site | Business unit | Visitors | Events | Minutes | Last active |" in md
    # Headline now labels active vs provisioned
    assert "active of **400** provisioned" in md


def test_render_pendo_markdown_omits_business_unit_section_for_unmapped_customer() -> None:
    md = render_customer_pendo_markdown(
        {
            "meta": {
                "exported_at_utc": "2026-06-29T12:00:00Z",
                "pendo_prefix": "Ford",
                "customer_query": "Ford",
                "days": 30,
                "window_start": "2026-05-30",
                "window_end": "2026-06-29",
            },
            "headline": {
                "active_users_7d": 4,
                "total_visitors": 10,
                "total_sites": 2,
                "weekly_active_rate_pct": 40,
                "total_events": 100,
                "total_minutes": 50,
                "feature_events": 20,
                "write_ratio_pct": 12,
                "vs_prior_period": {},
            },
            "sites": {"sites": [{"sitename": "Ford Dearborn", "visitors": 5, "total_events": 10, "total_minutes": 3, "last_active": "2026-06-29"}]},
            "features": {},
            "depth": {},
            "people": {"champions": [], "at_risk_users": []},
            "exports": {"total_exports": 0, "exports_per_active_user": 0, "active_users": 0, "by_feature": [], "top_exporters": []},
            "frustration": {"total_frustration_signals": 0, "totals": {}, "top_pages": [], "top_features": []},
            "kei": {},
            "trends": {"weekly_active_users": [], "comparison": {}},
            "core_feature_checklist": {"summary": {"total_tracked": 0, "adopted": 0, "not_adopted": 0, "declining": 0}, "entries": []},
            "unused_features": {"catalog_total": 0, "unused_count": 0, "unused_features": [], "truncated": False},
            "engagement": {"benchmarks": {}, "signals": []},
        }
    )
    assert "## 2.1 Business unit summary" not in md
    # §2 keeps the original columns (no Business unit) for unmapped customers
    assert "| Site | Visitors | Events | Minutes | Last active |" in md


def test_build_core_feature_checklist_statuses() -> None:
    catalog = {"f1": "Clear to Build dashboard", "f2": "Kei assistant", "f3": "Unused thing"}
    checklist = build_core_feature_checklist(
        customer="Ford",
        feature_catalog=catalog,
        feat_current={"f1": 20, "f2": 0},
        feat_prior={"f1": 40, "f2": 0},
    )
    by_label = {entry["label"]: entry for entry in checklist["entries"]}
    assert by_label["Clear to Build"]["status"] == "declining"
    assert by_label["Kei AI"]["status"] == "not_adopted"
    assert checklist["summary"]["declining"] >= 1
    assert checklist["summary"]["not_adopted"] >= 1


def test_build_unused_features_lists_zero_usage_catalog_entries() -> None:
    unused = build_unused_features(
        {"f1": "Used feature", "f2": "Unused feature"},
        {"f1": 5},
    )
    assert unused["unused_count"] == 1
    assert unused["unused_features"][0]["name"] == "Unused feature"


def test_build_usage_trends_adds_weekly_activity_and_rolling_avg() -> None:
    end_ms = 1_700_000_000_000
    pc = MagicMock()
    pc._get_visitor_partition.return_value = {"now_ms": end_ms}

    def _snap(_pc, _customer, start_ms, end_ms, **_kwargs):
        return {"active_7d": 5, "total_users": 10, "weekly_active_rate_pct": 50.0}

    page_rows = [
        {"visitorId": "v1", "day": end_ms - 2 * 86_400_000, "numEvents": 3, "numMinutes": 2},
    ]
    feat_rows = [
        {"visitorId": "v1", "day": end_ms - 2 * 86_400_000, "featureId": "f1", "numEvents": 4},
    ]

    with patch("src.export_customer_pendo_snapshot._snapshot_metrics", side_effect=_snap):
        trends = build_usage_trends(
            pc,
            "Ford",
            14,
            compare_days=14,
            visitor_ids={"v1"},
            day_buckets=(page_rows, feat_rows),
        )

    assert trends["compare_days"] == 14
    assert trends["weekly_active_users"][-1]["total_events"] == 7
    assert trends["comparison"]["total_events_pct_change"] is not None
    assert trends["weekly_active_users"][-1]["rolling_4w_avg_total_events"] is not None


def test_build_step_argv_export_pendo_compare_days() -> None:
    argv = build_step_argv(
        {"command": "export-pendo", "customer": "Ford", "days": 30, "compare_days": 14}
    )
    assert argv == ["--export-pendo", "--customer", "Ford", "--days", "30", "--compare-days", "14"]


def test_build_step_argv_export_pendo() -> None:
    argv = build_step_argv({"command": "export-pendo", "customer": "Ford", "days": 30})
    assert argv == ["--export-pendo", "--customer", "Ford", "--days", "30"]


def test_load_ford_pendo_7d_job() -> None:
    spec = load_job_spec("ford-pendo-7d")
    assert spec.name == "ford-pendo-7d"
    step = spec.steps[0]
    assert step["customer"] == "Ford"
    assert step["days"] == 7
    assert step["compare_days"] == 7
    assert build_step_argv(step) == [
        "--export-pendo",
        "--customer",
        "Ford",
        "--days",
        "7",
        "--compare-days",
        "7",
    ]


def test_load_ford_pendo_30d_job() -> None:
    spec = load_job_spec("ford-pendo-30d")
    assert spec.name == "ford-pendo-30d"
    step = spec.steps[0]
    assert step["customer"] == "Ford"
    assert step["days"] == 30
    assert step["compare_days"] == 30
    assert build_step_argv(step) == [
        "--export-pendo",
        "--customer",
        "Ford",
        "--days",
        "30",
        "--compare-days",
        "30",
    ]


def test_render_csr_markdown_includes_all_factories() -> None:
    from src.export_customer_pendo_snapshot import render_csr_markdown

    report = {
        "csr": {
            "scope": "single_customer_pendo_export",
            "csr_loaded": True,
            "csr_lookup_keys": ["Ford", "Ford Motor Company"],
            "csr_matched_lookup_key": "Ford Motor Company",
            "summary": {
                "factory_count": 2,
                "health_distribution": {"GREEN": 1, "RED": 1},
                "total_shortages": 12,
                "total_critical_shortages": 3,
                "inventory_totals": {"on_hand": 1000, "on_order": 200},
                "total_savings": 50000,
            },
            "merged_sites": [
                {
                    "factory": "Van Dyke",
                    "health_score": "RED",
                    "shortages": 10,
                    "on_hand_value": 800,
                    "savings_current_period": 40000,
                },
                {
                    "factory": "Cleveland",
                    "health_score": "GREEN",
                    "shortages": 2,
                    "on_hand_value": 200,
                    "savings_current_period": 10000,
                },
            ],
        }
    }
    md = render_csr_markdown(report, section_number=13)
    assert "## 13. CS Report" in md
    assert "### 13.1 Customer summary" in md
    assert "### 13.2 All factories" in md
    assert "Van Dyke" in md
    assert "Cleveland" in md
    assert "2 factories" in md


def test_render_customer_pendo_markdown_appends_csr_section() -> None:
    report = {
        "meta": {
            "pendo_prefix": "Ford",
            "customer_query": "Ford",
            "exported_at_utc": "2020-01-01T00:00:00Z",
            "window_start": "2020-01-01",
            "window_end": "2020-01-30",
            "days": 30,
            "compare_days": 30,
        },
        "headline": {
            "active_users_7d": 1,
            "total_visitors": 2,
            "total_sites": 1,
            "weekly_active_rate_pct": 50.0,
            "total_events": 100,
            "total_minutes": 200,
            "feature_events": 50,
            "write_ratio_pct": 60.0,
        },
        "sites": {"sites": [], "sites_active": 0, "sites_provisioned": 0},
        "features": {},
        "core_feature_checklist": [],
        "unused_features": [],
        "depth": {},
        "people": {},
        "exports": {},
        "frustration": {},
        "kei": {},
        "trends": {"weekly_active_users": []},
        "engagement": {"benchmarks": {}, "signals": []},
        "csr": {
            "csr_loaded": True,
            "csr_lookup_keys": ["Ford"],
            "csr_matched_lookup_key": "Ford",
            "summary": {"factory_count": 1},
            "merged_sites": [{"factory": "Plant A", "shortages": 1}],
        },
    }
    md = render_customer_pendo_markdown(report)
    assert "## 13. CS Report" in md
    assert "Plant A" in md


def test_merge_csr_customer_site_rows_unions_sections() -> None:
    from src.cs_report_client import merge_csr_customer_site_rows

    block = {
        "platform_health": {
            "sites": [{"factory": "A", "health_score": "RED", "shortages": 5}],
        },
        "supply_chain": {
            "sites": [{"factory": "A", "on_hand_value": 1000}],
        },
        "platform_value": {
            "sites": [{"factory": "A", "savings_current_period": 200}],
        },
    }
    merged = merge_csr_customer_site_rows(block)
    assert len(merged) == 1
    assert merged[0]["factory"] == "A"
    assert merged[0]["health_score"] == "RED"
    assert merged[0]["on_hand_value"] == 1000
    assert merged[0]["savings_current_period"] == 200
