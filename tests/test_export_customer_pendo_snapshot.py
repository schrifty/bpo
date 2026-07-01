"""Tests for single-customer Pendo usage export."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.export_customer_pendo_snapshot import (
    build_core_feature_checklist,
    build_customer_pendo_export_report,
    build_headline,
    build_unused_features,
    build_usage_trends,
    render_customer_pendo_markdown,
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


@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_succeeds_after_timeout(mock_sleep) -> None:
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


@patch("src.export_customer_pendo_snapshot.time.sleep")
def test_aggregate_with_retry_raises_after_max_attempts(mock_sleep) -> None:
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
    assert "## 1. Headline" in md
    assert "## 4. Core feature checklist" in md
    assert "## 5. Unused product features" in md
    assert "## 7. People" in md
    assert "## 8. Export behavior" in md
    assert "## 9. Frustration signals" in md
    assert "## 10. Kei AI" in md
    assert "## 11. Usage trends" in md


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
