"""Tests for single-customer Pendo usage export."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.export_customer_pendo_snapshot import (
    build_customer_pendo_export_report,
    build_headline,
    render_customer_pendo_markdown,
    resolve_pendo_customer_prefix,
)
from src.job_runner import build_step_argv, load_job_spec


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


@patch("src.export_customer_pendo_snapshot.build_usage_trends")
@patch("src.export_customer_pendo_snapshot._optional_salesforce_context")
def test_build_customer_pendo_export_report(mock_sf, mock_trends) -> None:
    mock_sf.return_value = {"salesforce_label": "Ford Motor Company", "active_arr_usd": 1000}
    mock_trends.return_value = {
        "comparison": {"active_users_7d_pct_change": 2.0},
        "weekly_active_users": [],
    }
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

    report = build_customer_pendo_export_report(pc, "Ford", days=30)

    assert report["meta"]["pendo_prefix"] == "Ford"
    assert report["meta"]["salesforce"]["salesforce_label"] == "Ford Motor Company"
    assert report["sites"]["sites"][0]["sitename"] == "Essex"
    pc.preload.assert_called_once_with(30)


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
                "salesforce": {"salesforce_label": "Ford Motor Company", "active_arr_usd": 1000, "entity_count": 5},
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
            "kei": {},
            "trends": {"weekly_active_users": [], "comparison": {}},
            "engagement": {"benchmarks": {}, "signals": []},
        }
    )
    assert "# Pendo usage — Ford" in md
    assert "## 1. Headline" in md
    assert "## 2. Sites" in md
    assert "## 5. Kei AI" in md


def test_load_ford_pendo_daily_job() -> None:
    spec = load_job_spec("ford-pendo-daily")
    assert spec.name == "ford-pendo-daily"
    assert spec.steps[0]["customer"] == "Ford"


def test_build_step_argv_export_pendo() -> None:
    argv = build_step_argv({"command": "export-pendo", "customer": "Ford", "days": 30, "format": "both"})
    assert argv == ["--export-pendo", "--customer", "Ford", "--days", "30", "--format", "both"]
