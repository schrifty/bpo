"""Tests for site/user Pendo detailed exports and top-ARR batch."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.export_pendo_detailed_snapshot import (
    _canonical_site_names_for_prefix,
    _index_rows_by_visitor,
    _pendo_detailed_export_file_stem,
    _sum_activity_indexed,
    _top_pages_and_features_in_window,
    build_customer_pendo_detailed_report,
    build_full_user_roster,
    build_site_detail_slices,
    load_top_ultimate_parents_by_arr_for_pendo,
    render_customer_pendo_detailed_markdown,
    render_top_arr_batch_manifest,
)
from src.job_runner import build_step_argv


def _visitor(vid: str, email: str, sitenames: list[str], lastvisit: int = 1_700_000_000_000) -> dict:
    return {
        "visitorId": vid,
        "metadata": {
            "agent": {
                "emailaddress": email,
                "role": "Buyer",
                "sitenames": sitenames,
            },
            "auto": {"lastvisit": lastvisit},
        },
    }


def test_pendo_detailed_export_file_stem() -> None:
    assert _pendo_detailed_export_file_stem("Ford", 30) == "Pendo Detailed Export  (Ford, 30d)"


def test_indexed_activity_matches_window_totals() -> None:
    now_ms = 1_700_100_000_000
    page_rows = [
        {"visitorId": "v1", "day": now_ms - 86400000, "pageId": "p1", "numEvents": 10, "numMinutes": 5},
        {"visitorId": "v2", "day": now_ms - 86400000, "pageId": "p2", "numEvents": 4, "numMinutes": 2},
    ]
    feat_rows = [{"visitorId": "v1", "day": now_ms - 86400000, "featureId": "f1", "numEvents": 3}]
    page_by_visitor, feat_by_visitor = _index_rows_by_visitor(page_rows, feat_rows)
    current = _sum_activity_indexed(
        page_by_visitor,
        feat_by_visitor,
        {"v1"},
        now_ms - 7 * 86400000,
        now_ms,
    )
    assert current["total_events"] == 13
    top_pages, top_features = _top_pages_and_features_in_window(
        page_by_visitor,
        feat_by_visitor,
        {"v1"},
        start_ms=now_ms - 7 * 86400000,
        end_ms=now_ms,
        page_catalog={"p1": "Dashboard"},
        feature_catalog={"f1": "Export"},
    )
    assert top_pages[0]["name"] == "Dashboard"
    assert top_features[0]["name"] == "Export"


def test_build_site_detail_slices_and_user_roster() -> None:
    pc = MagicMock()
    now_ms = 1_700_100_000_000
    visitors = [
        _visitor("v1", "a@ford.com", ["Ford Dearborn Engine"]),
        _visitor("v2", "b@ford.com", ["Ford Lima Engine"]),
        _visitor("v3", "c@ford.com", ["Ford Dearborn Engine", "Ford Lima Engine"]),
    ]
    page_rows = [
        {"visitorId": "v1", "day": now_ms - 86400000, "pageId": "p1", "numEvents": 10, "numMinutes": 5},
        {"visitorId": "v2", "day": now_ms - 86400000, "pageId": "p2", "numEvents": 4, "numMinutes": 2},
    ]
    feat_rows = [
        {"visitorId": "v1", "day": now_ms - 86400000, "featureId": "f1", "numEvents": 3},
        {"visitorId": "v3", "day": now_ms - 86400000, "featureId": "f1", "numEvents": 1},
    ]
    pc._get_page_catalog_cached.return_value = {"p1": "Dashboard", "p2": "Reports"}
    pc.get_feature_catalog.return_value = {"f1": "Export"}
    pc._get_page_events_cached.return_value = [
        {"visitorId": "v1", "pageId": "p1", "numEvents": 10, "numMinutes": 5},
        {"visitorId": "v2", "pageId": "p2", "numEvents": 4, "numMinutes": 2},
    ]
    pc._get_feature_events_cached.return_value = [
        {"visitorId": "v1", "featureId": "f1", "numEvents": 3},
    ]
    pc._build_user_activity.side_effect = lambda vs, _now: [
        {
            "email": (v.get("metadata") or {}).get("agent", {}).get("emailaddress", ""),
            "role": "Buyer",
            "last_visit": "2024-01-01",
            "days_inactive": 1.0,
        }
        for v in vs
    ]
    pc.get_sites_by_customer.return_value = {
        "by_customer": {
            "Ford": [
                {"sitename": "Ford Dearborn Engine", "total_events": 14},
                {"sitename": "Ford Lima Engine", "total_events": 4},
            ],
        },
    }

    sites = build_site_detail_slices(
        pc,
        "Ford",
        days=7,
        compare_days=7,
        customer_visitors=visitors,
        page_rows=page_rows,
        feat_rows=feat_rows,
        now_ms=now_ms,
    )
    assert len(sites) == 2
    assert sites[0]["sitename"] in {"Ford Dearborn Engine", "Ford Lima Engine"}
    assert sites[0]["visitors"] >= 1
    assert sites[0]["top_pages"]
    assert pc._get_page_events_cached.call_count == 1
    assert pc._get_feature_events_cached.call_count == 1

    roster = build_full_user_roster(
        pc,
        "Ford",
        days=7,
        compare_days=7,
        customer_visitors=visitors,
        page_rows=page_rows,
        feat_rows=feat_rows,
        now_ms=now_ms,
    )
    assert len(roster) == 3
    assert roster[0]["email"]
    assert roster[0]["events_current"] >= 0


def test_canonical_site_names_ignores_visitor_metadata_inflation() -> None:
    pc = MagicMock()
    pc.get_sites_by_customer.return_value = {
        "by_customer": {
            "Safran": [
                {"sitename": "Safran Ventilation Systems", "total_events": 100},
                {"sitename": "Safran Site B", "total_events": 50},
            ],
        },
    }
    names = _canonical_site_names_for_prefix(pc, "Safran", days=30)
    assert names == ["Safran Ventilation Systems", "Safran Site B"]
    pc.get_sites_by_customer.assert_called_once_with(days=30)


@patch("src.export_pendo_detailed_snapshot.build_customer_pendo_export_report")
def test_build_customer_pendo_detailed_report_extends_account(mock_account) -> None:
    mock_account.return_value = {
        "meta": {"pendo_prefix": "Ford", "days": 7, "compare_days": 7},
        "headline": {"total_sites": 2},
        "sites": {"sites": []},
    }
    pc = MagicMock()
    pc._get_visitor_partition.return_value = {"now_ms": 1_700_100_000_000}
    pc._filter_customer_visitors.return_value = ([_visitor("v1", "a@ford.com", ["Ford Dearborn Engine"])], [])
    with patch("src.export_pendo_detailed_snapshot._fetch_activity_day_buckets", return_value=([], [])):
        with patch(
            "src.export_pendo_detailed_snapshot.build_site_detail_slices",
            return_value=[{"sitename": "Ford Dearborn Engine", "visitors": 1}],
        ):
            with patch(
                "src.export_pendo_detailed_snapshot.build_full_user_roster",
                return_value=[{"email": "a@ford.com", "sites": ["Ford Dearborn Engine"]}],
            ):
                report = build_customer_pendo_detailed_report(pc, "Ford", days=7, compare_days=7)
    assert report["meta"]["profile_id"] == "customer_pendo_detailed_export"
    assert report["site_detail"]
    assert report["user_roster"]


def test_render_customer_pendo_detailed_markdown_includes_site_and_user_sections() -> None:
    report = {
        "meta": {
            "pendo_prefix": "Ford",
            "customer_query": "Ford",
            "exported_at_utc": "2026-01-01T00:00:00Z",
            "window_start": "2025-12-01",
            "window_end": "2026-01-01",
            "days": 30,
            "compare_days": 30,
        },
        "headline": {
            "active_users_7d": 5,
            "total_visitors": 10,
            "total_sites": 2,
            "weekly_active_rate_pct": 50,
            "total_events": 100,
            "total_minutes": 20,
            "feature_events": 30,
            "write_ratio_pct": 10,
            "vs_prior_period": {},
        },
        "engagement": {"account": {}, "engagement": {}, "benchmarks": {}, "signals": []},
        "sites": {"sites": [{"sitename": "Ford Dearborn Engine", "visitors": 3, "total_events": 50, "total_minutes": 10, "last_active": "2026-01-01"}]},
        "features": {},
        "core_feature_checklist": {"summary": {"total_tracked": 0, "adopted": 0, "not_adopted": 0, "declining": 0}, "entries": []},
        "unused_features": {"catalog_total": 1, "unused_count": 0, "unused_features": []},
        "depth": {"total_feature_events": 1, "active_users": 1, "write_ratio": 1, "read_events": 0, "write_events": 1, "collab_events": 0, "breakdown": []},
        "people": {"champions": [], "at_risk_users": []},
        "exports": {"total_exports": 0, "exports_per_active_user": 0, "active_users": 0},
        "frustration": {"total_frustration_signals": 0, "totals": {}},
        "kei": {"total_queries": 0, "unique_users": 0, "adoption_rate": 0, "executive_users": 0, "executive_queries": 0},
        "trends": {"weekly_active_users": [], "comparison": {}},
        "site_detail": [
            {
                "sitename": "Ford Dearborn Engine",
                "visitors": 3,
                "engagement": {"active_7d": 2, "active_30d": 1, "dormant": 0},
                "activity_current": {"total_events": 50, "page_minutes": 10, "feature_events": 5},
                "activity_pct_change": {"total_events": 10.0},
                "top_pages": [{"name": "Dashboard", "events": 20, "minutes": 5}],
                "top_features": [],
                "users": [{"email": "a@ford.com", "role": "Buyer", "last_visit": "2026-01-01", "days_inactive": 1}],
            }
        ],
        "user_roster": [
            {
                "email": "a@ford.com",
                "role": "Buyer",
                "sites": ["Ford Dearborn Engine"],
                "engagement_status": "active_7d",
                "last_visit": "2026-01-01",
                "days_inactive": 1,
                "events_current": 50,
                "page_minutes_current": 10,
                "feature_events_current": 5,
                "events_pct_change": 10.0,
            }
        ],
    }
    md = render_customer_pendo_detailed_markdown(report)
    assert "## 13. Site detail" in md
    assert "## 14. User roster" in md
    assert "Ford Dearborn Engine" in md
    assert "a@ford.com" in md


@patch("src.llm_export_csr.top_active_ultimate_parents_by_arr_for_llm_export")
@patch("src.salesforce_client.SalesforceClient")
@patch("src.pendo_client.PendoClient")
def test_load_top_ultimate_parents_by_arr_for_pendo(mock_pc_cls, mock_sf_cls, mock_top) -> None:
    mock_pc_cls.return_value.get_sites_by_customer.return_value = {"customer_list": ["Safran", "Ford"]}
    mock_sf_cls.return_value.get_portfolio_revenue_book_metrics.return_value = {
        "matched_customer_contract_rollups": [{"customer": "Safran", "current_arr": 100.0, "active": True}]
    }
    mock_top.return_value = [{"ultimate_parent": "Safran", "current_arr": 100.0, "pendo_customer_key": "Safran"}]
    rows = load_top_ultimate_parents_by_arr_for_pendo(5)
    assert rows[0]["ultimate_parent"] == "Safran"
    mock_top.assert_called_once()


def test_render_top_arr_batch_manifest() -> None:
    md = render_top_arr_batch_manifest(
        days=30,
        top_n=2,
        results=[
            {"selection": {"ultimate_parent": "Safran", "current_arr": 100.0, "pendo_customer_key": "Safran"}, "status": "ok", "stem": "Pendo Detailed Export  (Safran, 30d)"},
        ],
    )
    assert "Safran" in md
    assert "Pendo Detailed Export" in md


def test_build_step_argv_export_pendo_detailed_and_top_arr() -> None:
    assert build_step_argv({"command": "export-pendo-detailed", "customer": "Ford", "days": 30}) == [
        "--export-pendo-detailed",
        "--customer",
        "Ford",
        "--days",
        "30",
    ]
    assert build_step_argv({"command": "export-pendo-top-arr", "top_n": 5, "days": 30, "no_drive": True}) == [
        "--export-pendo-top-arr",
        "--top-n",
        "5",
        "--days",
        "30",
        "--no-drive",
    ]


def test_load_job_spec_pendo_top_arr_30d() -> None:
    from src.job_runner import load_job_spec

    spec = load_job_spec("pendo-top-arr-30d")
    assert spec.name == "pendo-top-arr-30d"
    assert len(spec.steps) == 1
    step = spec.steps[0]
    assert step["command"] == "export-pendo-top-arr"
    assert step["top_n"] == 5
    assert step["days"] == 30
    assert step["compare_days"] == 30
