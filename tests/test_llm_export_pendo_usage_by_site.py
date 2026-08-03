"""Tests for Pendo usage-by-site in the LLM portfolio export."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.export_llm_context_snapshot import _pendo_portfolio_topline
from src.llm_export_pendo_usage_by_site import (
    attach_pendo_usage_by_site_for_llm_export,
    compact_pendo_usage_by_site,
)


def test_attach_pendo_usage_by_site_builds_sites_and_customer_rollups():
    pc = MagicMock()
    pc.get_all_sites_usage_report.return_value = {
        "results": [
            {
                "siteid": "1",
                "sitename": "Acme Plant A",
                "customer": "Acme",
                "page_views": 10,
                "feature_clicks": 5,
                "total_events": 15,
                "total_minutes": 20,
            },
            {
                "siteid": "2",
                "sitename": "Acme Plant B",
                "customer": "Acme",
                "page_views": 3,
                "feature_clicks": 1,
                "total_events": 4,
                "total_minutes": 6,
            },
            {
                "siteid": "3",
                "sitename": "Beta HQ",
                "customer": "Beta",
                "page_views": 100,
                "feature_clicks": 50,
                "total_events": 150,
                "total_minutes": 200,
            },
        ],
        "total": 3,
    }
    report: dict = {}
    summary = attach_pendo_usage_by_site_for_llm_export(report, pc, days=90)
    assert summary["sites_total"] == 3
    assert summary["customers_with_sites"] == 2
    payload = report["pendo_usage_by_site"]
    assert payload["active_only"] is True
    assert len(payload["sites"]) == 3
    assert payload["by_customer"][0]["customer"] == "Beta"
    assert payload["by_customer"][0]["total_events"] == 150
    acme = next(r for r in payload["by_customer"] if r["customer"] == "Acme")
    assert acme["sites"] == 2
    assert acme["total_events"] == 19
    pc.get_all_sites_usage_report.assert_called_once_with(days=90, active_only=True)


def test_compact_pendo_usage_by_site_optional_limit():
    payload = {
        "source": "pendo",
        "days": 30,
        "active_only": True,
        "note": "Active only.",
        "sites_total": 3,
        "sites": [
            {"sitename": "A", "total_events": 3},
            {"sitename": "B", "total_events": 2},
            {"sitename": "C", "total_events": 1},
        ],
        "by_customer": [{"customer": "X", "sites": 3, "total_events": 6}],
    }
    slim = compact_pendo_usage_by_site(payload, site_limit=2)
    assert slim["sites_included"] == 2
    assert slim["sites_truncated"] is True
    assert [s["sitename"] for s in slim["sites"]] == ["A", "B"]
    assert len(slim["by_customer"]) == 1


def test_pendo_portfolio_topline_includes_usage_by_site():
    portfolio = {
        "customer_count": 1,
        "days": 90,
        "customers": [{"customer": "Acme", "total_users": 10, "active_users": 4, "login_pct": 40}],
        "portfolio_signals": [],
        "pendo_usage_by_site": {
            "source": "pendo",
            "days": 90,
            "active_only": True,
            "note": "Active only.",
            "sites_total": 1,
            "sites": [
                {
                    "sitename": "Acme Plant",
                    "customer": "Acme",
                    "page_views": 1,
                    "feature_clicks": 2,
                    "total_events": 3,
                    "total_minutes": 4,
                }
            ],
            "by_customer": [{"customer": "Acme", "sites": 1, "total_events": 3}],
        },
    }
    out = _pendo_portfolio_topline(portfolio, size_caps_enabled=True)
    assert "usage_by_site" in out
    assert out["usage_by_site"]["sites_total"] == 1
    assert out["usage_by_site"]["sites"][0]["sitename"] == "Acme Plant"
