"""Tests for Salesforce mapping on the all-customers LLM export script."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _export_mod():
    root = Path(__file__).resolve().parent.parent
    path = root / "scripts" / "export_llm_context_snapshot.py"
    spec = importlib.util.spec_from_file_location("export_llm_context_snapshot_test", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_salesforce_all_customers_empty_when_not_configured(monkeypatch):
    mod = _export_mod()
    monkeypatch.setattr("src.data_source_health._salesforce_configured", lambda: False)
    report: dict = {"customers": [{"customer": "Acme"}]}
    sf = mod._salesforce_for_all_customers_report(report)
    assert sf.get("error")
    assert sf.get("matched") is False
    assert sf.get("resolution") == "none"


def test_salesforce_all_customers_maps_revenue_book(monkeypatch):
    mod = _export_mod()

    def fake_enrich(r: dict) -> None:
        r["portfolio_revenue_book"] = {
            "configured": True,
            "empty": False,
            "pendo_customers": 2,
            "salesforce_matched_customers": 2,
            "salesforce_unmatched_customers": 0,
            "total_arr": 100.0,
            "active_installed_base_arr": 90.0,
            "churned_contract_arr": 10.0,
            "pipeline_arr": 5.5,
            "opportunity_count_this_year": 7,
            "active_customer_count": 1,
            "churned_customer_count": 1,
            "top_customers_by_arr": [
                {"customer": "Acme", "arr": 60.0, "active": True},
                {"customer": "Beta", "arr": 40.0, "active": False},
            ],
            "churned_customer_names_sample": [],
        }

    monkeypatch.setattr("src.data_source_health._salesforce_configured", lambda: True)
    monkeypatch.setattr("src.deck_variants.enrich_portfolio_report_with_revenue_book", fake_enrich)
    report: dict = {"customers": [{"customer": "Acme"}, {"customer": "Beta"}]}
    sf = mod._salesforce_for_all_customers_report(report)
    assert sf["resolution"] == "portfolio_aggregate"
    assert sf["matched"] is True
    assert sf["pipeline_arr"] == 5.5
    assert sf["opportunity_count_this_year"] == 7
    assert len(sf["accounts"]) == 2
    assert sf["accounts"][0]["Name"] == "Acme"

    compact = mod._compact_salesforce(sf, account_cap=6)
    assert compact.get("total_arr") == 100.0
    assert compact.get("salesforce_matched_customers") == 2
