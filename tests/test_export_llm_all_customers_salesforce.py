"""Tests for Salesforce mapping on the all-customers LLM export script."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.data_sources.loaders.salesforce_portfolio_aggregate import (
    salesforce_portfolio_aggregate_for_report,
)


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
    sf = salesforce_portfolio_aggregate_for_report(report)
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
    sf = salesforce_portfolio_aggregate_for_report(report)
    assert sf["resolution"] == "portfolio_aggregate"
    assert sf["matched"] is True
    assert sf["pipeline_arr"] == 5.5
    assert sf["opportunity_count_this_year"] == 7
    assert len(sf["accounts"]) == 2
    assert sf["accounts"][0]["Name"] == "Acme"

    compact = mod._compact_salesforce(sf, account_cap=6)
    assert compact.get("total_arr") == 100.0
    assert compact.get("salesforce_matched_customers") == 2


def test_export_coverage_manifest_and_markdown_section():
    mod = _export_mod()
    report: dict = {
        "customer": "All Customers",
        "generated": "2020-01-01T00:00:00Z",
        "days": 90,
        "portfolio_signals": [
            {"customer": "A", "signal": f"sig{i}"} for i in range(5)
        ],
        "_data_source_provenance": {
            "profile_id": "llm_export_all_customers",
            "sources": [{"source": "pendo_portfolio_rollup", "status": "ok"}],
        },
        "csr": {},
        "salesforce": {},
        "jira": {},
    }
    doc = mod.build_snapshot_document(report, markdown_soft_cap_bytes=99_999)
    cov = doc["export_coverage"]
    assert cov["profile_id"] == "llm_export_all_customers"
    assert len(cov["sources_in_profile"]) == 4
    assert len(cov["registry_excluded"]) == 5
    assert cov["markdown_soft_cap_bytes"] == 99_999
    assert cov["compaction"]["rollup_cap"] == max(cov["compaction"]["sf_accounts"] * 6, 72)
    assert cov["compaction"]["signals_cap"] is None
    assert len(doc["notable_signals_lines"]) == 5

    md = mod.render_markdown(doc, exported_at_utc="2020-01-01T00:00:00Z")
    assert "## Snapshot coverage & omission rationale" in md
    assert "99999 bytes (`--max-bytes`)" in md
    assert "leandna_item_master" in md
    assert "§5 shows the **full** ranked Pendo usage signal list" in md

    doc["_full_sf"] = report["salesforce"]
    doc["_full_csr"] = report["csr"]
    doc["_portfolio_raw"] = report
    mod._shrink_snapshot_params(doc, csr_site_limit=4, csr_string_cap=180, sf_accounts=4)
    assert doc["export_coverage"]["compaction"]["csr_site_limit"] == 4
    assert doc["export_coverage"]["compaction"]["rollup_cap"] == 72
    assert doc["export_coverage"]["compaction"]["signals_cap"] is None
    assert len(doc["notable_signals_lines"]) == 5


def test_portfolio_signal_lines_respects_optional_cap():
    mod = _export_mod()
    portfolio = {
        "portfolio_signals": [
            {"customer": "X", "signal": "a"},
            {"customer": "Y", "signal": "b"},
            {"customer": "Z", "signal": "c"},
        ]
    }
    assert len(mod._portfolio_signal_lines(portfolio, cap=None, line_max=200)) == 3
    assert len(mod._portfolio_signal_lines(portfolio, cap=2, line_max=200)) == 2
