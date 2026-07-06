"""Tests for Salesforce mapping on the all-customers LLM export module."""

from __future__ import annotations

from src.data_sources.loaders.salesforce_portfolio_aggregate import (
    salesforce_aggregate_from_rollups,
    salesforce_portfolio_aggregate_for_report,
)
from src import export_llm_context_snapshot as _export_mod


def test_salesforce_all_customers_empty_when_not_configured(monkeypatch):
    mod = _export_mod
    monkeypatch.setattr("src.data_source_health._salesforce_configured", lambda: False)
    report: dict = {"customers": [{"customer": "Acme"}]}
    sf = salesforce_portfolio_aggregate_for_report(report)
    assert sf.get("error")
    assert sf.get("matched") is False
    assert sf.get("resolution") == "none"


def test_salesforce_all_customers_maps_revenue_book(monkeypatch):
    mod = _export_mod

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
            "renewal_in_flight_customer_count": 0,
            "future_customer_count": 0,
            "active_arr": 90.0,
            "renewal_arr": 0.0,
            "current_arr": 90.0,
            "historical_arr": 100.0,
            "future_contract_arr": 0.0,
            "top_customers_by_arr": [
                {
                    "customer": "Acme",
                    "arr": 60.0,
                    "active": True,
                    "commercial_status": "ACTIVE",
                    "active_arr": 60.0,
                    "renewal_arr": 0.0,
                    "current_arr": 60.0,
                    "historical_arr": 60.0,
                },
                {
                    "customer": "Beta",
                    "arr": 40.0,
                    "active": False,
                    "commercial_status": "CHURNED",
                    "active_arr": 0.0,
                    "renewal_arr": 0.0,
                    "current_arr": 0.0,
                    "historical_arr": 40.0,
                },
            ],
            "churned_customer_names_sample": [],
        }

    monkeypatch.setattr("src.data_source_health._salesforce_configured", lambda: True)
    monkeypatch.setattr("src.deck_variants.enrich_portfolio_report_with_revenue_book", fake_enrich)
    report: dict = {"customers": [{"customer": "Acme"}, {"customer": "Beta"}]}
    sf = salesforce_portfolio_aggregate_for_report(report)
    assert sf["resolution"] == "portfolio_aggregate"
    assert sf["matched"] is True
    assert sf["customer_segment"] == "active"
    assert sf["pipeline_arr"] == 5.5
    assert sf["opportunity_count_this_year"] == 7
    assert len(sf["accounts"]) == 1
    assert sf["accounts"][0]["Name"] == "Acme"
    churned = salesforce_aggregate_from_rollups(
        [{"customer": "Beta", "arr": 40.0, "active": False}],
        segment="churned",
    )
    assert churned["customer_segment"] == "churned"
    assert churned["accounts"][0]["Name"] == "Beta"

    compact = mod._compact_salesforce(sf, account_cap=6)
    assert compact.get("total_arr") == 100.0
    assert compact.get("current_arr") == 90.0
    assert compact.get("salesforce_matched_customers") == 2
    assert "commercial_status" in (compact.get("salesforce_export_note") or "")
    acct0 = compact["accounts"][0]
    assert acct0.get("commercial_status") == "ACTIVE"
    assert acct0.get("current_arr") == 60.0


def test_compact_salesforce_future_segment_and_markdown():
    mod = _export_mod
    future_sf = salesforce_aggregate_from_rollups(
        [
            {
                "customer": "FutureCo",
                "commercial_status": "FUTURE",
                "active_arr": 0.0,
                "renewal_arr": 0.0,
                "current_arr": 0.0,
                "historical_arr": 25.0,
                "arr": 25.0,
            }
        ],
        segment="future_contract",
    )
    compact = mod._compact_salesforce(future_sf, account_cap=6)
    assert "FUTURE" in (compact.get("salesforce_export_note") or "")
    assert compact["accounts"][0]["commercial_status"] == "FUTURE"

    report: dict = {
        "customer": "All Customers",
        "generated": "2020-01-01T00:00:00Z",
        "days": 90,
        "portfolio_signals": [],
        "salesforce": {},
        "salesforce_future_contract_segment": {
            "segment": "future_contract",
            "do_not_merge_with_active_book": True,
            "customer_count": 1,
            "customers_headline": [{"customer": "FutureCo", "salesforce_only": True}],
            "salesforce": future_sf,
        },
    }
    doc = mod.build_snapshot_document(report, size_caps_enabled=False)
    assert doc["salesforce_future_contract_segment"]["customer_count"] == 1
    cov = doc["export_coverage"]
    assert cov["salesforce_future_contract_segment"]["customer_count"] == 1

    md = mod.render_markdown(doc, exported_at_utc="2020-01-01T00:00:00Z")
    assert "## 3b-future. Salesforce (future contracts — not current book)" in md
    assert "commercial_status = FUTURE" in md
    assert "FutureCo" in md


def test_export_coverage_manifest_and_markdown_section():
    mod = _export_mod
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
    doc = mod.build_snapshot_document(
        report,
        markdown_soft_cap_bytes=99_999,
        size_caps_enabled=True,
        csr_site_limit=15,
        csr_string_cap=400,
        sf_accounts=24,
        signal_line_max=280,
    )
    assert "leandna_data_api_reference" in doc
    lref = doc["leandna_data_api_reference"]
    assert "leandna_item_master" in lref["qbr_enrichment_dotted_paths"]
    assert lref["http_surfaces"]
    cov = doc["export_coverage"]
    assert cov["profile_id"] == "llm_export_all_customers"
    assert len(cov["sources_in_profile"]) == 6
    assert len(cov["registry_excluded"]) == 5
    assert cov["markdown_soft_cap_bytes"] == 99_999
    assert cov["compaction"]["rollup_cap"] == max(cov["compaction"]["sf_accounts"] * 6, 72)
    assert cov["compaction"]["signals_cap"] is None
    assert len(doc["notable_signals_lines"]) == 5

    md = mod.render_markdown(doc, exported_at_utc="2020-01-01T00:00:00Z")
    assert "## Snapshot coverage & omission rationale" in md
    assert "## Data Governance" in md
    assert "## LeanDNA Data API — data elements (reference)" in md
    cov_i = md.index("## Snapshot coverage & omission rationale")
    dg_i = md.index("## Data Governance")
    ldna_i = md.index("## LeanDNA Data API — data elements (reference)")
    int_i = md.index("## Integration coverage")
    assert cov_i < dg_i < ldna_i < int_i
    assert "No data-governance warnings were recorded" in md
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
    mod = _export_mod
    portfolio = {
        "portfolio_signals": [
            {"customer": "X", "signal": "a"},
            {"customer": "Y", "signal": "b"},
            {"customer": "Z", "signal": "c"},
        ]
    }
    assert len(mod._portfolio_signal_lines(portfolio, cap=None, line_max=200)) == 3
    assert len(mod._portfolio_signal_lines(portfolio, cap=2, line_max=200)) == 2
