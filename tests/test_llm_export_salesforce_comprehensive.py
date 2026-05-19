"""Tests for Salesforce comprehensive attachment on all-customers LLM export."""

from __future__ import annotations

from src import export_llm_context_snapshot as export_mod
from src.llm_export_salesforce_comprehensive import (
    attach_salesforce_comprehensive_for_llm_export,
    llm_export_sf_comprehensive_enabled,
)


def test_attach_comprehensive_skipped_when_disabled(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive.llm_export_sf_comprehensive_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive._salesforce_configured",
        lambda: True,
    )
    report: dict = {}
    summary = attach_salesforce_comprehensive_for_llm_export(report)
    assert summary["enabled"] is False
    assert report["salesforce_comprehensive_portfolio"]["skipped"] == (
        "disabled_via_BPO_LLM_EXPORT_SF_COMPREHENSIVE"
    )


def test_attach_comprehensive_fetches_per_label(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive.llm_export_sf_comprehensive_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive._salesforce_configured",
        lambda: True,
    )

    class FakeSf:
        def get_customer_salesforce_comprehensive(self, name: str, *, row_limit: int = 75):
            return {
                "customer": name,
                "matched": True,
                "categories": {"contacts": [{"Id": "c1"}]},
                "row_limit": row_limit,
            }

        def get_entity_accounts(self):
            return [{"Id": "a1", "Name": "Entity", "ARR__c": 1.0}]

    monkeypatch.setattr(
        "src.salesforce_client.SalesforceClient",
        FakeSf,
    )
    report = {
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Acme", "active": True},
                {"customer": "OldCo", "active": False},
            ],
            "expansion_kpis": {"configured": True, "pct_active_customers_expanding_cy": 12.5},
        }
    }
    summary = attach_salesforce_comprehensive_for_llm_export(report)
    assert summary["customers_requested"] == 2
    assert summary["customers_matched"] == 2
    block = report["salesforce_comprehensive_portfolio"]
    assert block["configured"] is True
    assert block["row_limit"] == 75
    assert "Acme" in block["by_customer"]
    assert block["by_customer"]["Acme"]["customer_segment"] == "active"
    assert block["by_customer"]["OldCo"]["customer_segment"] == "churned"
    assert len(block["entity_accounts"]) == 1
    assert block["portfolio_expansion_book"]["pct_active_customers_expanding_cy"] == 12.5


def test_compact_salesforce_includes_expansion_kpis():
    sf = {
        "resolution": "portfolio_aggregate",
        "matched": True,
        "expansion_kpis": {"configured": True, "pct_active_customers_expanding_cy": 5.0},
        "portfolio_expansion_book": {"configured": True, "calendar_year": 2026},
        "accounts": [],
    }
    compact = export_mod._compact_salesforce(sf, account_cap=4)
    assert compact["expansion_kpis"]["pct_active_customers_expanding_cy"] == 5.0
    assert compact["portfolio_expansion_book"]["calendar_year"] == 2026


def test_snapshot_document_includes_comprehensive_section():
    report = {
        "customer": "All Customers",
        "generated": "2020-01-01T00:00:00Z",
        "days": 90,
        "portfolio_signals": [],
        "csr": {},
        "salesforce": {"resolution": "portfolio_aggregate", "matched": True, "accounts": []},
        "jira": {},
        "salesforce_comprehensive_portfolio": {
            "configured": True,
            "by_customer": {"Acme": {"matched": True, "categories": {}}},
        },
    }
    doc = export_mod.build_snapshot_document(report)
    assert doc["salesforce_comprehensive_portfolio"]["by_customer"]["Acme"]["matched"] is True
    md = export_mod.render_markdown(doc, exported_at_utc="2020-01-01T00:00:00Z")
    assert "## 3c. Salesforce comprehensive" in md
    assert "by_customer" in md


def test_llm_export_sf_comprehensive_enabled_default_true(monkeypatch):
    monkeypatch.delenv("BPO_LLM_EXPORT_SF_COMPREHENSIVE", raising=False)
    assert llm_export_sf_comprehensive_enabled() is True
