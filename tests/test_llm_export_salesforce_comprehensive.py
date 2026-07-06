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
        "disabled_via_CORTEX_LLM_EXPORT_SF_COMPREHENSIVE"
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
    monkeypatch.setattr(
        "src.customer_identity.lookup_salesforce_identity",
        lambda _label: ([], None),
    )

    def fake_load(name: str, *, row_limit: int = 75, **_kw):
        return (
            {
                "customer": name,
                "matched": True,
                "categories": {"contacts": [{"Id": "c1"}]},
                "row_limit": row_limit,
            },
            "salesforce",
        )

    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.load_or_fetch_salesforce_comprehensive",
        fake_load,
    )

    class FakeSf:
        def get_entity_accounts(self):
            return [
                {"Id": "a1", "Name": "Commercial HVAC (Carrier)", "ARR__c": 100.0, "Contract_Status__c": "Active"},
                {"Id": "a2", "Name": "Residential HVAC (Carrier)", "ARR__c": 250.0, "Contract_Status__c": "Active"},
                {"Id": "a3", "Name": "Safran", "ARR__c": 50.0, "Contract_Status__c": "Churned"},
            ]

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
    monkeypatch.setenv("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP", "0")
    summary = attach_salesforce_comprehensive_for_llm_export(report)
    assert summary["customers_requested"] == 2
    assert summary["selection"] == "all_portfolio_labels"
    assert summary["customers_matched"] == 2
    block = report["salesforce_comprehensive_portfolio"]
    assert block["configured"] is True
    assert block["row_limit"] == 8
    assert "Acme" in block["by_customer"]
    assert block["by_customer"]["Acme"]["customer_segment"] == "active"
    assert block["by_customer"]["OldCo"]["customer_segment"] == "churned"
    assert len(block["entity_accounts"]) == 3
    assert block["portfolio_expansion_book"]["pct_active_customers_expanding_cy"] == 12.5
    # Every entity carries SF-first grouping labels for Ultimate Parent rollups.
    first = block["entity_accounts"][0]
    assert first["ultimate_parent_group"] == "Carrier"
    assert "division_group" in first and "corporate_group" in first
    # Pre-aggregated Ultimate Parent ARR rollup, sorted descending, collapses Carrier divisions.
    rollup = block["arr_by_ultimate_parent"]
    assert rollup[0]["ultimate_parent"] == "Carrier"
    assert rollup[0]["arr"] == 350.0
    assert rollup[0]["entity_count"] == 2
    assert rollup[0]["active"] is True
    safran = next(r for r in rollup if r["ultimate_parent"] == "Safran")
    assert safran["active"] is False


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


def test_compact_preserves_arr_by_ultimate_parent_when_entities_truncated():
    block = {
        "configured": True,
        "by_customer": {},
        "entity_accounts": [{"Id": f"a{i}", "Name": f"E{i}"} for i in range(100)],
        "arr_by_ultimate_parent": [
            {"ultimate_parent": "Carrier", "arr": 350.0, "entity_count": 2, "active": True},
            {"ultimate_parent": "Safran", "arr": 50.0, "entity_count": 1, "active": False},
        ],
    }
    compact = export_mod._compact_salesforce_comprehensive_portfolio(block, entity_account_cap=48)
    assert compact["entity_accounts_truncated"] is True
    assert compact["arr_by_ultimate_parent"][0]["ultimate_parent"] == "Carrier"
    assert compact["arr_by_ultimate_parent_count"] == 2


def test_llm_export_sf_comprehensive_enabled_default_true(monkeypatch):
    monkeypatch.delenv("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE", raising=False)
    assert llm_export_sf_comprehensive_enabled() is True


def test_attach_comprehensive_cap_uses_top_active_by_arr(monkeypatch):
    monkeypatch.setenv("CORTEX_LLM_EXPORT_SF_COMPREHENSIVE_CUSTOMER_CAP", "1")
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive.llm_export_sf_comprehensive_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive._salesforce_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.customer_identity.lookup_salesforce_identity",
        lambda _label: ([], None),
    )
    fetched: list[str] = []

    def fake_load(name: str, *, row_limit: int = 75, **_kw):
        fetched.append(name)
        return {"customer": name, "matched": True, "categories": {}}, "salesforce"

    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.load_or_fetch_salesforce_comprehensive",
        fake_load,
    )
    class FakeSf:
        def get_entity_accounts(self):
            return []

    monkeypatch.setattr("src.salesforce_client.SalesforceClient", FakeSf)
    report = {
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "SmallCo", "arr": 1000, "active": True},
                {"customer": "BigCo", "arr": 900_000, "active": True},
                {"customer": "OldCo", "active": False},
            ],
        }
    }
    summary = attach_salesforce_comprehensive_for_llm_export(report)
    assert summary["selection"] == "top_active_by_arr"
    assert summary["customers_requested"] == 1
    assert fetched == ["BigCo"]
    assert summary["selection_ranked"][0]["salesforce_label"] == "BigCo"
