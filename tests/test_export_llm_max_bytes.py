"""LLM export default max-bytes and §3c headline compaction."""

from __future__ import annotations

from src import export_llm_context_snapshot as mod


def test_llm_export_default_max_bytes_is_500k():
    assert mod.llm_export_default_max_bytes() == 500_000


def test_compact_salesforce_comprehensive_keeps_top_arr_and_samples():
    report = {
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "BigCo", "arr": 900_000, "active": True},
                {"customer": "SmallCo", "arr": 1_000, "active": True},
            ],
        },
    }
    block = {
        "configured": True,
        "by_customer": {
            "BigCo": {
                "matched": True,
                "pipeline_arr": 100.0,
                "categories": {
                    "contacts": [{"Id": f"C{i}"} for i in range(20)],
                },
            },
            "SmallCo": {"matched": True, "categories": {"contacts": [{"Id": "X"}]}},
        },
    }
    slim = mod._compact_salesforce_comprehensive_portfolio(
        block,
        report=report,
        top_customers=1,
        rows_per_category=3,
    )
    assert slim["export_compaction"]["mode"] == "headline"
    assert list(slim["by_customer"].keys()) == ["BigCo"]
    contacts = slim["by_customer"]["BigCo"]["categories"]["contacts"]
    assert contacts["row_count"] == 20
    assert len(contacts["sample"]) == 3


def test_build_snapshot_compacts_sf_comprehensive_when_caps_enabled():
    report = {
        "customer": "All Customers",
        "generated": "2020-01-01T00:00:00Z",
        "days": 90,
        "portfolio_signals": [],
        "csr": {},
        "salesforce": {},
        "jira": {},
        "salesforce_comprehensive_portfolio": {
            "configured": True,
            "by_customer": {"Acme": {"matched": True, "categories": {"cases": [{"Id": "1"}]}}},
        },
    }
    doc = mod.build_snapshot_document(report, size_caps_enabled=True)
    assert doc["salesforce_comprehensive_portfolio"]["export_compaction"]["mode"] == "headline"
