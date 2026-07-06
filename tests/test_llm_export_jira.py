"""Tests for LLM export Jira top-by-ARR attachment."""

from __future__ import annotations

from src.llm_export_jira import (
    _jira_merged_lookup_bundle,
    attach_jira_top_customers_for_llm_export,
)


def test_jira_merged_lookup_bundle_includes_division_names():
    row = {
        "ultimate_parent": "Carrier",
        "salesforce_labels": [
            "Commercial HVAC (Carrier)",
            "Residential HVAC (Carrier)",
        ],
        "pendo_customer_key": "carrier",
    }
    primary, match_terms, keys = _jira_merged_lookup_bundle(row)
    assert primary == "Carrier"
    assert "Commercial HVAC (Carrier)" in match_terms
    assert "Commercial HVAC" in match_terms
    assert "Residential HVAC" in match_terms
    assert "carrier" in [k.lower() for k in keys]


def test_attach_jira_top_customers_for_llm_export(monkeypatch):
    calls: list[tuple[str, list[str] | None]] = []

    class _Jc:
        def get_customer_jira(self, name, days=90, match_terms=None):
            calls.append((str(name), match_terms))
            return {"customer": name, "days": days, "total_issues": 3}

    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: _Jc())
    monkeypatch.setenv("CORTEX_LLM_EXPORT_JIRA_TOP_N", "1")
    monkeypatch.setenv("CORTEX_LLM_EXPORT_JIRA_WORKERS", "1")

    report = {
        "days": 90,
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Acme", "arr": 50.0, "active": True},
                {"customer": "Other", "arr": 1.0, "active": True},
            ],
        },
        "customers": [{"customer": "acme"}],
    }
    summary = attach_jira_top_customers_for_llm_export(report)
    assert summary["customers_selected"] == 1
    assert summary["customers_with_jira_data"] == 1
    assert report["jira"]["scope"] == "top_ultimate_parents_by_arr"
    assert "Acme" in report["jira"]["customers"]
    assert report["jira"]["customers"]["Acme"]["jira"]["total_issues"] == 3
    assert report["jira"]["customers"]["Acme"]["jira_merged_subsidiary_lookups"] is False
    assert len(calls) == 1
    assert calls[0][0] == "Acme"


def test_attach_jira_merges_subsidiary_match_terms(monkeypatch):
    calls: list[tuple[str, list[str] | None]] = []

    class _Jc:
        def get_customer_jira(self, name, days=90, match_terms=None):
            calls.append((str(name), match_terms))
            return {"customer": name, "total_issues": 10, "jsm_organizations_resolved": ["Carrier"]}

    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: _Jc())
    monkeypatch.setenv("CORTEX_LLM_EXPORT_JIRA_TOP_N", "1")
    monkeypatch.setenv("CORTEX_LLM_EXPORT_JIRA_WORKERS", "1")

    report = {
        "days": 90,
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Commercial HVAC (Carrier)", "arr": 400_000.0, "active": True},
                {"customer": "Residential HVAC (Carrier)", "arr": 300_000.0, "active": True},
                {"customer": "Small", "arr": 1.0, "active": True},
            ],
        },
        "customers": [{"customer": "carrier"}],
    }
    attach_jira_top_customers_for_llm_export(report)
    assert len(calls) == 1
    primary, match_terms = calls[0]
    assert primary == "Carrier"
    assert match_terms is not None
    assert "Commercial HVAC (Carrier)" in match_terms
    assert "Commercial HVAC" in match_terms
    assert "Residential HVAC" in match_terms
    block = report["jira"]["customers"]["Carrier"]
    assert block["jira_merged_subsidiary_lookups"] is True


def test_attach_jira_empty_selection():
    report = {"days": 30}
    summary = attach_jira_top_customers_for_llm_export(report)
    assert summary["customers_selected"] == 0
    assert report["jira"]["customers"] == {}
