"""Tests for LLM export Jira top-by-ARR attachment."""

from __future__ import annotations

from src.llm_export_jira import attach_jira_top_customers_for_llm_export


def test_attach_jira_top_customers_for_llm_export(monkeypatch):
    calls: list[str] = []

    class _Jc:
        def get_customer_jira(self, name, days=90):
            calls.append(str(name))
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
    assert report["jira"]["scope"] == "top_customers_by_arr"
    assert "Acme" in report["jira"]["customers"]
    assert report["jira"]["customers"]["Acme"]["jira"]["total_issues"] == 3
    assert len(calls) == 1


def test_attach_jira_empty_selection():
    report = {"days": 30}
    summary = attach_jira_top_customers_for_llm_export(report)
    assert summary["customers_selected"] == 0
    assert report["jira"]["customers"] == {}
