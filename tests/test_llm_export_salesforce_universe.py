"""LLM export: Salesforce Customer Entity universe without mandatory Pendo."""

from __future__ import annotations

from src.llm_export_salesforce_universe import merge_active_salesforce_customers_for_llm_export


def test_merge_adds_active_salesforce_only_customers(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: True,
    )

    def fake_active_rollups():
        rollups = [
            {"customer": "Acme Corp", "active": True, "arr": 10.0},
            {"customer": "NoPendo Parent", "active": True, "arr": 5.0},
        ]
        return (
            rollups,
            ["Acme Corp", "NoPendo Parent"],
            {"configured": True},
        )

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.active_salesforce_portfolio_rollups",
        fake_active_rollups,
    )

    report = {
        "customers": [{"customer": "Acme", "total_users": 10, "active_users": 2}],
        "customer_count": 1,
    }
    summary = merge_active_salesforce_customers_for_llm_export(report)
    names = [r["customer"] for r in report["customers"]]
    assert "Acme" in names
    assert "NoPendo Parent" in names
    assert "Churned LLC" not in names
    assert summary["added_salesforce_only_rows"] == 1
    sf_only = next(r for r in report["customers"] if r["customer"] == "NoPendo Parent")
    assert sf_only.get("salesforce_only") is True
