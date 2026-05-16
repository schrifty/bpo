"""LLM export: Salesforce Customer Entity universe without mandatory Pendo."""

from __future__ import annotations

from src.llm_export_salesforce_universe import (
    attach_churned_salesforce_segment_for_llm_export,
    merge_active_salesforce_customers_for_llm_export,
    merge_salesforce_universe_for_llm_export,
    strip_churned_customers_from_active_export,
)


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
    assert sf_only.get("customer_segment") == "active"


def test_attach_churned_segment_separate_from_active(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: True,
    )

    def fake_split():
        return (
            [{"customer": "Active Co", "active": True, "arr": 100.0}],
            [{"customer": "Gone LLC", "active": False, "arr": 5.0}],
            ["Active Co", "Gone LLC"],
            {"configured": True, "pipeline_arr": 999.0},
        )

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.salesforce_portfolio_rollups_split",
        fake_split,
    )

    report = {"customers": [{"customer": "Active Co", "total_users": 1}]}
    attach_churned_salesforce_segment_for_llm_export(report)
    seg = report["salesforce_churned_segment"]
    assert seg["do_not_merge_with_active_book"] is True
    assert seg["customer_count"] == 1
    assert seg["customers_headline"][0]["customer"] == "Gone LLC"
    assert seg["customers_headline"][0]["customer_segment"] == "churned"
    assert seg["salesforce"]["customer_segment"] == "churned"
    assert "pipeline_arr" not in seg["salesforce"]
    assert [r["customer"] for r in report["customers"]] == ["Active Co"]


def test_strip_removes_churned_from_active_pendo_sections():
    report = {
        "customers": [
            {"customer": "ActiveCo", "total_users": 10},
            {"customer": "GonePendo", "total_users": 5},
        ],
        "portfolio_signals": [
            {"customer": "ActiveCo", "signal": "ok"},
            {"customer": "GonePendo", "signal": "read-heavy"},
        ],
        "salesforce_churned_segment": {
            "customers_headline": [{"customer": "Gone LLC"}],
            "salesforce": {
                "matched_customer_contract_rollups": [{"customer": "Gone LLC", "active": False}],
            },
        },
    }

    def fake_resolve(label, prefixes):
        if label == "Gone LLC" and "GonePendo" in prefixes:
            return "GonePendo"
        return None

    import src.llm_export_salesforce_universe as mod

    orig = mod.resolve_sf_label_to_pendo_prefix
    mod.resolve_sf_label_to_pendo_prefix = fake_resolve
    try:
        summary = strip_churned_customers_from_active_export(report)
    finally:
        mod.resolve_sf_label_to_pendo_prefix = orig

    assert summary["removed_customer_rows"] == 1
    assert summary["removed_portfolio_signals"] == 1
    assert [r["customer"] for r in report["customers"]] == ["ActiveCo"]
    assert report["portfolio_signals"][0]["customer"] == "ActiveCo"


def test_merge_universe_calls_active_and_churn(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.merge_active_salesforce_customers_for_llm_export",
        lambda _r: calls.append("active") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.attach_churned_salesforce_segment_for_llm_export",
        lambda _r: calls.append("churn") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.strip_churned_customers_from_active_export",
        lambda _r: calls.append("strip") or {},
    )
    merge_salesforce_universe_for_llm_export({})
    assert calls == ["active", "churn", "strip"]
