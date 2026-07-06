"""LLM export: Salesforce Customer Entity universe without mandatory Pendo."""

from __future__ import annotations

from src.llm_export_salesforce_universe import (
    attach_churned_salesforce_segment_for_llm_export,
    attach_renewal_negotiation_segment_for_llm_export,
    merge_active_salesforce_customers_for_llm_export,
    merge_salesforce_universe_for_llm_export,
    partition_inactive_sf_rollups,
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
            [],
            [],
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


def test_merge_universe_fetches_salesforce_rollups_once(monkeypatch):
    split_calls = 0

    def fake_split():
        nonlocal split_calls
        split_calls += 1
        return (
            [{"customer": "Active Co", "active": True}],
            [{"customer": "Gone LLC", "active": False}],
            ["Active Co", "Gone LLC"],
            {"configured": True},
            [],
            [],
        )

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.salesforce_portfolio_rollups_split",
        fake_split,
    )

    report = {"customers": [{"customer": "Active Co", "total_users": 1}], "portfolio_signals": []}
    merge_salesforce_universe_for_llm_export(report)
    assert split_calls == 1
    assert report["salesforce_churned_segment"]["customer_count"] == 1


def test_merge_universe_calls_active_and_churn(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.merge_active_salesforce_customers_for_llm_export",
        lambda _r, **_: calls.append("active") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.attach_churned_salesforce_segment_for_llm_export",
        lambda _r, **_: calls.append("churn") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.attach_renewal_negotiation_segment_for_llm_export",
        lambda _r, **_: calls.append("renewal") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.attach_future_contract_segment_for_llm_export",
        lambda _r, **_: calls.append("future") or {},
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.strip_churned_customers_from_active_export",
        lambda _r: calls.append("strip") or {},
    )
    merge_salesforce_universe_for_llm_export({})
    assert calls == ["active", "churn", "renewal", "future", "strip"]


def test_partition_inactive_splits_renewal_from_churned_lost():
    rollups = [
        {
            "customer": "Ford",
            "commercial_status": "OUT_OF_CONTRACT_RENEWING",
            "renewal_in_flight": True,
        },
        {"customer": "Gone", "commercial_status": "CHURNED"},
        {"customer": "Later", "commercial_status": "FUTURE"},
        {"customer": "Active", "commercial_status": "ACTIVE"},
    ]
    lost, renewal, future = partition_inactive_sf_rollups(rollups)
    assert [r["customer"] for r in lost] == ["Gone"]
    assert [r["customer"] for r in renewal] == ["Ford"]
    assert [r["customer"] for r in future] == ["Later"]


def test_renewal_negotiation_segment_separate_from_churned(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_salesforce_universe._salesforce_configured",
        lambda: True,
    )

    def fake_split():
        return (
            [],
            [{"customer": "Gone LLC", "commercial_status": "CHURNED"}],
            [],
            {"configured": True},
            [{"customer": "Ford Motor Company", "commercial_status": "OUT_OF_CONTRACT_RENEWING"}],
            [],
        )

    monkeypatch.setattr(
        "src.llm_export_salesforce_universe.salesforce_portfolio_rollups_split",
        fake_split,
    )
    report: dict = {}
    attach_churned_salesforce_segment_for_llm_export(report)
    attach_renewal_negotiation_segment_for_llm_export(report)
    assert report["salesforce_churned_segment"]["customer_count"] == 1
    assert report["salesforce_churned_segment"]["customers_headline"][0]["customer"] == "Gone LLC"
    ren = report["salesforce_renewal_negotiation_segment"]
    assert ren["customer_count"] == 1
    assert ren["customers_headline"][0]["customer"] == "Ford Motor Company"
    assert ren["customers_headline"][0]["customer_segment"] == "renewal_negotiation"
