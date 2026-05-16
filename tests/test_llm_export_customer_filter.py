"""Unit tests for LLM all-customers export portfolio-row filters."""

from __future__ import annotations

from src.llm_export_customer_filter import (
    LlmExportCustomerFilterConfig,
    apply_llm_export_customer_filters,
)


def test_explicit_exclude_exact_case_insensitive(monkeypatch):
    def _noop_aggregate(_report):
        return {"error": None, "resolution": "none", "matched": False}

    monkeypatch.setattr(
        "src.data_sources.loaders.salesforce_portfolio_aggregate.salesforce_portfolio_aggregate_for_report",
        _noop_aggregate,
    )

    report = {
        "customers": [{"customer": "KeepCo"}, {"customer": "DROPme"}, {"customer": "dropme"}],
        "portfolio_signals": [
            {"customer": "KEEPco", "signal": "ok"},
            {"customer": "DROPme", "signal": "x"},
        ],
    }
    cfg = LlmExportCustomerFilterConfig(
        exclude_names_lower=frozenset({"dropme"}),
    )
    apply_llm_export_customer_filters(report, cfg)
    names = [r["customer"] for r in report["customers"]]
    assert names == ["KeepCo"]
    assert len(report["portfolio_signals"]) == 1
    assert report["portfolio_signals"][0]["customer"] == "KEEPco"


def test_exclude_sf_churned_matched(monkeypatch):
    def _noop_aggregate(_report):
        return {}

    monkeypatch.setattr(
        "src.data_sources.loaders.salesforce_portfolio_aggregate.salesforce_portfolio_aggregate_for_report",
        _noop_aggregate,
    )

    report = {
        "customers": [
            {"customer": "ActiveInc"},
            {"customer": "GoneCorp"},
            {"customer": "NoSfRow"},
        ],
        "portfolio_signals": [{"customer": "GoneCorp", "signal": "z"}],
        "salesforce": {
            "error": None,
            "matched_customer_contract_rollups": [
                {"customer": "ActiveInc", "active": True},
                {"customer": "GoneCorp", "active": False},
            ],
        },
    }
    cfg = LlmExportCustomerFilterConfig(exclude_sf_churned_matched=True)
    apply_llm_export_customer_filters(report, cfg)
    nc = [r["customer"] for r in report["customers"]]
    assert nc == ["ActiveInc", "NoSfRow"]
    assert report["portfolio_signals"] == []


def test_sf_allowlist_intersect(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_customer_filter._salesforce_configured",
        lambda: True,
    )

    def fake_active_allowlist():
        return (
            frozenset({"alpha", "gamma llc"}),
            ["Alpha Parent", "Gamma LLC"],
            {"configured": True},
        )

    monkeypatch.setattr(
        "src.llm_export_customer_filter.active_sf_allowlist_lower",
        fake_active_allowlist,
    )

    def _capture_aggregate(rep):
        rep["_aggregate_seen_customers_len"] = len(rep.get("customers") or [])
        return {"resolution": "portfolio_aggregate"}

    monkeypatch.setattr(
        "src.data_sources.loaders.salesforce_portfolio_aggregate.salesforce_portfolio_aggregate_for_report",
        _capture_aggregate,
    )

    report = {
        "customers": [
            {"customer": "Alpha"},
            {"customer": "Beta"},
            {"customer": "Gamma LLC", "salesforce_only": True},
        ],
        "portfolio_signals": [
            {"customer": "Beta", "signal": "noise"},
            {"customer": "Alpha", "signal": "sig"},
            {"customer": "Gamma LLC", "signal": "sf only"},
        ],
    }
    cfg = LlmExportCustomerFilterConfig(sf_allowlist=True)
    apply_llm_export_customer_filters(report, cfg)
    assert [r["customer"] for r in report["customers"]] == ["Alpha", "Gamma LLC"]
    assert len(report["portfolio_signals"]) == 2
    assert {s["customer"] for s in report["portfolio_signals"]} == {"Alpha", "Gamma LLC"}


def test_sf_allowlist_requires_salesforce(monkeypatch):
    monkeypatch.setattr(
        "src.llm_export_customer_filter._salesforce_configured",
        lambda: False,
    )
    report = {"customers": [{"customer": "X"}], "portfolio_signals": []}
    cfg = LlmExportCustomerFilterConfig(sf_allowlist=True)
    try:
        apply_llm_export_customer_filters(report, cfg)
    except RuntimeError as e:
        assert "Salesforce allowlist" in str(e)
    else:
        raise AssertionError("expected RuntimeError")
