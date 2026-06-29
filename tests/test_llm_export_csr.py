"""Tests for LLM export CS Report top-by-ARR attachment."""

from __future__ import annotations

from src.cs_report_client import load_csr_top_customers_by_arr
from src.llm_export_csr import (
    attach_csr_top_customers_for_llm_export,
    top_active_customers_by_arr_for_csr,
)


def test_top_active_customers_by_arr_for_csr():
    report = {
        "customers": [{"customer": "Duravant"}],
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Small", "arr": 10.0, "active": True},
                {"customer": "Big", "arr": 500.0, "active": True},
                {"customer": "Churned", "arr": 999.0, "active": False},
            ],
        },
    }
    rows = top_active_customers_by_arr_for_csr(report, top_n=2)
    assert len(rows) == 2
    assert rows[0]["salesforce_label"] == "Big"
    assert rows[0]["arr"] == 500.0
    assert rows[1]["salesforce_label"] == "Small"


def test_load_csr_top_customers_by_arr(monkeypatch):
    from src import cs_report_client as m

    def ph(name: str, **kwargs):
        return {"customer": name, "sites": [{"factory": "f1"}]}

    def sc(name: str, **kwargs):
        return {"customer": name, "sites": []}

    def pv(name: str, **kwargs):
        return {"customer": name, "sites": []}

    monkeypatch.setattr(m, "get_customer_platform_health", ph)
    monkeypatch.setattr(m, "get_customer_supply_chain", sc)
    monkeypatch.setattr(m, "get_customer_platform_value", pv)

    out = load_csr_top_customers_by_arr(
        [{"salesforce_label": "Acme", "arr": 100.0, "csr_lookup_name": "Acme"}]
    )
    assert out["scope"] == "top_customers_by_arr"
    assert "Acme" in out["customers"]
    assert out["customers"]["Acme"]["platform_health"]["customer"] == "Acme"


def test_attach_csr_top_customers_for_llm_export(monkeypatch):
    monkeypatch.setattr(
        "src.cs_report_client.load_csr_top_customers_by_arr",
        lambda sel: {
            "scope": "top_customers_by_arr",
            "top_n": len(sel),
            "selection_ranked": [],
            "customers": {
                "Acme": {
                    "platform_health": {"customer": "Acme"},
                    "supply_chain": {"customer": "Acme"},
                    "platform_value": {"customer": "Acme"},
                }
            },
        },
    )
    report = {
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Acme", "arr": 50.0, "active": True},
            ],
        },
        "customers": [],
    }
    summary = attach_csr_top_customers_for_llm_export(report)
    assert summary["customers_selected"] == 1
    assert summary["customers_with_csr_data"] == 1
    assert report["csr"]["customers"]["Acme"]["platform_health"]["customer"] == "Acme"
