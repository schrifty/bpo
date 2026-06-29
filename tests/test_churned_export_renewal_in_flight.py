"""Churn export: renewal-in-flight accounts are not stripped from active §1."""

from __future__ import annotations

from src.llm_export_salesforce_universe import (
    churned_sf_excluded_customer_keys,
    strip_churned_customers_from_active_export,
)


def test_renewal_in_flight_not_excluded_from_active_book():
    report = {
        "customers": [
            {"customer": "Ford", "total_users": 100},
            {"customer": "GoneCo", "total_users": 1},
        ],
        "salesforce_churned_segment": {
            "customers_headline": [
                {"customer": "Ford", "renewal_in_flight": True},
                {"customer": "GoneCo", "renewal_in_flight": False},
            ],
        },
    }
    excluded = churned_sf_excluded_customer_keys(report)
    assert "goneco" in excluded
    assert "ford" not in excluded
    summary = strip_churned_customers_from_active_export(report)
    assert summary["removed_customer_rows"] == 1
    assert [r["customer"] for r in report["customers"]] == ["Ford"]
