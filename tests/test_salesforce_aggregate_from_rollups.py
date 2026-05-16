"""Salesforce aggregate helper for export segments."""

from __future__ import annotations

from src.data_sources.loaders.salesforce_portfolio_aggregate import salesforce_aggregate_from_rollups


def test_churn_segment_omits_portfolio_wide_pipeline_fields():
    rollups = [{"customer": "Gone", "active": False, "arr": 12.0}]
    out = salesforce_aggregate_from_rollups(rollups, book={"pipeline_arr": 500.0}, segment="churned")
    assert out["customer_segment"] == "churned"
    assert out["segment_contract_arr"] == 12.0
    assert "pipeline_arr" not in out
    assert out.get("portfolio_book_note")
