"""Tests for explicit datasource profile used by the all-customers LLM export."""

from __future__ import annotations

from src.data_sources import (
    PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
    PROFILE_LLM_EXPORT_ALL_CUSTOMERS,
    SourceId,
    build_llm_export_snapshot_report,
)


def test_profile_llm_export_covers_four_sources():
    assert len(PROFILE_LLM_EXPORT_ALL_CUSTOMERS) == 4
    assert SourceId.PENDO_PORTFOLIO_ROLLUP in PROFILE_LLM_EXPORT_ALL_CUSTOMERS


def test_build_llm_export_provenance_on_portfolio_failure():
    class _Pc:
        def get_portfolio_report(self, **kwargs):
            return {"error": "unit test portfolio failure"}

    r = build_llm_export_snapshot_report(_Pc(), days=30)
    assert r.get("error") == "unit test portfolio failure"
    prov = r.get("_data_source_provenance") or {}
    assert prov.get("profile_id") == PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS
    sources = prov.get("sources") or []
    assert sources and sources[0]["source"] == str(SourceId.PENDO_PORTFOLIO_ROLLUP)
    assert sources[0]["status"] == "error"
