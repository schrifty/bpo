"""Tests for explicit datasource profile used by the all-customers LLM export."""

from __future__ import annotations

from src.data_sources import (
    PROFILE_LEANDNA_QBR_ENRICHMENTS,
    PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
    PROFILE_SINGLE_CUSTOMER_HEALTH_CORE,
    PROFILE_LLM_EXPORT_ALL_CUSTOMERS,
    SourceId,
    build_llm_export_snapshot_report,
)


def test_profile_llm_export_covers_four_sources():
    assert len(PROFILE_LLM_EXPORT_ALL_CUSTOMERS) == 4
    assert SourceId.PENDO_PORTFOLIO_ROLLUP in PROFILE_LLM_EXPORT_ALL_CUSTOMERS


def test_profile_leandna_qbr_enrichments_lists_three_api_bundles():
    assert len(PROFILE_LEANDNA_QBR_ENRICHMENTS) == 3
    assert SourceId.LEANDNA_ITEM_MASTER in PROFILE_LEANDNA_QBR_ENRICHMENTS
    assert SourceId.LEANDNA_SHORTAGE_TRENDS in PROFILE_LEANDNA_QBR_ENRICHMENTS
    assert SourceId.LEANDNA_LEAN_PROJECTS in PROFILE_LEANDNA_QBR_ENRICHMENTS


def test_profile_single_customer_health_core_lists_pendo_and_csr():
    assert len(PROFILE_SINGLE_CUSTOMER_HEALTH_CORE) == 2
    assert SourceId.PENDO_CUSTOMER_HEALTH in PROFILE_SINGLE_CUSTOMER_HEALTH_CORE
    assert SourceId.CS_REPORT_CUSTOMER_WEEK in PROFILE_SINGLE_CUSTOMER_HEALTH_CORE


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
