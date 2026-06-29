"""Tests for SF ↔ Pendo reconciliation heuristics."""

from __future__ import annotations

from src.llm_export_salesforce_universe import _row_has_pendo_metrics
from src.sf_pendo_reconcile import build_reconcile_report, score_sf_pendo_pair


def test_row_has_pendo_metrics() -> None:
    assert _row_has_pendo_metrics({"total_users": 10})
    assert not row_metrics({"salesforce_only": True})
    assert not row_metrics({})


def test_score_subsidiary_entity_name() -> None:
    score, reasons = score_sf_pendo_pair(
        "Key Technology",
        "Duravant",
        sf_entity_names=frozenset({"Duravant- Key Technology (Walla Walla)"}),
    )
    assert score >= 0.78
    assert any("entity" in r.lower() for r in reasons)


def test_score_acronym() -> None:
    score, reasons = score_sf_pendo_pair("Johnson Controls International", "JCI")
    assert score >= 0.75
    assert any("acronym" in r for r in reasons)


def test_build_reconcile_report_minimal() -> None:
    portfolio = {
        "customers": [
            {"customer": "Acme", "total_users": 5, "active_users": 2, "login_pct": 10.0},
            {"customer": "OrphanCo", "total_users": 1, "active_users": 1, "login_pct": 5.0},
        ]
    }
    rep = build_reconcile_report(portfolio, days=90, entity_accounts=[])
    assert rep.days == 90
    assert "Acme" in rep.pendo_prefixes or "OrphanCo" in rep.pendo_prefixes
