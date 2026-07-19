"""Tests for LLM export CS Report top-by-ARR attachment."""

from __future__ import annotations

from src.cs_report_client import load_csr_top_customers_by_arr
from src.export_llm_context_snapshot import _compact_csr
from src.llm_export_csr import (
    LLM_EXPORT_TOP_ARR_SCOPE,
    attach_csr_top_customers_for_llm_export,
    top_active_ultimate_parents_by_arr_for_llm_export,
)


def _csr_top_arr_fixture() -> dict:
    """Top-ARR CSR shape where the same factory appears in all three worksheets."""
    return {
        "scope": LLM_EXPORT_TOP_ARR_SCOPE,
        "top_n": 1,
        "selection_ranked": [],
        "customers": {
            "Acme": {
                "ultimate_parent": "Acme",
                "salesforce_label": "Acme",
                "arr": 100.0,
                "csr_lookup_name": "Acme",
                "platform_health": {
                    "customer": "Acme",
                    "factory_count": 2,
                    "health_distribution": {"RED": 1, "GREEN": 1},
                    "total_shortages": 12,
                    "total_critical_shortages": 3,
                    "sites": [
                        {"factory": "Plant A", "health_score": "RED", "shortages": 12, "supplier_commit_date_pct": 91.2},
                        {"factory": "Plant B", "health_score": "GREEN", "shortages": 0},
                    ],
                },
                "supply_chain": {
                    "customer": "Acme",
                    "totals": {"on_hand": 5000},
                    "sites": [
                        {"factory": "Plant A", "on_hand_value": 3000, "doi_days": 45.0},
                        {"factory": "Plant B", "on_hand_value": 2000, "doi_days": 30.0},
                    ],
                },
                "platform_value": {
                    "customer": "Acme",
                    "total_savings": 900,
                    "sites": [
                        {"factory": "Plant A", "savings_current_period": 700},
                    ],
                },
            }
        },
    }


def test_compact_csr_merges_factory_rows_across_worksheets():
    out = _compact_csr(_csr_top_arr_fixture(), site_limit=15, string_cap=400, size_caps_enabled=True)
    acme = out["customers"]["Acme"]
    # No per-section site duplication: single merged sites list.
    assert "platform_health" not in acme and "supply_chain" not in acme
    assert acme["sites_total"] == 2
    # Site-row keys are abbreviated; decode via the published field_legend (short -> long).
    legend = out["field_legend"]
    assert legend["fac"] == "factory"
    assert legend["hs"] == "health_score"
    assert legend["ohv"] == "on_hand_value"
    assert legend["scp"] == "savings_current_period"
    assert legend["scdp"] == "supplier_commit_date_pct"
    by_factory = {s["fac"]: s for s in acme["sites"]}
    plant_a = by_factory["Plant A"]
    # One factory row carries health + supply-chain + value metrics together (abbreviated keys).
    assert plant_a["hs"] == "RED"
    assert plant_a["ohv"] == 3000
    assert plant_a["scp"] == 700
    assert plant_a["scdp"] == 91.2
    # Long-form keys are not present on abbreviated site rows.
    assert "health_score" not in plant_a and "on_hand_value" not in plant_a
    # Section rollups preserved in summary with full-length keys.
    assert acme["summary"]["factory_count"] == 2
    assert acme["summary"]["total_shortages"] == 12
    assert acme["summary"]["inventory_totals"] == {"on_hand": 5000}
    assert acme["summary"]["total_savings"] == 900
    assert "schema_note" in out


def test_compact_csr_records_section_errors_without_sites():
    csr = _csr_top_arr_fixture()
    csr["customers"]["Acme"]["platform_value"] = {"error": "no rows", "source": "cs_report"}
    out = _compact_csr(csr, site_limit=15, string_cap=400, size_caps_enabled=True)
    acme = out["customers"]["Acme"]
    assert acme["section_errors"] == {"platform_value": "no rows"}
    # Health + supply rows still merge.
    assert acme["sites_total"] == 2


def test_compact_csr_field_legend_has_no_short_key_collisions():
    from src.cs_report_client import CSR_MERGED_SITE_EXPORT_COLUMNS, CSR_SITE_FIELD_ABBR, CSR_SITE_FIELD_LEGEND

    # Every export column has a short key; no two long names share one abbreviation.
    assert set(CSR_SITE_FIELD_ABBR.keys()) >= set(CSR_MERGED_SITE_EXPORT_COLUMNS)
    assert len(set(CSR_SITE_FIELD_ABBR.values())) == len(CSR_SITE_FIELD_ABBR)
    assert CSR_SITE_FIELD_LEGEND == {v: k for k, v in CSR_SITE_FIELD_ABBR.items()}


def test_render_cs_report_section_emits_summary_table_and_detail_without_summary():
    from src.export_llm_context_snapshot import _render_cs_report_section

    out = _compact_csr(_csr_top_arr_fixture(), site_limit=15, string_cap=400, size_caps_enabled=True)
    body = "\n".join(_render_cs_report_section(out))
    # §4.1 renders a per-customer summary markdown table with flattened nested columns.
    assert "### 4.1 Per-customer summary" in body
    assert "| customer |" in body and "| --- |" in body
    assert "health_RED" in body and "health_GREEN" in body
    assert "inv_on_hand" in body
    # Acme's rollups appear as a table row (factory_count=2, total_shortages=12, total_savings=900).
    acme_row = next(ln for ln in body.splitlines() if ln.startswith("| Acme |"))
    for token in ("| 2 |", "| 12 |", "| 900 |"):
        assert token in acme_row
    # §4.2 keeps factory detail as JSON but drops the now-redundant per-customer summary.
    assert "### 4.2 Per-customer factory detail" in body
    assert '"sites"' in body
    assert '"summary"' not in body


def test_render_cs_report_section_falls_back_to_json_without_customers():
    from src.export_llm_context_snapshot import _render_cs_report_section

    # Empty / legacy CSR has no per-customer summaries to tabulate -> single JSON blob, no table.
    body = "\n".join(_render_cs_report_section({"note": "not attached"}))
    assert "### 4.1" not in body
    assert '"note"' in body


def test_top_active_ultimate_parents_groups_carrier_divisions():
    report = {
        "customers": [{"customer": "carrier"}],
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Commercial HVAC (Carrier)", "arr": 400_000.0, "active": True},
                {"customer": "Residential HVAC (Carrier)", "arr": 300_000.0, "active": True},
                {"customer": "Other Co", "arr": 100_000.0, "active": True},
            ],
        },
    }
    rows = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=2)
    assert len(rows) == 2
    assert rows[0]["ultimate_parent"] == "Carrier"
    assert rows[0]["arr"] == 700_000.0
    assert set(rows[0]["salesforce_labels"]) == {
        "Commercial HVAC (Carrier)",
        "Residential HVAC (Carrier)",
    }
    assert rows[1]["ultimate_parent"] == "Other Co"


def test_top_active_customers_by_arr_for_csr():
    report = {
        "customers": [{"customer": "Duravant"}],
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Small", "arr": 10.0, "active": True, "current_arr": 10.0},
                {"customer": "Big", "arr": 500.0, "active": True, "current_arr": 500.0},
                {"customer": "Churned", "arr": 999.0, "active": False, "commercial_status": "CHURNED", "current_arr": 0.0},
            ],
        },
    }
    rows = top_active_ultimate_parents_by_arr_for_llm_export(report, top_n=2)
    assert len(rows) == 2
    assert rows[0]["ultimate_parent"] == "Big"
    assert rows[0]["arr"] == 500.0
    assert rows[1]["ultimate_parent"] == "Small"


def test_group_contract_rollups_full_book_includes_renewal_and_excludes_from_selection_filter():
    from src.llm_export_csr import group_contract_rollups_by_ultimate_parent

    rollups = [
        {"customer": "Ford Motor Company", "commercial_status": "OUT_OF_CONTRACT_RENEWING", "current_arr": 525_000.0, "historical_arr": 525_000.0, "renewal_arr": 525_000.0},
        {"customer": "ChurnedCo", "commercial_status": "CHURNED", "current_arr": 0.0, "historical_arr": 50_000.0},
        {"customer": "Commercial HVAC (Carrier)", "commercial_status": "ACTIVE", "current_arr": 600_000.0, "historical_arr": 600_000.0, "active_arr": 600_000.0},
        {"customer": "Residential HVAC (Carrier)", "commercial_status": "ACTIVE", "current_arr": 501_650.0, "historical_arr": 501_650.0, "active_arr": 501_650.0},
    ]
    full = group_contract_rollups_by_ultimate_parent(rollups, current_book_only=False)
    current = group_contract_rollups_by_ultimate_parent(rollups, current_book_only=True)
    assert len(full) == 3
    assert next(r for r in full if r["ultimate_parent"] == "Carrier")["current_arr"] == 1_101_650.0
    ford_full = next(r for r in full if r["ultimate_parent"] == "Ford Motor Company")
    assert ford_full["commercial_status"] == "OUT_OF_CONTRACT_RENEWING"
    assert len(current) == 2
    assert any(r["ultimate_parent"] == "Ford Motor Company" for r in current)
    assert any(r["ultimate_parent"] == "Carrier" for r in current)
    assert not any(r["ultimate_parent"] == "ChurnedCo" for r in current)


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
        [{"ultimate_parent": "Acme", "salesforce_label": "Acme", "arr": 100.0, "csr_lookup_name": "Acme"}]
    )
    assert out["scope"] == LLM_EXPORT_TOP_ARR_SCOPE
    assert "Acme" in out["customers"]
    assert out["customers"]["Acme"]["platform_health"]["customer"] == "Acme"


def test_attach_csr_top_customers_for_llm_export(monkeypatch):
    monkeypatch.setattr(
        "src.cs_report_client.load_csr_top_customers_by_arr",
        lambda sel: {
            "scope": LLM_EXPORT_TOP_ARR_SCOPE,
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
