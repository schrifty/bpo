"""Unit tests for portfolio-level Salesforce revenue rollup."""

from unittest.mock import patch

import pytest

from src.deck_variants import enrich_portfolio_report_with_revenue_book
from src.salesforce_client import SalesforceClient


def test_get_portfolio_revenue_book_metrics_active_vs_churned_and_unmatched():
    accounts = [
        {
            "Id": "001000000000001AA",
            "Name": "Acme Division East",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Active",
            "Contract_Contract_Start_Date__c": "2024-01-15",
            "Contract_Contract_End_Date__c": "2027-06-01",
            "ARR__c": 100_000,
            "parent_name": "",
            "ultimate_parent_name": "",
        },
        {
            "Id": "001000000000002AA",
            "Name": "BetaCo — former customer",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Churned",
            "Contract_Contract_Start_Date__c": "2023-01-01",
            "Contract_Contract_End_Date__c": "2025-03-01",
            "ARR__c": 50_000,
            "parent_name": "",
            "ultimate_parent_name": "",
        },
    ]
    with (
        patch.object(SalesforceClient, "get_entity_accounts", return_value=accounts),
        patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=12_345.0),
        patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=7),
        patch.object(SalesforceClient, "get_open_pipeline_opportunities", return_value=[]),
        patch.object(
            SalesforceClient,
            "renewal_in_flight_fields_for_entities",
            return_value={"renewal_in_flight": False},
        ),
        patch.object(SalesforceClient, "_portfolio_closed_won_opportunity_rows_cy", return_value=[]),
    ):
        sf = SalesforceClient()
        m = sf.get_portfolio_revenue_book_metrics(["Acme", "Beta", "NoMatch"])

    assert m["pendo_customers"] == 3
    assert m["salesforce_matched_customers"] == 2
    assert m["salesforce_unmatched_customers"] == 1
    assert m["total_arr"] == 150_000.0
    assert m["active_installed_base_arr"] == 100_000.0
    assert m["churned_contract_arr"] == 50_000.0
    assert m["pipeline_arr"] == 12_345.0
    assert m["opportunity_count_this_year"] == 7
    assert m["active_customer_count"] == 1
    assert m["churned_customer_count"] == 1
    assert m["top_customers_by_arr"][0]["customer"] == "Acme"
    assert len(m["matched_customer_contract_rollups"]) == 2
    acme_row = next(r for r in m["matched_customer_contract_rollups"] if r["customer"] == "Acme")
    assert acme_row["contract_end_date_nearest"] == "2027-06-01"
    assert acme_row["contract_statuses_distinct"] == ["Active"]
    assert acme_row["entity_row_count"] == 1
    ex = m.get("expansion_kpis") or {}
    assert ex.get("eligible_active_customer_count") == 1
    assert ex.get("active_customers_with_expansion_wins_cy") == 0


def test_expansion_kpis_from_closed_won_assigns_expanding_labels():
    per_name = {
        "Acme": [
            {
                "Id": "001000000000001AA",
                "Contract_Status__c": "Active",
            }
        ],
        "Beta": [
            {
                "Id": "001000000000002AA",
                "Contract_Status__c": "Active",
            }
        ],
        "Gamma": [
            {
                "Id": "001000000000003AA",
                "Contract_Status__c": "Active",
            }
        ],
    }
    rows = [
        {"AccountId": "001000000000001AA", "Type": "Expansion Business", "Amount": 50_000},
        {"AccountId": "001000000000001AA", "Type": "Expansion Business", "Amount": 10_000},
        {"AccountId": "001000000000003AA", "Type": "New Business", "Amount": 120_000},
    ]
    out = SalesforceClient._expansion_kpis_from_opportunities(
        per_name=per_name,
        names_clean=list(per_name.keys()),
        closed_won_rows=rows,
        calendar_year=2026,
    )
    assert out["eligible_active_customer_count"] == 3
    assert out["active_customers_with_expansion_wins_cy"] == 1
    assert out["pct_active_customers_expanding_cy"] == pytest.approx(100 / 3, rel=0.01)
    assert out["closed_won_expansion_deal_count_cy"] == 2
    assert out["closed_won_expansion_amount_sum_cy"] == 60_000.0
    assert out["active_customers_with_new_business_won_cy"] == 1


def test_enrich_portfolio_revenue_book_when_salesforce_not_configured():
    report: dict = {"customers": [{"customer": "A"}]}
    with patch("src.data_source_health._salesforce_configured", return_value=False):
        enrich_portfolio_report_with_revenue_book(report)
    assert report["portfolio_revenue_book"] == {"configured": False}
    assert report["portfolio_expansion_book"] == {"configured": False}
