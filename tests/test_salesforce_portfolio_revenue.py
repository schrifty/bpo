"""Unit tests for portfolio-level Salesforce revenue rollup."""

from unittest.mock import patch

import pytest

from src.deck_variants import enrich_portfolio_report_with_revenue_book
from src.salesforce_client import SalesforceClient


def test_get_portfolio_revenue_book_metrics_sf_first_corporate_groups():
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
        {
            "Id": "001000000000003AA",
            "Name": "Safran site",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Active",
            "ARR__c": 200_000,
            "parent_name": "Safran Cabin",
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
        m = sf.get_portfolio_revenue_book_metrics(usage_customer_names=["Acme", "BetaCo", "NoMatch"])

    assert m["source"] == "salesforce_entity_hierarchy"
    assert m["salesforce_entity_count"] == 3
    assert m["salesforce_reporting_groups"] == 3
    assert m["usage_tracked_customers"] == 3
    assert m["salesforce_matched_customers"] == 2
    assert m["salesforce_unmatched_customers"] == 1
    assert m["total_arr"] == 350_000.0
    assert m["active_installed_base_arr"] == 300_000.0
    assert m["churned_contract_arr"] == 50_000.0
    assert m["pipeline_arr"] == 12_345.0
    assert m["opportunity_count_this_year"] == 7
    assert m["active_customer_count"] == 2
    assert m["churned_customer_count"] == 1
    assert m["top_customers_by_arr"][0]["customer"] == "Safran"
    assert m["top_customers_by_arr"][0]["arr"] == 200_000.0
    assert len(m["matched_customer_contract_rollups"]) == 3
    acme_row = next(r for r in m["matched_customer_contract_rollups"] if r["customer"] == "Acme Division East")
    assert acme_row["commercial_status"] == "ACTIVE"
    assert acme_row["current_arr"] == 100_000.0
    assert acme_row["contract_end_date_nearest"] == "2027-06-01"
    assert acme_row["contract_statuses_distinct"] == ["Active"]
    assert acme_row["entity_row_count"] == 1
    ex = m.get("expansion_kpis") or {}
    assert ex.get("eligible_active_customer_count") == 2


def test_get_portfolio_revenue_book_metrics_without_usage_names():
    accounts = [
        {
            "Id": "001000000000001AA",
            "Name": "Acme Division East",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Active",
            "ARR__c": 100_000,
            "parent_name": "",
            "ultimate_parent_name": "",
        },
    ]
    with (
        patch.object(SalesforceClient, "get_entity_accounts", return_value=accounts),
        patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0),
        patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0),
        patch.object(SalesforceClient, "_portfolio_closed_won_opportunity_rows_cy", return_value=[]),
    ):
        sf = SalesforceClient()
        m = sf.get_portfolio_revenue_book_metrics()

    assert m["empty"] is False
    assert m["total_arr"] == 100_000.0
    assert m["usage_tracked_customers"] == 0


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


def test_enrich_portfolio_report_with_revenue_book_without_pendo_customers():
    report: dict = {"customers": []}
    accounts = [
        {
            "Id": "001000000000001AA",
            "Name": "Acme Division East",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Active",
            "ARR__c": 100_000,
            "parent_name": "",
            "ultimate_parent_name": "",
        },
    ]
    with (
        patch("src.data_source_health._salesforce_configured", return_value=True),
        patch.object(SalesforceClient, "get_entity_accounts", return_value=accounts),
        patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0),
        patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0),
        patch.object(SalesforceClient, "_portfolio_closed_won_opportunity_rows_cy", return_value=[]),
    ):
        enrich_portfolio_report_with_revenue_book(report)
    book = report["portfolio_revenue_book"]
    assert book.get("total_arr") == 100_000.0
    assert book.get("empty") is False
