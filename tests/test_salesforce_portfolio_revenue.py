"""Unit tests for portfolio-level Salesforce revenue rollup."""

from unittest.mock import patch

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


def test_enrich_portfolio_revenue_book_when_salesforce_not_configured():
    report: dict = {"customers": [{"customer": "A"}]}
    with patch("src.data_source_health._salesforce_configured", return_value=False):
        enrich_portfolio_report_with_revenue_book(report)
    assert report["portfolio_revenue_book"] == {"configured": False}
