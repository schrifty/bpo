"""Unit tests for portfolio-level Salesforce revenue rollup."""

from unittest.mock import patch

from src.deck_variants import enrich_portfolio_report_with_revenue_book
from src.salesforce_client import SalesforceClient


def test_get_portfolio_revenue_book_metrics_sf_first_corporate_groups():
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
        {
            "Id": "001000000000002AA",
            "Name": "BetaCo — former customer",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Churned",
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
    assert m["top_customers_by_arr"][0]["customer"] == "Safran"
    assert m["top_customers_by_arr"][0]["arr"] == 200_000.0


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
    ):
        sf = SalesforceClient()
        m = sf.get_portfolio_revenue_book_metrics()

    assert m["empty"] is False
    assert m["total_arr"] == 100_000.0
    assert m["usage_tracked_customers"] == 0


def test_enrich_portfolio_revenue_book_when_salesforce_not_configured():
    report: dict = {"customers": [{"customer": "A"}]}
    with patch("src.data_source_health._salesforce_configured", return_value=False):
        enrich_portfolio_report_with_revenue_book(report)
    assert report["portfolio_revenue_book"] == {"configured": False}


def test_enrich_portfolio_revenue_book_without_pendo_customers():
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
    ):
        enrich_portfolio_report_with_revenue_book(report)
    book = report["portfolio_revenue_book"]
    assert book.get("total_arr") == 100_000.0
    assert book.get("empty") is False
