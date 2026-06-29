"""Portfolio book metrics: renewal-in-flight excluded from churned_customer_count."""

from __future__ import annotations

from unittest.mock import patch

from src.salesforce_client import SalesforceClient


def test_renewal_in_flight_not_counted_as_churned_customer():
    accounts = [
        {
            "Id": "001000000000001AA",
            "Name": "Ford Motor Co/Plant",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Expired",
            "Contract_Contract_End_Date__c": "2026-04-10",
            "ARR__c": 100_000,
            "parent_name": "Ford Motor Company",
            "ultimate_parent_name": "",
        },
        {
            "Id": "001000000000002AA",
            "Name": "GoneCo — former site",
            "LeanDNA_Entity_Name__c": "",
            "Contract_Status__c": "Churned",
            "Contract_Contract_End_Date__c": "2025-01-01",
            "ARR__c": 25_000,
            "parent_name": "GoneCo",
            "ultimate_parent_name": "",
        },
    ]

    def renewal_fields(entities, *, all_matched_churned):
        name = (entities[0].get("Name") or "") if entities else ""
        if "Ford Motor" in name:
            return {"renewal_in_flight": True, "pipeline_arr_including_parent_accounts": 500_000}
        return {"renewal_in_flight": False}

    with (
        patch.object(SalesforceClient, "get_entity_accounts", return_value=accounts),
        patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0),
        patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0),
        patch.object(SalesforceClient, "get_open_pipeline_opportunities", return_value=[]),
        patch.object(
            SalesforceClient,
            "renewal_in_flight_fields_for_entities",
            side_effect=renewal_fields,
        ),
        patch.object(SalesforceClient, "_portfolio_closed_won_opportunity_rows_cy", return_value=[]),
    ):
        sf = SalesforceClient()
        m = sf.get_portfolio_revenue_book_metrics(["Ford Motor Company", "GoneCo"])

    assert m["churned_customer_count"] == 1
    assert m["churned_contract_arr"] == 25_000.0
    assert m["renewal_in_flight_customer_count"] == 1
    assert m["renewal_in_flight_contract_arr"] == 100_000.0
    assert "Ford Motor Company" in (m.get("renewal_in_flight_customer_names_sample") or [])
    assert "GoneCo" in (m.get("churned_customer_names_sample") or [])
