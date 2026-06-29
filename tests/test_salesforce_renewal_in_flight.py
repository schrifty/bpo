"""Salesforce renewal-in-flight and parent-account pipeline scope."""

from __future__ import annotations

from unittest.mock import patch

from src.salesforce_client import (
    SalesforceClient,
    opportunity_account_scope_ids,
)


def test_opportunity_account_scope_ids_includes_parent():
    rows = [
        {"Id": "001000000000001AA", "ParentId": "00100000000000PAA"},
        {"Id": "001000000000002AA", "ParentId": "00100000000000PAA"},
    ]
    scope = opportunity_account_scope_ids(rows)
    assert set(scope) == {
        "001000000000001AA",
        "001000000000002AA",
        "00100000000000PAA",
    }


def test_renewal_in_flight_when_churned_entities_and_parent_pipeline():
    entities = [
        {
            "Id": "001000000000001AA",
            "ParentId": "00100000000000PAA",
            "Name": "Plant",
            "Contract_Status__c": "Expired",
            "ARR__c": 100_000,
        },
    ]

    def pipe(ids, *, open_only=True):
        if set(ids) == {"001000000000001AA"}:
            return 0.0
        if "00100000000000PAA" in ids:
            return 930_000.0
        return 0.0

    def opps(ids, *, limit=8):
        if "00100000000000PAA" in ids:
            return [
                {
                    "name": "Renewal Opp",
                    "stage": "5-Contracts",
                    "type": "Renewal",
                    "arr": 930_000,
                }
            ]
        return []

    sf = SalesforceClient()
    with (
        patch.object(sf, "get_advanced_pipeline_arr", side_effect=pipe),
        patch.object(sf, "get_open_pipeline_opportunities", side_effect=opps),
    ):
        fields = sf.renewal_in_flight_fields_for_entities(entities, all_matched_churned=True)

    assert fields["renewal_in_flight"] is True
    assert fields["pipeline_arr_including_parent_accounts"] == 930_000.0
    assert fields["open_pipeline_opportunities_sample"][0]["type"] == "Renewal"


def test_portfolio_rollup_includes_renewal_fields_for_churned():
    accounts = [
        {
            "Id": "001E",
            "ParentId": "001P",
            "Name": "Ford Motor Co/Plant",
            "Contract_Status__c": "Expired",
            "Contract_Contract_End_Date__c": "2026-04-10",
            "ARR__c": 105_000,
            "parent_name": "Ford Motor Company",
            "ultimate_parent_name": "",
        },
    ]
    with (
        patch.object(SalesforceClient, "get_entity_accounts", return_value=accounts),
        patch.object(
            SalesforceClient,
            "renewal_in_flight_fields_for_entities",
            return_value={
                "renewal_in_flight": True,
                "pipeline_arr_including_parent_accounts": 1_455_000.0,
                "open_pipeline_opportunities_sample": [{"name": "Ford - PTO Expansion"}],
            },
        ),
        patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=1_455_000.0),
        patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=2),
        patch.object(SalesforceClient, "_portfolio_closed_won_opportunity_rows_cy", return_value=[]),
    ):
        m = SalesforceClient().get_portfolio_revenue_book_metrics(["Ford Motor Company"])

    row = next(r for r in m["matched_customer_contract_rollups"] if r["customer"] == "Ford Motor Company")
    assert row["active"] is False
    assert row["renewal_in_flight"] is True
    assert row["pipeline_arr_including_parent_accounts"] == 1_455_000.0
