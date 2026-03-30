"""Unit tests for Salesforce comprehensive customer fetch and deck wiring."""

import requests
from unittest.mock import MagicMock, patch

from src.deck_loader import resolve_deck
from src.salesforce_client import (
    SalesforceClient,
    _parse_salesforce_rest_errors,
    _soql_like_literal,
    _strip_sf_attributes,
)
from src.slides_client import _build_slide_jql_speaker_notes, _filter_salesforce_comprehensive_slide_plan

# Pretend all comprehensive categories are API-queryable so mocked tests never call real describe.
_ALL_QUERYABLE_FOR_COMPREHENSIVE = frozenset(
    {
        "Contact",
        "Opportunity",
        "OpportunityLineItem",
        "Case",
        "Task",
        "Event",
        "Contract",
        "Order",
        "Quote",
        "Asset",
        "User",
        "CampaignMember",
        "Campaign",
        "Lead",
        "Product2",
        "Pricebook2",
    }
)


def test_strip_sf_attributes_strips_metadata_key():
    assert _strip_sf_attributes({"Id": "1", "attributes": {"type": "Account"}}) == {"Id": "1"}


def test_parse_salesforce_rest_errors_extracts_query_message():
    r = requests.Response()
    r.status_code = 400
    r._content = (
        b'[{"message":"No such column \'Foo__c\'","errorCode":"INVALID_FIELD"}]'
    )
    r.headers["Content-Type"] = "application/json"
    msg = _parse_salesforce_rest_errors(r)
    assert "INVALID_FIELD" in msg
    assert "Foo__c" in msg


def test_query_mainstream_object_retries_with_fallback_fields_on_400():
    client = SalesforceClient()
    client._token = "t"
    client._instance_url = "https://example.salesforce.com"

    bad = requests.Response()
    bad.status_code = 400
    bad._content = b'[{"errorCode":"INVALID_FIELD","message":"No such column"}]'
    bad.headers["Content-Type"] = "application/json"

    good = requests.Response()
    good.status_code = 200
    good._content = b'{"records":[{"Id":"500xx","attributes":{}}],"done":true}'
    good.headers["Content-Type"] = "application/json"

    with patch("src.salesforce_client.requests.get", side_effect=[bad, good]) as g:
        rows = client.query_mainstream_object("Case", where="AccountId = '001XX'", limit=5)

    assert len(rows) == 1 and rows[0].get("Id") == "500xx"
    assert g.call_count == 2


def test_soql_like_literal_escapes_percent_and_quote():
    s = _soql_like_literal("O'Reilly 100%")
    assert "''" in s or "O''Reilly" in s
    assert "\\%" in s


def test_get_customer_salesforce_comprehensive_skips_queries_when_unmatched():
    client = SalesforceClient()
    base = {
        "customer": "Nobody",
        "accounts": [],
        "account_ids": [],
        "matched": False,
        "opportunity_count_this_year": 0,
        "pipeline_arr": 0.0,
    }
    with patch.object(SalesforceClient, "get_customer_salesforce", return_value=base):
        with patch.object(SalesforceClient, "expand_descendant_account_ids", return_value=[]):
            out = client.get_customer_salesforce_comprehensive("Nobody")
    assert out["matched"] is False
    assert out["categories"] == {}
    assert out["category_errors"] == {}


def test_get_customer_salesforce_comprehensive_skips_sobjects_not_queryable():
    """When global describe says an object is not queryable, do not call SOQL for that category."""
    client = SalesforceClient()
    base = {
        "customer": "Acme",
        "accounts": [{"Id": "001XX"}],
        "account_ids": ["001XX"],
        "matched": True,
        "opportunity_count_this_year": 1,
        "pipeline_arr": 1.0,
    }
    q_only = frozenset({"Contact", "Opportunity", "Contract", "Account"})
    mocks = {
        "query_contacts": MagicMock(return_value=[{"Id": "c1"}]),
        "query_opportunities": MagicMock(return_value=[]),
        "query_opportunity_line_items": MagicMock(return_value=[]),
        "query_cases": MagicMock(return_value=[]),
        "query_tasks": MagicMock(return_value=[]),
        "query_events": MagicMock(return_value=[]),
        "query_contracts": MagicMock(return_value=[]),
        "query_orders": MagicMock(return_value=[]),
        "query_quotes": MagicMock(return_value=[]),
        "query_assets": MagicMock(return_value=[]),
        "query_users": MagicMock(return_value=[]),
        "query_campaign_members": MagicMock(return_value=[]),
        "query_campaigns": MagicMock(return_value=[]),
        "query_leads": MagicMock(return_value=[]),
        "query_products": MagicMock(return_value=[]),
        "query_pricebooks": MagicMock(return_value=[]),
    }
    with patch.object(SalesforceClient, "get_customer_salesforce", return_value=base):
        with patch.object(SalesforceClient, "expand_descendant_account_ids", return_value=["001XX"]):
            with patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0):
                with patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0):
                    with patch.object(SalesforceClient, "get_queryable_sobject_names", return_value=q_only):
                        with patch.multiple(SalesforceClient, **mocks):
                            out = client.get_customer_salesforce_comprehensive("Acme", row_limit=5)

    assert mocks["query_contacts"].called
    assert mocks["query_opportunities"].called
    assert mocks["query_contracts"].called
    assert not mocks["query_cases"].called
    assert not mocks["query_opportunity_line_items"].called
    assert out["categories"]["cases"] == []
    assert "Case" in (out["category_errors"].get("cases") or "")
    assert out["categories"]["campaign_members"] == []
    assert "CampaignMember" in (out["category_errors"].get("campaign_members") or "")


def test_get_customer_salesforce_comprehensive_isolates_query_errors():
    client = SalesforceClient()
    base = {
        "customer": "Acme",
        "accounts": [{"Id": "001XX"}],
        "account_ids": ["001XX"],
        "matched": True,
        "opportunity_count_this_year": 1,
        "pipeline_arr": 1.0,
    }
    mocks = {
        "query_contacts": MagicMock(return_value=[{"Id": "c1", "LastName": "Smith"}]),
        "query_opportunities": MagicMock(side_effect=RuntimeError("SOQL boom")),
        "query_opportunity_line_items": MagicMock(return_value=[]),
        "query_cases": MagicMock(return_value=[]),
        "query_tasks": MagicMock(return_value=[]),
        "query_events": MagicMock(return_value=[]),
        "query_contracts": MagicMock(return_value=[]),
        "query_orders": MagicMock(return_value=[]),
        "query_quotes": MagicMock(return_value=[]),
        "query_assets": MagicMock(return_value=[]),
        "query_users": MagicMock(return_value=[]),
        "query_campaign_members": MagicMock(return_value=[]),
        "query_campaigns": MagicMock(return_value=[]),
        "query_leads": MagicMock(return_value=[]),
        "query_products": MagicMock(return_value=[]),
        "query_pricebooks": MagicMock(return_value=[]),
    }
    with patch.object(SalesforceClient, "get_customer_salesforce", return_value=base):
        with patch.object(SalesforceClient, "expand_descendant_account_ids", return_value=["001XX"]):
            with patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0):
                with patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0):
                    with patch.object(
                        SalesforceClient,
                        "get_queryable_sobject_names",
                        return_value=_ALL_QUERYABLE_FOR_COMPREHENSIVE,
                    ):
                        with patch.multiple(SalesforceClient, **mocks):
                            out = client.get_customer_salesforce_comprehensive("Acme", row_limit=5)

    assert out["categories"]["contacts"]
    assert out["categories"]["opportunities"] == []
    assert "opportunities" in out["category_errors"]


def test_resolve_deck_salesforce_includes_sf_category_per_row():
    plan = resolve_deck("salesforce_comprehensive", "TestCo")
    assert "error" not in plan
    rows = [s for s in plan["slides"] if s.get("slide_type") == "salesforce_category"]
    cats = {r.get("sf_category") for r in rows}
    assert "contacts" in cats
    assert "entity_accounts" in cats
    assert plan["slides"][0].get("slide_type") == "salesforce_comprehensive_cover"


def test_filter_salesforce_comprehensive_drops_empty_categories():
    plan = resolve_deck("salesforce_comprehensive", "TestCo")
    assert "error" not in plan
    sfc = {
        "accounts": [{"Id": "a1"}],
        "categories": {
            "contacts": [{"Id": "c1"}],
            "opportunities": [],
            "cases": [{"Id": "k1"}],
        },
    }
    filtered = _filter_salesforce_comprehensive_slide_plan(plan["slides"], sfc)
    cats = [s.get("sf_category") for s in filtered if s.get("slide_type") == "salesforce_category"]
    assert "entity_accounts" in cats
    assert "contacts" in cats
    assert "cases" in cats
    assert "opportunities" not in cats
    assert filtered[0].get("slide_type") == "salesforce_comprehensive_cover"
    assert any(s.get("slide_type") == "data_quality" for s in filtered)


def test_expand_descendant_account_ids_walks_parent_id_chain():
    client = SalesforceClient()

    def fake_query(soql: str):
        if "ParentId IN ('001A')" in soql:
            return [{"Id": "001B"}]
        if "ParentId IN ('001B')" in soql:
            return [{"Id": "001C"}]
        return []

    with patch.object(client, "_query", side_effect=fake_query):
        out = client.expand_descendant_account_ids(["001A"])
    assert out == ["001A", "001B", "001C"]


def test_get_customer_salesforce_comprehensive_sets_account_ids_expanded():
    client = SalesforceClient()
    base = {
        "customer": "Acme",
        "accounts": [{"Id": "001A"}],
        "account_ids": ["001A"],
        "matched": True,
        "opportunity_count_this_year": 0,
        "pipeline_arr": 0.0,
    }
    mocks = {
        "query_contacts": MagicMock(return_value=[]),
        "query_opportunities": MagicMock(return_value=[]),
        "query_opportunity_line_items": MagicMock(return_value=[]),
        "query_cases": MagicMock(return_value=[]),
        "query_tasks": MagicMock(return_value=[]),
        "query_events": MagicMock(return_value=[]),
        "query_contracts": MagicMock(return_value=[]),
        "query_orders": MagicMock(return_value=[]),
        "query_quotes": MagicMock(return_value=[]),
        "query_assets": MagicMock(return_value=[]),
        "query_users": MagicMock(return_value=[]),
        "query_campaign_members": MagicMock(return_value=[]),
        "query_campaigns": MagicMock(return_value=[]),
        "query_leads": MagicMock(return_value=[]),
        "query_products": MagicMock(return_value=[]),
        "query_pricebooks": MagicMock(return_value=[]),
    }
    with patch.object(SalesforceClient, "get_customer_salesforce", return_value=base):
        with patch.object(
            SalesforceClient, "expand_descendant_account_ids", return_value=["001A", "001B"]
        ):
            with patch.object(SalesforceClient, "get_opportunity_creation_this_year", return_value=0):
                with patch.object(SalesforceClient, "get_advanced_pipeline_arr", return_value=0.0):
                    with patch.object(
                        SalesforceClient,
                        "get_queryable_sobject_names",
                        return_value=_ALL_QUERYABLE_FOR_COMPREHENSIVE,
                    ):
                        with patch.multiple(SalesforceClient, **mocks):
                            out = client.get_customer_salesforce_comprehensive("Acme", row_limit=5)
    assert out["account_ids_expanded"] == ["001A", "001B"]


def test_speaker_notes_salesforce_slide_does_not_list_unrelated_jql():
    report = {
        "jira": {"jql_queries": ["project = HELP ORDER BY created DESC"]},
        "salesforce_comprehensive": {"accounts": [], "categories": {"contacts": []}},
    }
    entry = {
        "slide_type": "salesforce_category",
        "title": "Salesforce — Contacts",
        "sf_category": "contacts",
    }
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "HELP" not in notes
    assert "Salesforce" in notes and "SOQL" in notes


def test_speaker_notes_data_quality_still_lists_deck_jql_when_scoped_empty():
    report = {"jira": {"jql_queries": ["project = LEAN"]}}
    entry = {"slide_type": "data_quality", "title": "Data Quality"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "LEAN" in notes
    assert "Jira issue search: Jira - project = LEAN" in notes


def test_speaker_notes_jql_structured_description_trace_format():
    report = {
        "jira": {
            "jql_queries": [
                {"description": "HELP test slice", "jql": "project = HELP ORDER BY created DESC"},
            ],
        },
    }
    entry = {"slide_type": "data_quality", "title": "Data Quality"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "HELP test slice: Jira - project = HELP ORDER BY created DESC" in notes


def test_speaker_notes_timestamp_first_line_has_seconds():
    report = {"jira": {"jql_queries": ["project = X"]}}
    entry = {"slide_type": "data_quality", "title": "Data Quality"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    first = notes.split("\n", 1)[0]
    parts = first.split()
    assert len(parts) >= 2
    assert parts[1].count(":") == 2


def test_speaker_notes_benchmarks_includes_data_traces():
    """Peer Benchmarks is Pendo-only; traces come from benchmarks.data_traces on the health report."""
    report = {
        "benchmarks": {
            "customer_active_rate": 39.0,
            "data_traces": [
                {
                    "description": "Weekly active rate (this account)",
                    "source": "Pendo",
                    "query": "active_7d / total_visitors (7-day window)",
                },
            ],
        },
        "account": {"total_visitors": 100, "total_sites": 5},
    }
    entry = {"slide_type": "benchmarks", "title": "Peer Benchmarks"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "Weekly active rate (this account): Pendo - active_7d / total_visitors (7-day window)" in notes
