"""Unit tests for Salesforce comprehensive customer fetch and deck wiring."""

import requests
from unittest.mock import MagicMock, patch

from src.deck_loader import resolve_deck
from src.salesforce_client import (
    SalesforceClient,
    _customer_name_matches_entity_account,
    _parse_salesforce_rest_errors,
    _relationship_json_key_for_lookup,
    _soql_like_literal,
    _strip_sf_attributes,
    clear_salesforce_read_cache,
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


def test_query_soql_hits_read_cache_second_call(monkeypatch):
    import src.salesforce_client as sfc

    monkeypatch.setattr(sfc, "BPO_SALESFORCE_CACHE_TTL_SECONDS", 86400)
    monkeypatch.setattr(sfc, "BPO_SALESFORCE_CACHE_FORCE_REFRESH", False)
    clear_salesforce_read_cache()
    client = SalesforceClient()
    client._token = "t"
    client._instance_url = "https://example.salesforce.com"

    good = requests.Response()
    good.status_code = 200
    good._content = b'{"records":[{"Id":"001","x":1}],"done":true}'
    good.headers["Content-Type"] = "application/json"

    with patch("src.salesforce_client.requests.get", return_value=good) as g:
        r1 = client.query_soql("SELECT Id FROM Account LIMIT 1")
        r2 = client.query_soql("SELECT Id FROM Account LIMIT 1")
    assert g.call_count == 1
    assert r1[0]["Id"] == r2[0]["Id"] == "001"
    r1[0]["x"] = 99
    with patch("src.salesforce_client.requests.get", return_value=good) as g2:
        r3 = client.query_soql("SELECT Id FROM Account LIMIT 1")
    assert g2.call_count == 0
    assert r3[0].get("x") == 1


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


def test_speaker_notes_benchmarks_uses_on_slide_kpi_labels():
    """Peer Benchmarks pipeline traces use the same KPI titles as ``_benchmarks_slide`` (not ad-hoc descriptions)."""
    report = {
        "benchmarks": {
            "customer_active_rate": 39.0,
            "peer_median_rate": 35.0,
            "peer_count": 100,
            "cohort_median_rate": None,
            "cohort_count": 0,
            "cohort_name": "",
        },
        "account": {"total_visitors": 100, "total_sites": 5},
    }
    entry = {"slide_type": "benchmarks", "title": "Peer Benchmarks"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "Weekly active rate (this account): Pendo -" in notes
    assert "All-customer median (100 accounts): Pendo -" in notes
    assert "Delta: Pendo -" in notes
    assert "Account size: Pendo -" in notes


def test_speaker_notes_health_snapshot_uses_on_slide_row_labels():
    """Account Health Snapshot traces use the same row prefixes as the slide body (text before ``:``)."""
    report = {
        "engagement": {
            "active_7d": 642,
            "active_30d": 100,
            "dormant": 50,
            "active_rate_7d": 39.1,
        },
        "benchmarks": {
            "peer_median_rate": 40.0,
            "peer_count": 80,
            "cohort_median_rate": None,
            "cohort_count": 0,
            "cohort_name": "",
        },
        "account": {"total_visitors": 1642, "total_sites": 3},
    }
    entry = {"slide_type": "health", "title": "Account Health Snapshot"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "Active This Week: Pendo -" in notes
    assert "Weekly Active Rate: Pendo -" in notes
    assert "Customer Users: Pendo -" in notes


def test_speaker_notes_platform_value_includes_kpis_and_table_columns():
    """Platform Value & ROI uses canonical traces so KPI headlines and table columns appear in notes."""
    report = {
        "cs_platform_value": {
            "customer": "Acme",
            "source": "cs_report",
            "factory_count": 3,
            "total_savings": 96_500_000,
            "total_open_ia_value": 269_000_000,
            "total_recs_created_30d": 188_000,
            "total_pos_placed_30d": 66_680,
            "total_overdue_tasks": 2_027,
            "sites": [
                {"factory": "Site A", "savings_current_period": 1, "recs_created_30d": 2},
            ],
        },
    }
    entry = {"slide_type": "platform_value", "title": "Platform Value & ROI"}
    notes = _build_slide_jql_speaker_notes(report, entry)
    assert "Savings achieved: CS Report -" in notes and "$96.5M" in notes
    assert "Open IA pipeline: CS Report -" in notes and "$269.0M" in notes
    assert "Recs created (30d): CS Report -" in notes and "188" in notes
    assert "POs placed (30d): CS Report -" in notes and "66,680" in notes
    assert "Overdue tasks: CS Report -" in notes and "2,027" in notes
    assert "Factory: CS Report -" in notes
    assert "Savings: CS Report -" in notes
    assert "Recs (30d): CS Report -" in notes


def test_relationship_json_key_for_lookup_custom_field():
    assert _relationship_json_key_for_lookup("Ultimate_Parent_Account__c") == "Ultimate_Parent_Account__r"


def test_customer_name_matches_entity_account_parent_and_ultimate():
    needle = "BOMBARDIER"
    base = {
        "Name": "Other",
        "LeanDNA_Entity_Name__c": "",
        "parent_name": "",
        "ultimate_parent_name": "",
    }
    assert not _customer_name_matches_entity_account(needle, base)
    assert _customer_name_matches_entity_account(
        needle,
        {**base, "parent_name": "Bombardier Inc. Subsidiary"},
    )
    assert _customer_name_matches_entity_account(
        needle,
        {**base, "ultimate_parent_name": "Bombardier Aerospace"},
    )
