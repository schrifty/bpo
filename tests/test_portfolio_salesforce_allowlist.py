"""Unit tests for Salesforce-derived portfolio allowlist (no live APIs)."""

import logging

from src.portfolio_salesforce_allowlist import (
    format_salesforce_label_activity_hint,
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
    salesforce_allowlist_pendo_keys,
    summarize_salesforce_customer_query_activity,
    summarize_salesforce_label_activity,
)


def test_portfolio_labels_rollup_dedupes_case():
    rows = [
        {"Name": "Plant A", "parent_name": "", "ultimate_parent_name": "Acme"},
        {"Name": "Plant B", "parent_name": "", "ultimate_parent_name": "acme"},
        {"Name": "StandaloneCo", "parent_name": "", "ultimate_parent_name": ""},
    ]
    labels = portfolio_labels_from_entity_accounts(rows)
    assert labels == ["Acme", "StandaloneCo"]


def test_resolve_sf_label_exact_and_word_boundary():
    prefix = {"Spirit", "MicroVention", "Honeywell"}
    assert resolve_sf_label_to_pendo_prefix("Spirit AeroSystems Holdings", prefix) == "Spirit"
    assert resolve_sf_label_to_pendo_prefix("microvention", prefix) == "MicroVention"
    assert resolve_sf_label_to_pendo_prefix("Honeywell Intl", prefix) == "Honeywell"


def test_resolve_sf_label_first_token():
    prefix = {"ACME"}
    assert resolve_sf_label_to_pendo_prefix("ACME North America Region", prefix) == "ACME"


def test_salesforce_allowlist_skips_unmatched_and_excluded():
    accounts = [{"Name": "GoodCo", "parent_name": "", "ultimate_parent_name": ""}]
    pendo = {"GoodCo", "BadNoise"}
    unmatched_meta = salesforce_allowlist_pendo_keys(
        entity_accounts=accounts + [{"Name": "NoPendoCorp", "parent_name": "", "ultimate_parent_name": ""}],
        pendo_prefixes=pendo,
        is_excluded=lambda _k: False,
    )
    ordered, meta = unmatched_meta
    assert ordered == ["GoodCo"]
    unmatched = meta["salesforce_labels_unmatched"]
    assert any(
        (u.get("salesforce_label") if isinstance(u, dict) else u) == "NoPendoCorp"
        for u in unmatched
    )

    ordered2, meta2 = salesforce_allowlist_pendo_keys(
        entity_accounts=accounts,
        pendo_prefixes=pendo,
        is_excluded=lambda k: k == "GoodCo",
    )
    assert ordered2 == []
    assert meta2["salesforce_labels_excluded_after_resolve"]


def test_summarize_salesforce_label_activity_active_vs_churned():
    active_rows = [
        {
            "Name": "ACE US",
            "parent_name": "",
            "ultimate_parent_name": "ACE Thermal Systems",
            "Contract_Status__c": "Active",
            "Contract_Contract_End_Date__c": "2027-12-31",
            "ARR__c": 120000,
        },
    ]
    churned_rows = [
        {
            "Name": "Old Site",
            "parent_name": "",
            "ultimate_parent_name": "ChurnedCo",
            "Contract_Status__c": "Churned",
            "ARR__c": 5000,
        },
    ]
    act = summarize_salesforce_label_activity("ACE Thermal Systems", active_rows)
    assert act["active_in_salesforce"] is True
    assert act["arr_active"] == 120000.0
    assert "Active" in act["contract_statuses_distinct"]
    hint = format_salesforce_label_activity_hint(act)
    assert "active/non-churned" in hint
    assert "120,000" in hint

    churn = summarize_salesforce_label_activity("ChurnedCo", churned_rows)
    assert churn["all_entities_churned"] is True
    assert "churned only" in format_salesforce_label_activity_hint(churn)


def test_summarize_salesforce_customer_query_matches_entity_fields():
    rows = [
        {
            "Name": "Industrial US Plant",
            "LeanDNA_Entity_Name__c": "",
            "parent_name": "",
            "ultimate_parent_name": "Industrial US",
            "Contract_Status__c": "Active",
            "ARR__c": 80000,
        },
    ]
    act = summarize_salesforce_customer_query_activity("Industrial", rows)
    assert act["entity_row_count"] == 1
    assert act["active_in_salesforce"] is True
    assert "Industrial US" in act["portfolio_labels_matched"]


def test_unmatched_warning_includes_salesforce_activity(caplog):
    accounts = [
        {
            "Name": "ACE Plant",
            "parent_name": "",
            "ultimate_parent_name": "ACE Thermal Systems",
            "Contract_Status__c": "Active",
            "ARR__c": 50000,
        },
    ]
    with caplog.at_level(logging.WARNING):
        _ordered, meta = salesforce_allowlist_pendo_keys(
            entity_accounts=accounts,
            pendo_prefixes={"OtherCo"},
            is_excluded=lambda _k: False,
        )
    assert _ordered == []
    assert len(meta["salesforce_labels_unmatched"]) == 1
    assert meta["salesforce_labels_unmatched"][0]["salesforce_activity"]["active_in_salesforce"] is True
    assert "ACE Thermal Systems" in caplog.text
    assert "active/non-churned" in caplog.text


def test_salesforce_allowlist_collapses_duplicate_sf_labels_to_one_pendo_key():
    rows = [
        {"Name": "Site 1", "ultimate_parent_name": "ParentCo", "parent_name": ""},
        {"Name": "Site 2", "ultimate_parent_name": "ParentCo", "parent_name": ""},
    ]
    ordered, _meta = salesforce_allowlist_pendo_keys(
        entity_accounts=rows,
        pendo_prefixes={"ParentCo"},
        is_excluded=lambda _k: False,
    )
    assert ordered == ["ParentCo"]
