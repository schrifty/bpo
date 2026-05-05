"""Unit tests for Salesforce-derived portfolio allowlist (no live APIs)."""

from src.portfolio_salesforce_allowlist import (
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
    salesforce_allowlist_pendo_keys,
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
    assert "NoPendoCorp" in meta["salesforce_labels_unmatched"]

    ordered2, meta2 = salesforce_allowlist_pendo_keys(
        entity_accounts=accounts,
        pendo_prefixes=pendo,
        is_excluded=lambda k: k == "GoodCo",
    )
    assert ordered2 == []
    assert meta2["salesforce_labels_excluded_after_resolve"]


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
