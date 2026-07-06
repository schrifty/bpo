"""Tests for umbrella ultimate-parent JSM organization match expansion."""

from __future__ import annotations

from unittest.mock import patch

from src.jira_client import JiraClient, _merge_jsm_customer_alias_terms
from src.jsm_umbrella_match import (
    expand_umbrella_jsm_match_terms,
    jsm_directory_prefix_organizations,
)
from src.llm_export_jira import _jira_merged_lookup_bundle


def test_merge_jsm_customer_alias_terms_includes_cs_report_aliases():
    merged = _merge_jsm_customer_alias_terms(["Safran"])
    assert "Safran" in merged
    assert "Safran SA" in merged
    assert "Safran Aerosystems" in merged


def test_expand_umbrella_jsm_match_terms_from_salesforce_labels():
    terms = expand_umbrella_jsm_match_terms(
        "Carrier",
        salesforce_labels=["Commercial HVAC (Carrier)", "Residential HVAC (Carrier)"],
    )
    assert "Commercial HVAC (Carrier)" in terms
    assert "Commercial HVAC" in terms
    assert "Residential HVAC" in terms
    assert "Carrier" not in terms


def test_jira_merged_lookup_bundle_expands_safran_umbrella():
    row = {
        "ultimate_parent": "Safran",
        "salesforce_labels": ["Safran"],
        "pendo_customer_key": None,
    }
    primary, match_terms, keys = _jira_merged_lookup_bundle(row)
    assert primary == "Safran"
    assert "Safran SA" in match_terms
    assert "Safran Aerosystems" in match_terms


def test_jsm_directory_prefix_organizations():
    cands = [
        "Safran",
        "Safran SA",
        "Safran Aerosystems",
        "Carrier",
        "Safran Group",
    ]
    out = jsm_directory_prefix_organizations("Safran", cands)
    assert out == ["Safran Aerosystems", "Safran Group", "Safran SA"]


@patch("src.jira_client._load_jsm_org_alias_map", return_value={})
def test_customer_match_clause_safran_umbrella_orgs_multiple(_mock_aliases):
    jc = JiraClient.__new__(JiraClient)
    jc._jsm_cache_key = "t1"
    jc._jsm_llm_org_resolve_cache = {}
    orgs = [
        "Safran SA",
        "Safran Aerosystems",
        "Safran Cabin and Seats",
        "Safran Electrical and Power",
        "Safran Electronics and Defense",
        "Carrier",
    ]
    with patch.object(jc, "_list_jsm_organization_names", return_value=orgs):
        frag, resolved = jc._customer_match_clause("Safran", organizations_only=True)
    assert frag.startswith("Organizations in (")
    assert len(resolved) >= 5
    assert "Carrier" not in resolved
    assert "Safran SA" in resolved
