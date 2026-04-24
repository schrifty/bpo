"""Fuzzy resolution of JSM organization names for customer-scoped JQL."""

from unittest.mock import patch

from src.jira_client import (
    JiraClient,
    _fuzzy_pick_jsm_organizations,
    _norm_org_for_match,
    _score_jsm_org_candidate,
)


def test_norm_org_for_match_collapses_punctuation():
    assert _norm_org_for_match("  Foo,  Bar. Inc  ") == "foo bar inc"


def test_score_exact_and_substring():
    assert _score_jsm_org_candidate("acme corp", "ACME Corp") == 1.0
    s = _score_jsm_org_candidate("Daikin", "Daikin North America Inc")
    assert s >= 0.85


def test_fuzzy_pick_prefers_clear_winner():
    cands = ["Daikin North America, Inc.", "Daikin Europe Ltd", "OtherCo"]
    out = _fuzzy_pick_jsm_organizations(["Daikin North America"], cands)
    assert out == ["Daikin North America, Inc."]


def test_fuzzy_pick_skips_ambiguous_tie():
    cands = ["Daikin Corp", "Daikin Corp"]  # duplicate names — same score for any query
    out = _fuzzy_pick_jsm_organizations(["Daikin"], cands)
    # Two identical tops → ambiguous branch skips
    assert out == []


def test_fuzzy_pick_empty_candidates():
    assert _fuzzy_pick_jsm_organizations(["Anything"], []) == []


def test_customer_match_clause_organizations_only_omits_text_for_metrics():
    jc = JiraClient.__new__(JiraClient)
    with patch.object(jc, "_list_jsm_organization_names", return_value=[]):
        frag, _ = jc._customer_match_clause("Carrier", organizations_only=True)
    assert "summary" not in frag and "description" not in frag
    assert "Organizations" in frag
    with patch.object(jc, "_list_jsm_organization_names", return_value=[]):
        frag2, _ = jc._customer_match_clause("Carrier", organizations_only=False)
    assert "summary" in frag2


def test_customer_project_text_match_clause_uses_summary_description_not_orgs():
    """CUSTOMER/LEAN: customer scope is summary+description, not JSM Organizations."""
    jc = JiraClient.__new__(JiraClient)  # no Jira __init__ / API
    frag, orgs = jc._customer_project_text_match_clause("Carrier", ["CARR"])
    assert orgs == []
    assert "Organizations" not in frag
    assert "summary" in frag
    assert "description" in frag
    # Extra alias terms OR together
    assert " OR " in frag or "Carrier" in frag
