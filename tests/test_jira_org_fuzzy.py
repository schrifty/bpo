"""Fuzzy resolution of JSM organization names for customer-scoped JQL."""

from unittest.mock import patch

from src.jira_client import (
    JiraClient,
    _fuzzy_pick_jsm_organizations,
    _norm_org_for_match,
    _score_jsm_org_candidate,
)
from src.jsm_org_llm import _initials_subsequence, prefilter_organizations_for_llm


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
    jc._jsm_cache_key = "t1"
    jc._jsm_llm_org_resolve_cache = {}
    with patch.object(jc, "_list_jsm_organization_names", return_value=["Carrier"]):
        frag, _ = jc._customer_match_clause("Carrier", organizations_only=True)
    assert "summary" not in frag and "description" not in frag
    assert "Organizations" in frag
    with patch.object(jc, "_list_jsm_organization_names", return_value=["Carrier"]):
        frag2, _ = jc._customer_match_clause("Carrier", organizations_only=False)
    assert "summary" in frag2


def test_initials_subsequence_fits_jci_on_johnson_controls_inc():
    assert _initials_subsequence("JCI", "Johnson Controls Inc")


def test_prefilter_includes_all_when_small():
    orgs = ["A", "B", "C"]
    out, note = prefilter_organizations_for_llm(["x"], orgs, 10_000)
    assert out == orgs and note is None


@patch("src.jira_client._fuzzy_pick_jsm_organizations", return_value=[])
@patch("src.jira_client._load_jsm_org_alias_map", return_value={})
@patch("src.jsm_org_llm.resolve_jsm_customer_organizations_llm", return_value=[])
def test_customer_match_no_directory_name_and_no_fuzzy_uses_no_match_jql(
    _mock_fuzzy, _mock_aliases, mock_llm
):
    """Nickname not in JSM list does not generate a bogus Organizations literal; LLM may add (here empty)."""
    jc = JiraClient.__new__(JiraClient)
    jc._jsm_cache_key = "t1"
    jc._jsm_llm_org_resolve_cache = {}
    with patch.object(jc, "_list_jsm_organization_names", return_value=["Johnson"]):
        frag, orgs = jc._customer_match_clause("JCI", organizations_only=True)
    assert "___BPO_NO_ORG_MATCH___" in frag
    assert orgs == []


@patch("src.jira_client._fuzzy_pick_jsm_organizations", return_value=[])
@patch("src.jira_client._load_jsm_org_alias_map", return_value={})
@patch("src.jsm_org_llm.resolve_jsm_customer_organizations_llm", return_value=["Johnson"])
def test_customer_match_llm_adds_resolved_directory_name(
    _mock_fuzzy, _mock_aliases, mock_llm
):
    jc = JiraClient.__new__(JiraClient)
    jc._jsm_cache_key = "t1"
    jc._jsm_llm_org_resolve_cache = {}
    with patch.object(jc, "_list_jsm_organization_names", return_value=["Johnson", "Acme"]):
        frag, orgs = jc._customer_match_clause("JCI", organizations_only=True)
    assert 'Organizations = "Johnson"' in frag
    assert "Johnson" in orgs
    mock_llm.assert_called_once()


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
