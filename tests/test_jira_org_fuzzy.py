"""Fuzzy resolution of JSM organization names for customer-scoped JQL."""

from src.jira_client import (
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
