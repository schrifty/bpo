"""CUSTOMER/LEAN JQL: Epic and SUT issue types excluded from support slide fetches."""

from src.jira_client import _CUSTOMER_LEAN_ISSUETYPE_EXCLUSION, _jql_customer_lean_exclude_epic_sut


def test_jql_customer_lean_exclude_epic_sut_only_on_customer_lean():
    assert _jql_customer_lean_exclude_epic_sut("CUSTOMER") == f" AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION}"
    assert _jql_customer_lean_exclude_epic_sut("LEAN") == f" AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION}"
    assert _jql_customer_lean_exclude_epic_sut("HELP") == ""
    assert _jql_customer_lean_exclude_epic_sut("ER") == ""


def test_epic_sut_inclusion_string():
    assert "Epic" in _CUSTOMER_LEAN_ISSUETYPE_EXCLUSION
    assert "SUT" in _CUSTOMER_LEAN_ISSUETYPE_EXCLUSION
