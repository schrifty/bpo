"""JSM org alias file merges extra search terms (e.g. JCI -> Johnson Controls)."""

from src import jira_client


def test_merge_jsm_alias_adds_configured_extras_for_jci():
    jira_client._jsm_org_alias_map = None
    out = jira_client._merge_jsm_customer_alias_terms(["JCI"])
    assert "JCI" in out
    assert "Johnson Controls" in out
    assert "Johnson Controls International" in out


def test_merge_deduplicates():
    jira_client._jsm_org_alias_map = None
    out = jira_client._merge_jsm_customer_alias_terms(["JCI", "JCI", "Johnson Controls"])
    assert out.count("Johnson Controls") == 1
