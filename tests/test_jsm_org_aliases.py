"""JSM org alias file merges extra search terms."""

from src import jira_client


def test_merge_jsm_alias_adds_configured_extras(tmp_path, monkeypatch):
    alias_file = tmp_path / "jsm_organization_aliases.yaml"
    alias_file.write_text(
        "ABC:\n"
        "  - Example Manufacturing\n"
        "  - Example Manufacturing International\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(jira_client, "_JSM_ORG_ALIAS_FILE", alias_file)
    jira_client._jsm_org_alias_map = None
    out = jira_client._merge_jsm_customer_alias_terms(["ABC"])
    assert "ABC" in out
    assert "Example Manufacturing" in out
    assert "Example Manufacturing International" in out


def test_merge_deduplicates(monkeypatch):
    monkeypatch.setattr(
        jira_client,
        "_load_jsm_org_alias_map",
        lambda: {"abc": ["Example Manufacturing"]},
    )
    jira_client._jsm_org_alias_map = None
    out = jira_client._merge_jsm_customer_alias_terms(["ABC", "ABC", "Example Manufacturing"])
    assert out.count("Example Manufacturing") == 1
