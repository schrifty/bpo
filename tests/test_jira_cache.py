"""Tests for Jira read API disk cache."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.jira_cache import cache_get, cache_set, clear_jira_cache_for_tests


def test_jira_disk_cache_round_trip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr("src.config.CORTEX_JIRA_CACHE_TTL_SECONDS", 20 * 3600)
    clear_jira_cache_for_tests()
    cache_set("tenant1", "search_jql", {"jql": "project = HELP"}, [{"id": "1"}])
    assert cache_get("tenant1", "search_jql", {"jql": "project = HELP"}) == [{"id": "1"}]
    assert cache_get("tenant2", "search_jql", {"jql": "project = HELP"}) is None


def test_search_uses_disk_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr("src.config.CORTEX_JIRA_CACHE_TTL_SECONDS", 20 * 3600)
    clear_jira_cache_for_tests()

    from src.jira_client import JiraClient

    client = JiraClient.__new__(JiraClient)
    client._jsm_cache_key = "tenant-test"
    client.api_base_url = "https://example.atlassian.net"
    client._headers = {}
    client._jql_log = []
    client._jql_lock = __import__("threading").Lock()

    with patch.object(JiraClient, "_record_jql"), patch("src.jira_client.requests.post") as post:
        first = client._search("project = HELP", max_results=10)
        second = client._search("project = HELP", max_results=10)
    assert first == second == []
    post.assert_called_once()
