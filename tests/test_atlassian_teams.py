"""Tests for JiraClient.get_atlassian_teams (Atlassian Teams API), fully mocked."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.jira_client import JiraClient, clear_atlassian_teams_cache_for_tests


@pytest.fixture(autouse=True)
def _clear_atlassian_teams_cache() -> None:
    clear_atlassian_teams_cache_for_tests()
    yield
    clear_atlassian_teams_cache_for_tests()


def _client(*, org="ORG123", api_key=None):
    c = JiraClient.__new__(JiraClient)  # bypass real connection setup
    c.api_base_url = "https://jira.example"
    c.base_url = "https://jira.example"
    c._headers = {"Authorization": "Basic x"}
    c.atlassian_org_id = org
    c.atlassian_api_key = api_key
    c._atlassian_user_name_cache = {}
    return c


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_get_atlassian_teams_no_org_id() -> None:
    c = _client(org=None)
    out = c.get_atlassian_teams()
    assert out["teams"] == []
    assert "ATLASSIAN_ORG_ID" in out["error"]


def test_get_atlassian_teams_parses_teams_members_and_names() -> None:
    teams_page = {"entities": [
        {"teamId": "t1", "displayName": "Data Pipeline", "state": "ACTIVE"},
        {"teamId": "t2", "displayName": "Support Engineering", "state": "ACTIVE"},
    ], "cursor": None}
    members = {
        "t1": {"results": [{"accountId": "a1"}, {"accountId": "a2"}], "pageInfo": {"hasNextPage": False}},
        "t2": {"results": [{"accountId": "a3"}], "pageInfo": {"hasNextPage": False}},
    }
    bulk = {"values": [
        {"accountId": "a1", "displayName": "Alice", "accountType": "atlassian"},
        {"accountId": "a2", "displayName": "Bob", "accountType": "atlassian"},
        {"accountId": "a3", "displayName": "Carol", "accountType": "atlassian"},
    ]}

    def fake_get(url, **kw):
        if url.endswith("/teams"):
            return _Resp(200, teams_page)
        if "/rest/api/3/user/bulk" in url:
            return _Resp(200, bulk)
        return _Resp(404, {})

    def fake_post(url, **kw):
        for tid, payload in members.items():
            if url.endswith(f"/teams/{tid}/members"):
                return _Resp(200, payload)
        return _Resp(404, {})

    c = _client()
    with patch("src.jira_client.requests.get", side_effect=fake_get), \
         patch("src.jira_client.requests.post", side_effect=fake_post):
        out = c.get_atlassian_teams()

    assert out["error"] is None
    by_name = {t["name"]: t for t in out["teams"]}
    assert by_name["Data Pipeline"]["members"] == ["Alice", "Bob"]
    assert by_name["Data Pipeline"]["member_count"] == 2
    assert by_name["Support Engineering"]["members"] == ["Carol"]


def test_get_atlassian_teams_unreachable_fails_loud() -> None:
    c = _client()
    with patch("src.jira_client.requests.get", side_effect=lambda *a, **k: _Resp(403, {"m": "no"})):
        out = c.get_atlassian_teams()
    assert out["teams"] == []
    assert "unreachable" in out["error"]


def test_get_atlassian_teams_uses_in_process_cache(monkeypatch) -> None:
    monkeypatch.setattr("src.config.CORTEX_ATLASSIAN_TEAMS_CACHE_TTL_SECONDS", 3600)
    c = _client()
    calls = {"n": 0}

    def fake_get(url, **kw):
        if url.endswith("/teams"):
            calls["n"] += 1
            return _Resp(200, {"entities": [], "cursor": None})
        return _Resp(404, {})

    with patch("src.jira_client.requests.get", side_effect=fake_get):
        out1 = c.get_atlassian_teams(with_members=False, resolve_names=False)
        n_after_first = calls["n"]
        out2 = c.get_atlassian_teams(with_members=False, resolve_names=False)
    assert out1["error"] is None and out2["error"] is None
    assert n_after_first >= 1
    assert calls["n"] == n_after_first  # second call served from in-process cache
