"""Tests for Jira agile board discovery (mocked HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests


def test_discover_development_boards_groups_by_project() -> None:
    from src.jira_agile_discovery import discover_development_boards

    client = MagicMock()
    client.base_url = "https://example.atlassian.net"
    client._headers = {}

    board_list = {
        "values": [
            {
                "id": 10,
                "name": "LEAN board",
                "type": "scrum",
                "location": {"projectKey": "LEAN"},
            },
        ],
        "isLast": True,
    }

    def fake_get(url, **kwargs):
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        if url.endswith("/board"):
            resp.json.return_value = board_list
        elif "/sprint" in url:
            state = (kwargs.get("params") or {}).get("state")
            if state == "active":
                resp.json.return_value = {
                    "values": [{"id": 1, "name": "Sprint 1", "state": "active", "startDate": "2026-05-01"}]
                }
            else:
                resp.json.return_value = {"values": []}
        elif "/backlog" in url:
            resp.json.return_value = {"total": 5, "issues": [{"key": "LEAN-1"}], "isLast": True}
        else:
            resp.json.return_value = {}
        return resp

    with patch("src.jira_agile_discovery.requests.get", side_effect=fake_get):
        payload = discover_development_boards(client)

    assert payload["board_count"] == 1
    assert "LEAN" in payload["by_project"]
    assert payload["boards"][0]["active"] is True


def test_paginate_agile_multiple_pages() -> None:
    from src.jira_agile_discovery import _paginate_agile

    client = MagicMock()
    client.base_url = "https://example.atlassian.net"
    client._headers = {}

    calls = {"n": 0}

    def fake_get(url, **kwargs):
        calls["n"] += 1
        resp = MagicMock()
        resp.ok = True
        resp.raise_for_status = MagicMock()
        if calls["n"] == 1:
            resp.json.return_value = {"values": [{"id": 1}], "isLast": False, "startAt": 0}
        else:
            resp.json.return_value = {"values": [{"id": 2}], "isLast": True, "startAt": 1}
        return resp

    with patch("src.jira_agile_discovery.requests.get", side_effect=fake_get):
        rows = _paginate_agile(client, "/rest/agile/1.0/board")

    assert len(rows) == 2
