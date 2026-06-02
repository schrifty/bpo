"""Unit tests for sprint delivery % (mocked Jira)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.jira_sprint_delivery import (
    _issue_is_done,
    _pick_latest_closed_sprint,
    board_sprint_delivery,
    get_sprint_delivery_by_team,
)


def test_pick_latest_closed_sprint() -> None:
    sprints = {
        "recent_closed": [
            {"id": 1, "name": "Sprint 1", "end": "2026-04-01"},
            {"id": 2, "name": "Sprint 2", "end": "2026-05-01"},
        ]
    }
    picked = _pick_latest_closed_sprint(sprints)
    assert picked is not None
    assert picked["id"] == 2


def test_issue_is_done_uses_status_category() -> None:
    fields = {"status": {"statusCategory": {"key": "done"}, "name": "Done"}}
    assert _issue_is_done(fields, {}) is True
    fields_open = {"status": {"statusCategory": {"key": "indeterminate"}, "name": "In Progress"}}
    assert _issue_is_done(fields_open, {"in progress": "indeterminate"}) is False


def test_board_sprint_delivery_counts_done_issues() -> None:
    client = MagicMock()
    client.base_url = "https://example.atlassian.net"
    client._headers = {}

    sprint = {"id": 99, "name": "Sprint 99", "end": "2026-05-15", "state": "closed"}
    issues = {
        "total": 3,
        "isLast": True,
        "issues": [
            {"key": "A-1", "fields": {"status": {"statusCategory": {"key": "done"}}, "issuetype": {"name": "Story"}}},
            {"key": "A-2", "fields": {"status": {"statusCategory": {"key": "done"}}, "issuetype": {"name": "Story"}}},
            {"key": "A-3", "fields": {"status": {"statusCategory": {"key": "new"}}, "issuetype": {"name": "Story"}}},
        ],
    }

    def fake_get(url, **kwargs):
        resp = MagicMock()
        resp.ok = True
        resp.raise_for_status = MagicMock()
        if url.endswith("/sprint"):
            resp.json.return_value = {"values": [sprint]}
        elif "/sprint/99/issue" in url:
            resp.json.return_value = issues
        else:
            resp.json.return_value = {}
        return resp

    board = {"board_id": 44, "team_label": "LEAN Engineering", "name": "LEAN board", "project_key": "LEAN"}

    with patch("src.jira_sprint_delivery._fetch_board_sprints", return_value={"recent_closed": [sprint]}):
        with patch("src.jira_sprint_delivery.requests.get", side_effect=fake_get):
            result = board_sprint_delivery(
                client,
                board,
                status_map={},
                excluded_issue_types=(),
                max_issues=100,
                timeout=5.0,
            )

    assert result["committed"] == 3
    assert result["delivered"] == 2
    assert result["delivery_pct"] == round(200 / 3, 3)


def test_average_team_delivery_pct() -> None:
    from src.jira_sprint_delivery import average_team_delivery_pct

    teams = [
        {"team": "A", "delivered": 9, "committed": 10},
        {"team": "B", "delivered": 4, "committed": 5},
        {"team": "C", "error": "no sprint"},
    ]
    assert average_team_delivery_pct(teams) == 85.0


def test_get_sprint_delivery_by_team_returns_average() -> None:
    client = MagicMock()
    payload = {
        "team": "LEAN Engineering",
        "board_id": 44,
        "committed": 10,
        "delivered": 9,
        "delivery_pct": 90.0,
    }
    payload_b = {
        "team": "Data Integration",
        "board_id": 46,
        "committed": 5,
        "delivered": 4,
        "delivery_pct": 80.0,
    }
    with patch("src.jira_sprint_delivery.load_status_category_map", return_value={}):
        with patch(
            "src.jira_sprint_delivery.board_sprint_delivery",
            side_effect=[payload, payload_b],
        ):
            out = get_sprint_delivery_by_team(client, board_ids=[44, 46])
    assert out["average_delivery_pct"] == 85.0
    assert "numerator" not in out
    assert len(out["teams"]) == 2


def test_get_sprint_delivery_metric_value_minimal() -> None:
    from src.jira_sprint_delivery import get_sprint_delivery_metric_value

    client = MagicMock()
    with patch(
        "src.jira_sprint_delivery.get_sprint_delivery_by_team",
        return_value={"average_delivery_pct": 85.0, "teams": []},
    ):
        out = get_sprint_delivery_metric_value(client)
    assert out == {"numerator": 85.0, "denominator": 100.0}
