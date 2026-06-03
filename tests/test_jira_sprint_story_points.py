"""Unit tests for sprint story points (mocked Jira)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.jira_client import STORY_POINTS_FIELD
from src.jira_sprint_story_points import (
    board_sprint_story_points,
    get_sprint_story_points_by_team,
    parse_story_points,
    sum_delivered_story_points,
)


def test_parse_story_points() -> None:
    assert parse_story_points({STORY_POINTS_FIELD: 3}) == 3.0
    assert parse_story_points({}) == 0.0
    assert parse_story_points({STORY_POINTS_FIELD: "bad"}) == 0.0


def test_board_sprint_story_points_sums_done_only() -> None:
    client = MagicMock()
    client.base_url = "https://example.atlassian.net"
    client._headers = {}

    sprint = {"id": 99, "name": "Sprint 99", "end": "2026-05-15", "state": "closed"}
    issues = {
        "total": 2,
        "isLast": True,
        "issues": [
            {
                "key": "A-1",
                "fields": {
                    "status": {"statusCategory": {"key": "done"}},
                    "issuetype": {"name": "Story"},
                    STORY_POINTS_FIELD: 5,
                },
            },
            {
                "key": "A-2",
                "fields": {
                    "status": {"statusCategory": {"key": "new"}},
                    "issuetype": {"name": "Story"},
                    STORY_POINTS_FIELD: 3,
                },
            },
        ],
    }

    def fake_get(url, **kwargs):
        resp = MagicMock()
        resp.ok = True
        resp.raise_for_status = MagicMock()
        resp.json.return_value = issues
        return resp

    board = {"board_id": 44, "team_label": "LEAN Engineering", "name": "LEAN board", "project_key": "LEAN"}

    with patch("src.jira_sprint_story_points.requests.get", side_effect=fake_get):
        result = board_sprint_story_points(
            client,
            board,
            status_map={},
            excluded_issue_types=(),
            max_issues=100,
            timeout=5.0,
            sprint=sprint,
        )

    assert result["story_points_delivered"] == 5.0
    assert result["story_points_committed"] == 8.0
    assert result["delivered_issues"] == 1
    assert result["committed_issues"] == 2


def test_sum_delivered_story_points() -> None:
    teams = [
        {"story_points_delivered": 10.0},
        {"story_points_delivered": 5.5},
        {"error": "x"},
    ]
    assert sum_delivered_story_points(teams) == 15.5


def test_get_sprint_story_points_by_team() -> None:
    client = MagicMock()
    row = {
        "team": "LEAN Engineering",
        "board_id": 44,
        "story_points_delivered": 12.0,
        "story_points_committed": 20.0,
    }
    with patch("src.jira_sprint_story_points.load_status_category_map", return_value={}):
        with patch("src.jira_sprint_story_points.board_sprint_story_points", return_value=row):
            out = get_sprint_story_points_by_team(client, board_ids=[44])
    assert out["total_story_points_delivered"] == 12.0
    assert len(out["teams"]) == 1


def test_get_sprint_story_points_metric_value_minimal() -> None:
    from src.jira_sprint_story_points import get_sprint_story_points_metric_value

    client = MagicMock()
    with patch(
        "src.jira_sprint_story_points.get_sprint_story_points_by_team",
        return_value={"total_story_points_delivered": 240.0, "teams": []},
    ):
        out = get_sprint_story_points_metric_value(client)
    assert out == {"numerator": 240.0, "denominator": 1.0}
