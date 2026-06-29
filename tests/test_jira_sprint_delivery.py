"""Unit tests for sprint delivery % (mocked Jira)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.jira_sprint_delivery import (
    SprintSelector,
    _agile_team_name,
    _issue_is_done,
    _lead_time_days,
    _pick_latest_closed_sprint,
    board_sprint_delivery,
    get_sprint_delivery_by_team,
    get_sprint_delivery_history,
    lean_sprint_delivery_by_agile_team,
    sprint_matches_selector,
)


def test_agile_team_name_handles_shapes() -> None:
    assert _agile_team_name({"value": "Supply Insights"}) == "Supply Insights"
    assert _agile_team_name({"name": "Infrastructure"}) == "Infrastructure"
    assert _agile_team_name([{"value": "Data Pipeline"}]) == "Data Pipeline"
    assert _agile_team_name("Procurement Management") == "Procurement Management"
    assert _agile_team_name(None) is None
    assert _agile_team_name("") is None


def test_lead_time_days_created_to_resolved() -> None:
    days = _lead_time_days("2026-06-01T00:00:00.000+0000", "2026-06-06T00:00:00.000+0000")
    assert days is not None and round(days, 1) == 5.0
    # End before start is rejected.
    assert _lead_time_days("2026-06-06T00:00:00.000+0000", "2026-06-01T00:00:00.000+0000") is None
    assert _lead_time_days(None, "2026-06-06T00:00:00.000+0000") is None


def test_lean_sprint_delivery_by_agile_team_groups_and_counts() -> None:
    board = {"board_id": 44, "name": "LEAN Scrum", "team_label": "LEAN Engineering"}
    sprint = {"id": 2598, "name": "Sprint 595"}
    done = {"status": {"statusCategory": {"key": "done"}, "name": "Closed"}}
    open_ = {"status": {"statusCategory": {"key": "indeterminate"}, "name": "In Progress"}}

    def _issue(team, base):
        f = dict(base)
        f["customfield_10633"] = {"value": team} if team else None
        f["created"] = "2026-06-01T00:00:00.000+0000"
        f["resolutiondate"] = "2026-06-05T00:00:00.000+0000" if base is done else None
        return {"fields": f}

    issues = [
        _issue("Supply Insights", done),
        _issue("Supply Insights", done),
        _issue("Supply Insights", open_),
        _issue("Infrastructure", done),
        _issue(None, done),  # -> Unassigned
    ]

    with patch(
        "src.jira_sprint_delivery._fetch_sprint_issues",
        return_value=(issues, len(issues)),
    ):
        rows = lean_sprint_delivery_by_agile_team(
            MagicMock(),
            board,
            status_map={},
            excluded_issue_types=(),
            sprint=sprint,
        )

    by_team = {r["team"]: r for r in rows}
    si = by_team["Supply Insights"]
    # LEAN reports throughput (issues resolved), not a say/do ratio.
    assert si["throughput"] == 2
    assert si["delivered"] == 2
    assert si["committed"] is None
    assert si["delivery_pct"] is None
    assert si["story_points_delivered"] is None
    assert round(si["median_lead_days"]) == 4
    # Real teams sort by throughput volume; "Unassigned" sinks to the bottom.
    assert rows[0]["team"] == "Supply Insights"
    assert rows[-1]["team"] == "Unassigned"


def test_board_sprint_report_parses_completed_vs_committed() -> None:
    payload = {
        "contents": {
            "completedIssues": [{}, {}, {}],
            "issuesNotCompletedInCurrentSprint": [{}, {}, {}, {}, {}, {}, {}],
            "puntedIssues": [{}],
            "completedIssuesEstimateSum": {"value": 12.0},
            "issuesNotCompletedEstimateSum": {"value": 28.0},
        }
    }
    resp = MagicMock()
    resp.ok = True
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    with patch("src.jira_sprint_delivery.requests.get", return_value=resp):
        from src.jira_sprint_delivery import board_sprint_report
        rep = board_sprint_report(MagicMock(base_url="https://x", _headers={}), 36, 2939)
    assert rep is not None
    assert rep["completed"] == 3
    assert rep["committed"] == 10  # 3 completed + 7 not completed (punted excluded)
    assert round(rep["delivery_pct"]) == 30
    assert rep["completed_sp"] == 12.0
    assert rep["committed_sp"] == 40.0


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

    with patch("src.jira_sprint_delivery.requests.get", side_effect=fake_get):
        result = board_sprint_delivery(
            client,
            board,
            status_map={},
            excluded_issue_types=(),
            max_issues=100,
            timeout=5.0,
            sprint=sprint,
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


def test_sprint_matches_selector() -> None:
    sprint = {"id": 1, "name": "Sprint595", "state": "closed"}
    assert sprint_matches_selector(sprint, SprintSelector(sprint_number=595)) is True
    assert sprint_matches_selector(sprint, SprintSelector(sprint_number=594)) is False
    week = {"id": 2, "name": "Week 14", "state": "closed"}
    assert sprint_matches_selector(week, SprintSelector(week="14")) is True
    assert sprint_matches_selector(sprint, SprintSelector(sprint_id=1)) is True


def test_get_sprint_delivery_history_groups_by_board() -> None:
    client = MagicMock()
    row = {
        "team": "LEAN Engineering",
        "board_id": 44,
        "sprint": {"name": "Sprint 1"},
        "delivered": 5,
        "committed": 5,
        "delivery_pct": 100.0,
    }
    with patch("src.jira_sprint_delivery.load_status_category_map", return_value={}):
        with patch(
            "src.jira_sprint_delivery.list_board_sprints",
            return_value=[{"id": 1, "name": "Sprint 1", "state": "closed", "end": "2026-05-01"}],
        ):
            with patch("src.jira_sprint_delivery.board_sprint_delivery", return_value=row):
                out = get_sprint_delivery_history(client, board_ids=[44], history_count=2)
    assert out["mode"] == "history"
    assert len(out["boards"]) == 1
    assert len(out["boards"][0]["sprints"]) == 1
    assert out["boards"][0]["sprints"][0]["delivery_pct"] == 100.0
