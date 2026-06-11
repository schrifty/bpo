"""Tests for the engineering team roster builder."""

from __future__ import annotations

from src.eng_team_roster import build_eng_team_roster


def _issue(assignee: str | None, team: str | None, done: bool = False):
    cat = "done" if done else "indeterminate"
    return {
        "fields": {
            "assignee": {"displayName": assignee} if assignee else None,
            "customfield_10633": {"value": team} if team else None,
            "status": {"statusCategory": {"key": cat}},
        }
    }


class _FakeClient:
    def __init__(self, issues):
        self._issues = issues

    def _search(self, *args, **kwargs):
        return self._issues


def test_roster_assigns_home_team_by_argmax(monkeypatch) -> None:
    # No leads configured for this test.
    monkeypatch.setattr("src.eng_team_roster._load_team_leads", lambda: {})
    issues = [
        # Alice: 3 on Supply Insights, 1 on Data Pipeline -> home = Supply Insights.
        _issue("Alice", "Supply Insights"),
        _issue("Alice", "Supply Insights"),
        _issue("Alice", "Supply Insights"),
        _issue("Alice", "Data Pipeline"),
        # Bob: only Data Pipeline.
        _issue("Bob", "Data Pipeline"),
        _issue("Bob", "Data Pipeline"),
        # Carol: only Supply Insights.
        _issue("Carol", "Supply Insights"),
        # Noise: missing team / missing assignee are ignored.
        _issue("Dave", None),
        _issue(None, "Supply Insights"),
    ]
    roster = build_eng_team_roster(_FakeClient(issues))
    assert roster["error"] is None
    assert roster["total_engineers"] == 3  # Alice, Bob, Carol
    by_team = {t["team"]: t for t in roster["teams"]}
    # Alice counted once, on Supply Insights only.
    assert set(by_team["Supply Insights"]["members"]) == {"Alice", "Carol"}
    assert by_team["Supply Insights"]["headcount"] == 2
    assert by_team["Data Pipeline"]["members"] == ["Bob"]
    # Teams sorted by headcount descending.
    assert roster["teams"][0]["headcount"] >= roster["teams"][-1]["headcount"]


def test_roster_applies_configured_lead(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.eng_team_roster._load_team_leads",
        lambda: {"Supply Insights": "Grace Hopper"},
    )
    roster = build_eng_team_roster(_FakeClient([_issue("Alice", "Supply Insights")]))
    team = roster["teams"][0]
    assert team["team"] == "Supply Insights"
    assert team["lead"] == "Grace Hopper"
    assert roster["leads_configured"] is True


def test_roster_handles_fetch_error() -> None:
    class _BoomClient:
        def _search(self, *a, **k):
            raise RuntimeError("jira down")

    roster = build_eng_team_roster(_BoomClient())
    assert roster["teams"] == []
    assert roster["total_engineers"] == 0
    assert "jira down" in roster["error"]
