"""Tests for the engineering team roster builder."""

from __future__ import annotations

from src.eng_team_roster import build_eng_team_roster, build_engineer_audience_scope
from src.slide_engineering_portfolio import eng_team_roster_slide


def test_roster_slide_does_not_repeat_lead_in_members() -> None:
    report = {
        "eng_portfolio": {
            "team_roster": {
                "total_engineers": 4,
                "window_days": 90,
                "teams": [
                    {
                        "team": "Supply Insights",
                        "headcount": 4,
                        "lead": "Kevin Cua",
                        "members": ["Khubaib Khan", "David Henry", "Kevin Cua", "Muneeb Ahmed"],
                    }
                ],
            }
        }
    }
    reqs: list = []
    eng_team_roster_slide(reqs, "sid_r", report, 0)
    members_line = next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_r_mm0"
    )
    assert members_line.startswith("Lead: Kevin Cua —")
    # Name appears once (in the prefix), not again in the member list.
    assert members_line.count("Kevin Cua") == 1


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


def test_roster_prefers_atlassian_dev_teams(monkeypatch) -> None:
    # Leads are existing members so the unique-engineer count is unaffected.
    monkeypatch.setattr(
        "src.eng_team_roster._load_team_leads",
        lambda: {"Supply Insights": "Alice", "Inventory Optimization": "Bob"},
    )

    class _TeamsClient:
        atlassian_org_id = "org-1"

        def get_atlassian_teams(self, timeout=60.0):
            return {
                "error": None,
                "teams": [
                    {"name": "Dev - Supply Insights", "member_count": 2,
                     "members": ["Alice", "Bob"]},
                    {"name": "Dev - IOP", "member_count": 1, "members": ["Bob"]},
                    # Non-dev team is ignored.
                    {"name": "Product", "member_count": 3,
                     "members": ["X", "Y", "Z"]},
                ],
            }

    roster = build_eng_team_roster(_TeamsClient())
    assert roster["source"] == "atlassian_teams"
    by_team = {t["team"]: t for t in roster["teams"]}
    assert set(by_team) == {"Supply Insights", "Inventory Optimization"}  # "Dev - IOP" aliased
    assert by_team["Supply Insights"]["lead"] == "Alice"
    assert by_team["Inventory Optimization"]["lead"] == "Bob"
    # Unique members across teams (Bob counted once).
    assert roster["total_engineers"] == 2


def test_roster_adds_configured_lead_missing_from_membership(monkeypatch) -> None:
    # A configured lead not present in Atlassian membership is added to their own team
    # so the roster never shows someone leading a team they aren't listed on.
    monkeypatch.setattr(
        "src.eng_team_roster._load_team_leads",
        lambda: {"Supply Insights": "Grace Hopper"},
    )

    class _TeamsClient:
        atlassian_org_id = "org-1"

        def get_atlassian_teams(self, timeout=60.0):
            return {
                "error": None,
                "teams": [
                    {"name": "Dev - Supply Insights", "member_count": 2, "members": ["Alice", "Bob"]},
                ],
            }

    roster = build_eng_team_roster(_TeamsClient())
    team = roster["teams"][0]
    assert "Grace Hopper" in team["members"]
    assert team["headcount"] == 3


def test_roster_falls_back_when_no_dev_teams(monkeypatch) -> None:
    monkeypatch.setattr("src.eng_team_roster._load_team_leads", lambda: {})

    class _NoDevTeamsClient:
        atlassian_org_id = "org-1"

        def get_atlassian_teams(self, timeout=60.0):
            return {"error": None, "teams": [{"name": "Product", "member_count": 1,
                                              "members": ["X"]}]}

        def _search(self, *a, **k):
            return [_issue("Alice", "Supply Insights")]

    roster = build_eng_team_roster(_NoDevTeamsClient())
    assert roster["source"] == "jira_activity"
    assert roster["teams"][0]["team"] == "Supply Insights"


def test_roster_handles_fetch_error() -> None:
    class _BoomClient:
        def _search(self, *a, **k):
            raise RuntimeError("jira down")

    roster = build_eng_team_roster(_BoomClient())
    assert roster["teams"] == []
    assert roster["total_engineers"] == 0
    assert "jira down" in roster["error"]


def test_audience_scope_splits_dev_and_non_dev_names() -> None:
    class _TeamsClient:
        atlassian_org_id = "org-1"

        def get_atlassian_teams(self, timeout=60.0):
            return {
                "error": None,
                "teams": [
                    {
                        "name": "Dev - Supply Insights",
                        "member_account_ids": ["a1", "a2"],
                        "members": ["Alice", "Bob"],
                    },
                    {
                        "name": "Dev - IOP",
                        "member_account_ids": ["a2"],
                        "members": ["Bob"],
                    },
                    {
                        "name": "Product",
                        "member_account_ids": ["p1", "p2"],
                        "members": ["Carol", "Dave"],
                    },
                ],
            }

        def resolve_account_names(self, account_ids, timeout=30.0):
            return {
                "a1": "Alice",
                "a2": "Bob",
                "p1": "Carol",
                "p2": "Dave",
            }

        def resolve_account_emails(self, account_ids, timeout=30.0):
            return {
                "a1": "alice@x.com",
                "a2": "bob@x.com",
                "p1": "carol@x.com",
                "p2": "dave@x.com",
            }

    scope = build_engineer_audience_scope(_TeamsClient())
    assert scope["error"] is None
    assert scope["engineer_names"] == {"alice", "bob"}
    assert scope["non_engineer_names"] == {"carol", "dave"}
    assert scope["headcount"] == 2
    assert scope["non_engineer_headcount"] == 2
    assert scope["emails"] == {"alice@x.com", "bob@x.com"}
    assert scope["non_engineer_emails"] == {"carol@x.com", "dave@x.com"}


def test_audience_scope_person_on_dev_and_product_counts_as_engineer() -> None:
    class _TeamsClient:
        atlassian_org_id = "org-1"

        def get_atlassian_teams(self, timeout=60.0):
            return {
                "error": None,
                "teams": [
                    {
                        "name": "Dev - Core",
                        "member_account_ids": ["a1"],
                        "members": ["Alice"],
                    },
                    {
                        "name": "Product",
                        "member_account_ids": ["a1"],
                        "members": ["Alice"],
                    },
                ],
            }

        def resolve_account_names(self, account_ids, timeout=30.0):
            return {"a1": "Alice"}

        def resolve_account_emails(self, account_ids, timeout=30.0):
            return {"a1": "alice@x.com"}

    scope = build_engineer_audience_scope(_TeamsClient())
    assert scope["engineer_names"] == {"alice"}
    assert scope["non_engineer_names"] == set()
    assert scope["emails"] == {"alice@x.com"}
