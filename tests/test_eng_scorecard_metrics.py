"""Tests for Engineering MFR scorecard metric generators."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from src.eng_scorecard_metrics import (
    get_ai_assisted_prs_pct,
    get_ai_automated_prs_pct,
    get_ai_assisted_automated_prs_pct,
    get_ai_spend_pct,
    get_ai_spend_per_issue,
    get_defect_introduction_rate,
    get_headcount_plus_ai_spend_per_issue,
    get_growth_allocation_pct,
    get_issues_shipped,
    get_prs_merged,
    get_tokens_per_dev,
    get_wau_pct,
    get_weekly_active_ai_users,
    pr_is_ai_assisted,
    pr_is_ai_automated,
)


class _FakeJira:
    def __init__(
        self,
        *,
        headcount: int = 10,
        emails: set[str] | None = None,
        shipped_count: int | None = 20,
        bugs_created_count: int | None = 5,
        closed: list[dict[str, Any]] | None = None,
        search_error: str | None = None,
    ) -> None:
        self.headcount = headcount
        self.emails = emails or {"a@ex.com", "b@ex.com"}
        self.shipped_count = shipped_count
        self.bugs_created_count = bugs_created_count
        self.closed = closed
        self.search_error = search_error
        self.atlassian_org_id = "org"

    def jql_match_count(self, jql: str, **kwargs: object) -> int | None:
        if "issuetype = Bug" in jql and "created >=" in jql:
            return self.bugs_created_count
        return self.shipped_count

    def _search(self, jql: str, **kwargs: object) -> list[dict[str, Any]]:
        if self.search_error:
            raise RuntimeError(self.search_error)
        return self.closed or []


class _FakeCursor:
    def __init__(self, events: list[dict[str, Any]] | None = None, *, fail: bool = False) -> None:
        self.events = events or []
        self.fail = fail

    def get_usage_events(self, start: object, end: object, **kwargs: object) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError("cursor down")
        return self.events


def _patch_scope(monkeypatch: pytest.MonkeyPatch, jira: _FakeJira) -> None:
    monkeypatch.setattr(
        "src.eng_team_roster.build_engineer_audience_scope",
        lambda client, timeout=60.0: {
            "error": None,
            "headcount": jira.headcount,
            "emails": jira.emails,
        },
    )


def test_get_weekly_active_ai_users(monkeypatch: pytest.MonkeyPatch) -> None:
    jira = _FakeJira(headcount=4, emails={"a@ex.com", "b@ex.com", "c@ex.com", "d@ex.com"})
    _patch_scope(monkeypatch, jira)
    events = [
        {"userEmail": "a@ex.com", "tokenUsage": {"inputTokens": 1, "outputTokens": 1}, "chargedCents": 1},
        {"userEmail": "b@ex.com", "tokenUsage": {"inputTokens": 1, "outputTokens": 1}, "chargedCents": 1},
        {"userEmail": "outsider@ex.com", "tokenUsage": {"inputTokens": 9, "outputTokens": 9}, "chargedCents": 9},
    ]
    out = get_weekly_active_ai_users(_FakeCursor(events), jira, days=7)
    assert out["numerator"] == 2.0
    assert out["denominator"] == 4.0
    assert out["active_users"] == 2
    assert out["value"] == 50.0
    assert out["scope"] == "engineering_department"
    # Deprecated alias still works.
    assert get_wau_pct(_FakeCursor(events), jira, days=7)["value"] == 50.0


def test_get_tokens_per_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    jira = _FakeJira(headcount=2, emails={"a@ex.com", "b@ex.com"})
    _patch_scope(monkeypatch, jira)
    events = [
        {"userEmail": "a@ex.com", "tokenUsage": {"inputTokens": 100, "outputTokens": 50}, "chargedCents": 10},
        {"userEmail": "b@ex.com", "tokenUsage": {"inputTokens": 50, "outputTokens": 0}, "chargedCents": 5},
    ]
    out = get_tokens_per_dev(_FakeCursor(events), jira, days=30)
    assert out["numerator"] == 200.0
    assert out["denominator"] == 2.0
    assert out["value"] == 100.0


def test_get_issues_shipped() -> None:
    out = get_issues_shipped(_FakeJira(shipped_count=17), days=84)
    assert out["value"] == 17
    assert out["window_days"] == 84
    assert "resolved >= -84d" in out["jql"]


def test_get_issues_shipped_default_is_12_weeks() -> None:
    out = get_issues_shipped(_FakeJira(shipped_count=17))
    assert out["window_days"] == 84
    assert "resolved >= -84d" in out["jql"]


def test_get_issues_shipped_fails_loud() -> None:
    out = get_issues_shipped(_FakeJira(shipped_count=None))
    assert "error" in out


def test_get_defect_introduction_rate() -> None:
    out = get_defect_introduction_rate(
        _FakeJira(bugs_created_count=4, shipped_count=20),
        days=30,
    )
    assert out["bugs_created"] == 4
    assert out["issues_shipped"] == 20
    assert out["value"] == 20.0
    assert "issuetype = Bug" in out["bugs_jql"]
    assert "created >= -30d" in out["bugs_jql"]


def test_get_defect_introduction_rate_zero_shipped() -> None:
    out = get_defect_introduction_rate(
        _FakeJira(bugs_created_count=1, shipped_count=0),
        days=30,
    )
    assert "error" in out
    assert "Issues Shipped is 0" in out["error"]


def test_get_defect_introduction_rate_fails_loud_on_bugs_count() -> None:
    out = get_defect_introduction_rate(
        _FakeJira(bugs_created_count=None, shipped_count=20),
        days=30,
    )
    assert "error" in out
    assert "numerator" in out["error"]


def test_get_growth_allocation_pct(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = [
        {"fields": {"issuetype": {"name": "Story"}, "labels": []}},
        {"fields": {"issuetype": {"name": "Story"}, "labels": []}},
        {"fields": {"issuetype": {"name": "Bug"}, "labels": []}},
        {"fields": {"issuetype": {"name": "Task"}, "labels": ["escalation"]}},
    ]
    jira = _FakeJira(closed=closed)
    out = get_growth_allocation_pct(jira, days=30)
    assert out["numerator"] == 2.0  # two Stories
    assert out["denominator"] == 4.0
    assert out["planned"] == 2
    assert out["unplanned"] == 2


def test_get_ai_spend_pct(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as config_mod

    monkeypatch.setattr(config_mod, "CORTEX_ENGINEERING_MONTHLY_SPEND_USD", 500_000.0)
    monkeypatch.setattr(
        "src.cursor_ai_usage_metrics.get_monthly_ai_spend",
        lambda client, timeout=60.0: {"value": 10000.0, "spend_usd": 10000.0},
    )
    out = get_ai_spend_pct(_FakeCursor())
    assert out["numerator"] == 10000.0
    assert out["denominator"] == 500_000.0
    assert out["value"] == 2.0


def test_get_ai_spend_pct_missing_eng_spend(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as config_mod

    monkeypatch.setattr(config_mod, "CORTEX_ENGINEERING_MONTHLY_SPEND_USD", None)
    monkeypatch.delenv("CORTEX_ENGINEERING_MONTHLY_SPEND_USD", raising=False)
    out = get_ai_spend_pct(_FakeCursor())
    assert "error" in out
    assert "CORTEX_ENGINEERING_MONTHLY_SPEND_USD" in out["error"]


def test_get_ai_spend_per_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    jira = _FakeJira(headcount=2, emails={"a@ex.com"}, shipped_count=4)
    _patch_scope(monkeypatch, jira)
    events = [
        {"userEmail": "a@ex.com", "tokenUsage": {"inputTokens": 1, "outputTokens": 1, "totalCents": 0}, "chargedCents": 200},
    ]
    out = get_ai_spend_per_issue(_FakeCursor(events), jira, days=30)
    assert out["spend_usd"] == 2.0
    assert out["issues_shipped"] == 4
    assert out["value"] == 0.5


def test_get_headcount_plus_ai_spend_per_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as config_mod

    monkeypatch.setattr(config_mod, "CORTEX_ENGINEERING_MONTHLY_SPEND_USD", 30_000.0)
    jira = _FakeJira(headcount=2, emails={"a@ex.com"}, shipped_count=4)
    _patch_scope(monkeypatch, jira)
    events = [
        {"userEmail": "a@ex.com", "tokenUsage": {"inputTokens": 1, "outputTokens": 1, "totalCents": 0}, "chargedCents": 200},
    ]
    out = get_headcount_plus_ai_spend_per_issue(_FakeCursor(events), jira, days=30)
    assert out["headcount_usd"] == 30_000.0
    assert out["ai_spend_usd"] == 2.0
    assert out["total_usd"] == 30_002.0
    assert out["issues_shipped"] == 4
    assert out["value"] == 7500.5


def test_get_headcount_plus_ai_spend_per_issue_prorates_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.config as config_mod

    monkeypatch.setattr(config_mod, "CORTEX_ENGINEERING_MONTHLY_SPEND_USD", 30_000.0)
    jira = _FakeJira(headcount=2, emails={"a@ex.com"}, shipped_count=2)
    _patch_scope(monkeypatch, jira)
    events = [
        {"userEmail": "a@ex.com", "tokenUsage": {"inputTokens": 1, "outputTokens": 1, "totalCents": 0}, "chargedCents": 100},
    ]
    out = get_headcount_plus_ai_spend_per_issue(_FakeCursor(events), jira, days=15)
    assert out["headcount_usd"] == 15_000.0
    assert out["ai_spend_usd"] == 1.0
    assert out["value"] == 7500.5


def test_get_headcount_plus_ai_spend_per_issue_requires_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.config as config_mod

    monkeypatch.setattr(config_mod, "CORTEX_ENGINEERING_MONTHLY_SPEND_USD", None)
    monkeypatch.delenv("CORTEX_ENGINEERING_MONTHLY_SPEND_USD", raising=False)
    jira = _FakeJira(headcount=2, emails={"a@ex.com"}, shipped_count=4)
    _patch_scope(monkeypatch, jira)
    out = get_headcount_plus_ai_spend_per_issue(_FakeCursor([]), jira, days=30)
    assert "error" in out
    assert "CORTEX_ENGINEERING_MONTHLY_SPEND_USD" in out["error"]


def test_pr_is_ai_assisted_and_automated_markers() -> None:
    assert pr_is_ai_assisted({"title": "feat", "body": "Made with Cursor\n", "user": {"login": "alice"}})
    assert pr_is_ai_assisted({"title": "x", "body": "Co-authored-by: Cursor <cursoragent@cursor.com>", "user": {}})
    assert pr_is_ai_assisted({"title": "x", "body": "", "user": {"login": "cursoragent"}})
    assert pr_is_ai_assisted({"title": "x", "body": "", "user": {"login": "bob"}, "labels": [{"name": "ai-assisted"}]})
    assert not pr_is_ai_assisted({"title": "fix typo", "body": "no ai here", "user": {"login": "bob"}})
    # Commit trailers count as assisted when provided.
    assert pr_is_ai_assisted(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "fix\n\nCo-authored-by: Cursor <cursoragent@cursor.com>"}}],
    )
    assert pr_is_ai_assisted(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "feat\n\nMade-with: Cursor"}}],
    )
    assert not pr_is_ai_assisted(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "chore: no attribution"}}],
    )
    # Automated is stricter: agent author / generated markers, not mere "Made with Cursor".
    assert pr_is_ai_automated({"title": "x", "body": "", "user": {"login": "cursoragent"}})
    assert pr_is_ai_automated({"title": "x", "body": "Generated with Cursor", "user": {"login": "alice"}})
    assert not pr_is_ai_automated({"title": "feat", "body": "Made with Cursor\n", "user": {"login": "alice"}})
    # Commit-level automated: agent author or Generated/Created markers — not Co-authored-by alone.
    assert not pr_is_ai_automated(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "fix\n\nCo-authored-by: Cursor <cursoragent@cursor.com>"}}],
    )
    assert pr_is_ai_automated(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "Generated with Cursor"}, "author": {"login": "alice"}}],
    )
    assert pr_is_ai_automated(
        {"title": "x", "body": "plain", "user": {"login": "bob"}},
        commits=[{"commit": {"message": "agent change"}, "author": {"login": "cursoragent"}}],
    )


def test_get_ai_assisted_and_automated_prs_pct(monkeypatch: pytest.MonkeyPatch) -> None:
    pulls = [
        {"number": 1, "title": "a", "body": "Made with Cursor", "user": {"login": "alice"}},
        {"number": 2, "title": "b", "body": "plain", "user": {"login": "bob"}},
        {"number": 3, "title": "c", "body": "Co-authored-by: Cursor <x@y>", "user": {"login": "carol"}},
        {"number": 4, "title": "d", "body": "Generated with Cursor", "user": {"login": "dave"}},
        {"number": 5, "title": "e", "body": "", "user": {"login": "cursoragent"}},
        {"number": 6, "title": "f", "body": "plain trailer only", "user": {"login": "erin"}},
        {"number": 7, "title": "g", "body": "plain agent commit", "user": {"login": "frank"}},
    ]

    class _Gh:
        def list_merged_pulls_since(self, owner, repo, *, since, max_pulls=None):
            return pulls

        def list_pull_commits(self, owner, repo, number, *, max_commits=None):
            if int(number) == 6:
                return [
                    {
                        "commit": {
                            "message": (
                                "LEAN-1 fix\n\nCo-authored-by: Cursor <cursoragent@cursor.com>"
                            )
                        }
                    }
                ]
            if int(number) == 7:
                return [
                    {
                        "commit": {"message": "agent landed this"},
                        "author": {"login": "cursoragent"},
                    }
                ]
            return [{"commit": {"message": "no cursor here"}}]

    monkeypatch.setattr("src.github_client.github_configured", lambda: True)
    monkeypatch.setattr("src.github_client.GitHubClient", lambda: _Gh())
    monkeypatch.setattr(
        "src.github_client._resolve_repo_specs",
        lambda **kwargs: [("leandna-apex", "app")],
    )
    monkeypatch.setattr("src.github_client._github_org", lambda: "leandna-apex")
    monkeypatch.setattr("src.github_client._github_repos_env", lambda: None)
    monkeypatch.setattr(
        "src.engineer_identity_map.build_engineer_identity_map",
        lambda **kwargs: {"configured": False},
    )
    monkeypatch.setattr("src.engineer_identity_map.load_github_email_aliases", lambda: ({}, None))

    assisted = get_ai_assisted_prs_pct(days=30)
    # a,c,d,e + f (co-authored trailer) + g (agent commit → automated ⊂ assisted); not b
    assert assisted["numerator"] == 6.0
    assert assisted["denominator"] == 7.0
    assert assisted["value"] == round(100.0 * 6 / 7, 2)
    assert assisted["commit_trailer_checks"] >= 1

    automated = get_ai_automated_prs_pct(days=30)
    # d + e (PR signals) + g (cursoragent commit author); not f (co-authored only)
    assert automated["numerator"] == 3.0
    assert automated["denominator"] == 7.0
    assert automated["value"] == round(100.0 * 3 / 7, 2)
    assert automated["commit_trailer_checks"] >= 1

    # Deprecated alias still points at assisted.
    assert get_ai_assisted_automated_prs_pct(days=30)["value"] == assisted["value"]


def test_get_ai_assisted_prs_pct_github_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.github_client.github_configured", lambda: False)
    out = get_ai_assisted_prs_pct(days=30)
    assert "error" in out


def test_get_prs_merged(monkeypatch: pytest.MonkeyPatch) -> None:
    pulls = [
        {"number": 1, "title": "a", "body": "", "user": {"login": "alice"}},
        {"number": 2, "title": "b", "body": "", "user": {"login": "bob"}},
        {"number": 3, "title": "c", "body": "", "user": {"login": "carol"}},
    ]

    class _Gh:
        def list_merged_pulls_since(self, owner, repo, *, since, max_pulls=None):
            return pulls

        def list_pull_commits(self, owner, repo, number, *, max_commits=None):
            return []

    monkeypatch.setattr("src.github_client.github_configured", lambda: True)
    monkeypatch.setattr("src.github_client.GitHubClient", lambda: _Gh())
    monkeypatch.setattr(
        "src.github_client._resolve_repo_specs",
        lambda **kwargs: [("leandna-apex", "app")],
    )
    monkeypatch.setattr("src.github_client._github_org", lambda: "leandna-apex")
    monkeypatch.setattr("src.github_client._github_repos_env", lambda: None)
    monkeypatch.setattr(
        "src.engineer_identity_map.build_engineer_identity_map",
        lambda **kwargs: {
            "configured": True,
            "canonical_emails": ["alice@ex.com", "bob@ex.com"],
            "login_to_email": {"alice": "alice@ex.com", "bob": "bob@ex.com"},
        },
    )
    monkeypatch.setattr("src.engineer_identity_map.load_github_email_aliases", lambda: ({}, None))
    monkeypatch.setattr(
        "src.github_productivity_report._resolve_contributor_login",
        lambda login, **kwargs: {
            "alice": "alice@ex.com",
            "bob": "bob@ex.com",
        }.get(login),
    )
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", MagicMock)
    out = get_prs_merged(days=30)
    assert out["value"] == 2  # carol excluded (not in engineer map)
    assert out["scope"] == "engineers"


def test_get_weekly_active_ai_users_cursor_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    jira = _FakeJira()
    _patch_scope(monkeypatch, jira)
    out = get_weekly_active_ai_users(_FakeCursor(fail=True), jira)
    assert "error" in out
