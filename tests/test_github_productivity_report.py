"""Tests for GitHub productivity report builder."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.github_productivity_report import (
    build_github_productivity_report,
    compute_github_delivery_insights,
    github_qa_blob,
)


def _identity() -> dict:
    return {
        "configured": True,
        "canonical_emails": ["dev@leandna.com"],
        "login_to_email": {"dev": "dev@leandna.com"},
        "warnings": [],
    }


def test_build_productivity_report_aggregates(monkeypatch):
    monkeypatch.setattr("src.github_productivity_report.github_configured", lambda: True)
    recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT12:00:00Z")

    gh = MagicMock()
    gh.get_authenticated_user.return_value = {"login": "bot"}
    gh.get_repo.return_value = {"full_name": "acme/web", "default_branch": "main", "pushed_at": recent}
    gh.list_commits.return_value = [
        {
            "commit": {
                "author": {"email": "dev@leandna.com", "date": recent},
            }
        }
    ]
    gh.list_pull_requests.return_value = [
        {
            "state": "closed",
            "merged_at": recent,
            "created_at": recent,
            "updated_at": recent,
            "user": {"login": "dev"},
        }
    ]
    gh.list_releases.return_value = []
    gh.get_contributor_stats.return_value = [
        {
            "author": {"login": "dev"},
            "weeks": [{"w": datetime.now(timezone.utc).timestamp(), "a": 100, "d": 10, "c": 2}],
        }
    ]

    monkeypatch.setattr(
        "src.github_productivity_report._resolve_repo_specs",
        lambda **kw: [("acme", "web")],
    )
    monkeypatch.setattr("src.github_productivity_report.cache_get", lambda *a, **k: None)

    report = build_github_productivity_report(
        window_days=14,
        client=gh,
        identity=_identity(),
        use_cache=False,
    )
    assert report is not None
    assert report["company_engineers"]["commits"] == 1
    assert report["company_engineers"]["merged_prs"] == 1
    assert report["company_engineers"]["lines_added"] == 100
    assert report["by_email"]["dev@leandna.com"]["commits"] == 1
    assert report["top_contributors"][0]["email"] == "dev@leandna.com"
    assert report["weekly"][0]["merged_prs"] == 1
    assert report["weekly"][0]["engineer_merged_prs"] == 1
    assert report["company_engineers"]["median_pr_cycle_hours"] == 0.0


def test_github_qa_blob():
    blob = github_qa_blob({"configured": True, "api": "rest", "user_login": "bot"})
    assert blob["configured"] is True
    assert blob["user_login"] == "bot"


def test_merged_prs_count_without_engineer_author_mapping(monkeypatch):
    monkeypatch.setattr("src.github_productivity_report.github_configured", lambda: True)
    recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT12:00:00Z")

    gh = MagicMock()
    gh.get_authenticated_user.return_value = {"login": "bot"}
    gh.get_repo.return_value = {"full_name": "acme/web", "default_branch": "main", "pushed_at": recent}
    gh.list_commits.return_value = []
    gh.list_pull_requests.return_value = [
        {
            "state": "closed",
            "merged_at": recent,
            "created_at": recent,
            "updated_at": recent,
            "user": {"login": "xyzzy_unmapped"},
        }
    ]
    gh.list_releases.return_value = []
    gh.get_contributor_stats.return_value = []

    monkeypatch.setattr(
        "src.github_productivity_report._resolve_repo_specs",
        lambda **kw: [("acme", "web")],
    )
    monkeypatch.setattr(
        "src.github_productivity_report.load_github_email_aliases",
        lambda: ({}, {}),
    )
    monkeypatch.setattr("src.github_productivity_report.cache_get", lambda *a, **k: None)

    report = build_github_productivity_report(
        window_days=14,
        client=gh,
        identity=_identity(),
        use_cache=False,
    )
    assert report is not None
    assert report["repos_summary"][0]["merged_prs"] == 1
    assert report["company_all"]["merged_prs"] == 1
    assert report["company_engineers"]["merged_prs"] == 0


def test_compute_github_delivery_insights_flags_review_backlog():
    gp = {
        "configured": True,
        "window_days": 30,
        "company_all": {"open_prs": 40, "releases": 2},
        "company_engineers": {"merged_prs": 20, "median_pr_cycle_hours": 23.6},
        "weekly": [
            {"engineer_commits": 50, "engineer_merged_prs": 8},
            {"engineer_commits": 45, "engineer_merged_prs": 6},
        ],
    }
    insights = compute_github_delivery_insights(gp)
    assert "backlog" in insights["takeaway"].lower() or "open" in insights["takeaway"].lower()
    assert "median_pr_cycle_hours" not in insights["speaker_guidance"]
    assert "23" in insights["speaker_guidance"] or "24h" in insights["speaker_guidance"]


def test_compute_github_delivery_insights_detects_commit_merge_gap():
    gp = {
        "configured": True,
        "window_days": 30,
        "company_all": {"open_prs": 5, "releases": 1},
        "company_engineers": {"merged_prs": 12, "median_pr_cycle_hours": 18},
        "weekly": [
            {"engineer_commits": 40, "engineer_merged_prs": 2},
            {"engineer_commits": 35, "engineer_merged_prs": 1},
        ],
    }
    insights = compute_github_delivery_insights(gp)
    assert "outpac" in insights["takeaway"].lower() or "commits" in insights["takeaway"].lower()


def test_build_productivity_report_uses_cache(monkeypatch):
    monkeypatch.setattr("src.github_productivity_report.github_configured", lambda: True)
    cached = {"configured": True, "company_engineers": {"commits": 99}}
    calls = {"get": 0, "set": 0}

    def _get(key, ttl_seconds=None):
        calls["get"] += 1
        return cached

    def _set(key, data, ttl_seconds=None):
        calls["set"] += 1

    monkeypatch.setattr("src.github_productivity_report.cache_get", _get)
    monkeypatch.setattr("src.github_productivity_report.cache_set", _set)

    report = build_github_productivity_report(window_days=7, identity=_identity())
    assert report == cached
    assert calls["get"] == 1
    assert calls["set"] == 0
