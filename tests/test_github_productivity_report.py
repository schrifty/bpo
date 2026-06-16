"""Tests for GitHub productivity report builder."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.github_productivity_report import build_github_productivity_report, github_qa_blob


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

    report = build_github_productivity_report(
        window_days=14,
        client=gh,
        identity=_identity(),
    )
    assert report is not None
    assert report["company_engineers"]["commits"] == 1
    assert report["company_engineers"]["merged_prs"] == 1
    assert report["company_engineers"]["lines_added"] == 100
    assert report["by_email"]["dev@leandna.com"]["commits"] == 1


def test_github_qa_blob():
    blob = github_qa_blob({"configured": True, "api": "rest", "user_login": "bot"})
    assert blob["configured"] is True
    assert blob["user_login"] == "bot"
