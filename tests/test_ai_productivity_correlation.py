"""Tests for Cursor × GitHub productivity correlation."""

from __future__ import annotations

from src.ai_productivity_correlation import build_ai_productivity_correlation


def test_correlation_builds_company_and_individual_rows():
    cursor = {
        "configured": True,
        "window_days": 30,
        "usage_engineers": {
            "configured": True,
            "active_window": 1,
            "totals": {"total_tokens": 5000, "charged_cents_window": 250.0},
        },
        "engineer_usage_by_email": {
            "dev@leandna.com": {"tokens": 5000, "cents": 250.0, "events": 10},
        },
    }
    github = {
        "configured": True,
        "window_days": 30,
        "since": "2026-05-01T00:00:00+00:00",
        "generated_at": "2026-06-01T00:00:00+00:00",
        "company_engineers": {
            "commits": 10,
            "merged_prs": 2,
            "lines_added": 500,
        },
        "by_email": {
            "dev@leandna.com": {
                "commits": 10,
                "merged_prs": 2,
                "lines_added": 500,
                "lines_deleted": 50,
                "repos_touched": ["acme/web"],
            }
        },
        "warnings": [],
    }
    identity = {"configured": True, "canonical_emails": ["dev@leandna.com"]}

    out = build_ai_productivity_correlation(cursor, github, identity)
    assert out is not None
    assert out["company"]["total_tokens"] == 5000
    assert out["company"]["commits"] == 10
    assert out["company"]["tokens_per_commit"] == 500.0
    assert out["individuals"][0]["email"] == "dev@leandna.com"
    assert out["individuals"][0]["commits_per_1k_tokens"] == 2.0


def test_correlation_returns_none_without_cursor():
    assert build_ai_productivity_correlation(None, {"configured": True}) is None
