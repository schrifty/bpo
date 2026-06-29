"""Tests for Cursor × GitHub productivity correlation."""

from __future__ import annotations

from src.ai_productivity_correlation import _pearson, build_ai_productivity_correlation


def test_pearson_fallback_without_statistics_correlation(monkeypatch) -> None:
    import statistics

    monkeypatch.delattr(statistics, "correlation", raising=False)
    # Perfect positive correlation
    assert _pearson([1.0, 2.0, 3.0, 4.0], [2.0, 4.0, 6.0, 8.0]) == 1.0


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


def test_correlation_weekly_trend_and_quadrants():
    cursor = {
        "configured": True,
        "window_days": 30,
        "usage_engineers": {
            "configured": True,
            "active_window": 2,
            "totals": {"total_tokens": 10_000, "charged_cents_window": 500.0},
            "daily": [
                {"date": "2026-05-05", "total_tokens": 4000},
                {"date": "2026-05-12", "total_tokens": 6000},
            ],
        },
        "engineer_usage_by_email": {
            "high@leandna.com": {"tokens": 7000, "cents": 350.0, "events": 20},
            "low@leandna.com": {"tokens": 3000, "cents": 150.0, "events": 8},
        },
    }
    github = {
        "configured": True,
        "window_days": 30,
        "since": "2026-05-01T00:00:00+00:00",
        "generated_at": "2026-06-01T00:00:00+00:00",
        "company_engineers": {"commits": 15, "merged_prs": 3, "lines_added": 800},
        "by_email": {
            "high@leandna.com": {"commits": 12, "merged_prs": 2, "lines_added": 600},
            "low@leandna.com": {"commits": 3, "merged_prs": 1, "lines_added": 200},
        },
        "weekly": [
            {"week": "2026-W19", "commits": 5, "engineer_commits": 5},
            {"week": "2026-W20", "commits": 10, "engineer_commits": 10},
        ],
        "warnings": [],
    }
    out = build_ai_productivity_correlation(cursor, github, {"configured": True, "canonical_emails": []})
    assert out is not None
    assert out["weekly_trend"]
    assert sum(out["quadrant_counts"].values()) >= 1
    assert out["top_yield"]
    assert out["quadrants"]["high_tokens_high_output"]
