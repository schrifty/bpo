"""Render and enrichment tests for GitHub × Cursor productivity slides."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.deck_data_enrichment import filter_github_productivity_slides
from src.slide_engineering_portfolio import (
    ai_output_correlation_slide,
    ai_productivity_matrix_slide,
    github_change_profile_slide,
    github_delivery_flow_slide,
    github_engineer_contribution_slide,
    github_engineering_output_slide,
)


def _github_report() -> dict:
    return {
        "github_productivity": {
            "configured": True,
            "window_days": 30,
            "repos": ["acme/web", "acme/api"],
            "takeaways": {
                "github_output": "42 commits and 8 merged PRs across 2 repos (30d, dev-* engineers).",
                "github_contribution": "4 active contributors merged 8 PRs in 30d.",
                "github_delivery": "Review backlog is building—prioritize reviewer capacity before adding parallel work.",
                "github_change": "3,200 lines added and 800 deleted (30d).",
            },
            "delivery_insights": {
                "takeaway": "Review backlog is building—prioritize reviewer capacity before adding parallel work.",
                "speaker_guidance": "Use this slide to judge whether Engineering is shipping through review or accumulating hidden WIP.",
            },
            "top_contributors": [
                {"email": "dev@leandna.com", "commits": 25, "merged_prs": 5, "lines_net": 2000},
                {"email": "peer@leandna.com", "commits": 17, "merged_prs": 3, "lines_net": 1200},
            ],
            "weekly": [
                {"week": "2026-W20", "label": "W20", "engineer_commits": 10, "engineer_merged_prs": 2},
                {"week": "2026-W21", "label": "W21", "engineer_commits": 15, "engineer_merged_prs": 3},
            ],
            "company_all": {"open_prs": 3, "releases": 1},
            "company_engineers": {
                "commits": 42,
                "merged_prs": 8,
                "lines_added": 3200,
                "lines_deleted": 800,
                "contributor_count": 2,
                "median_pr_cycle_hours": 18.5,
            },
            "repos_summary": [
                {
                    "full_name": "acme/web",
                    "commits": 25,
                    "merged_prs": 5,
                    "lines_added": 2000,
                    "lines_deleted": 400,
                },
                {
                    "full_name": "acme/api",
                    "commits": 17,
                    "merged_prs": 3,
                    "lines_added": 1200,
                    "lines_deleted": 400,
                },
            ],
        },
    }


def _ai_report() -> dict:
    base = _github_report()
    base["ai_productivity"] = {
        "configured": True,
        "window_days": 30,
        "company": {
            "total_tokens": 500_000,
            "commits": 42,
            "tokens_per_commit": 11904.76,
            "cents_per_merged_pr": 125.0,
            "commits_per_1k_tokens": 0.084,
            "token_commit_correlation": 0.72,
        },
        "weekly_trend": [
            {"week": "2026-W20", "label": "W20", "tokens": 120_000, "commits": 10},
            {"week": "2026-W21", "label": "W21", "tokens": 180_000, "commits": 15},
        ],
        "quadrant_counts": {
            "high_tokens_high_output": 2,
            "high_tokens_low_output": 1,
            "low_tokens_high_output": 1,
            "low_tokens_low_output": 0,
        },
        "medians": {"tokens": 40_000, "commits": 10},
        "individuals": [
            {
                "email": "dev@leandna.com",
                "tokens": 50_000,
                "commits": 12,
                "merged_prs": 4,
                "commits_per_1k_tokens": 0.24,
            },
            {
                "email": "peer@leandna.com",
                "tokens": 30_000,
                "commits": 8,
                "merged_prs": 2,
                "commits_per_1k_tokens": 0.27,
            },
        ],
        "top_yield": [
            {
                "email": "peer@leandna.com",
                "tokens": 30_000,
                "commits": 8,
                "commits_per_1k_tokens": 0.27,
            },
        ],
        "takeaways": {
            "correlation": "42 GitHub commits vs 500,000 Cursor tokens (30d); token↔commit r=0.72.",
            "matrix": "Yield ranks commits per 1K tokens; high-token/low-output quadrant flags coaching candidates.",
        },
    }
    return base


def test_github_engineering_output_slide_renders():
    reqs: list = []
    idx = github_engineering_output_slide(reqs, "gh1", _github_report(), 0)
    assert idx == 1
    assert any(r.get("createSlide") for r in reqs)
    text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "Repos Updated" in text
    assert "Repos tracked" not in text


def test_eng_divider_slide_renders_section_title():
    from src.slide_engineering_portfolio import eng_divider_slide

    reqs: list = []
    report = {"_current_slide": {"title": "GitHub Insights"}}
    idx = eng_divider_slide(reqs, "div1", report, 0)
    assert idx == 1
    text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "GitHub Insights" in text


def test_github_engineer_contribution_slide_renders():
    reqs: list = []
    idx = github_engineer_contribution_slide(reqs, "ghc", _github_report(), 0)
    assert idx == 1
    text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "GitHub Engineer Contribution" in text
    assert "Top 3 share" in text
    assert "dev" in text


def test_github_delivery_flow_slide_renders_with_chart():
    report = _github_report()
    charts = MagicMock()
    charts.add_combo_chart.return_value = ("ss", "chart1")
    report["_charts"] = charts
    reqs: list = []
    idx = github_delivery_flow_slide(reqs, "gdf", report, 0)
    assert idx == 1
    charts.add_combo_chart.assert_called_once()
    text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "open PRs vs" not in text.lower() or "backlog" in text.lower() or "review" in text.lower()


def test_github_change_profile_slide_renders_table():
    reqs: list = []
    idx = github_change_profile_slide(reqs, "gcp", _github_report(), 0)
    assert idx == 1
    assert any(r.get("createTable") for r in reqs)


def test_ai_output_correlation_slide_renders_with_chart():
    report = _ai_report()
    charts = MagicMock()
    charts.add_combo_chart.return_value = ("ss", "chart1")
    report["_charts"] = charts
    reqs: list = []
    idx = ai_output_correlation_slide(reqs, "ai1", report, 0)
    assert idx == 1
    charts.add_combo_chart.assert_called_once()


def test_ai_productivity_matrix_slide_renders_table():
    reqs: list = []
    idx = ai_productivity_matrix_slide(reqs, "mx1", _ai_report(), 0)
    assert idx == 1
    assert any(r.get("createTable") for r in reqs)
    text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "AI Productivity Matrix - Engineering" in text
    assert "High token / high out" in text
    assert "Commits / 1K tokens" in text
    assert "C/1K tok" not in text
    assert "High tok /" not in text
    assert "Low tok /" not in text
    assert "peer@leandna.com" in text or "peer" in text


def test_filter_cursor_only_slide_plan(monkeypatch):
    from src.deck_data_enrichment import filter_cursor_only_slide_plan

    plan = [
        {"slide_type": "eng_exec_summary"},
        {"slide_type": "cursor_cost"},
        {"slide_type": "cursor_efficiency"},
        {"slide_type": "data_quality"},
    ]
    assert filter_cursor_only_slide_plan(plan, deck_id="engineering-portfolio") == plan

    monkeypatch.setattr("src.deck_data_enrichment.BPO_CURSOR_SLIDES_ONLY", True)
    filtered = filter_cursor_only_slide_plan(plan, deck_id="engineering-portfolio")
    assert [e["slide_type"] for e in filtered] == ["cursor_cost", "cursor_efficiency"]


def test_filter_github_productivity_slides_drops_when_unconfigured():
    plan = [
        {"slide_type": "github_engineering_output"},
        {"slide_type": "github_engineer_contribution"},
        {"slide_type": "github_delivery_flow"},
        {"slide_type": "github_change_profile"},
        {"slide_type": "ai_output_correlation"},
        {"slide_type": "ai_productivity_matrix"},
        {"slide_type": "data_quality"},
    ]
    filtered = filter_github_productivity_slides({}, plan, deck_id="engineering-portfolio")
    assert filtered == [{"slide_type": "data_quality"}]

    partial = filter_github_productivity_slides(
        {"github_productivity": {"configured": True}},
        plan,
        deck_id="engineering-portfolio",
    )
    assert [e["slide_type"] for e in partial] == [
        "github_engineering_output",
        "github_engineer_contribution",
        "github_delivery_flow",
        "github_change_profile",
        "data_quality",
    ]
