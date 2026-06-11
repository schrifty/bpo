"""Render tests for eng_team_scorecard slide."""

from __future__ import annotations

from src.slide_engineering_portfolio import eng_team_scorecard_slide


def test_eng_team_scorecard_slide_renders_table() -> None:
    reqs: list = []
    report = {
        "eng_portfolio": {
            "days": 30,
            "team_scorecard": {
                "window_days": 30,
                "summary": {
                    "average_delivery_pct": 88.0,
                    "total_story_points_delivered": 24.0,
                    "average_median_cycle_days": 5.2,
                },
                "teams": [
                    {
                        "team": "LEAN Engineering",
                        "board_id": 44,
                        "sprint_name": "Sprint 590",
                        "delivery_pct": 90.0,
                        "story_points_delivered": 12.0,
                        "story_points_committed": 20.0,
                        "median_cycle_days": 4.5,
                        "delivered": 9,
                        "committed": 10,
                    },
                    {
                        "team": "Data Integration",
                        "board_id": 46,
                        "sprint_name": "Sprint 12",
                        "delivery_pct": 86.0,
                        "story_points_delivered": 12.0,
                        "story_points_committed": 14.0,
                        "median_cycle_days": 5.8,
                        "delivered": 6,
                        "committed": 7,
                    },
                ],
            },
        }
    }
    eng_team_scorecard_slide(reqs, "sid_sc", report, 0)
    assert any(
        r.get("createSlide") or r.get("createShape") or r.get("insertText")
        for r in reqs
        if isinstance(r, dict)
    )


def test_eng_team_scorecard_slide_missing_data() -> None:
    reqs: list = []
    report = {"eng_portfolio": {"team_scorecard": {"teams": []}}}
    eng_team_scorecard_slide(reqs, "sid_sc2", report, 0)
    assert reqs
