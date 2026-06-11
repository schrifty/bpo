"""Tests for eng_team_scorecard merge helpers."""

from __future__ import annotations

from src.eng_team_scorecard import (
    build_eng_team_scorecard,
    merge_team_scorecard_rows,
    summarize_team_scorecard,
)


def test_merge_team_scorecard_rows_combines_sources() -> None:
    delivery = {
        "teams": [
            {
                "team": "LEAN Engineering",
                "board_id": 44,
                "delivery_pct": 90.0,
                "delivered": 9,
                "committed": 10,
                "sprint": {"name": "Sprint 590", "end": "2026-06-01"},
            }
        ]
    }
    story_points = {
        "teams": [
            {
                "team": "LEAN Engineering",
                "board_id": 44,
                "story_points_delivered": 12.0,
                "story_points_committed": 20.0,
            }
        ]
    }
    cycle_time = {
        "teams": [
            {
                "team": "LEAN Engineering",
                "board_id": 44,
                "median_days": 4.5,
                "measured": 8,
            }
        ]
    }
    rows = merge_team_scorecard_rows(
        delivery=delivery,
        story_points=story_points,
        cycle_time=cycle_time,
    )
    lean = next(row for row in rows if row["board_id"] == 44)
    assert lean["delivery_pct"] == 90.0
    assert lean["story_points_delivered"] == 12.0
    assert lean["median_cycle_days"] == 4.5
    assert lean["sprint_name"] == "Sprint 590"


def test_summarize_team_scorecard() -> None:
    summary = summarize_team_scorecard(
        [
            {"delivery_pct": 80.0, "median_cycle_days": 4.0, "story_points_delivered": 10.0},
            {"delivery_pct": 100.0, "median_cycle_days": 6.0, "story_points_delivered": 5.5},
        ]
    )
    assert summary["average_delivery_pct"] == 90.0
    assert summary["average_median_cycle_days"] == 5.0
    assert summary["total_story_points_delivered"] == 15.5


def test_build_eng_team_scorecard_monkeypatched(monkeypatch) -> None:
    class FakeClient:
        pass

    monkeypatch.setattr(
        "src.eng_team_scorecard.get_sprint_delivery_by_team",
        lambda client, **kwargs: {
            "teams": [{"board_id": 44, "team": "LEAN Engineering", "delivery_pct": 75.0, "delivered": 3, "committed": 4, "sprint": {"name": "Sprint 1"}}],
        },
    )
    monkeypatch.setattr(
        "src.eng_team_scorecard.get_sprint_story_points_by_team",
        lambda client, **kwargs: {
            "teams": [{"board_id": 44, "team": "LEAN Engineering", "story_points_delivered": 8.0, "story_points_committed": 12.0}],
        },
    )
    monkeypatch.setattr(
        "src.eng_team_scorecard.get_dev_team_cycle_times",
        lambda client, **kwargs: {
            "teams": [{"board_id": 44, "team": "LEAN Engineering", "median_days": 3.25, "measured": 5}],
        },
    )

    payload = build_eng_team_scorecard(FakeClient(), days=30)
    assert len(payload["teams"]) == 3
    lean = next(row for row in payload["teams"] if row["board_id"] == 44)
    assert lean["delivery_pct"] == 75.0
    assert payload["summary"]["average_delivery_pct"] == 75.0
