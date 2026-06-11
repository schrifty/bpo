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


def test_merge_threads_lead_time_and_dedupes_shared_sprints() -> None:
    # Boards 36 and 46 share sprint 2939; the global sprint-issue endpoint returns the
    # same issues for both, so the merge must count that sprint once (no double count).
    delivery = {
        "teams": [
            {"team": "CUSTOMER Active Scrum", "board_id": 36, "delivery_pct": 30.0,
             "delivered": 34, "committed": 113, "sprint": {"name": "Week of Jun 1", "id": 2939}},
            {"team": "Data Integration", "board_id": 46, "delivery_pct": 30.0,
             "delivered": 34, "committed": 113, "sprint": {"name": "Week of Jun 1", "id": 2939}},
            {"team": "LEAN Engineering", "board_id": 44, "delivery_pct": 100.0,
             "delivered": 59, "committed": 59, "sprint": {"name": "Sprint595", "id": 2598}},
        ]
    }
    story_points = {
        "teams": [
            {"board_id": 36, "story_points_delivered": 218.0, "story_points_committed": 525.0},
            {"board_id": 46, "story_points_delivered": 218.0, "story_points_committed": 525.0},
            {"board_id": 44, "story_points_delivered": 0.0, "story_points_committed": 0.0},
        ]
    }
    cycle_time = {
        "teams": [
            {"board_id": 36, "median_days": 0.83, "lead_time_median_days": 7.56, "lead_time_measured": 145},
            {"board_id": 46, "median_days": 0.83, "lead_time_median_days": 7.56, "lead_time_measured": 143},
            {"board_id": 44, "median_days": 0.82, "lead_time_median_days": 4.87, "lead_time_measured": 33},
        ]
    }
    rows = merge_team_scorecard_rows(delivery=delivery, story_points=story_points, cycle_time=cycle_time)
    sprint_ids = [(r.get("sprint") or {}).get("id") for r in rows]
    assert sprint_ids.count(2939) == 1, "shared sprint collapsed to one row"
    customer = next(r for r in rows if (r.get("sprint") or {}).get("id") == 2939)
    assert customer["shared_board_ids"] == [36, 46]
    assert "Data Integration" in customer["team"]
    assert customer["median_lead_days"] == 7.56

    summary = summarize_team_scorecard(rows)
    assert summary["total_story_points_delivered"] == 218.0  # not 436 (no double count)
    assert summary["average_median_lead_days"] == round((7.56 + 4.87) / 2, 1)


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
