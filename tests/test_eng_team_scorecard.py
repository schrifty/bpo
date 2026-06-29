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


def test_weighted_delivery_pct_not_simple_mean() -> None:
    """A small team at 100% must not drag the portfolio up; weight by commitment."""
    summary = summarize_team_scorecard(
        [
            {"delivery_pct": 100.0, "delivered": 59, "committed": 59},
            {"delivery_pct": 30.0, "delivered": 34, "committed": 113},
        ]
    )
    # Simple mean would be 65%; weighted is (59+34)/(59+113) = 54.1%.
    assert summary["average_delivery_pct"] == 65.0
    assert summary["weighted_delivery_pct"] == 54.1
    assert summary["total_delivered"] == 93
    assert summary["total_committed"] == 172


def test_weighted_delivery_pct_none_without_commitment() -> None:
    summary = summarize_team_scorecard([{"delivery_pct": 80.0}])
    assert summary["weighted_delivery_pct"] is None
    assert summary["total_committed"] is None


def test_build_eng_team_scorecard_monkeypatched(monkeypatch) -> None:
    """LEAN board is split into per-Agile-Team rows; other boards stay board-level."""
    class FakeClient:
        pass

    # LEAN (board 44) now flows through the Agile-Team split path — throughput, no say/do.
    monkeypatch.setattr("src.eng_team_scorecard.load_status_category_map", lambda client, **kw: {})
    monkeypatch.setattr(
        "src.eng_team_scorecard.lean_sprint_delivery_by_agile_team",
        lambda client, board, **kwargs: [
            {"team": "Supply Insights", "board_id": 44, "delivery_pct": None,
             "delivered": 15, "committed": None, "throughput": 15, "sprint_name": "Sprint 595",
             "story_points_delivered": None, "median_lead_days": 14.3},
            {"team": "Infrastructure", "board_id": 44, "delivery_pct": None,
             "delivered": 8, "committed": None, "throughput": 8, "sprint_name": "Sprint 595",
             "story_points_delivered": None, "median_lead_days": 1.9},
        ],
    )
    # CUSTOMER/DI delivery now comes from the authoritative sprint report.
    monkeypatch.setattr(
        "src.eng_team_scorecard.board_sprint_report",
        lambda client, board_id, sprint_id, **kw: {
            "completed": 31, "not_completed": 101, "punted": 0, "committed": 132,
            "delivery_pct": 23.485, "completed_sp": 149.0, "committed_sp": 621.0,
        },
    )
    # Other scrum boards (36 / 46) keep the board-level path.
    # Boards 36 and 46 share one sprint (id 2939) and dedupe to a single CUSTOMER row.
    monkeypatch.setattr(
        "src.eng_team_scorecard.get_sprint_delivery_by_team",
        lambda client, **kwargs: {
            "teams": [
                {"board_id": 36, "team": "CUSTOMER Active Scrum", "delivery_pct": 30.0,
                 "delivered": 34, "committed": 113, "sprint": {"name": "Week of Jun 1", "id": 2939}},
                {"board_id": 46, "team": "Data Integration", "delivery_pct": 30.0,
                 "delivered": 34, "committed": 113, "sprint": {"name": "Week of Jun 1", "id": 2939}},
            ],
        },
    )
    monkeypatch.setattr(
        "src.eng_team_scorecard.get_sprint_story_points_by_team",
        lambda client, **kwargs: {
            "teams": [
                {"board_id": 36, "story_points_delivered": 218.0, "story_points_committed": 525.0},
                {"board_id": 46, "story_points_delivered": 218.0, "story_points_committed": 525.0},
            ],
        },
    )
    monkeypatch.setattr(
        "src.eng_team_scorecard.get_dev_team_cycle_times",
        lambda client, **kwargs: {
            "teams": [
                {"board_id": 36, "median_days": 0.8, "lead_time_median_days": 7.7},
                {"board_id": 46, "median_days": 0.8, "lead_time_median_days": 7.7},
            ],
        },
    )

    payload = build_eng_team_scorecard(FakeClient(), days=30)
    # 2 LEAN agile-team rows + 1 CUSTOMER board row.
    assert len(payload["teams"]) == 3
    assert payload["lean_agile_team_count"] == 2
    # LEAN rows come first, carry no story points, and have no say/do %.
    assert payload["teams"][0]["team"] == "Supply Insights"
    assert payload["teams"][0]["story_points_delivered"] is None
    assert payload["teams"][0]["delivery_pct"] is None
    # CUSTOMER row carries the authoritative sprint-report numbers.
    cust = payload["teams"][-1]
    assert cust["delivered"] == 31 and cust["committed"] == 132
    assert cust["story_points_delivered"] == 149.0
    # Weighted delivery only counts committed boards (LEAN excluded): 31/132 = 23.5%.
    assert payload["summary"]["weighted_delivery_pct"] == 23.5
    assert payload["summary"]["total_delivered"] == 31
    assert payload["summary"]["total_committed"] == 132
    # Throughput is org-wide: 15 + 8 (LEAN) + 31 (CUSTOMER) = 54.
    assert payload["summary"]["total_throughput"] == 54
