"""Render tests for the engineering sprint-velocity slide."""

from __future__ import annotations

from src.slide_engineering_portfolio import eng_velocity_slide


def _velocity_history() -> dict:
    return {
        "boards": [
            {
                "team": "LEAN Engineering",
                "board_id": 44,
                "board_name": "LEAN Engineering",
                "sprints": [
                    {"sprint": {"name": "Sprint 592"}, "story_points_delivered": 30, "delivered_issues": 10},
                    {"sprint": {"name": "Sprint 591"}, "story_points_delivered": 20, "delivered_issues": 8},
                ],
            },
        ],
    }


def _report(*, with_velocity: bool) -> dict:
    eng = {
        "days": 30,
        "in_flight_count": 12,
        "closed_count": 40,
        "by_status": {"In Progress": 6, "To Do": 4, "In Review": 2},
        "throughput": [
            {"label": f"W{i}", "week": f"2026-W{i:02d}", "created": 5 + i, "resolved": 4 + i}
            for i in range(1, 13)
        ],
        "insights": {"velocity": ["Velocity climbing", "Backlog stable"]},
    }
    if with_velocity:
        eng["sprint_velocity"] = _velocity_history()
    return {"eng_portfolio": eng}


def test_velocity_slide_renders_story_points_without_charts() -> None:
    reqs: list = []
    # charts absent -> no embedded chart, but the per-sprint SP table must still render.
    eng_velocity_slide(reqs, "sid_v", _report(with_velocity=True), 0)

    title_inserts = [
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_v_ttl"
    ]
    assert title_inserts and title_inserts[0] == "Velocity"
    sub_inserts = [
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_v_sub"
    ]
    assert sub_inserts and "SP" in sub_inserts[0]

    all_text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    # Per-sprint table header (SP / Tix) is present.
    assert "SP" in all_text and "Tix" in all_text


def test_velocity_slide_falls_back_to_ticket_throughput() -> None:
    reqs: list = []
    eng_velocity_slide(reqs, "sid_v2", _report(with_velocity=False), 0)
    title_inserts = [
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_v2_ttl"
    ]
    assert title_inserts
    # Legacy weekly view keys off tickets, not story points.
    assert "Story Points" not in title_inserts[0]
    all_text = " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )
    assert "Created" in all_text and "Closed" in all_text
