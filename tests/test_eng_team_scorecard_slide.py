"""Render tests for eng_team_scorecard slide."""

from __future__ import annotations

from src.slide_engineering_portfolio import eng_team_scorecard_slide
from src.slides_theme import CONTENT_W


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

    create_tables = [r["createTable"] for r in reqs if isinstance(r, dict) and "createTable" in r]
    assert len(create_tables) == 1, "scorecard should render exactly one native table"
    table = create_tables[0]
    assert table["rows"] == 3, "header row + two team rows"
    assert table["columns"] == 6

    # Column widths must justify the table to the full content width (no left-shifted gap).
    widths = [
        req["updateTableColumnProperties"]["tableColumnProperties"]["columnWidth"]["magnitude"]
        for req in reqs
        if isinstance(req, dict) and "updateTableColumnProperties" in req
    ]
    assert len(widths) == 6
    assert abs(sum(widths) - CONTENT_W) < 1.0, "table should span CONTENT_W edge to edge"

    # Numeric columns (Delivery/Story pts/Cycle/Done) right-aligned; text columns left.
    alignments = {
        (
            req["updateParagraphStyle"]["cellLocation"]["rowIndex"],
            req["updateParagraphStyle"]["cellLocation"]["columnIndex"],
        ): req["updateParagraphStyle"]["style"]["alignment"]
        for req in reqs
        if isinstance(req, dict)
        and "updateParagraphStyle" in req
        and "cellLocation" in req["updateParagraphStyle"]
    }
    assert alignments[(1, 0)] == "START"
    assert alignments[(1, 2)] == "END"
    assert alignments[(1, 5)] == "END"


def test_eng_team_scorecard_slide_missing_data() -> None:
    reqs: list = []
    report = {"eng_portfolio": {"team_scorecard": {"teams": []}}}
    eng_team_scorecard_slide(reqs, "sid_sc2", report, 0)
    assert reqs
