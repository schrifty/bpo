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
                    "total_throughput": 15,
                    "average_median_lead_days": 5.2,
                },
                "teams": [
                    {
                        "team": "Supply Insights",
                        "board_id": 44,
                        "sprint_name": "Sprint 590",
                        "median_lead_days": 4.5,
                        "throughput": 9,
                    },
                    {
                        "team": "Data Integration",
                        "board_id": 46,
                        "sprint_name": "Sprint 12",
                        "median_lead_days": 5.8,
                        "throughput": 6,
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
    assert table["columns"] == 4, "Team, Latest sprint, Closed, Lead time"

    # Column widths must justify the table to the full content width (no left-shifted gap).
    widths = [
        req["updateTableColumnProperties"]["tableColumnProperties"]["columnWidth"]["magnitude"]
        for req in reqs
        if isinstance(req, dict) and "updateTableColumnProperties" in req
    ]
    assert len(widths) == 4
    assert abs(sum(widths) - CONTENT_W) < 1.0, "table should span CONTENT_W edge to edge"

    # Text columns (Team, Latest sprint) left; numeric columns (Closed, Lead time) right.
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
    assert alignments[(1, 3)] == "END"

    kpi_outlines = [
        r["createShape"]["objectId"]
        for r in reqs
        if isinstance(r, dict)
        and "createShape" in r
        and str(r["createShape"].get("objectId", "")).startswith("sid_sc_kpi")
        and r["createShape"].get("shapeType") == "RECTANGLE"
    ]
    assert len(kpi_outlines) >= 3, "scorecard should render shared KPI metric cards via _kpi_metric_card"
    kpi_values = {
        r["insertText"]["objectId"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId", "").endswith("_v")
    }
    assert "sid_sc_kpi0_v" in kpi_values


def test_eng_team_scorecard_slide_missing_data() -> None:
    reqs: list = []
    report = {"eng_portfolio": {"team_scorecard": {"teams": []}}}
    eng_team_scorecard_slide(reqs, "sid_sc2", report, 0)
    assert reqs
