"""Render tests for the reorganized engineering team slides."""

from __future__ import annotations

from src.slide_engineering_portfolio import (
    eng_backlog_health_slide,
    eng_capacity_slide,
    eng_current_sprint_slide,
)
from src.slide_registry import _SLIDE_BUILDERS
from src.slide_metadata import SLIDE_DATA_REQUIREMENTS


def _report() -> dict:
    return {
        "eng_portfolio": {
            "days": 30,
            "in_flight_count": 24,
            "closed_count": 31,
            "by_status": {"In Progress": 8, "In Review": 4, "To Do": 12},
            "by_type": {"Story": 14, "Bug": 6, "Task": 4},
            "by_assignee": {"Alice Smith": 9, "Bob Jones": 7, "Carol Lee": 5, "Unassigned": 3},
            "sprint": {"name": "Sprint 592", "start": "2026-05-28", "end": "2026-06-10"},
            "themes": [
                {"theme": "[Forecasting]", "total": 10, "in_progress": 4, "bugs": 2},
                {"theme": "[Integrations]", "total": 6, "in_progress": 2, "bugs": 0},
            ],
            "insights": {"sprint_snapshot": ["Forecasting dominates WIP", "Two bugs in flight"]},
            "project_snapshots": {
                "LEAN": {
                    "project_key": "LEAN",
                    "base_url": "https://example.atlassian.net",
                    "open_count": 40,
                    "by_status_open": {"In Progress": 12, "Waiting on Engineering": 18, "To Do": 10},
                    "median_open_age_days": 28.0,
                    "avg_resolved_cycle_days": 12.5,
                    "resolved_in_6mo_count": 120,
                    "open_age_buckets": {"0-7d": 8, "8-30d": 14, "31-90d": 6, "90d+": 12},
                    "open_over_90_count": 12,
                    "oldest_open_age_days": 210.0,
                    "assignee_resolved_table": [
                        {"assignee": "Alice Smith", "2w": 3, "1m": 7, "3m": 18, "6m": 30},
                        {"assignee": "Bob Jones", "2w": 2, "1m": 5, "3m": 12, "6m": 22},
                    ],
                }
            },
        }
    }


def test_new_slides_registered() -> None:
    for key in ("eng_current_sprint", "eng_backlog_health", "eng_capacity"):
        assert key in _SLIDE_BUILDERS
        assert SLIDE_DATA_REQUIREMENTS[key] == ["eng_portfolio"]


def test_current_sprint_renders_kpis_and_takeaway_title() -> None:
    reqs: list = []
    eng_current_sprint_slide(reqs, "sid_cs", _report(), 0)
    title = next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_cs_ttl"
    )
    assert "Sprint 592" in title and "Bug" in title
    # Four KPI cards (value boxes _kpi0_v .. _kpi3_v).
    kpi_values = {
        r["insertText"]["objectId"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId", "").startswith("sid_cs_kpi")
    }
    assert "sid_cs_kpi3_v" in kpi_values


def test_backlog_health_title_flags_aging() -> None:
    reqs: list = []
    eng_backlog_health_slide(reqs, "sid_bh", _report(), 0)
    title = next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_bh_ttl"
    )
    assert "Over 90d" in title and "28" in title


def test_backlog_health_marks_capped_resolved() -> None:
    report = _report()
    snap = report["eng_portfolio"]["project_snapshots"]["LEAN"]
    snap["resolved_in_6mo_count"] = 1500
    snap["resolved_in_6mo_capped"] = True
    reqs: list = []
    eng_backlog_health_slide(reqs, "sid_bh2", report, 0)
    texts = [
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r
    ]
    assert any("1,500+" in t for t in texts), "capped resolved count should render with a + suffix"


def test_capacity_table_justifies_and_right_aligns() -> None:
    from src.slides_theme import CONTENT_W

    reqs: list = []
    eng_capacity_slide(reqs, "sid_cap", _report(), 0)

    tables = [r["createTable"] for r in reqs if isinstance(r, dict) and "createTable" in r]
    assert len(tables) == 1
    assert tables[0]["columns"] == 4

    widths = [
        req["updateTableColumnProperties"]["tableColumnProperties"]["columnWidth"]["magnitude"]
        for req in reqs
        if isinstance(req, dict) and "updateTableColumnProperties" in req
    ]
    assert abs(sum(widths) - CONTENT_W) < 1.0

    aligns = {
        (
            req["updateParagraphStyle"]["cellLocation"]["rowIndex"],
            req["updateParagraphStyle"]["cellLocation"]["columnIndex"],
        ): req["updateParagraphStyle"]["style"]["alignment"]
        for req in reqs
        if isinstance(req, dict)
        and "updateParagraphStyle" in req
        and "cellLocation" in req["updateParagraphStyle"]
    }
    assert aligns[(1, 0)] == "START"
    assert aligns[(1, 1)] == "END"

    # Engineer with most open WIP (Alice, 9) sorts to the first data row.
    first_cell = next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict)
        and "insertText" in r
        and r["insertText"].get("cellLocation") == {"rowIndex": 1, "columnIndex": 0}
    )
    assert "Alice" in first_cell
