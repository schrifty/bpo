"""Tests for the VP-level synthesis slides and their backing flow/work-split data."""

from __future__ import annotations

from datetime import date

from src.jira_client import compute_eng_flow, compute_eng_work_split
from src.slide_engineering_portfolio import (
    eng_exec_summary_slide,
    eng_flow_bottlenecks_slide,
    eng_work_split_slide,
)
from src.slide_metadata import SLIDE_DATA_REQUIREMENTS
from src.slide_registry import _SLIDE_BUILDERS

_TODAY = date(2026, 6, 10)

_IN_FLIGHT = [
    {"key": "L-A", "summary": "A", "status": "In Progress", "type": "Story", "labels": [], "created": "2026-06-01", "updated": "2026-06-09"},
    {"key": "L-B", "summary": "B", "status": "In Review", "type": "Bug", "labels": [], "created": "2026-05-01", "updated": "2026-06-01"},
    {"key": "L-C", "summary": "C", "status": "In Review", "type": "Task", "labels": ["customer_escalation"], "created": "2026-04-01", "updated": "2026-05-20"},
    {"key": "L-D", "summary": "D", "status": "Open", "type": "Story", "labels": [], "created": "2026-06-05", "updated": "2026-06-05"},
    {"key": "L-E", "summary": "E", "status": "In Progress", "type": "Story", "labels": [], "created": "2026-06-05", "updated": "2026-06-08"},
]
_CLOSED = [
    {"key": "L-X", "summary": "X", "status": "Closed", "type": "Story", "labels": [], "created": "2026-05-01", "updated": "2026-05-08"},
    {"key": "L-Y", "summary": "Y", "status": "Closed", "type": "Bug", "labels": [], "created": "2026-05-08", "updated": "2026-05-15"},
    {"key": "L-Z", "summary": "Z", "status": "Closed", "type": "Story", "labels": [], "created": "2026-05-15", "updated": "2026-05-22"},
]


def test_compute_eng_flow_identifies_active_and_stalled() -> None:
    flow = compute_eng_flow(_IN_FLIGHT, _CLOSED, today=_TODAY)
    assert flow["active_count"] == 4
    assert flow["in_progress"] == 2
    assert flow["in_review"] == 2
    assert flow["stale_gt5"] == 2
    assert flow["stale_gt10"] == 1
    assert flow["median_active_age_days"] == 24.5
    assert flow["oldest_active_age_days"] == 70
    # Worst-stalled item (idle longest) sorts first.
    assert flow["stale_items"][0]["key"] == "L-C"
    assert len(flow["cycle_trend"]) >= 2
    assert flow["cycle_delta_days"] is not None


def test_compute_eng_work_split_classifies_reactive() -> None:
    ws = compute_eng_work_split(_IN_FLIGHT, _CLOSED, escalated_to_eng=7)
    assert ws["wip"] == {"planned": 3, "unplanned": 2, "total": 5}
    assert ws["reactive_wip_pct"] == 40
    assert ws["unplanned_breakdown"] == {"Bugs": 1, "Escalations / other": 1}
    assert ws["closed"]["unplanned"] == 1
    assert ws["reactive_closed_pct"] == 33
    assert ws["escalated_to_eng"] == 7


def _report() -> dict:
    return {
        "eng_portfolio": {
            "days": 30,
            "sprint": {"name": "Sprint 592", "start": "2026-05-28", "end": "2026-06-10"},
            "team_scorecard": {"summary": {
                "total_throughput": 89,
                "total_committed": 130,
                "total_delivered": 30,
            }},
            "by_assignee": {"Alice": 9, "Bob": 7, "Carol": 5, "Dan": 1},
            "blocker_critical": [{"key": "LEAN-1"}, {"key": "LEAN-2"}],
            "project_snapshots": {
                "LEAN": {
                    "open_count": 40,
                    "open_over_90_count": 12,
                    "oldest_open_age_days": 210.0,
                    "median_open_age_days": 28.0,
                }
            },
            "flow": {
                "active_count": 4, "in_progress": 2, "in_review": 2,
                "stale_gt5": 2, "stale_gt10": 1,
                "median_active_age_days": 24.5, "oldest_active_age_days": 70,
                "stale_items": [
                    {"key": "L-C", "summary": "C", "status": "In Review", "assignee": "Bob", "idle_days": 21, "age_days": 70},
                    {"key": "L-B", "summary": "B", "status": "In Review", "assignee": "Alice", "idle_days": 9, "age_days": 40},
                ],
                "cycle_trend": [
                    {"week": "2026-W19", "median_cycle_days": 9, "closed": 3},
                    {"week": "2026-W20", "median_cycle_days": 12, "closed": 2},
                ],
                "cycle_delta_days": 3.0,
            },
            "work_split": {
                "wip": {"planned": 3, "unplanned": 2, "total": 5},
                "closed": {"planned": 20, "unplanned": 11, "total": 31},
                "reactive_wip_pct": 50,
                "reactive_closed_pct": 35,
                "unplanned_breakdown": {"Bugs": 1, "Escalations / other": 1},
                "escalated_to_eng": 7,
            },
        }
    }


def _title(reqs: list, sid: str) -> str:
    return next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == f"{sid}_ttl"
    )


def _has_obj(reqs: list, oid: str) -> bool:
    return any(
        isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == oid
        for r in reqs
    )


def test_new_vp_slides_registered() -> None:
    for key in ("eng_exec_summary", "eng_flow_bottlenecks", "eng_work_split"):
        assert key in _SLIDE_BUILDERS
        assert SLIDE_DATA_REQUIREMENTS[key] == ["eng_portfolio"]


def _all_text(reqs: list) -> str:
    return " ".join(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r
    )


def test_exec_summary_flags_attention_and_renders_callouts() -> None:
    reqs: list = []
    eng_exec_summary_slide(reqs, "sid_es", _report(), 0)
    title = _title(reqs, "sid_es")
    # Blockers are the one critical (red) item; the rest are amber watch items. The title
    # must reflect both counts, not just the reds, so it never undercounts the bullets.
    assert "Critical" in title and "Watch" in title
    assert _has_obj(reqs, "sid_es_kpi3_v")  # four headline KPI cards
    assert _has_obj(reqs, "sid_es_risk_b0")  # watch-list bullet
    assert _has_obj(reqs, "sid_es_act_b0")  # decisions-needed bullet
    # Sprint backlog is framed as hygiene/carryover, not a delivery miss.
    body = _all_text(reqs)
    assert "unfinished issues" in body
    assert "commitments slipping" not in body
    assert "delivery" not in body.lower()


def test_flow_slide_titles_bottleneck_and_lists_stalled() -> None:
    reqs: list = []
    eng_flow_bottlenecks_slide(reqs, "sid_fl", _report(), 0)
    assert "Stalled" in _title(reqs, "sid_fl")
    assert _has_obj(reqs, "sid_fl_kpi3_v")
    # Stalled-items table created with worst offender first.
    tables = [r["createTable"] for r in reqs if isinstance(r, dict) and "createTable" in r]
    assert len(tables) == 1
    first_cell = next(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict)
        and "insertText" in r
        and r["insertText"].get("cellLocation") == {"rowIndex": 1, "columnIndex": 0}
    )
    assert first_cell == "L-C"


def test_work_split_titles_reactive_dominance() -> None:
    reqs: list = []
    eng_work_split_slide(reqs, "sid_ws", _report(), 0)
    assert "Reactive Work Dominating" in _title(reqs, "sid_ws")
    assert "50%" in _title(reqs, "sid_ws")
    assert _has_obj(reqs, "sid_ws_kpi3_v")
