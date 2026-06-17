"""Tests for the VP-level synthesis slides and their backing flow/work-split data."""

from __future__ import annotations

from datetime import date

from src.jira_client import compute_eng_flow, compute_eng_work_split
from src.slide_engineering_portfolio import (
    eng_exec_summary_slide,
    eng_flow_bottlenecks_slide,
    eng_toc_slide,
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


def test_compute_eng_flow_separates_abandoned_from_recent_stalls() -> None:
    in_flight = [
        {"key": "R1", "summary": "recent stall", "status": "In Progress", "type": "Story", "labels": [], "created": "2026-01-01", "updated": "2026-05-01"},
        {"key": "Z1", "summary": "zombie", "status": "In Progress", "type": "Story", "labels": [], "created": "2020-01-01", "updated": "2020-02-01"},
        {"key": "F1", "summary": "fresh", "status": "In Review", "type": "Story", "labels": [], "created": "2026-06-08", "updated": "2026-06-09"},
    ]
    stage = {"R1": 30.0, "Z1": 800.0, "F1": 2.0}
    flow = compute_eng_flow(in_flight, [], today=_TODAY, stage_age_by_key=stage, abandoned_days=180)
    assert flow["stale_recent"] == 1            # R1 only (10 < 30 <= 180)
    assert flow["abandoned_in_stage"] == 1      # Z1 (800d in stage)
    keys = [r["key"] for r in flow["attention_items"]]
    assert "R1" in keys and "Z1" not in keys    # zombie excluded from actionable list
    assert flow["abandoned_items"][0]["key"] == "Z1"
    # Stage median computed only on non-abandoned items (excludes the 800d zombie).
    assert flow["by_status_median_active"].get("In Progress") == 30.0


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
                "stale_recent": 2, "abandoned_in_stage": 3, "abandoned_days": 180,
                "by_status_median_active": {"In Progress": 6.0, "In Review": 12.0},
                "median_active_age_days": 24.5, "oldest_active_age_days": 70,
                "attention_items": [
                    {"key": "L-C", "summary": "C", "status": "In Review", "assignee": "Bob", "idle_days": 21, "age_days": 70, "days_in_status": 21, "sprint_count": 1, "carryover": False, "flagged": False, "priority": "Major"},
                    {"key": "L-B", "summary": "B", "status": "In Review", "assignee": "Alice", "idle_days": 9, "age_days": 40, "days_in_status": 9, "sprint_count": 1, "carryover": False, "flagged": False, "priority": "Major"},
                ],
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


def _subtitle(reqs: list, sid: str) -> str:
    return next(
        (
            r["insertText"]["text"]
            for r in reqs
            if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == f"{sid}_sub"
        ),
        "",
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
    assert _title(reqs, "sid_es") == "Executive Summary"
    sub = _subtitle(reqs, "sid_es")
    # Blockers are the one critical (red) item; the rest are amber watch items. The subtitle
    # verdict must reflect both counts, not just the reds, so it never undercounts the bullets.
    assert "critical" in sub and "watch" in sub
    assert _has_obj(reqs, "sid_es_kpi3_v")  # four headline KPI cards
    assert _has_obj(reqs, "sid_es_risk_b0")  # watch-list bullet
    assert _has_obj(reqs, "sid_es_act_b0")  # decisions-needed bullet
    # Sprint backlog is framed as hygiene/carryover, not a delivery miss.
    body = _all_text(reqs)
    assert "unfinished issues" in body
    assert "commitments slipping" not in body
    assert "delivery" not in body.lower()


def test_exec_summary_feeds_initiative_risk() -> None:
    report = _report()
    report["eng_portfolio"]["epic_progress"] = {
        "at_risk_count": 2,
        "epics": [
            {"key": "LEAN-100", "at_risk": True},
            {"key": "LEAN-200", "at_risk": True},
            {"key": "LEAN-300", "at_risk": False},
        ],
    }
    reqs: list = []
    eng_exec_summary_slide(reqs, "sid_ir", report, 0)
    body = _all_text(reqs)
    assert "2 initiatives stalled" in body
    assert "LEAN-100" in body and "LEAN-200" in body


def test_flow_slide_titles_bottleneck_and_lists_stalled() -> None:
    reqs: list = []
    eng_flow_bottlenecks_slide(reqs, "sid_fl", _report(), 0)
    assert _title(reqs, "sid_fl") == "Flow & Bottlenecks"
    assert "stalled" in _subtitle(reqs, "sid_fl")
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


def test_toc_slide_registered_and_renders_sections() -> None:
    assert "eng_toc" in _SLIDE_BUILDERS
    assert SLIDE_DATA_REQUIREMENTS["eng_toc"] == []
    reqs: list = []
    nxt = eng_toc_slide(reqs, "sid_toc", {}, 3)
    assert nxt == 4
    assert _title(reqs, "sid_toc") == "Agenda"
    body = _all_text(reqs)
    for section in (
        "Executive Summary", "Team & Org", "Outcomes", "Operational Health", "Quality",
        "Backlog & Support", "Engineering Output", "AI Tooling", "Productivity", "Appendix",
    ):
        assert section in body


def test_eng_divider_slide_renders_jira_section_on_navy_bg():
    from src.slide_engineering_portfolio import eng_divider_slide
    from src.slides_theme import NAVY, WHITE

    reqs: list = []
    report = {"_current_slide": {"title": "Team & Org"}}
    eng_divider_slide(reqs, "div_j", report, 0)
    bg = next(
        r["updatePageProperties"]["pageProperties"]["pageBackgroundFill"]["solidFill"]["color"]["rgbColor"]
        for r in reqs
        if isinstance(r, dict) and "updatePageProperties" in r
    )
    assert bg == NAVY
    title_style = next(
        r["updateTextStyle"]["style"]["foregroundColor"]["opaqueColor"]["rgbColor"]
        for r in reqs
        if isinstance(r, dict)
        and r.get("updateTextStyle", {}).get("objectId") == "div_j_sec"
    )
    assert title_style == WHITE


def test_work_split_titles_reactive_dominance() -> None:
    reqs: list = []
    eng_work_split_slide(reqs, "sid_ws", _report(), 0)
    assert _title(reqs, "sid_ws") == "Planned vs. Unplanned"
    assert "Reactive work dominating" in _subtitle(reqs, "sid_ws")
    assert "50%" in _subtitle(reqs, "sid_ws")
    assert _has_obj(reqs, "sid_ws_kpi3_v")
