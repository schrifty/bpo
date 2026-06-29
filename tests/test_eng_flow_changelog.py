"""Tests for changelog-derived time-in-status flow signals (Tier 2)."""

from __future__ import annotations

from datetime import datetime, timezone

from src.jira_client import compute_eng_flow, compute_status_timeline, summarize_status_flow

_NOW = datetime(2026, 6, 10, 0, 0, 0, tzinfo=timezone.utc)


def _hist(when: str, frm: str, to: str) -> dict:
    return {"created": when, "items": [{"field": "status", "fromString": frm, "toString": to}]}


def test_timeline_computes_time_in_status_and_current_age() -> None:
    histories = [
        _hist("2026-06-01T00:00:00.000+0000", "Open", "In Progress"),
        _hist("2026-06-05T00:00:00.000+0000", "In Progress", "In Review"),
    ]
    tl = compute_status_timeline(
        histories,
        created="2026-05-30T00:00:00.000+0000",
        current_status="In Review",
        now=_NOW,
    )
    # Open: created→first transition = 2 days; In Progress: 4 days; In Review: 5 days to now.
    assert tl["time_in_status"]["Open"] == 2.0
    assert tl["time_in_status"]["In Progress"] == 4.0
    assert tl["time_in_status"]["In Review"] == 5.0
    # Days in current status = since the last status change (Jun 5 → Jun 10).
    assert tl["days_in_current_status"] == 5.0
    assert tl["transitions"] == 2


def test_timeline_no_transitions_uses_created() -> None:
    tl = compute_status_timeline(
        [], created="2026-06-03T00:00:00.000+0000", current_status="In Progress", now=_NOW
    )
    assert tl["days_in_current_status"] == 7.0
    assert tl["transitions"] == 0


def test_timeline_counts_reopens() -> None:
    histories = [
        _hist("2026-06-02T00:00:00.000+0000", "In Review", "Closed"),
        _hist("2026-06-04T00:00:00.000+0000", "Closed", "Reopened"),
    ]
    tl = compute_status_timeline(
        histories, created="2026-06-01T00:00:00.000+0000", current_status="Reopened", now=_NOW
    )
    assert tl["reopened"] >= 1


def test_summarize_flags_blocked_and_per_status_median() -> None:
    items = [
        {
            "key": "L-1", "status": "In Review", "created": "2026-06-01T00:00:00.000+0000",
            "flagged": True,
            "changelog": [_hist("2026-06-04T00:00:00.000+0000", "In Progress", "In Review")],
        },
        {
            "key": "L-2", "status": "In Review", "created": "2026-06-01T00:00:00.000+0000",
            "flagged": False,
            "changelog": [_hist("2026-06-08T00:00:00.000+0000", "In Progress", "In Review")],
        },
        {
            "key": "L-3", "status": "In Progress", "created": "2026-06-07T00:00:00.000+0000",
            "flagged": False,
            "changelog": [],
        },
    ]
    summary = summarize_status_flow(items, now=_NOW)
    assert summary["source"] == "changelog"
    assert summary["blocked_count"] == 1
    assert summary["enriched_count"] == 3
    # In Review median of [6, 2] days = 4; In Progress = 3 days.
    assert summary["by_status_median_days"]["In Review"] == 4.0
    assert summary["by_status_median_days"]["In Progress"] == 3.0
    # Items mutated in place with precise current-status age.
    assert items[0]["days_in_status"] == 6.0
    assert items[1]["flagged"] is False


def test_stage_age_overrides_fresh_update_proxy() -> None:
    # Item was edited yesterday (fresh ``updated``) but has been In Review for 66 days.
    # The proxy alone would call it healthy; changelog stage age must win.
    from datetime import date

    today = date(2026, 6, 10)
    in_flight = [
        {"key": "L-STUCK", "summary": "Stuck in review", "status": "In Review",
         "type": "Story", "labels": [], "created": "2026-04-01", "updated": "2026-06-09",
         "sprint_count": 1, "story_points": 5},
    ]
    # Proxy-only: idle = 1 day → not stalled.
    proxy = compute_eng_flow(in_flight, [], today=today)
    assert proxy["stale_gt10"] == 0
    # With changelog stage age = 66 days → counted as stalled and surfaced.
    enriched = compute_eng_flow(
        in_flight, [], today=today,
        stage_age_by_key={"L-STUCK": 66.0},
        flagged_keys=set(),
    )
    assert enriched["stale_gt10"] == 1
    assert enriched["attention_items"][0]["key"] == "L-STUCK"
    assert enriched["attention_items"][0]["days_in_status"] == 66.0


def test_flagged_drives_blocked_count_and_ranks_first() -> None:
    from datetime import date

    today = date(2026, 6, 10)
    in_flight = [
        {"key": "L-OLD", "summary": "old", "status": "In Progress", "type": "Story",
         "labels": [], "created": "2026-01-01", "updated": "2026-05-01", "sprint_count": 1},
        {"key": "L-FLAG", "summary": "blocked", "status": "In Progress", "type": "Story",
         "labels": [], "created": "2026-06-08", "updated": "2026-06-08", "sprint_count": 1},
    ]
    flow = compute_eng_flow(in_flight, [], today=today, flagged_keys={"L-FLAG"})
    assert flow["blocked_count"] == 1
    # Flagged item ranks ahead of the merely-old one.
    assert flow["attention_items"][0]["key"] == "L-FLAG"
