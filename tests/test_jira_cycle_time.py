"""Unit tests for Jira cycle-time computation (no HTTP)."""

from __future__ import annotations

from datetime import datetime, timezone

from src.jira_cycle_time import (
    DEFAULT_EXCLUDED_ISSUE_TYPES,
    _issue_excluded,
    active_cycle_days_from_changelog,
    build_done_issues_jql,
    drop_upper_outliers,
    history_window_days,
    summarize_cycle_times,
    trailing_month_periods,
)


def _dt(y: int, m: int, d: int, h: int = 0) -> datetime:
    return datetime(y, m, d, h, tzinfo=timezone.utc)


def test_active_cycle_days_simple_path() -> None:
    status_map = {
        "open": "new",
        "in progress": "indeterminate",
        "done": "done",
    }
    histories = [
        {
            "created": "2026-05-01T10:00:00.000+0000",
            "items": [{"field": "status", "fromString": "Open", "toString": "In Progress"}],
        },
        {
            "created": "2026-05-04T10:00:00.000+0000",
            "items": [{"field": "status", "fromString": "In Progress", "toString": "Done"}],
        },
    ]
    days = active_cycle_days_from_changelog(
        histories,
        status_map=status_map,
        created=_dt(2026, 5, 1, 9),
        initial_status="Open",
    )
    assert days is not None
    assert 2.9 < days < 3.1


def test_active_cycle_days_none_without_in_progress() -> None:
    status_map = {"open": "new", "done": "done"}
    histories = [
        {
            "created": "2026-05-02T10:00:00.000+0000",
            "items": [{"field": "status", "fromString": "Open", "toString": "Done"}],
        },
    ]
    assert (
        active_cycle_days_from_changelog(
            histories,
            status_map=status_map,
            created=_dt(2026, 5, 1),
            initial_status="Open",
        )
        is None
    )


def test_summarize_cycle_times_empty() -> None:
    stats = summarize_cycle_times([])
    assert stats["median_days"] is None


def test_summarize_cycle_times_median() -> None:
    stats = summarize_cycle_times([1.0, 2.0, 10.0])
    assert stats["median_days"] == 2.0
    assert stats["mean_days"] == round(13.0 / 3, 2)


def test_trailing_month_periods_count() -> None:
    periods = trailing_month_periods(6, end=__import__("datetime").date(2026, 5, 16))
    assert len(periods) == 6
    assert periods[-1] == "2026-05"
    assert periods[0] == "2025-12"


def test_history_window_days_covers_six_months() -> None:
    assert history_window_days(6) >= 150


def test_build_done_issues_jql_excludes_types() -> None:
    jql = build_done_issues_jql(30, ("Epic", "Sub-task", "SUT"))
    assert "issuetype not in" in jql
    assert "Epic" in jql
    assert '"Sub-task"' in jql


def test_subtask_and_sut_not_excluded_by_default() -> None:
    assert not _issue_excluded(
        {"issuetype": {"name": "Bug", "subtask": True}}, DEFAULT_EXCLUDED_ISSUE_TYPES
    )
    assert not _issue_excluded({"issuetype": {"name": "SUT"}}, DEFAULT_EXCLUDED_ISSUE_TYPES)
    assert _issue_excluded({"issuetype": {"name": "Epic"}}, DEFAULT_EXCLUDED_ISSUE_TYPES)


def test_drop_upper_outliers_removes_extreme_tail() -> None:
    measured = [(f"T-{i}", 1.0) for i in range(20)]
    measured.append(("T-outlier", 500.0))
    kept, dropped, cutoff = drop_upper_outliers(measured, sigma=4.0)
    assert len(dropped) == 1
    assert dropped[0]["key"] == "T-outlier"
    assert cutoff is not None
    assert len(kept) == 20
