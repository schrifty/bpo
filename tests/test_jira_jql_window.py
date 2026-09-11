"""Tests for Jira trailing-window JQL helpers."""

from __future__ import annotations

from datetime import date

from src.jira_jql_window import jql_trailing_clause, trailing_window_bounds


def test_relative_clause_without_as_of() -> None:
    assert jql_trailing_clause("created", 30) == "created >= -30d"
    assert jql_trailing_clause("resolved", 7) == "resolved >= -7d"


def test_calendar_clause_with_as_of() -> None:
    clause = jql_trailing_clause("resolved", 30, date(2026, 8, 31))
    assert 'resolved >= "2026-08-02"' in clause
    assert 'resolved < "2026-09-01"' in clause
    start, end = trailing_window_bounds(30, date(2026, 8, 31))
    assert start.date().isoformat() == "2026-08-02"
    assert end.date().isoformat() == "2026-09-01"
