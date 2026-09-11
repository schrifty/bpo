"""Tests for Customer-Reported Bugs (LEAN bugs created previous calendar month)."""

from __future__ import annotations

from datetime import date

from src.jira_customer_reported_bugs import (
    customer_reported_bugs_created_jql,
    get_customer_reported_bugs_created,
)


class _FakeClient:
    def __init__(self, count: int | None):
        self._count = count
        self.last_jql: str | None = None

    def jql_match_count(self, jql: str, *, data_description: str | None = None) -> int | None:
        self.last_jql = jql
        return self._count


def test_customer_reported_bugs_created_jql_is_previous_calendar_month() -> None:
    jql = customer_reported_bugs_created_jql(as_of=date(2026, 9, 10))
    assert "project = LEAN" in jql
    assert "type = Bug" in jql
    assert 'createdDate >= "2026-08-01"' in jql
    assert 'createdDate < "2026-09-01"' in jql
    assert 'labels = "jira_escalated"' in jql


def test_get_customer_reported_bugs_created_returns_inflow() -> None:
    client = _FakeClient(41)
    result = get_customer_reported_bugs_created(client, as_of=date(2026, 9, 10))
    assert result["value"] == 41
    assert result["month"] == "2026-08"
    assert result["method"] == "actual_previous_month"
    assert result["created_start"] == "2026-08-01"
    assert result["created_end_exclusive"] == "2026-09-01"
    assert client.last_jql == result["jql"]
    assert 'createdDate >= "2026-08-01"' in result["jql"]
    assert 'labels = "jira_escalated"' in result["jql"]


def test_get_customer_reported_bugs_created_fails_loud_when_count_missing() -> None:
    result = get_customer_reported_bugs_created(_FakeClient(None), as_of=date(2026, 9, 10))
    assert "error" in result
    assert "value" not in result
    assert "2026-08" in result["error"]
