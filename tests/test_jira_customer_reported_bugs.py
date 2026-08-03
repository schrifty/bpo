"""Tests for Customer-Reported Bugs (metric 2035) generator."""

from __future__ import annotations

from src.jira_customer_reported_bugs import (
    CUSTOMER_REPORTED_BUGS_JQL,
    get_customer_reported_bug_count,
)


class _FakeClient:
    def __init__(self, count: int | None):
        self._count = count
        self.last_jql: str | None = None

    def jql_match_count(self, jql: str, *, data_description: str | None = None) -> int | None:
        self.last_jql = jql
        return self._count


def test_jql_matches_eng_portfolio_open_bug_scope() -> None:
    assert "project = LEAN" in CUSTOMER_REPORTED_BUGS_JQL
    assert "issuetype = Bug" in CUSTOMER_REPORTED_BUGS_JQL
    assert '"In Progress"' in CUSTOMER_REPORTED_BUGS_JQL
    assert '"Open"' in CUSTOMER_REPORTED_BUGS_JQL


def test_get_customer_reported_bug_count_returns_value() -> None:
    client = _FakeClient(12)
    result = get_customer_reported_bug_count(client)
    assert result["value"] == 12
    assert result["jql"] == CUSTOMER_REPORTED_BUGS_JQL
    assert client.last_jql == CUSTOMER_REPORTED_BUGS_JQL


def test_get_customer_reported_bug_count_fails_loud_when_count_missing() -> None:
    result = get_customer_reported_bug_count(_FakeClient(None))
    assert "error" in result
    assert "value" not in result
