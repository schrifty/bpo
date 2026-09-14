"""Tests for Customer-Reported Bugs (LEAN bugs created previous calendar month)."""

from __future__ import annotations

from datetime import date

from src.jira_customer_reported_bugs import (
    build_customer_reported_bug_pressure,
    customer_reported_bugs_created_jql,
    customer_reported_bugs_window_jql,
    get_customer_reported_bugs_created,
)


class _FakeClient:
    def __init__(self, count: int | None):
        self._count = count
        self.last_jql: str | None = None

    def jql_match_count(self, jql: str, *, data_description: str | None = None) -> int | None:
        self.last_jql = jql
        return self._count


class _FakeSearchClient:
    def __init__(self, issues: list[dict], error: Exception | None = None):
        self._issues = issues
        self._error = error
        self.last_jql: str | None = None

    def _search(self, jql: str, **kwargs: object) -> list[dict]:
        self.last_jql = jql
        if self._error:
            raise self._error
        return self._issues


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


def _reported_bug(priority: str | None, *, resolved: bool = False) -> dict:
    return {
        "fields": {
            "priority": {"name": priority} if priority else None,
            "resolution": {"name": "Done"} if resolved else None,
        }
    }


def test_window_jql_scopes_lean_escalated_bugs() -> None:
    jql = customer_reported_bugs_window_jql(days=45)
    assert "project = LEAN" in jql
    assert "issuetype = Bug" in jql
    assert 'labels = "jira_escalated"' in jql
    assert "created >= -45d" in jql
    assert "HELP" not in jql


def test_build_pressure_counts_open_and_priority_mix() -> None:
    client = _FakeSearchClient([
        _reported_bug("Blocker: production down"),
        _reported_bug("Critical"),
        _reported_bug("Major", resolved=True),
        _reported_bug("Major", resolved=True),
        _reported_bug(None),
    ])
    out = build_customer_reported_bug_pressure(client, days=30)
    assert out["error"] is None
    assert out["total"] == 5
    assert out["open"] == 3
    assert out["resolved"] == 2
    assert out["open_blocker_critical"] == 2
    assert out["by_priority"] == {"Major": 2, "Blocker": 1, "Critical": 1, "Unknown": 1}
    # Priority links keep the full Jira priority name, and Unknown queries empty priority.
    assert 'priority = "Blocker: production down"' in out["jql_by_priority_short"]["Blocker"]
    assert "priority is EMPTY" in out["jql_by_priority_short"]["Unknown"]
    assert out["label"] == "jira_escalated"
    assert out["days"] == 30


def test_build_pressure_degrades_on_search_failure() -> None:
    out = build_customer_reported_bug_pressure(
        _FakeSearchClient([], error=RuntimeError("jira 503")), days=30
    )
    assert out["total"] == 0
    assert "jira 503" in out["error"]


def test_customer_reported_bugs_target_is_provisional_fifteen() -> None:
    from src.metrics_registry import load_metrics_registry, registry_metric_target

    entry = load_metrics_registry()["metrics"]["Customer-Reported Bugs"]
    assert registry_metric_target(entry) == 15.0
    assert entry["direction"] == "lower"
    guidance = entry["mgmt_guidance"]
    assert "15" in guidance
    assert "asterisk" in guidance.lower() or "enhancement" in guidance.lower()
