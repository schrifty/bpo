"""Tests for operational HELP support KPIs and P90 TTR."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.jira_p90_ttr import get_p90_ttr
from src.jira_support_ops_metrics import (
    get_data_escalation_count,
    get_data_escalation_rate,
    get_engineering_escalation_count,
    get_engineering_escalation_rate,
    get_help_resolved_created_ratio,
    get_help_ticket_count,
    get_open_help,
    get_open_help_over_30d_pct,
    help_ticket_count_jql,
    open_help_jql,
)


def _issue(key: str, ttr_ms: int | None) -> dict:
    sla = (
        {
            "completedCycles": [
                {"elapsedTime": {"millis": ttr_ms}, "breached": False}
            ]
        }
        if ttr_ms is not None
        else {"completedCycles": [], "ongoingCycle": None}
    )
    return {
        "key": key,
        "fields": {
            "summary": f"Ticket {key}",
            "status": {"name": "Closed"},
            "issuetype": {"name": "Help"},
            "project": {"key": "HELP"},
            "priority": {"name": "Major"},
            "created": "2026-04-01T10:00:00.000+0000",
            "updated": "2026-04-10T10:00:00.000+0000",
            "resolution": {"name": "Done"},
            "resolutiondate": "2026-04-10T10:00:00.000+0000",
            "labels": [],
            "customfield_10502": [],
            "customfield_10665": sla,
            "customfield_10666": {"completedCycles": []},
        },
    }


@pytest.fixture
def jira_client(monkeypatch):
    monkeypatch.setenv("JIRA_AUTH_MODE", "site")
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "u@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    from src.jira_client import JiraClient, reset_shared_jira_client

    reset_shared_jira_client()
    return JiraClient()


def test_get_help_resolved_created_ratio_previous_calendar_month() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [100, 85]
    out = get_help_resolved_created_ratio(client, as_of=date(2026, 9, 10))
    assert out["created"] == 100
    assert out["resolved"] == 85
    assert out["value"] == 85.0
    assert out["month"] == "2026-08"
    assert 'createdDate >= "2026-08-01"' in out["created_jql"]
    assert 'createdDate < "2026-09-01"' in out["created_jql"]
    assert 'resolved >= "2026-08-01"' in out["resolved_jql"]
    assert 'resolved < "2026-09-01"' in out["resolved_jql"]


def test_get_help_resolved_created_ratio_zero_created() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [0, 5]
    out = get_help_resolved_created_ratio(client, as_of=date(2026, 9, 10))
    assert "error" in out


def test_help_ticket_count_jql_is_previous_calendar_month() -> None:
    jql = help_ticket_count_jql(as_of=date(2026, 9, 10))
    assert "project = HELP" in jql
    assert "Outage" in jql
    assert 'createdDate >= "2026-08-01"' in jql
    assert 'createdDate < "2026-09-01"' in jql


def test_get_help_ticket_count() -> None:
    client = MagicMock()
    client.jql_match_count.return_value = 142
    out = get_help_ticket_count(client, as_of=date(2026, 9, 10))
    assert out["value"] == 142
    assert out["month"] == "2026-08"
    assert out["method"] == "actual_previous_month"
    assert out["created_start"] == "2026-08-01"
    assert out["created_end_exclusive"] == "2026-09-01"
    assert "project = HELP" in out["jql"]
    assert out["business_days"] == 21
    assert out["created_per_business_day"] == round(142 / 21, 2)


def test_get_help_ticket_count_fails_loud_when_count_missing() -> None:
    client = MagicMock()
    client.jql_match_count.return_value = None
    out = get_help_ticket_count(client, as_of=date(2026, 9, 10))
    assert "error" in out
    assert "value" not in out


def test_ticket_count_kpi_is_registered_without_target() -> None:
    from src.metrics_registry import load_metrics_registry
    from src.metrics_upsert import _GENERATORS

    entry = load_metrics_registry()["metrics"]["Ticket Count"]
    assert entry["metric-generator"] == "get_help_ticket_count"
    assert entry.get("target") in (None, "")
    assert "direction" not in entry or entry.get("direction") in (None, "")
    assert "get_help_ticket_count" in _GENERATORS


def test_get_engineering_escalation_rate() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [200, 10]
    out = get_engineering_escalation_rate(client, as_of=date(2026, 9, 10))
    assert out["help_created"] == 200
    assert out["escalated"] == 10
    assert out["project"] == "LEAN"
    assert out["value"] == 5.0
    assert out["month"] == "2026-08"
    assert 'createdDate >= "2026-08-01"' in out["help_created_jql"]
    assert 'createdDate < "2026-09-01"' in out["escalated_jql"]
    assert "project = CUSTOMER" not in out["escalated_jql"]


def test_get_engineering_escalation_count() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [200, 10]
    out = get_engineering_escalation_count(client, as_of=date(2026, 9, 10))
    assert out["value"] == 10
    assert out["help_created"] == 200
    assert out["month"] == "2026-08"


def test_get_data_escalation_rate() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [200, 5]
    out = get_data_escalation_rate(client, as_of=date(2026, 9, 10))
    assert out["value"] == 2.5
    assert out["project"] == "CUSTOMER"
    assert "project = LEAN" not in out["escalated_jql"]


def test_get_data_escalation_count() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [200, 5]
    out = get_data_escalation_count(client, as_of=date(2026, 9, 10))
    assert out["value"] == 5


def test_get_engineering_escalation_rate_zero_help() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [0, 3]
    out = get_engineering_escalation_rate(client, as_of=date(2026, 9, 10))
    assert "error" in out
    assert "Engineering Escalation Rate" in out["error"]


def test_split_escalation_kpis_are_registered() -> None:
    from src.metrics_registry import load_metrics_registry
    from src.metrics_upsert import _GENERATORS

    metrics = load_metrics_registry()["metrics"]
    assert "Engineering Escalation Rate (30 Days)" not in metrics
    assert "HELP Resolved / Created (30 Days)" not in metrics
    eng = metrics["Engineering Escalation Rate"]
    data = metrics["Data Escalation Rate"]
    assert metrics["Engineering Escalations"]["metric-generator"] == "get_engineering_escalation_count"
    assert metrics["Data Escalations"]["metric-generator"] == "get_data_escalation_count"
    assert eng["metric-generator"] == "get_engineering_escalation_rate"
    assert eng.get("target") in (None, "")
    assert data["metric-generator"] == "get_data_escalation_rate"
    assert "get_engineering_escalation_count" in _GENERATORS
    assert "get_open_help" in _GENERATORS
    assert "get_open_help_over_30d_pct" in _GENERATORS


def test_open_help_jql_is_as_of_snapshot() -> None:
    jql = open_help_jql(as_of=date(2026, 8, 31))
    assert 'createdDate < "2026-09-01"' in jql
    assert 'resolved >= "2026-09-01"' in jql
    aged = open_help_jql(as_of=date(2026, 8, 31), min_age_days=30)
    assert 'createdDate < "2026-08-01"' in aged


def test_get_open_help() -> None:
    client = MagicMock()
    client.jql_match_count.return_value = 40
    out = get_open_help(client, as_of=date(2026, 8, 31))
    assert out["value"] == 40
    assert out["as_of"] == "2026-08-31"


def test_get_open_help_over_30d_pct() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [40, 10]
    out = get_open_help_over_30d_pct(client, as_of=date(2026, 8, 31))
    assert out["value"] == 25.0
    assert out["numerator"] == 10.0
    assert out["denominator"] == 40.0


def test_get_help_p90_ttr_hours(jira_client) -> None:
    # 10 evenly spaced hours → P90 near the top of the range
    raw = [_issue(f"HELP-{i}", i * 10 * 3600 * 1000) for i in range(1, 11)]
    with patch.object(jira_client, "_jql_match_total", return_value=10), patch.object(
        jira_client, "_search", return_value=raw
    ) as mock_search:
        out = jira_client.get_help_p90_ttr(days=30)

    assert out["measured"] == 10
    assert out["metric"] == "p90_ttr_hours"
    assert out["value"] == 90  # 9th of 0..9 index for p90 on 10 items → 90h
    assert "resolved >= -30d" in mock_search.call_args.args[0]


def test_get_help_p90_ttr_as_of_uses_calendar_bounds(jira_client) -> None:
    raw = [_issue(f"HELP-{i}", i * 10 * 3600 * 1000) for i in range(1, 11)]
    with patch.object(jira_client, "_jql_match_total", return_value=10), patch.object(
        jira_client, "_search", return_value=raw
    ) as mock_search:
        jira_client.get_help_p90_ttr(days=30, as_of=date(2026, 8, 31))

    jql = mock_search.call_args.args[0]
    assert 'resolved >= "2026-08-02"' in jql
    assert 'resolved < "2026-09-01"' in jql


def test_get_p90_ttr_wrapper(jira_client) -> None:
    raw = [
        _issue("HELP-1", 20 * 3600 * 1000),
        _issue("HELP-2", 40 * 3600 * 1000),
        _issue("HELP-3", 60 * 3600 * 1000),
        _issue("HELP-4", 80 * 3600 * 1000),
        _issue("HELP-5", 100 * 3600 * 1000),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=5), patch.object(
        jira_client, "_search", return_value=raw
    ):
        out = get_p90_ttr(jira_client, days=30)

    assert "error" not in out
    assert out["window_days"] == 30
    assert out["measured"] == 5
    assert out["value"] == 4.17  # 100h / 24
    assert out["p90_hours"] == 100.0
