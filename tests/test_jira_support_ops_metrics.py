"""Tests for operational HELP support KPIs and P90 TTR."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.jira_p90_ttr import get_p90_ttr
from src.jira_support_ops_metrics import (
    get_help_backlog_over_30d_pct,
    get_help_escalation_rate,
    get_help_resolved_created_ratio,
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


def test_get_help_resolved_created_ratio() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [100, 85]
    out = get_help_resolved_created_ratio(client, days=30)
    assert out["created"] == 100
    assert out["resolved"] == 85
    assert out["value"] == 85.0
    assert out["numerator"] == 85.0
    assert out["denominator"] == 100.0
    assert "created >= -30d" in out["created_jql"]
    assert "resolved >= -30d" in out["resolved_jql"]


def test_get_help_resolved_created_ratio_zero_created() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [0, 5]
    out = get_help_resolved_created_ratio(client, days=30)
    assert "error" in out


def test_get_help_backlog_over_30d_pct() -> None:
    client = MagicMock()
    client.jql_match_count.side_effect = [200, 40]
    out = get_help_backlog_over_30d_pct(client, days=30)
    assert out["open_total"] == 200
    assert out["over_age"] == 40
    assert out["value"] == 20.0
    assert "statusCategory != Done" in out["open_jql"]
    assert "created <= -30d" in out["over_jql"]


def test_get_help_escalation_rate() -> None:
    client = MagicMock()
    # help created, lean, customer
    client.jql_match_count.side_effect = [200, 10, 5]
    out = get_help_escalation_rate(client, days=30)
    assert out["help_created"] == 200
    assert out["lean_escalated"] == 10
    assert out["customer_escalated"] == 5
    assert out["escalated"] == 15
    assert out["value"] == 7.5
    assert 'labels = "jira_escalated"' in out["lean_jql"]
    assert "project = CUSTOMER" in out["customer_jql"]


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
    assert out["value"] == 100  # p90 index on 5 values → last sample (100h)
