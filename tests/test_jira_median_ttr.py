"""Tests for HELP median TTR (LeanDNA metric 2171) from JSM SLA cycles."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.jira_median_ttr import get_median_ttr


def _issue(key: str, ttr_ms: int | None, *, breached: bool = False) -> dict:
    sla = (
        {
            "completedCycles": [
                {
                    "elapsedTime": {"millis": ttr_ms},
                    "breached": breached,
                }
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


def test_get_help_median_ttr_hours(jira_client) -> None:
    # 24h, 48h, 72h → median 48h
    raw = [
        _issue("HELP-1", 24 * 3600 * 1000),
        _issue("HELP-2", 48 * 3600 * 1000),
        _issue("HELP-3", 72 * 3600 * 1000),
        _issue("HELP-4", None),  # no completed cycle — excluded from median
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=4), patch.object(
        jira_client, "_search", return_value=raw
    ) as mock_search:
        out = jira_client.get_help_median_ttr(days=30)

    assert out["value"] == 48
    assert out["metric"] == "median_ttr_hours"
    assert out["window_days"] == 30
    assert out["ttr"]["measured"] == 3
    assert out["median_hours"] == 48.0
    assert "resolution is not EMPTY" in mock_search.call_args.args[0]
    assert "resolved >= -30d" in mock_search.call_args.args[0]


def test_get_help_median_ttr_fails_when_no_completed_cycles(jira_client) -> None:
    with patch.object(jira_client, "_jql_match_total", return_value=1), patch.object(
        jira_client, "_search", return_value=[_issue("HELP-1", None)]
    ):
        out = jira_client.get_help_median_ttr(days=30)

    assert "error" in out
    assert "value" not in out


def test_get_help_median_ttr_invalid_days(jira_client) -> None:
    out = jira_client.get_help_median_ttr(days=0)
    assert "error" in out


def test_get_median_ttr_wrapper_returns_value(jira_client) -> None:
    raw = [
        _issue("HELP-1", 40 * 3600 * 1000),
        _issue("HELP-2", 44 * 3600 * 1000),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=2), patch.object(
        jira_client, "_search", return_value=raw
    ):
        out = get_median_ttr(jira_client, days=30)

    assert out["value"] == 42
    assert out["window_days"] == 30
    assert out["measured"] == 2
    assert "error" not in out


def test_get_median_ttr_wrapper_propagates_error(jira_client) -> None:
    with patch.object(jira_client, "_jql_match_total", return_value=0), patch.object(
        jira_client, "_search", return_value=[]
    ):
        out = get_median_ttr(jira_client, days=30)

    assert "error" in out
    assert "value" not in out
