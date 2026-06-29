"""Tests for HELP resolved-window TTR SLA adherence % (LeanDNA metric 1911-style)."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def _issue(key: str, ttr_ms: int | None, *, breached: bool = False) -> dict:
    sla = {
        "completedCycles": [
            {
                "elapsedTime": {"millis": ttr_ms},
                "breached": breached,
            }
        ]
    } if ttr_ms is not None else {"completedCycles": [], "ongoingCycle": None}
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


def test_compute_ttr_sla_adherence_pct() -> None:
    from src.jira_client import JiraClient

    issues = [
        {"project": "HELP", "ttr_ms": 1, "ttr_breached": False},
        {"project": "HELP", "ttr_ms": 2, "ttr_breached": True},
        {"project": "HELP", "ttr_ms": None, "ttr_breached": False},
        {"project": "HELP", "ttr_ms": 3, "ttr_breached": False, "ttr_waiting": True},
    ]
    out = JiraClient._compute_ttr_sla_adherence_pct(issues)
    assert out["measured"] == 3
    assert out["met"] == 2
    assert out["breached"] == 1
    assert out["pct"] == pytest.approx(66.7, abs=0.1)
    assert out["waiting"] == 1


def test_get_help_time_to_resolution_30d_adherence(jira_client) -> None:
    raw = [
        _issue("HELP-1", 4 * 3600 * 1000, breached=False),
        _issue("HELP-2", 8 * 3600 * 1000, breached=True),
        _issue("HELP-3", None),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=3), patch.object(
        jira_client, "_search", return_value=raw
    ) as mock_search:
        out = jira_client.get_help_time_to_resolution(days=30)

    assert out["project"] == "HELP"
    assert out["metric"] == "ttr_sla_adherence_pct"
    assert out["window_days"] == 30
    assert out["resolved_in_window"] == 3
    adh = out["ttr_sla_adherence"]
    assert adh["measured"] == 2
    assert adh["met"] == 1
    assert adh["pct"] == 50.0
    assert "resolution is not EMPTY" in mock_search.call_args.args[0]
    assert "resolved >= -30d" in mock_search.call_args.args[0]


def test_get_help_time_to_resolution_customer_scope(jira_client) -> None:
    with patch.object(
        jira_client,
        "_help_project_customer_filter",
        return_value=('Organizations = "Acme"', ["Acme"]),
    ), patch.object(jira_client, "_jql_match_total", return_value=0), patch.object(
        jira_client, "_search", return_value=[]
    ) as mock_search:
        out = jira_client.get_help_time_to_resolution(
            days=30,
            customer_name="Acme Corp",
            include_tickets=True,
        )

    assert out["customer"] == "Acme Corp"
    assert out["tickets"] == []
    assert 'Organizations = "Acme"' in mock_search.call_args.args[0]


def test_get_help_time_to_resolution_invalid_days(jira_client) -> None:
    out = jira_client.get_help_time_to_resolution(days=0)
    assert "error" in out
