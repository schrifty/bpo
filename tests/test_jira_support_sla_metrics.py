"""Tests for HELP median TTFR and SLA adherence metric generators."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.jira_median_ttfr import get_median_ttfr
from src.jira_sla_adherence import get_sla_adherence


def _issue(
    key: str,
    *,
    ttfr_ms: int | None = None,
    ttfr_breached: bool = False,
    ttr_ms: int | None = None,
    ttr_breached: bool = False,
) -> dict:
    def _sla(ms: int | None, breached: bool) -> dict:
        if ms is None:
            return {"completedCycles": [], "ongoingCycle": None}
        return {
            "completedCycles": [
                {
                    "elapsedTime": {"millis": ms},
                    "breached": breached,
                }
            ]
        }

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
            "customfield_10665": _sla(ttr_ms, ttr_breached),
            "customfield_10666": _sla(ttfr_ms, ttfr_breached),
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


def test_get_help_median_ttfr_hours(jira_client) -> None:
    # 12h, 24h, 36h → median 24h
    raw = [
        _issue("HELP-1", ttfr_ms=12 * 3600 * 1000),
        _issue("HELP-2", ttfr_ms=24 * 3600 * 1000),
        _issue("HELP-3", ttfr_ms=36 * 3600 * 1000),
        _issue("HELP-4", ttfr_ms=None),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=4), patch.object(
        jira_client, "_search", return_value=raw
    ) as mock_search:
        out = jira_client.get_help_median_ttfr(days=30)

    assert out["value"] == 24
    assert out["metric"] == "median_ttfr_hours"
    assert out["window_days"] == 30
    assert out["ttfr"]["measured"] == 3
    assert out["median_hours"] == 24.0
    assert "resolution is not EMPTY" in mock_search.call_args.args[0]
    assert "resolved >= -30d" in mock_search.call_args.args[0]


def test_get_help_median_ttfr_fails_when_no_completed_cycles(jira_client) -> None:
    with patch.object(jira_client, "_jql_match_total", return_value=1), patch.object(
        jira_client, "_search", return_value=[_issue("HELP-1", ttfr_ms=None)]
    ):
        out = jira_client.get_help_median_ttfr(days=30)

    assert "error" in out
    assert "value" not in out


def test_get_median_ttfr_wrapper_returns_value(jira_client) -> None:
    raw = [
        _issue("HELP-1", ttfr_ms=10 * 3600 * 1000),
        _issue("HELP-2", ttfr_ms=14 * 3600 * 1000),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=2), patch.object(
        jira_client, "_search", return_value=raw
    ):
        out = get_median_ttfr(jira_client, days=30)

    assert out["value"] == 12
    assert out["window_days"] == 30
    assert out["measured"] == 2
    assert "error" not in out


def test_get_help_sla_adherence_pct(jira_client) -> None:
    raw = [
        _issue("HELP-1", ttfr_ms=1000, ttr_ms=2000),
        _issue("HELP-2", ttfr_ms=1000, ttfr_breached=True, ttr_ms=2000),
        _issue("HELP-3", ttfr_ms=None, ttr_ms=None),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=3), patch.object(
        jira_client, "_search", return_value=raw
    ):
        out = jira_client.get_help_sla_adherence(days=30)

    assert out["value"] == 50.0
    assert out["metric"] == "sla_adherence_pct"
    assert out["sla_adherence"]["measured"] == 2
    assert out["sla_adherence"]["met"] == 1
    assert out["numerator"] == 1.0
    assert out["denominator"] == 2.0


def test_get_sla_adherence_wrapper_returns_value(jira_client) -> None:
    raw = [
        _issue("HELP-1", ttfr_ms=1000, ttr_ms=2000),
        _issue("HELP-2", ttfr_ms=1000, ttr_ms=2000, ttr_breached=True),
        _issue("HELP-3", ttfr_ms=1000, ttr_ms=2000),
    ]
    with patch.object(jira_client, "_jql_match_total", return_value=3), patch.object(
        jira_client, "_search", return_value=raw
    ):
        out = get_sla_adherence(jira_client, days=30)

    assert out["value"] == pytest.approx(66.7)
    assert out["met"] == 2
    assert out["measured"] == 3
    assert out["breached"] == 1
    assert out["window_days"] == 30
    assert "error" not in out


def test_get_sla_adherence_fails_when_no_measured(jira_client) -> None:
    with patch.object(jira_client, "_jql_match_total", return_value=1), patch.object(
        jira_client, "_search", return_value=[_issue("HELP-1")]
    ):
        out = get_sla_adherence(jira_client, days=30)

    assert "error" in out
    assert "value" not in out
