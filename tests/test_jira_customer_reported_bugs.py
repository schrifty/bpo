"""Tests for open customer-reported bugs (metric 2035) and EOM stock."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from src.jira_customer_reported_bugs import (
    CUSTOMER_REPORTED_BUGS_JQL,
    customer_reported_bugs_jql,
    get_customer_reported_bug_count,
    get_customer_reported_bugs_eom,
)
from src.kpi_observation import KPIObservation
from src.kpi_store import (
    GRAIN_DAILY,
    OPEN_CUSTOMER_REPORTED_BUGS_METRIC,
    connect,
    stored_kpi_from_observation,
    upsert_kpi,
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


def test_customer_reported_bugs_jql_uses_was_on_for_past_dates() -> None:
    jql = customer_reported_bugs_jql(as_of=date(2026, 8, 31))
    assert "status WAS IN" in jql
    assert 'ON "2026-08-31"' in jql
    assert customer_reported_bugs_jql(as_of=date.today()) == CUSTOMER_REPORTED_BUGS_JQL


def test_get_customer_reported_bug_count_historical_uses_was_on() -> None:
    client = _FakeClient(9)
    result = get_customer_reported_bug_count(client, as_of=date(2026, 8, 31))
    assert result["value"] == 9
    assert result["as_of"] == "2026-08-31"
    assert "WAS IN" in result["jql"]
    assert client.last_jql == result["jql"]


def test_get_customer_reported_bug_count_fails_loud_when_count_missing() -> None:
    result = get_customer_reported_bug_count(_FakeClient(None))
    assert "error" in result
    assert "value" not in result


def test_get_customer_reported_bugs_eom_uses_daily_open_count(
    tmp_path: Path,
) -> None:
    db = tmp_path / "kpi.sqlite"
    conn = connect(db)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name=OPEN_CUSTOMER_REPORTED_BUGS_METRIC,
            grain=GRAIN_DAILY,
            period_key="2026-08-31",
            observation=KPIObservation(
                value=14, numerator=14, denominator=1, as_of="2026-08-31", origin="live"
            ),
            generator="get_customer_reported_bugs",
        ),
    )
    conn.close()
    result = get_customer_reported_bugs_eom(
        as_of=date(2026, 9, 10), db_path=db, skip_s3=True
    )
    assert result["value"] == 14
    assert result["as_of"] == "2026-08-31"
    assert result["source_metric"] == OPEN_CUSTOMER_REPORTED_BUGS_METRIC


def test_get_customer_reported_bugs_eom_fails_loud_without_eom_snapshot(
    tmp_path: Path,
) -> None:
    db = tmp_path / "kpi.sqlite"
    connect(db).close()
    result = get_customer_reported_bugs_eom(
        as_of=date(2026, 9, 10), db_path=db, skip_s3=True
    )
    assert "error" in result
    assert "2026-08-31" in result["error"]
    assert "value" not in result


def test_get_customer_reported_bugs_eom_fails_loud_when_daily_row_errored(
    tmp_path: Path,
) -> None:
    db = tmp_path / "kpi.sqlite"
    conn = connect(db)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name=OPEN_CUSTOMER_REPORTED_BUGS_METRIC,
            grain=GRAIN_DAILY,
            period_key="2026-08-31",
            observation=KPIObservation(error="Jira down", origin="live", as_of="2026-08-31"),
            generator="get_customer_reported_bugs",
        ),
    )
    conn.close()
    result = get_customer_reported_bugs_eom(
        as_of=date(2026, 9, 10), db_path=db, skip_s3=True
    )
    assert "error" in result
    assert "Jira down" in result["error"]
    assert "value" not in result
