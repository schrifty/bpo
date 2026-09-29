"""Support Experience generators: SLA history, ticket volume, and escalation rate."""

from datetime import date
from pathlib import Path

import pytest

from src.healthscore_web.escalation_rate import (
    backfill_escalation_rate,
    escalation_points,
    escalation_value,
    get_escalation_rate,
)
from src.healthscore_web.sla_adherence import backfill_sla_adherence
from src.healthscore_web.store import connect, observations_for_metric, period_key_for
from src.healthscore_web.ticket_volume_trend import (
    backfill_ticket_volume_trend,
    get_ticket_volume_trend,
    ticket_volume_points,
    volume_change_pct,
)

AS_OF = date(2026, 9, 28)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
ENTITIES = [
    {"id": SITE, "name": "Site A"},
    {"id": OTHER, "name": "Other"},
]
ORGS = {SITE: ["Acme"], OTHER: []}


def _issue(
    key: str,
    created: str,
    *,
    org: str = "Acme",
    severity: str = "",
    labels: list[str] | None = None,
) -> dict:
    return {
        "key": key,
        "created": created,
        "organizations": [org],
        "severity": severity,
        "labels": labels or [],
    }


def test_ticket_volume_points_follow_the_three_times_rule() -> None:
    assert ticket_volume_points(30, 10) == 0
    assert ticket_volume_points(29, 10) == 3
    assert ticket_volume_points(0, 0) == 3
    assert ticket_volume_points(4, 0) == 0
    assert volume_change_pct(12, 10) == 20.0
    assert volume_change_pct(0, 0) == 0.0
    assert volume_change_pct(4, 0) is None


def test_ticket_volume_attributes_by_organization_and_leaves_a_miss_unscored() -> None:
    issues = [
        _issue("H-1", "2026-09-01"),
        _issue("H-2", "2026-08-01"),
        _issue("H-3", "2026-05-01"),
        _issue("H-4", "2026-04-01"),
        _issue("H-5", "2026-09-10", org="Elsewhere"),
    ]
    result = get_ticket_volume_trend(
        entities=ENTITIES,
        issues=issues,
        organizations_by_entity=ORGS,
        as_of=AS_OF,
        persist=False,
    )
    rows = {row["entity_id"]: row for row in result["readings"]}
    assert rows[SITE]["points"] == 3
    assert rows[SITE]["meta"]["current_count"] == 2
    assert rows[SITE]["meta"]["prior_count"] == 1
    assert rows[SITE]["value"] == 100.0
    assert rows[SITE]["period_key"] == period_key_for("monthly", AS_OF)
    assert rows[OTHER]["points"] is None
    assert rows[OTHER]["error"]


def test_escalation_scores_zero_when_any_severe_or_escalated_ticket_exists() -> None:
    assert escalation_points(0, 0, 0) == 2
    assert escalation_value(0, 0, 0) == 0.0
    assert escalation_points(4, 1, 0) == 0
    assert escalation_value(4, 1, 0) == 25.0
    assert escalation_points(4, 0, 1) == 0
    issues = [
        _issue("H-1", "2026-08-02", severity="S1"),
        _issue("H-2", "2026-08-03"),
        _issue("H-3", "2026-09-02", labels=["jira_escalated"]),
        _issue("H-4", "2026-08-04", org="Elsewhere", severity="S2"),
    ]
    result = get_escalation_rate(
        entities=ENTITIES,
        issues=issues,
        organizations_by_entity=ORGS,
        as_of=AS_OF,
        persist=False,
    )
    rows = {row["entity_id"]: row for row in result["readings"]}
    assert rows[SITE]["period_key"] == "2026-08"
    assert rows[SITE]["meta"]["tickets"] == 2
    assert rows[SITE]["meta"]["sev_12"] == 1
    assert rows[SITE]["points"] == 0
    assert rows[SITE]["value"] == 50.0
    assert rows[OTHER]["points"] is None


def test_backfill_skips_a_period_already_stored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    from src.healthscore_web.store import upsert_reading

    upsert_reading(
        conn,
        entity_id=SITE,
        entity_name="Site A",
        metric_name="sla_adherence",
        grain="monthly",
        as_of="2026-09-28",
        period_key=period_key_for("monthly", AS_OF),
        value=100,
        points=3,
        generator="manual-seed",
    )
    conn.close()
    august_end = date(2026, 8, 31)
    result = backfill_sla_adherence(
        months=2,
        entities=ENTITIES,
        as_of=AS_OF,
        issues_by_as_of={
            AS_OF.isoformat(): [
                {
                    "key": "H-1",
                    "organizations": ["Acme"],
                    "ttfr_ms": 1,
                    "ttfr_breached": False,
                    "ttr_ms": None,
                    "ttr_breached": False,
                }
            ],
            august_end.isoformat(): [],
        },
        organizations_by_entity=ORGS,
        persist=True,
    )
    assert result["months_skipped"] == [period_key_for("monthly", AS_OF)]
    assert [row["period_key"] for row in result["months"]] == [period_key_for("monthly", august_end)]
    conn = connect()
    try:
        stored = observations_for_metric(conn, SITE, "sla_adherence")
    finally:
        conn.close()
    seeded = next(row for row in stored if row["period_key"] == period_key_for("monthly", AS_OF))
    assert seeded["generator"] == "manual-seed"
    older = next(row for row in stored if row["period_key"] == period_key_for("monthly", august_end))
    assert older["points"] is None


def test_volume_and_escalation_backfill_write_six_months(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    issues = [_issue("H-1", "2026-08-15")]
    volume = backfill_ticket_volume_trend(
        months=6,
        entities=ENTITIES,
        as_of=AS_OF,
        issues=issues,
        organizations_by_entity=ORGS,
        persist=True,
    )
    escalation = backfill_escalation_rate(
        months=6,
        entities=ENTITIES,
        as_of=AS_OF,
        issues=issues,
        organizations_by_entity=ORGS,
        persist=True,
    )
    assert volume["months_scored"] == 6
    assert escalation["months_scored"] == 6
    conn = connect()
    try:
        volume_rows = observations_for_metric(conn, SITE, "ticket_volume_trend")
        escalation_rows = observations_for_metric(conn, SITE, "escalation_rate")
    finally:
        conn.close()
    assert len(volume_rows) == 6
    assert len(escalation_rows) == 6
    latest_escalation = escalation_rows[-1]
    assert latest_escalation["period_key"] == "2026-08"
    assert latest_escalation["points"] == 2
