"""Tests for kpi-snapshot grain mapping and persist/dry-run (no live APIs)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from src.kpi_snapshot import (
    grain_for_generator,
    iter_history_snapshot_plan,
    iter_snapshot_metrics,
    kpi_snapshot_exit_code,
    period_key_for,
    run_kpi_snapshot,
)
from src.kpi_store import GRAIN_DAILY, GRAIN_MONTH, connect, list_kpis
from src.kpi_store_s3 import KPIStoreS3Error
from src.metrics_upsert import MetricUpsertContext, MetricUpsertError


def _ctx(**kwargs) -> MetricUpsertContext:
    defaults = dict(
        entry_date="2026-09-10",
        requested_sites=None,
        skip_catalog=True,
        timeout_seconds=5.0,
        verbose=False,
        dry_run=True,
        days=30,
        max_issues_per_board=50,
        workers=1,
        metric_name_filter=None,
    )
    defaults.update(kwargs)
    return MetricUpsertContext(**defaults)


def _registry() -> dict:
    return yaml.safe_load(
        """
metrics:
  "PRs Merged":
    metric-generator: get_prs_merged
    tags: [engineering]
  "SLA Adherence (30 Days)":
    metric-generator: get_sla_adherence
    tags: [support, sla]
  "Customer-Reported Bugs":
    metric-generator: get_customer_reported_bugs_created
    tags: [engineering, quality]
  "No Gen":
    metric-generator: null
    tags: [engineering]
"""
    )


def test_grain_and_period_key() -> None:
    assert grain_for_generator("get_prs_merged") == GRAIN_MONTH
    assert grain_for_generator("get_sla_adherence") == GRAIN_DAILY
    assert grain_for_generator("get_customer_reported_bugs_created") == GRAIN_MONTH
    assert grain_for_generator("get_customer_reported_bugs_eom") == GRAIN_MONTH
    assert grain_for_generator("get_help_ticket_count") == GRAIN_MONTH
    assert period_key_for(GRAIN_DAILY, date(2026, 9, 10)) == "2026-09-10"
    assert period_key_for(GRAIN_MONTH, date(2026, 9, 10)) == "2026-08"
    assert period_key_for(GRAIN_MONTH, date(2026, 1, 3)) == "2025-12"


def test_iter_snapshot_metrics_skips_null_generator_and_honors_tag() -> None:
    reg = _registry()
    names = [n for n, _ in iter_snapshot_metrics(reg)]
    assert names == [
        "PRs Merged",
        "SLA Adherence (30 Days)",
        "Customer-Reported Bugs",
    ]
    tagged = [n for n, _ in iter_snapshot_metrics(reg, tag="quality")]
    assert tagged == ["Customer-Reported Bugs"]
    month_only = [n for n, _ in iter_snapshot_metrics(reg, grain=GRAIN_MONTH)]
    assert month_only == ["PRs Merged", "Customer-Reported Bugs"]
    daily_only = [n for n, _ in iter_snapshot_metrics(reg, grain=GRAIN_DAILY)]
    assert daily_only == ["SLA Adherence (30 Days)"]
    one = [n for n, _ in iter_snapshot_metrics(reg, metric_name_filter="PRs Merged")]
    assert one == ["PRs Merged"]


def test_dry_run_does_not_write_db(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"

    def invoke(name, **kwargs):
        return {"value": 7, "numerator": 7, "denominator": 1}

    summary = run_kpi_snapshot(
        _ctx(dry_run=True),
        dry_run=True,
        registry=_registry(),
        db_path=db,
        skip_s3=True,
        invoke=invoke,
    )
    assert summary["ok"] is True
    assert summary["written"] == 0
    assert summary["considered"] == 3
    assert not db.exists()


def test_persist_skip_s3_upserts_rows(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"

    def invoke(name, **kwargs):
        if name == "get_prs_merged":
            return {"value": 500, "numerator": 500, "denominator": 1, "as_of": "2026-08-01"}
        return {"value": 18, "numerator": 18, "denominator": 1}

    summary = run_kpi_snapshot(
        _ctx(dry_run=False),
        dry_run=False,
        registry=_registry(),
        db_path=db,
        skip_s3=True,
        invoke=invoke,
    )
    assert summary["ok"] is True
    assert summary["written"] == 3
    conn = connect(db)
    month = list_kpis(conn, grain=GRAIN_MONTH)
    daily = list_kpis(conn, grain=GRAIN_DAILY)
    conn.close()
    assert {r.metric_name for r in month} == {"PRs Merged", "Customer-Reported Bugs"}
    assert {r.period_key for r in month} == {"2026-08"}
    prs = next(r for r in month if r.metric_name == "PRs Merged")
    assert prs.observation.value == 500
    assert daily[0].metric_name == "SLA Adherence (30 Days)"
    assert daily[0].period_key == "2026-09-10"


def test_history_snapshot_plan_includes_until_and_month_ends() -> None:
    plan = iter_history_snapshot_plan(date(2026, 9, 11), 2)
    assert (date(2026, 9, 11), GRAIN_DAILY) in plan
    assert (date(2026, 9, 11), GRAIN_MONTH) in plan
    assert (date(2026, 8, 31), GRAIN_DAILY) in plan
    assert (date(2026, 9, 1), GRAIN_MONTH) in plan
    assert (date(2026, 7, 31), GRAIN_DAILY) in plan
    assert (date(2026, 8, 1), GRAIN_MONTH) in plan


def test_history_months_requires_tag_or_metric() -> None:
    with pytest.raises(ValueError, match="--history-months"):
        run_kpi_snapshot(
            _ctx(dry_run=True),
            dry_run=True,
            registry=_registry(),
            skip_s3=True,
            history_months=2,
            invoke=lambda *a, **k: {"value": 1},
        )


def test_history_months_persists_multiple_period_keys(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"
    seen_dates: list[str] = []

    def invoke(name, **kwargs):
        seen_dates.append(kwargs["ctx"].entry_date)
        return {"value": 18, "numerator": 18, "denominator": 1}

    summary = run_kpi_snapshot(
        _ctx(dry_run=False, metric_name_filter="SLA Adherence (30 Days)"),
        dry_run=False,
        registry=_registry(),
        db_path=db,
        skip_s3=True,
        history_months=2,
        invoke=invoke,
    )
    assert summary["ok"] is True
    conn = connect(db)
    daily = list_kpis(conn, grain=GRAIN_DAILY)
    conn.close()
    keys = {r.period_key for r in daily}
    assert "2026-09-10" in keys
    assert "2026-08-31" in keys
    assert "2026-07-31" in keys


def test_history_months_skips_sla_gaps_without_failing(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"

    def invoke(name, **kwargs):
        date_s = kwargs["ctx"].entry_date
        if date_s < "2026-08-01":
            return {"error": "no completed Time to resolution SLA cycles for HELP tickets resolved in the last 30d"}
        return {"value": 18, "numerator": 18, "denominator": 1}

    summary = run_kpi_snapshot(
        _ctx(dry_run=False, metric_name_filter="SLA Adherence (30 Days)"),
        dry_run=False,
        registry=_registry(),
        db_path=db,
        skip_s3=True,
        history_months=2,
        invoke=invoke,
    )
    assert summary["ok"] is True
    conn = connect(db)
    daily = list_kpis(conn, grain=GRAIN_DAILY)
    conn.close()
    assert all(r.observation.ok for r in daily)
    keys = {r.period_key for r in daily}
    assert "2026-09-10" in keys
    assert "2026-07-31" not in keys


def test_generator_error_fails_run_and_still_writes_error_row(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"

    def invoke(name, **kwargs):
        if name == "get_prs_merged":
            raise MetricUpsertError("github down")
        return {"value": 1, "numerator": 1, "denominator": 1}

    summary = run_kpi_snapshot(
        _ctx(dry_run=False, metric_name_filter=None),
        dry_run=False,
        registry=_registry(),
        db_path=db,
        skip_s3=True,
        invoke=invoke,
    )
    assert kpi_snapshot_exit_code(summary) == 1
    assert summary["written"] == 3
    conn = connect(db)
    prs = [r for r in list_kpis(conn) if r.metric_name == "PRs Merged"][0]
    conn.close()
    assert prs.observation.error == "github down"


def test_persist_without_s3_uri_fails_loud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CORTEX_KPI_STORE_S3_URI", raising=False)

    def invoke(name, **kwargs):
        return {"value": 1}

    with pytest.raises(KPIStoreS3Error, match="CORTEX_KPI_STORE_S3_URI"):
        run_kpi_snapshot(
            _ctx(dry_run=False),
            dry_run=False,
            registry=_registry(),
            db_path=tmp_path / "kpi.sqlite",
            skip_s3=False,
            invoke=invoke,
        )
