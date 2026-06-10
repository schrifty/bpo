"""Tests for my-metrics.yaml generator → LeanDNA upsert orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from src.metrics_upsert import (
    MetricUpsertContext,
    MetricUpsertError,
    iter_metrics_to_upsert,
    parse_generator_parts,
    run_metrics_upsert,
    upsert_one_registry_metric,
)


def _ctx(**overrides) -> MetricUpsertContext:
    base = dict(
        entry_date="2026-06-01",
        requested_sites=None,
        skip_catalog=False,
        timeout_seconds=30.0,
        verbose=False,
        dry_run=True,
        days=30,
        max_issues_per_board=100,
        workers=2,
        metric_name_filter=None,
    )
    base.update(overrides)
    return MetricUpsertContext(**base)


def test_iter_metrics_to_upsert_skips_null_generators() -> None:
    registry = {
        "metrics": {
            "A": {"metric-id": 1, "metric-generator": "gen_a"},
            "B": {"metric-id": 2, "metric-generator": None},
            "C": {"metric-id": 3, "metric-generator": "gen_c"},
        }
    }
    names = [n for n, _ in iter_metrics_to_upsert(registry)]
    assert names == ["A", "C"]


def test_parse_percent_scalar_uses_registry_total_as_denominator() -> None:
    registry = {
        "metrics": {
            "KPI Automation %": {"metric-id": 1, "metric-generator": "x"},
            "Other": {"metric-id": 2, "metric-generator": None},
        }
    }
    parts = parse_generator_parts(2, metric_name="KPI Automation %", registry=registry)
    assert parts.numerator == 2.0
    assert parts.denominator == 2.0


def test_parse_cycle_time_payload_median_of_teams() -> None:
    raw = {
        "teams": [
            {"median_days": 10.0},
            {"median_days": 20.0},
            {"error": "boom"},
        ]
    }
    parts = parse_generator_parts(raw, metric_name="Engineering Cycle Time", registry={})
    assert parts.numerator == 15.0
    assert parts.denominator == 1.0


def test_parse_sprint_delivery_metric_value() -> None:
    raw = {"numerator": 85.0, "denominator": 100}
    parts = parse_generator_parts(raw, metric_name="Sprint Delivery %", registry={})
    assert parts.numerator == 85.0
    assert parts.denominator == 100.0


def test_parse_sprint_story_points_metric_value() -> None:
    raw = {"numerator": 240.0, "denominator": 1.0}
    parts = parse_generator_parts(raw, metric_name="Sprint Story Points Delivered", registry={})
    assert parts.numerator == 240.0
    assert parts.denominator == 1.0


def test_iter_metrics_to_upsert_includes_generator_without_metric_id() -> None:
    registry = {
        "metrics": {
            "Ready": {"metric-id": 1, "metric-generator": "gen_a"},
            "Pending id": {"metric-id": None, "metric-generator": "gen_b"},
        }
    }
    names = [n for n, _ in iter_metrics_to_upsert(registry)]
    assert names == ["Ready", "Pending id"]


def test_dry_run_does_not_call_data_api(tmp_path: Path) -> None:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "KPI Automation %":
    metric-id: 2076
    metric-generator: get_kpi_automation_pct
""".strip(),
        encoding="utf-8",
    )
    with patch("src.metrics_upsert.run_upsert") as mock_upsert:
        summary = run_metrics_upsert(_ctx(dry_run=True), registry_path=path)
    mock_upsert.assert_not_called()
    assert summary["ok"] is True
    assert summary["attempted"] == 1
    assert summary["results"][0]["dry_run"] is True


def test_upsert_one_unknown_generator_fails() -> None:
    entry = {"metric-id": 1, "metric-generator": "not_registered"}
    with pytest.raises(MetricUpsertError, match="unknown metric-generator"):
        upsert_one_registry_metric(
            "X",
            entry,
            registry={"metrics": {}},
            ctx=_ctx(dry_run=True),
        )


def test_run_metrics_upsert_filtered_metric_missing_id(tmp_path: Path) -> None:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "Sprint Story Points Delivered":
    metric-id: null
    metric-generator: get_sprint_story_points_by_team
""".strip(),
        encoding="utf-8",
    )
    with patch(
        "src.metrics_upsert.resolve_registry_metric_id",
        return_value=type(
            "R",
            (),
            {"metric_id": 2099, "source": "catalog", "detail": "found in catalog"},
        )(),
    ), patch(
        "src.metrics_upsert.invoke_metric_generator",
        return_value={"numerator": 240.0, "denominator": 1.0},
    ):
        summary = run_metrics_upsert(
            _ctx(dry_run=True, metric_name_filter="Sprint Story Points Delivered"),
            registry_path=path,
        )
    assert summary["ok"] is True
    assert summary["attempted"] == 1
    assert summary["results"][0]["metric_id"] == 2099
    assert summary["results"][0]["numerator"] == 240.0


def test_run_metrics_upsert_reports_generator_failure(tmp_path: Path) -> None:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "Bad":
    metric-id: 1
    metric-generator: not_registered
""".strip(),
        encoding="utf-8",
    )
    summary = run_metrics_upsert(_ctx(dry_run=True), registry_path=path)
    assert summary["ok"] is False
    assert summary["failed"] == ["Bad"]
