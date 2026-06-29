"""Tests for config/my-metrics.yaml registry helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.metrics_registry import (
    count_fully_defined_metrics,
    get_kpi_automation_pct,
    is_fully_defined_metric,
    load_metrics_registry,
)


def test_is_fully_defined_metric() -> None:
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": "fn"}) is True
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": None}) is False
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": ""}) is False
    assert is_fully_defined_metric({"metric-generator": "fn"}) is False


def test_get_kpi_automation_pct_on_repo_registry() -> None:
    registry = load_metrics_registry()
    n = get_kpi_automation_pct(registry=registry)
    assert n == count_fully_defined_metrics(registry=registry)
    assert n >= 2  # Engineering Cycle Time + KPI Automation %


def test_count_fully_defined_metrics_with_temp_registry(tmp_path: Path) -> None:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "A":
    metric-id: 1
    metric-generator: gen_a
  "B":
    metric-id: 2
    metric-generator: null
  "C":
    metric-id: 3
    metric-generator: gen_c
""".strip(),
        encoding="utf-8",
    )
    reg = load_metrics_registry(path=path)
    assert count_fully_defined_metrics(registry=reg) == 2
    assert get_kpi_automation_pct(registry=reg) == 2


def test_load_metrics_registry_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_metrics_registry(path=tmp_path / "missing.yaml")
