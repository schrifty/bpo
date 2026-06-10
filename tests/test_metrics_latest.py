"""Tests for latest MetricDataPoint helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.metrics_latest import (
    DEFAULT_RECENT_DATAPOINT_COUNT,
    fetch_latest_datapoint_for_metric_id,
    fetch_latest_datapoint_with_fallbacks,
    fetch_recent_datapoints_for_metric_id,
    format_datapoint_line,
    format_metric_recent_block,
    latest_datapoint_from_rows,
    recent_datapoints_from_rows,
)
from src.metrics_registry import (
    datapoint_metric_ids_for_entry,
    has_metric_id,
    iter_metrics_with_id,
    load_metrics_registry,
    registry_datapoint_metric_id,
)


def test_has_metric_id() -> None:
    assert has_metric_id({"metric-id": 1}) is True
    assert has_metric_id({"metric-id": None}) is False
    assert has_metric_id({"metric-id": ""}) is False


def test_iter_metrics_with_id_on_repo_registry() -> None:
    rows = iter_metrics_with_id()
    assert rows
    assert all(isinstance(mid, int) for _name, mid, _entry in rows)
    assert all(has_metric_id(entry) for _name, _mid, entry in rows)


def test_iter_metrics_with_id_temp_registry(tmp_path: Path) -> None:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "With id":
    metric-id: 10
    metric-generator: null
  "Without id":
    metric-id: null
    metric-generator: gen
""".strip(),
        encoding="utf-8",
    )
    reg = load_metrics_registry(path=path)
    rows = iter_metrics_with_id(registry=reg)
    assert rows == [("With id", 10, {"metric-id": 10, "metric-generator": None})]


def test_latest_datapoint_from_rows_picks_newest() -> None:
    rows = [
        {"dataPointDate": "2026-06-01", "value": 1},
        {"dataPointDate": "2026-06-10", "value": 99},
        {"dataPointDate": "2026-06-05", "value": 50},
    ]
    rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    date, value = latest_datapoint_from_rows(rows)
    assert date == "2026-06-10"
    assert value == 99


def test_latest_datapoint_from_rows_empty() -> None:
    assert latest_datapoint_from_rows([]) == (None, None)


def test_recent_datapoints_from_rows_returns_newest_first() -> None:
    rows = [
        {"dataPointDate": "2026-06-01", "value": 1},
        {"dataPointDate": "2026-06-10", "value": 99},
        {"dataPointDate": "2026-06-05", "value": 50},
        {"dataPointDate": "2026-05-20", "value": 25},
    ]
    rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    recent = recent_datapoints_from_rows(rows, limit=3)
    assert [(p.date, p.value) for p in recent] == [
        ("2026-06-10", 99),
        ("2026-06-05", 50),
        ("2026-06-01", 1),
    ]


def test_recent_datapoints_from_rows_empty() -> None:
    assert recent_datapoints_from_rows([]) == ()


def test_format_metric_recent_block() -> None:
    from src.metrics_latest import DatapointValue, MetricRecentDatapointsRow

    block = format_metric_recent_block(
        MetricRecentDatapointsRow(
            metric_name="Time to Value",
            metric_id=1860,
            recent=(
                DatapointValue(date="2026-06-01", value=166.5),
                DatapointValue(date="2026-05-25", value=170.0),
            ),
        )
    )
    assert block == [
        "Time to Value:",
        "  2026-06-01: 166.5",
        "  2026-05-25: 170.0",
    ]


def test_format_datapoint_line() -> None:
    assert format_datapoint_line(date="2026-06-10", value=85.0) == "2026-06-10: 85.0"
    assert format_datapoint_line(date=None, value=None) == "(no datapoints)"


def test_registry_datapoint_metric_id() -> None:
    assert registry_datapoint_metric_id({"datapoint-metric-id": 1860}) == 1860
    assert registry_datapoint_metric_id({"metric-id": 2021}) is None


def test_datapoint_metric_ids_for_entry() -> None:
    entry = {"metric-id": 2021, "datapoint-metric-id": 1860}
    assert datapoint_metric_ids_for_entry(entry, 2021) == [2021, 1860]


def test_fetch_latest_datapoint_with_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.metrics_latest import DatapointValue, fetch_latest_datapoint_with_fallbacks

    calls: list[int] = []

    def fake_fetch(
        metric_id: int,
        *,
        requested_sites: str | None = None,
        lookback_days: int = 365,
        timeout_seconds: float = 60.0,
        limit: int = 1,
    ) -> tuple[tuple[DatapointValue, ...], str | None]:
        calls.append(metric_id)
        if metric_id == 2021:
            return (), None
        if metric_id == 1860:
            return (DatapointValue(date="2026-06-01", value=166.5),), None
        return (), "unexpected"

    monkeypatch.setattr("src.metrics_latest.fetch_recent_datapoints_for_metric_id", fake_fetch)
    date, value, error = fetch_latest_datapoint_with_fallbacks([2021, 1860])
    assert calls == [2021, 1860]
    assert error is None
    assert date == "2026-06-01"
    assert value == 166.5


def test_fetch_latest_datapoint_for_metric_id(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_resolve(metric_id: int, **kwargs: object) -> dict:
        assert metric_id == 42
        return {"id": 42, "siteId": 416}

    def fake_fetch(
        metric_id: int,
        *,
        start_date: str,
        end_date: str,
        requested_sites: str | None,
        timeout_seconds: float,
    ) -> tuple[list[dict], None]:
        assert metric_id == 42
        assert requested_sites == "416"
        return (
            [
                {"dataPointDate": "2026-05-01T00:00:00", "value": 10},
                {"dataPointDate": "2026-06-01T00:00:00", "value": 20},
            ],
            None,
        )

    monkeypatch.setattr("src.metrics_latest.resolve_metric_catalog_row", fake_resolve)
    monkeypatch.setattr("src.metrics_latest.fetch_metric_datapoints", fake_fetch)

    date, value, error = fetch_latest_datapoint_for_metric_id(42, requested_sites="416")
    assert error is None
    assert date == "2026-06-01"
    assert value == 20
