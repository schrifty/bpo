"""Tests for LeanDNA metric catalog helpers."""

from __future__ import annotations

from src.leandna_metrics_catalog import (
    filter_metric_catalog,
    format_metric_brief_lines,
    is_catalog_id_token,
)
from src.metrics_registry import is_fully_defined_metric


def test_is_catalog_id_token() -> None:
    assert is_catalog_id_token("638") is True
    assert is_catalog_id_token("job") is False


def test_filter_metric_catalog_by_id() -> None:
    catalog = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    rows = filter_metric_catalog(catalog, filter_token="2", use_all=False, max_metrics=50)
    assert len(rows) == 1
    assert rows[0]["id"] == 2


def test_format_metric_brief_lines() -> None:
    lines = format_metric_brief_lines([{"id": 1, "name": "OTIF", "metricType": "Manual", "siteId": 416}])
    assert len(lines) == 1
    assert "OTIF" in lines[0]


def test_registry_fully_defined() -> None:
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": "fn"}) is True
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": None}) is False
