"""Tests for KPI tagging: registry helpers and tag-scoped current-value fetch."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.metrics_latest import (
    DatapointValue,
    MetricRecentDatapointsRow,
    fetch_recent_datapoints_by_tag,
    format_metric_recent_block,
)
from src.metrics_registry import (
    all_registry_tags,
    entry_has_tag,
    iter_metrics_by_tag,
    load_metrics_registry,
    normalize_tag,
    registry_metric_tags,
)


def _write_registry(tmp_path: Path) -> dict:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "Alpha":
    metric-id: 10
    metric-generator: gen_a
    tags: [Engineering]
  "Beta":
    metric-id: null
    metric-generator: gen_b
    tags: [engineering, "Data_Integration"]
  "Gamma":
    metric-id: 30
    metric-generator: null
    tags: [support]
  "Delta":
    metric-id: 40
    metric-generator: null
""".strip(),
        encoding="utf-8",
    )
    return load_metrics_registry(path=path)


def test_normalize_tag() -> None:
    assert normalize_tag("Engineering") == "engineering"
    assert normalize_tag("Data_Integration") == "data-integration"
    assert normalize_tag("  Supply  Chain  ") == "supply-chain"
    assert normalize_tag(None) == ""
    assert normalize_tag("   ") == ""


def test_registry_metric_tags_normalizes_and_dedupes() -> None:
    entry = {"tags": ["Engineering", "engineering", "Data_Integration"]}
    assert registry_metric_tags(entry) == ["engineering", "data-integration"]
    assert registry_metric_tags({"tags": "AI"}) == ["ai"]
    assert registry_metric_tags({}) == []
    assert registry_metric_tags({"tags": None}) == []


def test_entry_has_tag_is_normalized() -> None:
    entry = {"tags": ["Data_Integration"]}
    assert entry_has_tag(entry, "data-integration") is True
    assert entry_has_tag(entry, "Data Integration") is True
    assert entry_has_tag(entry, "engineering") is False
    assert entry_has_tag(entry, "") is False


def test_iter_metrics_by_tag(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    names = [name for name, _ in iter_metrics_by_tag("engineering", registry=reg)]
    assert names == ["Alpha", "Beta"]
    # Normalized input matches the underscore form on Beta.
    assert [n for n, _ in iter_metrics_by_tag("Data Integration", registry=reg)] == ["Beta"]
    assert iter_metrics_by_tag("", registry=reg) == []
    assert iter_metrics_by_tag("missing", registry=reg) == []


def test_all_registry_tags_counts_sorted(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    assert all_registry_tags(registry=reg) == [
        ("engineering", 2),
        ("data-integration", 1),
        ("support", 1),
    ]


def test_repo_registry_has_tags() -> None:
    tags = all_registry_tags()
    assert tags
    tag_names = {t for t, _ in tags}
    assert "engineering" in tag_names


def test_format_block_shows_tags_and_no_id() -> None:
    block = format_metric_recent_block(
        MetricRecentDatapointsRow(
            metric_name="AI Token Usage",
            metric_id=0,
            recent=(),
            automated=True,
            tags=("ai", "engineering"),
        )
    )
    assert block[0] == "AI Token Usage [automated] [tags: ai, engineering]:"
    assert block[1] == "  (no metric-id in registry — no stored value)"


def test_fetch_recent_datapoints_by_tag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    monkeypatch.setattr("src.metrics_latest.iter_metrics_by_tag", lambda tag: iter_metrics_by_tag(tag, registry=reg))

    def fake_fetch(metric_ids, **kwargs):
        assert metric_ids == [10]
        return (DatapointValue(date="2026-06-01", value=42),), None

    monkeypatch.setattr("src.metrics_latest.fetch_recent_datapoints_with_fallbacks", fake_fetch)

    rows = fetch_recent_datapoints_by_tag("engineering")
    assert [r.metric_name for r in rows] == ["Alpha", "Beta"]

    alpha = rows[0]
    assert alpha.metric_id == 10
    assert alpha.recent[0].value == 42
    assert alpha.tags == ("engineering",)

    beta = rows[1]
    assert beta.metric_id == 0  # no metric-id → listed without value
    assert beta.recent == ()
    assert beta.error is None
