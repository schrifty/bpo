"""Tests for KPIObservation and kpi_service resolve modes."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.kpi_observation import (
    KPIObservation,
    observation_from_generator_raw,
    observation_from_stored_datapoint,
)
from src.kpi_service import (
    format_kpi_resolved_block,
    resolve_kpi,
    resolve_kpis_by_tag,
)
from src.metrics_latest import DatapointValue
from src.metrics_registry import load_metrics_registry
from src.metrics_upsert import MetricUpsertContext


def _ctx() -> MetricUpsertContext:
    return MetricUpsertContext(
        entry_date="2026-07-24",
        requested_sites=None,
        skip_catalog=True,
        timeout_seconds=30.0,
        verbose=False,
        dry_run=True,
        days=30,
        max_issues_per_board=50,
        workers=1,
        metric_name_filter=None,
    )


def _write_registry(tmp_path: Path) -> dict:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(
        """
metrics:
  "Live Only":
    metric-id: null
    metric-generator: gen_live
    tags: [engineering]
  "Stored Only":
    metric-id: 10
    metric-generator: null
    tags: [engineering]
  "Both":
    metric-id: 20
    metric-generator: gen_both
    tags: [engineering]
  "Neither":
    metric-id: null
    metric-generator: null
    tags: [engineering]
""".strip(),
        encoding="utf-8",
    )
    return load_metrics_registry(path=path)


def test_observation_from_generator_ratio_and_pct() -> None:
    pct = observation_from_generator_raw(
        {"numerator": 1, "denominator": 4},
        metric_name="% WAU",
    )
    assert pct.origin == "live"
    assert pct.value == 25.0
    assert pct.display_value == 25.0

    ratio = observation_from_generator_raw(
        {"numerator": 10, "denominator": 2},
        metric_name="Tokens / Dev",
    )
    assert ratio.display_value == 5.0


def test_observation_from_generator_error_and_scalar() -> None:
    err = observation_from_generator_raw({"error": "boom"}, metric_name="X")
    assert err.error == "boom"
    assert err.ok is False

    scalar = observation_from_generator_raw(42, metric_name="Count")
    assert scalar.value == 42
    assert scalar.numerator == 42.0


def test_resolve_live_without_metric_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_invoke(name, *, registry, ctx):
        assert name == "gen_live"
        return {"numerator": 3, "denominator": 10, "value": 0.3}

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    row = resolve_kpi(
        "Live Only",
        reg["metrics"]["Live Only"],
        mode="live",
        registry=reg,
        ctx=_ctx(),
    )
    assert row.observation.origin == "live"
    assert row.observation.ok
    assert row.observation.display_value == 0.3
    assert row.metric_id is None

    block = format_kpi_resolved_block(row)
    assert block == ["Live Only [engineering] 0.3"]


def test_format_kpi_resolved_block_clean_line() -> None:
    from src.kpi_observation import KPIObservation
    from src.kpi_service import KPIResolved

    row = KPIResolved(
        metric_name="% WAU",
        entry={},
        observation=KPIObservation(value=42.5, origin="live", as_of="2026-07-24"),
        tags=("mfr", "engineering", "ai"),
        automated=True,
        description="ignored in text format",
        metric_id=None,
    )
    assert format_kpi_resolved_block(row) == ["% WAU [mfr, engineering, ai] 42.5"]

    empty = KPIResolved(
        metric_name="Neither",
        entry={},
        observation=KPIObservation(origin="none", warnings=("no metric-generator — cannot compute live value",)),
        tags=(),
        automated=False,
        description=None,
        metric_id=None,
    )
    assert format_kpi_resolved_block(empty) == [
        "Neither [] (no metric-generator — cannot compute live value)"
    ]


def test_live_is_default_mode_and_never_reads_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def boom_fetch(entry, **kwargs):
        raise AssertionError("live mode must not read the Data API")

    def fake_invoke(name, *, registry, ctx):
        return {"value": 42}

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", boom_fetch)
    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    # "Both" has a metric-id, but the default (live) must still ignore storage.
    row = resolve_kpi("Both", reg["metrics"]["Both"], registry=reg, ctx=_ctx())
    assert row.observation.origin == "live"
    assert row.observation.display_value == 42
    assert row.recent_stored == ()


def test_resolve_stored_without_id_warns(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    row = resolve_kpi(
        "Live Only",
        reg["metrics"]["Live Only"],
        mode="stored",
        registry=reg,
        ctx=_ctx(),
    )
    assert row.observation.origin == "none"
    assert row.observation.display_value is None
    assert any("no metric-id" in w for w in row.observation.warnings)


def test_resolve_stored_reads_data_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_fetch(entry, **kwargs):
        return (DatapointValue(date="2026-07-20", value=99),), None

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)

    row = resolve_kpi("Both", reg["metrics"]["Both"], mode="stored", registry=reg, ctx=_ctx())
    assert row.observation.origin == "stored"
    assert row.observation.display_value == 99


def test_invalid_mode_rejected(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    with pytest.raises(ValueError):
        resolve_kpi("Both", reg["metrics"]["Both"], mode="auto", registry=reg, ctx=_ctx())


def test_resolve_kpis_by_tag_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_invoke(name, *, registry, ctx):
        return {"value": 1}

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    rows = resolve_kpis_by_tag("engineering", registry=reg, ctx=_ctx())
    assert [r.metric_name for r in rows] == ["Live Only", "Stored Only", "Both", "Neither"]
    by_name = {r.metric_name: r for r in rows}
    # Live mode: everything with a generator resolves live, regardless of metric-id.
    assert by_name["Live Only"].observation.origin == "live"
    assert by_name["Both"].observation.origin == "live"
    # No generator → nothing to compute; storage is not consulted in live mode.
    assert by_name["Stored Only"].observation.origin == "none"
    assert by_name["Neither"].observation.origin == "none"


def test_observation_passthrough() -> None:
    raw = KPIObservation(value=3, origin="live", as_of="2026-07-24")
    out = observation_from_generator_raw(raw, metric_name="X")
    assert out.value == 3


def test_stored_datapoint_helper() -> None:
    obs = observation_from_stored_datapoint(date_s="2026-07-01", value=12, metric_id=5)
    assert obs.origin == "stored"
    assert obs.display_value == 12
    assert obs.meta.get("metric_id") == 5
