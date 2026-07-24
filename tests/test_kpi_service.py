"""Tests for KPIObservation and kpi_service resolve modes."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.kpi_observation import (
    KPIObservation,
    observation_from_generator_raw,
    observation_from_stored_datapoint,
    stored_is_fresh,
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


def test_stored_is_fresh() -> None:
    assert stored_is_fresh("2099-01-01", max_age_hours=48) is True
    assert stored_is_fresh("2000-01-01", max_age_hours=48) is False
    assert stored_is_fresh(None, max_age_hours=48) is False


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
        allow_stored_fetch=False,
    )
    assert row.observation.origin == "live"
    assert row.observation.ok
    assert row.observation.display_value == 0.3
    assert row.metric_id is None

    block = format_kpi_resolved_block(row)
    assert "[live]" in block[1]


def test_resolve_stored_without_id_warns(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    row = resolve_kpi(
        "Live Only",
        reg["metrics"]["Live Only"],
        mode="stored",
        registry=reg,
        ctx=_ctx(),
        allow_stored_fetch=False,
    )
    assert row.observation.origin == "none"
    assert row.observation.display_value is None
    assert any("no metric-id" in w for w in row.observation.warnings)


def test_resolve_auto_prefers_fresh_stored(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    called = {"live": 0}

    def fake_fetch(entry, **kwargs):
        return (DatapointValue(date="2099-07-20", value=99),), None

    def fake_invoke(name, *, registry, ctx):
        called["live"] += 1
        return {"value": 1}

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)
    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    row = resolve_kpi(
        "Both",
        reg["metrics"]["Both"],
        mode="auto",
        registry=reg,
        ctx=_ctx(),
        max_stored_age_hours=48 * 24 * 365,  # generous for fixed far-future date
    )
    assert row.observation.origin == "stored"
    assert row.observation.display_value == 99
    assert called["live"] == 0


def test_resolve_auto_falls_back_to_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_fetch(entry, **kwargs):
        return (), None  # no datapoints

    def fake_invoke(name, *, registry, ctx):
        return {"value": 7}

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)
    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    row = resolve_kpi(
        "Both",
        reg["metrics"]["Both"],
        mode="auto",
        registry=reg,
        ctx=_ctx(),
    )
    assert row.observation.origin == "live"
    assert row.observation.display_value == 7


def test_resolve_auto_stale_stored_uses_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_fetch(entry, **kwargs):
        return (DatapointValue(date="2000-01-01", value=1),), None

    def fake_invoke(name, *, registry, ctx):
        return {"value": 55}

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)
    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    row = resolve_kpi(
        "Both",
        reg["metrics"]["Both"],
        mode="auto",
        registry=reg,
        ctx=_ctx(),
        max_stored_age_hours=48,
    )
    assert row.observation.origin == "live"
    assert row.observation.display_value == 55


def test_resolve_kpis_by_tag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_invoke(name, *, registry, ctx):
        return {"value": 1}

    def fake_fetch(entry, **kwargs):
        mid = entry.get("metric-id")
        if mid == 10:
            return (DatapointValue(date="2099-01-01", value=10),), None
        if mid == 20:
            return (DatapointValue(date="2099-01-01", value=20),), None
        return (), None

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)
    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)

    rows = resolve_kpis_by_tag(
        "engineering",
        mode="auto",
        registry=reg,
        ctx=_ctx(),
        max_stored_age_hours=48 * 24 * 365,
    )
    assert [r.metric_name for r in rows] == ["Live Only", "Stored Only", "Both", "Neither"]
    by_name = {r.metric_name: r for r in rows}
    assert by_name["Live Only"].observation.origin == "live"
    assert by_name["Stored Only"].observation.origin == "stored"
    assert by_name["Both"].observation.origin == "stored"
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
