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
    resolve_kpis,
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
  "Support Only":
    metric-id: null
    metric-generator: null
    tags: [support]
  "Eng AI":
    metric-id: null
    metric-generator: gen_live
    tags: [engineering, ai]
""".strip(),
        encoding="utf-8",
    )
    return load_metrics_registry(path=path)


def test_observation_from_generator_ratio_and_pct() -> None:
    pct = observation_from_generator_raw(
        {"numerator": 1, "denominator": 4, "value": 25.0, "scope": "engineering_department"},
        metric_name="Weekly Active AI Users",
    )
    assert pct.origin == "live"
    assert pct.value == 25.0
    assert pct.display_value == 25.0

    ratio = observation_from_generator_raw(
        {"numerator": 10, "denominator": 2},
        metric_name="Tokens per Dev",
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
    assert "Live Only" in block[0]
    assert "engineering" in block[0]
    assert "0.3" in block[0]


def test_format_kpi_resolved_line_columnar() -> None:
    from src.kpi_observation import KPIObservation
    from src.kpi_service import KPIColumnWidths, KPIResolved, format_kpi_resolved_line

    widths = KPIColumnWidths(name=20, tags=28)
    row = KPIResolved(
        metric_name="Weekly Active AI Users",
        entry={},
        observation=KPIObservation(value=42.5, origin="live", as_of="2026-07-24"),
        tags=("engineering", "ai"),
        automated=True,
        description="ignored in text format",
        metric_id=None,
    )
    assert format_kpi_resolved_line(row, widths=widths) == [
        f"{'Weekly Active AI Users':<20}  {'engineering, ai':<28}  42.5"
    ]

    empty = KPIResolved(
        metric_name="Neither",
        entry={},
        observation=KPIObservation(origin="none", warnings=("no metric-generator — cannot compute live value",)),
        tags=(),
        automated=False,
        description=None,
        metric_id=None,
    )
    assert format_kpi_resolved_line(empty, widths=widths) == [
        f"{'Neither':<20}  {'—':<28}  no metric-generator — cannot compute live value"
    ]
    assert widths.header == f"{'KPI':<20}  {'TAGS':<28}  VALUE"


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


def test_resolve_stored_without_rows_warns(tmp_path: Path) -> None:
    from src.kpi_store import connect

    reg = _write_registry(tmp_path)
    conn = connect(tmp_path / "kpi.sqlite")
    row = resolve_kpi(
        "Live Only",
        reg["metrics"]["Live Only"],
        mode="stored",
        registry=reg,
        ctx=_ctx(),
        store_conn=conn,
    )
    conn.close()
    assert row.observation.origin == "none"
    assert row.observation.display_value is None
    assert any("no observation in KPI store" in w for w in row.observation.warnings)


def test_resolve_stored_requires_conn(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    with pytest.raises(ValueError, match="store_conn"):
        resolve_kpi(
            "Live Only",
            reg["metrics"]["Live Only"],
            mode="stored",
            registry=reg,
            ctx=_ctx(),
        )


def test_resolve_stored_reads_sqlite(tmp_path: Path) -> None:
    from src.kpi_observation import KPIObservation
    from src.kpi_store import GRAIN_MONTH, connect, stored_kpi_from_observation, upsert_kpi

    reg = _write_registry(tmp_path)
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Live Only",
            grain=GRAIN_MONTH,
            period_key="2026-08",
            observation=KPIObservation(value=7, origin="live", as_of="2026-08-01"),
            generator="gen_live",
        ),
    )
    # Daily grain must not be preferred over the month-close generator grain.
    from src.kpi_store import GRAIN_DAILY

    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Live Only",
            grain=GRAIN_DAILY,
            period_key="2026-09-10",
            observation=KPIObservation(value=99, origin="live", as_of="2026-09-10"),
            generator="gen_live",
        ),
    )
    row = resolve_kpi(
        "Live Only",
        {**reg["metrics"]["Live Only"], "metric-generator": "get_prs_merged"},
        mode="stored",
        registry=reg,
        ctx=_ctx(),
        store_conn=conn,
    )
    conn.close()
    assert row.observation.origin == "stored"
    assert row.observation.display_value == 7
    assert row.recent_stored[0].value == 7


def test_resolve_leandna_without_id_warns(tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)
    row = resolve_kpi(
        "Live Only",
        reg["metrics"]["Live Only"],
        mode="leandna",
        registry=reg,
        ctx=_ctx(),
    )
    assert row.observation.origin == "none"
    assert row.observation.display_value is None
    assert any("no metric-id" in w for w in row.observation.warnings)


def test_resolve_leandna_reads_data_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_fetch(entry, **kwargs):
        return (DatapointValue(date="2026-07-20", value=99),), None

    monkeypatch.setattr("src.kpi_service.fetch_stored_recent", fake_fetch)

    row = resolve_kpi("Both", reg["metrics"]["Both"], mode="leandna", registry=reg, ctx=_ctx())
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
    assert [r.metric_name for r in rows] == ["Live Only", "Stored Only", "Both", "Neither", "Eng AI"]
    by_name = {r.metric_name: r for r in rows}
    # Live mode: everything with a generator resolves live, regardless of metric-id.
    assert by_name["Live Only"].observation.origin == "live"
    assert by_name["Both"].observation.origin == "live"
    # No generator → nothing to compute; storage is not consulted in live mode.
    assert by_name["Stored Only"].observation.origin == "none"
    assert by_name["Neither"].observation.origin == "none"


def test_iter_resolve_kpis_by_tag_yields_incrementally(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src.kpi_service import iter_resolve_kpis_by_tag

    reg = _write_registry(tmp_path)
    calls: list[str] = []

    def fake_invoke(name, *, registry, ctx):
        calls.append(name)
        return {"value": len(calls)}

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    it = iter_resolve_kpis_by_tag("engineering", registry=reg, ctx=_ctx())
    first = next(it)
    assert first.metric_name == "Live Only"
    assert first.observation.display_value == 1
    # Second generator KPI is "Both"; Stored Only / Neither have no generator.
    assert calls == ["gen_live"]
    second = next(it)
    assert second.metric_name == "Stored Only"
    third = next(it)
    assert third.metric_name == "Both"
    assert third.observation.display_value == 2
    assert calls == ["gen_live", "gen_both"]
    fourth = next(it)
    assert fourth.metric_name == "Neither"
    fifth = next(it)
    assert fifth.metric_name == "Eng AI"
    assert calls == ["gen_live", "gen_both", "gen_live"]


def test_resolve_kpis_by_tag_stored_does_not_invoke_generators(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from src.kpi_observation import KPIObservation
    from src.kpi_service import resolve_kpis_by_tag
    from src.kpi_store import GRAIN_DAILY, connect, stored_kpi_from_observation, upsert_kpi

    reg = _write_registry(tmp_path)
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Both",
            grain=GRAIN_DAILY,
            period_key="2026-09-10",
            observation=KPIObservation(value=5, origin="live", as_of="2026-09-10"),
            generator="gen_both",
        ),
    )

    def boom_invoke(name, *, registry, ctx):
        raise AssertionError("stored mode must not run generators")

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", boom_invoke)
    rows = resolve_kpis_by_tag(
        "engineering",
        mode="stored",
        registry=reg,
        ctx=_ctx(),
        store_conn=conn,
    )
    conn.close()
    by_name = {r.metric_name: r for r in rows}
    assert by_name["Both"].observation.display_value == 5
    assert by_name["Live Only"].observation.origin == "none"
    assert by_name["Neither"].observation.origin == "none"


def test_resolve_kpis_all_and_tag_set(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reg = _write_registry(tmp_path)

    def fake_invoke(name, *, registry, ctx):
        return {"value": 1}

    monkeypatch.setattr("src.kpi_service.invoke_metric_generator", fake_invoke)

    all_rows = resolve_kpis(registry=reg, ctx=_ctx())
    assert [r.metric_name for r in all_rows] == [
        "Live Only",
        "Stored Only",
        "Both",
        "Neither",
        "Support Only",
        "Eng AI",
    ]
    tagged = resolve_kpis(tags=("engineering", "ai"), registry=reg, ctx=_ctx())
    assert [r.metric_name for r in tagged] == ["Eng AI"]


def test_resolve_kpis_stored_reads_store_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src.kpi_observation import KPIObservation
    from src.kpi_store import GRAIN_DAILY, connect, list_kpis as real_list_kpis
    from src.kpi_store import stored_kpi_from_observation, upsert_kpi

    reg = _write_registry(tmp_path)
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Both",
            grain=GRAIN_DAILY,
            period_key="2026-09-10",
            observation=KPIObservation(value=5, origin="live", as_of="2026-09-10"),
            generator="gen_both",
        ),
    )
    calls = {"n": 0}

    def counting_list_kpis(*args, **kwargs):
        calls["n"] += 1
        return real_list_kpis(*args, **kwargs)

    monkeypatch.setattr("src.kpi_service.list_kpis", counting_list_kpis)
    rows = resolve_kpis(mode="stored", registry=reg, ctx=_ctx(), store_conn=conn)
    conn.close()
    assert calls["n"] == 1
    by_name = {r.metric_name: r for r in rows}
    assert by_name["Both"].observation.display_value == 5
    assert by_name["Support Only"].observation.origin == "none"


def test_observation_passthrough() -> None:
    raw = KPIObservation(value=3, origin="live", as_of="2026-07-24")
    out = observation_from_generator_raw(raw, metric_name="X")
    assert out.value == 3


def test_stored_datapoint_helper() -> None:
    obs = observation_from_stored_datapoint(date_s="2026-07-01", value=12, metric_id=5)
    assert obs.origin == "stored"
    assert obs.display_value == 12
    assert obs.meta.get("metric_id") == 5
