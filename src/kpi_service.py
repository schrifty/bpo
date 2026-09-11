"""Resolve KPIs live from generators, or inspect stored SQLite history.

Reads are live by default: registry generators compute the value each time.
Generating never writes. Persist with ``kpi-snapshot`` (SQLite/S3) or
``metrics-upsert`` / ``materialize_kpi`` (LeanDNA, metric-id required).

``mode="stored"`` reads the SQLite KPI store (S3-backed). ``mode="leandna"``
inspects LeanDNA Data API datapoints and is not the default stored path.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from src.kpi_observation import (
    KPIObservation,
    observation_from_generator_raw,
    observation_from_stored_datapoint,
)
from src.kpi_store import StoredKPI, connect, grain_for_generator, list_kpis
from src.kpi_store_s3 import prepare_kpi_store_for_read
from src.metrics_latest import (
    DEFAULT_RECENT_DATAPOINT_COUNT,
    DatapointValue,
    fetch_recent_datapoints_with_fallbacks,
    format_datapoint_line,
)
from src.metrics_registry import (
    datapoint_metric_ids_for_entry,
    has_metric_generator,
    has_metric_id,
    is_automated_metric,
    iter_all_metrics,
    iter_metrics_by_tags,
    load_metrics_registry,
    registry_metric_description,
    registry_metric_tags,
)
from src.metrics_upsert import MetricUpsertContext, MetricUpsertError, invoke_metric_generator

logger = logging.getLogger(__name__)

ResolveMode = Literal["live", "stored", "leandna"]
RESOLVE_MODES: tuple[str, ...] = ("live", "stored", "leandna")
DEFAULT_RESOLVE_MODE: ResolveMode = "live"


@dataclass(frozen=True)
class KPIResolved:
    """One registry KPI with a resolved observation (live, stored, or none)."""

    metric_name: str
    entry: dict[str, Any]
    observation: KPIObservation
    tags: tuple[str, ...]
    automated: bool
    description: str | None
    metric_id: int | None
    recent_stored: tuple[DatapointValue, ...] = ()


def default_resolve_context(
    *,
    days: int = 30,
    timeout_seconds: float = 60.0,
    requested_sites: str | None = None,
    verbose: bool = False,
    max_issues_per_board: int = 500,
    workers: int = 6,
) -> MetricUpsertContext:
    """Context for live generation without implying a Data API write."""
    return MetricUpsertContext(
        entry_date=date.today().isoformat(),
        requested_sites=requested_sites,
        skip_catalog=True,
        timeout_seconds=timeout_seconds,
        verbose=verbose,
        dry_run=True,
        days=days,
        max_issues_per_board=max_issues_per_board,
        workers=workers,
        metric_name_filter=None,
    )


def _metric_id_or_none(entry: dict[str, Any]) -> int | None:
    if not has_metric_id(entry):
        return None
    return int(entry["metric-id"])


def _empty_observation(
    *,
    warning: str | None = None,
    error: str | None = None,
) -> KPIObservation:
    warnings = (warning,) if warning else ()
    return KPIObservation(origin="none", warnings=warnings, error=error)


def generate_live_observation(
    metric_name: str,
    entry: dict[str, Any],
    *,
    registry: dict[str, Any],
    ctx: MetricUpsertContext,
) -> KPIObservation:
    """Run the registry generator and normalize to ``KPIObservation`` (no write)."""
    if not has_metric_generator(entry):
        return _empty_observation(warning="no metric-generator — cannot compute live value")
    gen_name = str(entry.get("metric-generator") or "").strip()
    try:
        raw = invoke_metric_generator(gen_name, registry=registry, ctx=ctx)
    except MetricUpsertError as e:
        return KPIObservation(error=str(e), origin="live", as_of=date.today().isoformat())
    except Exception as e:  # noqa: BLE001 — surface integration failures to callers
        return KPIObservation(
            error=f"{type(e).__name__}: {e}",
            origin="live",
            as_of=date.today().isoformat(),
        )
    return observation_from_generator_raw(raw, metric_name=metric_name, origin="live")


def _datapoint_from_stored_row(row: Any) -> DatapointValue:
    date_s = (row.observation.as_of or row.period_key or "").strip()
    return DatapointValue(date=date_s, value=row.observation.display_value)


def observations_from_kpi_store(
    conn: sqlite3.Connection,
    metric_name: str,
    entry: dict[str, Any],
    *,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    stored_rows: Sequence[StoredKPI] | None = None,
) -> tuple[KPIObservation, tuple[DatapointValue, ...]]:
    """Newest SQLite observations for *metric_name* (grain from the generator).

    Pass *stored_rows* to skip a per-KPI ``list_kpis`` query (bulk resolve).
    """
    gen = str(entry.get("metric-generator") or "").strip()
    grain = grain_for_generator(gen) if gen else None
    limit = max(1, recent_count)
    if stored_rows is not None:
        rows = list(stored_rows)[:limit]
    else:
        rows = list_kpis(conn, metric_name=metric_name, grain=grain, limit=limit)
    if not rows:
        warning = (
            "no observation in KPI store"
            if gen
            else "no metric-generator — nothing stored by kpi-snapshot"
        )
        return _empty_observation(warning=warning), ()
    current = rows[0].observation
    recent = tuple(
        _datapoint_from_stored_row(row)
        for i, row in enumerate(rows)
        if row.observation.ok or i == 0
    )
    return current, recent


def fetch_stored_recent(
    entry: dict[str, Any],
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> tuple[tuple[DatapointValue, ...], str | None]:
    """Read recent LeanDNA Data API datapoints for a registry entry (empty if no metric-id)."""
    if not has_metric_id(entry):
        return (), None
    metric_id = int(entry["metric-id"])
    return fetch_recent_datapoints_with_fallbacks(
        datapoint_metric_ids_for_entry(entry, metric_id),
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        limit=limit,
    )


def _observation_from_leandna(
    entry: dict[str, Any],
    *,
    metric_id: int | None,
    requested_sites: str | None,
    lookback_days: int,
    timeout_seconds: float,
    recent_count: int,
) -> tuple[KPIObservation, tuple[DatapointValue, ...]]:
    if metric_id is None:
        return _empty_observation(warning="no metric-id in registry — no stored value"), ()
    recent, stored_error = fetch_stored_recent(
        entry,
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        limit=recent_count,
    )
    if stored_error:
        return KPIObservation(error=stored_error, origin="stored", as_of=None), recent
    if not recent:
        return _empty_observation(warning="no datapoints"), ()
    return (
        observation_from_stored_datapoint(
            date_s=recent[0].date,
            value=recent[0].value,
            metric_id=metric_id,
        ),
        recent,
    )


def resolve_kpi(
    metric_name: str,
    entry: dict[str, Any],
    *,
    mode: ResolveMode = DEFAULT_RESOLVE_MODE,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    store_conn: sqlite3.Connection | None = None,
    stored_rows: Sequence[StoredKPI] | None = None,
) -> KPIResolved:
    """Resolve one KPI under ``mode`` (``live`` default, ``stored``, or ``leandna``).

    * **live** (default) — run the ``metric-generator``. The KPI store and Data API
      are never read.
    * **stored** — read the SQLite KPI store (requires ``store_conn``).
    * **leandna** — inspect LeanDNA Data API datapoints (``metric-id`` required).
    """
    if mode not in RESOLVE_MODES:
        raise ValueError(f"mode must be one of {RESOLVE_MODES}, got {mode!r}")

    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or default_resolve_context(
        timeout_seconds=timeout_seconds,
        requested_sites=requested_sites,
    )
    tags = tuple(registry_metric_tags(entry))
    description = registry_metric_description(entry)
    automated = is_automated_metric(entry)
    metric_id = _metric_id_or_none(entry)

    recent: tuple[DatapointValue, ...] = ()

    if mode == "live":
        observation = generate_live_observation(metric_name, entry, registry=reg, ctx=resolve_ctx)
    elif mode == "stored":
        if store_conn is None:
            raise ValueError("mode='stored' requires store_conn (SQLite KPI store)")
        observation, recent = observations_from_kpi_store(
            store_conn,
            metric_name,
            entry,
            recent_count=recent_count,
            stored_rows=stored_rows,
        )
    else:
        observation, recent = _observation_from_leandna(
            entry,
            metric_id=metric_id,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            recent_count=recent_count,
        )

    return KPIResolved(
        metric_name=metric_name,
        entry=entry,
        observation=observation,
        tags=tags,
        automated=automated,
        description=description,
        metric_id=metric_id,
        recent_stored=recent,
    )


def _index_stored_kpis(
    conn: sqlite3.Connection,
) -> tuple[dict[str, list[StoredKPI]], dict[tuple[str, str], list[StoredKPI]]]:
    """One ``list_kpis`` pass, indexed by name and by (name, grain). Newest first."""
    by_name: dict[str, list[StoredKPI]] = {}
    by_name_grain: dict[tuple[str, str], list[StoredKPI]] = {}
    for row in list_kpis(conn):
        by_name.setdefault(row.metric_name, []).append(row)
        by_name_grain.setdefault((row.metric_name, row.grain), []).append(row)
    return by_name, by_name_grain


def _stored_rows_for_entry(
    metric_name: str,
    entry: dict[str, Any],
    *,
    by_name: dict[str, list[StoredKPI]],
    by_name_grain: dict[tuple[str, str], list[StoredKPI]],
) -> list[StoredKPI]:
    gen = str(entry.get("metric-generator") or "").strip()
    if gen:
        return by_name_grain.get((metric_name, grain_for_generator(gen)), [])
    return by_name.get(metric_name, [])


def _registry_metrics_for_tags(
    tags: Sequence[str] | None,
    *,
    registry: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    if tags:
        return iter_metrics_by_tags(tags, registry=registry)
    return iter_all_metrics(registry=registry)


def iter_resolve_kpis(
    *,
    tags: Sequence[str] | None = None,
    mode: ResolveMode = DEFAULT_RESOLVE_MODE,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    store_conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
    skip_s3: bool = False,
    s3_client: Any | None = None,
    s3_uri: str | None = None,
) -> Iterator[KPIResolved]:
    """Yield registry KPIs (all, or AND tag-set) as each one resolves.

    ``tags=None`` or empty is the full catalog. Non-empty *tags* require every
    tag (AND). Stored mode reads SQLite once, then attaches rows in memory.
    Live and LeanDNA still compute/fetch per KPI (generators / Data API have
    no bulk value endpoint).
    """
    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or default_resolve_context(
        timeout_seconds=timeout_seconds,
        requested_sites=requested_sites,
    )
    own_conn: sqlite3.Connection | None = None
    conn = store_conn
    if mode == "stored" and conn is None:
        path = prepare_kpi_store_for_read(
            Path(db_path) if db_path else None,
            s3_client=s3_client,
            uri=s3_uri,
            skip_s3=skip_s3,
        )
        own_conn = connect(path)
        conn = own_conn
    stored_by_name: dict[str, list[StoredKPI]] = {}
    stored_by_name_grain: dict[tuple[str, str], list[StoredKPI]] = {}
    if mode == "stored" and conn is not None:
        stored_by_name, stored_by_name_grain = _index_stored_kpis(conn)
    try:
        for name, entry in _registry_metrics_for_tags(tags, registry=reg):
            stored_rows = (
                _stored_rows_for_entry(
                    name,
                    entry,
                    by_name=stored_by_name,
                    by_name_grain=stored_by_name_grain,
                )
                if mode == "stored"
                else None
            )
            yield resolve_kpi(
                name,
                entry,
                mode=mode,
                registry=reg,
                ctx=resolve_ctx,
                requested_sites=requested_sites,
                lookback_days=lookback_days,
                timeout_seconds=timeout_seconds,
                recent_count=recent_count,
                store_conn=conn,
                stored_rows=stored_rows,
            )
    finally:
        if own_conn is not None:
            own_conn.close()


def resolve_kpis(
    *,
    tags: Sequence[str] | None = None,
    mode: ResolveMode = DEFAULT_RESOLVE_MODE,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    store_conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
    skip_s3: bool = False,
    s3_client: Any | None = None,
    s3_uri: str | None = None,
) -> list[KPIResolved]:
    """Resolve every registry KPI, or those matching an AND tag-set."""
    return list(
        iter_resolve_kpis(
            tags=tags,
            mode=mode,
            registry=registry,
            ctx=ctx,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            recent_count=recent_count,
            store_conn=store_conn,
            db_path=db_path,
            skip_s3=skip_s3,
            s3_client=s3_client,
            s3_uri=s3_uri,
        )
    )


def iter_resolve_kpis_by_tag(
    tag: str,
    *,
    mode: ResolveMode = DEFAULT_RESOLVE_MODE,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    store_conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
    skip_s3: bool = False,
    s3_client: Any | None = None,
    s3_uri: str | None = None,
) -> Iterator[KPIResolved]:
    """Yield each registry KPI carrying *tag* as soon as it resolves (live by default)."""
    yield from iter_resolve_kpis(
        tags=(tag,),
        mode=mode,
        registry=registry,
        ctx=ctx,
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        recent_count=recent_count,
        store_conn=store_conn,
        db_path=db_path,
        skip_s3=skip_s3,
        s3_client=s3_client,
        s3_uri=s3_uri,
    )


def resolve_kpis_by_tag(
    tag: str,
    *,
    mode: ResolveMode = DEFAULT_RESOLVE_MODE,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    store_conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
    skip_s3: bool = False,
    s3_client: Any | None = None,
    s3_uri: str | None = None,
) -> list[KPIResolved]:
    """Resolve every registry KPI carrying *tag* (live by default)."""
    return resolve_kpis(
        tags=(tag,),
        mode=mode,
        registry=registry,
        ctx=ctx,
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        recent_count=recent_count,
        store_conn=store_conn,
        db_path=db_path,
        skip_s3=skip_s3,
        s3_client=s3_client,
        s3_uri=s3_uri,
    )


def materialize_kpi(
    metric_name: str,
    entry: dict[str, Any],
    *,
    registry: dict[str, Any],
    ctx: MetricUpsertContext,
) -> dict[str, Any]:
    """Explicitly write a live-generated value to the LeanDNA Data API.

    Generation alone never writes. Call this (or ``metrics-upsert``) when you
    intend to persist.
    """
    from src.metrics_upsert import upsert_one_registry_metric

    return upsert_one_registry_metric(metric_name, entry, registry=registry, ctx=ctx)


@dataclass(frozen=True)
class KPIColumnWidths:
    """Fixed column widths for streamed tabular KPI output."""

    name: int
    tags: int

    @property
    def header(self) -> str:
        return f"{'KPI':<{self.name}}  {'TAGS':<{self.tags}}  VALUE"


def _tags_cell(tags: tuple[str, ...] | list[str]) -> str:
    return ", ".join(tags) if tags else "—"


def _value_cell(row: KPIResolved) -> str:
    obs = row.observation
    if obs.error:
        return f"error: {obs.error}"
    if obs.display_value is None:
        if obs.warnings:
            return obs.warnings[0]
        return "—"
    return str(obs.display_value)


def column_widths_for_metrics(
    metrics: list[tuple[str, dict[str, Any]]],
) -> KPIColumnWidths:
    """Compute name/tags column widths from registry rows (before values resolve)."""
    name_w = len("KPI")
    tags_w = len("TAGS")
    for name, entry in metrics:
        name_w = max(name_w, len(str(name)))
        tags_w = max(tags_w, len(_tags_cell(tuple(registry_metric_tags(entry)))))
    return KPIColumnWidths(name=name_w, tags=tags_w)


def column_widths_for_tag(
    tag: str,
    *,
    registry: dict[str, Any] | None = None,
) -> KPIColumnWidths:
    """Column widths for every KPI carrying *tag* (cheap; no generators/API)."""
    return column_widths_for_tags((tag,), registry=registry)


def column_widths_for_tags(
    tags: Sequence[str] | None = None,
    *,
    registry: dict[str, Any] | None = None,
) -> KPIColumnWidths:
    """Column widths for all KPIs, or an AND tag-set (cheap; no generators/API)."""
    reg = registry if registry is not None else load_metrics_registry()
    return column_widths_for_metrics(_registry_metrics_for_tags(tags, registry=reg))


def format_kpi_resolved_line(
    row: KPIResolved,
    *,
    widths: KPIColumnWidths | None = None,
    indent: str = "  ",
) -> list[str]:
    """Columnar lines: ``KPI  TAGS  VALUE`` (plus optional warning/history indents)."""
    w = widths or column_widths_for_metrics([(row.metric_name, row.entry)])
    tags = _tags_cell(row.tags)
    value = _value_cell(row)
    lines = [f"{row.metric_name:<{w.name}}  {tags:<{w.tags}}  {value}"]

    obs = row.observation
    # When a warning is the VALUE cell, don't repeat it underneath.
    used_warning_as_value = obs.display_value is None and not obs.error and bool(obs.warnings)
    extra_warnings = obs.warnings[1:] if used_warning_as_value else obs.warnings
    for warning in extra_warnings:
        lines.append(f"{indent}warning: {warning}")

    if row.recent_stored and obs.origin == "stored" and len(row.recent_stored) > 1:
        for point in row.recent_stored[1:]:
            lines.append(f"{indent}history: {format_datapoint_line(date=point.date, value=point.value)}")

    return lines


def format_kpi_resolved_block(row: KPIResolved, *, indent: str = "  ") -> list[str]:
    """Backward-compatible alias for :func:`format_kpi_resolved_line`."""
    return format_kpi_resolved_line(row, indent=indent)


def kpi_resolved_to_json(row: KPIResolved) -> dict[str, Any]:
    obs = row.observation
    return {
        "metric_name": row.metric_name,
        "metric_id": row.metric_id,
        "automated": row.automated,
        "tags": list(row.tags),
        "description": row.description,
        "origin": obs.origin,
        "current_value": obs.display_value,
        "current_value_date": obs.as_of,
        "numerator": obs.numerator,
        "denominator": obs.denominator,
        "window_days": obs.window_days,
        "source": list(obs.source),
        "warnings": list(obs.warnings),
        "error": obs.error,
        "recent_stored": [{"date": p.date, "value": p.value} for p in row.recent_stored],
    }
