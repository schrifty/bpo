"""Resolve KPIs live from generators.

Reads are always live: registry generators compute the value each time. The
LeanDNA Data API is the system of record for *stored* KPI history — this module
never reads a stored datapoint to satisfy a normal read, and generating never
writes. Persist explicitly with ``metrics-upsert`` / ``materialize_kpi``.

An explicit ``mode="stored"`` is offered only to *inspect* what the Data API
currently holds; it is not part of the default read path.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from src.kpi_observation import (
    KPIObservation,
    observation_from_generator_raw,
    observation_from_stored_datapoint,
)
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
    iter_metrics_by_tag,
    load_metrics_registry,
    registry_metric_description,
    registry_metric_tags,
)
from src.metrics_upsert import MetricUpsertContext, MetricUpsertError, invoke_metric_generator

logger = logging.getLogger(__name__)

ResolveMode = Literal["live", "stored"]
RESOLVE_MODES: tuple[str, ...] = ("live", "stored")
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


def fetch_stored_recent(
    entry: dict[str, Any],
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> tuple[tuple[DatapointValue, ...], str | None]:
    """Read recent Data API datapoints for a registry entry (empty if no metric-id)."""
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
) -> KPIResolved:
    """Resolve one KPI under ``mode`` (``live`` default, or ``stored``).

    * **live** (default) — run the ``metric-generator`` and return a freshly
      computed value. The Data API is never read.
    * **stored** — read the LeanDNA Data API only, to inspect what is persisted
      (``metric-id`` required for a value). Not part of the default read path.
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
    else:  # stored — explicit Data API inspection only
        if metric_id is None:
            observation = _empty_observation(warning="no metric-id in registry — no stored value")
        else:
            recent, stored_error = fetch_stored_recent(
                entry,
                requested_sites=requested_sites,
                lookback_days=lookback_days,
                timeout_seconds=timeout_seconds,
                limit=recent_count,
            )
            if stored_error:
                observation = KPIObservation(error=stored_error, origin="stored", as_of=None)
            elif not recent:
                observation = _empty_observation(warning="no datapoints")
            else:
                observation = observation_from_stored_datapoint(
                    date_s=recent[0].date,
                    value=recent[0].value,
                    metric_id=metric_id,
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
) -> Iterator[KPIResolved]:
    """Yield each registry KPI carrying *tag* as soon as it resolves (live by default)."""
    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or default_resolve_context(
        timeout_seconds=timeout_seconds,
        requested_sites=requested_sites,
    )
    for name, entry in iter_metrics_by_tag(tag, registry=reg):
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
) -> list[KPIResolved]:
    """Resolve every registry KPI carrying *tag* (live by default)."""
    return list(
        iter_resolve_kpis_by_tag(
            tag,
            mode=mode,
            registry=registry,
            ctx=ctx,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            recent_count=recent_count,
        )
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
    reg = registry if registry is not None else load_metrics_registry()
    return column_widths_for_metrics(iter_metrics_by_tag(tag, registry=reg))


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
