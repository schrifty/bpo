"""Resolve KPIs live from generators and/or from LeanDNA Data API storage.

Persistence is optional. Registry rows define how to compute a KPI; ``metric-id``
is only required when reading or writing the Data API store. Decks and CLIs
should call ``resolve_kpi`` / ``resolve_kpis_by_tag`` instead of assuming a
stored datapoint exists.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from src.kpi_observation import (
    KPIObservation,
    observation_from_generator_raw,
    observation_from_stored_datapoint,
    stored_is_fresh,
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

ResolveMode = Literal["auto", "live", "stored"]
RESOLVE_MODES: tuple[str, ...] = ("auto", "live", "stored")

DEFAULT_STORED_MAX_AGE_HOURS = 48.0


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


def stored_max_age_hours(*, override: float | None = None) -> float:
    if override is not None:
        return float(override)
    raw = (os.environ.get("CORTEX_KPI_STORED_MAX_AGE_HOURS") or "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            logger.warning("Invalid CORTEX_KPI_STORED_MAX_AGE_HOURS=%r; using %s", raw, DEFAULT_STORED_MAX_AGE_HOURS)
    return DEFAULT_STORED_MAX_AGE_HOURS


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
    mode: ResolveMode = "auto",
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    max_stored_age_hours: float | None = None,
    allow_stored_fetch: bool = True,
) -> KPIResolved:
    """Resolve one KPI under ``mode`` (``auto`` | ``live`` | ``stored``).

    * **live** — always run ``metric-generator`` when present.
    * **stored** — read LeanDNA Data API only (``metric-id`` required for a value).
    * **auto** — use a fresh stored datapoint when available; otherwise live; otherwise
      empty / stale stored with a warning.
    """
    if mode not in RESOLVE_MODES:
        raise ValueError(f"mode must be one of {RESOLVE_MODES}, got {mode!r}")

    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or default_resolve_context(
        timeout_seconds=timeout_seconds,
        requested_sites=requested_sites,
    )
    age_h = stored_max_age_hours(override=max_stored_age_hours)
    tags = tuple(registry_metric_tags(entry))
    description = registry_metric_description(entry)
    automated = is_automated_metric(entry)
    metric_id = _metric_id_or_none(entry)

    recent: tuple[DatapointValue, ...] = ()
    stored_error: str | None = None
    if allow_stored_fetch and mode in ("auto", "stored") and metric_id is not None:
        recent, stored_error = fetch_stored_recent(
            entry,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            limit=recent_count,
        )

    def _from_stored() -> KPIObservation:
        if metric_id is None:
            return _empty_observation(warning="no metric-id in registry — no stored value")
        if stored_error:
            return KPIObservation(error=stored_error, origin="stored", as_of=None)
        if not recent:
            return _empty_observation(warning="no datapoints")
        return observation_from_stored_datapoint(
            date_s=recent[0].date,
            value=recent[0].value,
            metric_id=metric_id,
        )

    if mode == "live":
        observation = generate_live_observation(metric_name, entry, registry=reg, ctx=resolve_ctx)
    elif mode == "stored":
        observation = _from_stored()
    else:
        # auto
        stored_obs = _from_stored() if (allow_stored_fetch and metric_id is not None) else _empty_observation()
        if (
            stored_obs.origin == "stored"
            and stored_obs.ok
            and stored_is_fresh(stored_obs.as_of, max_age_hours=age_h)
        ):
            observation = stored_obs
        elif has_metric_generator(entry):
            observation = generate_live_observation(metric_name, entry, registry=reg, ctx=resolve_ctx)
            if not observation.ok and stored_obs.origin == "stored" and stored_obs.display_value is not None:
                # Live failed but we have a (possibly stale) stored value — keep stored + warn.
                warnings = stored_obs.warnings + (
                    f"live generation failed ({observation.error}); using stored value",
                )
                if not stored_is_fresh(stored_obs.as_of, max_age_hours=age_h):
                    warnings = warnings + (f"stored value older than {age_h:g}h",)
                observation = KPIObservation(
                    value=stored_obs.value,
                    numerator=stored_obs.numerator,
                    denominator=stored_obs.denominator,
                    as_of=stored_obs.as_of,
                    window_days=stored_obs.window_days,
                    source=stored_obs.source,
                    warnings=warnings,
                    error=None,
                    origin="stored",
                    meta=dict(stored_obs.meta),
                )
        elif stored_obs.origin == "stored" and stored_obs.display_value is not None:
            warnings = stored_obs.warnings
            if not stored_is_fresh(stored_obs.as_of, max_age_hours=age_h):
                warnings = warnings + (f"stored value older than {age_h:g}h (no generator for refresh)",)
            observation = KPIObservation(
                value=stored_obs.value,
                numerator=stored_obs.numerator,
                denominator=stored_obs.denominator,
                as_of=stored_obs.as_of,
                window_days=stored_obs.window_days,
                source=stored_obs.source,
                warnings=warnings,
                error=stored_obs.error,
                origin="stored",
                meta=dict(stored_obs.meta),
            )
        else:
            if metric_id is None and not has_metric_generator(entry):
                observation = _empty_observation(
                    warning="no metric-id and no metric-generator — nothing to resolve",
                )
            else:
                observation = stored_obs if stored_obs.warnings or stored_obs.error else _empty_observation(
                    warning="no datapoints",
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


def resolve_kpis_by_tag(
    tag: str,
    *,
    mode: ResolveMode = "auto",
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    recent_count: int = DEFAULT_RECENT_DATAPOINT_COUNT,
    max_stored_age_hours: float | None = None,
    allow_stored_fetch: bool = True,
) -> list[KPIResolved]:
    """Resolve every registry KPI carrying *tag*."""
    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or default_resolve_context(
        timeout_seconds=timeout_seconds,
        requested_sites=requested_sites,
    )
    out: list[KPIResolved] = []
    for name, entry in iter_metrics_by_tag(tag, registry=reg):
        out.append(
            resolve_kpi(
                name,
                entry,
                mode=mode,
                registry=reg,
                ctx=resolve_ctx,
                requested_sites=requested_sites,
                lookback_days=lookback_days,
                timeout_seconds=timeout_seconds,
                recent_count=recent_count,
                max_stored_age_hours=max_stored_age_hours,
                allow_stored_fetch=allow_stored_fetch,
            )
        )
    return out


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


def format_kpi_resolved_block(row: KPIResolved, *, indent: str = "  ") -> list[str]:
    """Human-readable lines for one resolved KPI."""
    tag = "[automated]" if row.automated else "[manual]"
    description = (row.description or "").strip()
    name_and_description = f"{row.metric_name} - {description}" if description else row.metric_name
    tag_suffix = f" [tags: {', '.join(row.tags)}]" if row.tags else ""
    header = f"{name_and_description} {tag}{tag_suffix}:"
    obs = row.observation
    lines = [header]

    if obs.error:
        lines.append(f"{indent}(error: {obs.error}) [origin={obs.origin}]")
        return lines

    if obs.display_value is None:
        if obs.warnings:
            for w in obs.warnings:
                lines.append(f"{indent}({w})")
        else:
            lines.append(f"{indent}(no value)")
        return lines

    as_of = obs.as_of or "?"
    origin = obs.origin
    lines.append(f"{indent}{format_datapoint_line(date=as_of, value=obs.display_value)} [{origin}]")
    for w in obs.warnings:
        lines.append(f"{indent}warning: {w}")

    # Under stored/auto, show older history when available.
    if row.recent_stored and origin == "stored" and len(row.recent_stored) > 1:
        for point in row.recent_stored[1:]:
            lines.append(f"{indent}{format_datapoint_line(date=point.date, value=point.value)}")

    return lines


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
