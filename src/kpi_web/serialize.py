"""Serialize registry / ``KPIResolved`` rows for the KPI web JSON API."""

from __future__ import annotations

from typing import Any

from src.kpi_observation import KPIObservation
from src.kpi_service import KPIResolved
from src.metrics_latest import DatapointValue
from src.metrics_registry import (
    has_metric_generator,
    has_metric_id,
    is_automated_metric,
    registry_metric_description,
    registry_metric_direction,
    registry_metric_grain,
    registry_metric_mgmt_guidance,
    registry_metric_owner,
    registry_metric_tags,
    registry_metric_target,
    registry_metric_unit,
)


def observation_to_dict(obs: KPIObservation) -> dict[str, Any]:
    """JSON shape for one observation — preserves errors/warnings (fail loud)."""
    return {
        "ok": obs.ok,
        "value": obs.display_value,
        "raw_value": obs.value,
        "numerator": obs.numerator,
        "denominator": obs.denominator,
        "as_of": obs.as_of,
        "window_days": obs.window_days,
        "source": list(obs.source),
        "warnings": list(obs.warnings),
        "error": obs.error,
        "origin": obs.origin,
    }


def history_to_list(rows: tuple[DatapointValue, ...] | list[DatapointValue]) -> list[dict[str, Any]]:
    return [{"date": r.date, "value": r.value} for r in rows]


def catalog_entry_to_dict(name: str, entry: dict[str, Any]) -> dict[str, Any]:
    """Catalog metadata without resolving a live/stored value."""
    target = None
    direction = None
    unit = None
    target_error = None
    try:
        target = registry_metric_target(entry)
        direction = registry_metric_direction(entry)
        unit = registry_metric_unit(entry)
    except ValueError as exc:
        target_error = str(exc)
    return {
        "name": name,
        "owner": registry_metric_owner(entry),
        "description": registry_metric_description(entry),
        "mgmt_guidance": registry_metric_mgmt_guidance(entry),
        "tags": registry_metric_tags(entry),
        "target": target,
        "direction": direction,
        "unit": unit,
        "grain": registry_metric_grain(entry),
        "target_error": target_error,
        "metric_id": int(entry["metric-id"]) if has_metric_id(entry) else None,
        "metric_generator": (
            str(entry.get("metric-generator") or "").strip() or None
            if has_metric_generator(entry)
            else None
        ),
        "automated": is_automated_metric(entry),
    }


def resolved_to_dict(row: KPIResolved, *, include_history: bool = True) -> dict[str, Any]:
    base = catalog_entry_to_dict(row.metric_name, row.entry)
    base["observation"] = observation_to_dict(row.observation)
    if include_history:
        base["history"] = history_to_list(row.recent_stored)
    return base
