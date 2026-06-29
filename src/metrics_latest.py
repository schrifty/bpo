"""Recent MetricDataPoint rows per ``config/my-metrics.yaml`` registry entry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.leandna_metrics_client import (
    fetch_metric_datapoints,
    metric_requested_sites,
    resolve_metric_catalog_row,
    resolve_metric_datapoint_window,
)
from src.metrics_registry import (
    datapoint_metric_ids_for_entry,
    is_automated_metric,
    iter_metrics_with_id,
    registry_metric_description,
)

DEFAULT_RECENT_DATAPOINT_COUNT = 3


@dataclass(frozen=True)
class DatapointValue:
    date: str
    value: Any


@dataclass(frozen=True)
class MetricRecentDatapointsRow:
    metric_name: str
    metric_id: int
    recent: tuple[DatapointValue, ...]
    error: str | None = None
    automated: bool = False
    description: str | None = None


def datapoint_value_from_row(row: dict[str, Any]) -> DatapointValue | None:
    raw_date = str(row.get("dataPointDate") or "").strip()
    if not raw_date:
        return None
    return DatapointValue(date=raw_date[:10], value=row.get("value"))


def recent_datapoints_from_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> tuple[DatapointValue, ...]:
    """Return up to *limit* newest datapoints, newest first."""
    out: list[DatapointValue] = []
    for row in reversed(rows):
        point = datapoint_value_from_row(row)
        if point is None:
            continue
        out.append(point)
        if len(out) >= max(1, limit):
            break
    return tuple(out)


def latest_datapoint_from_rows(rows: list[dict[str, Any]]) -> tuple[str | None, Any]:
    """Return ``(date, value)`` for the newest row by ``dataPointDate``."""
    recent = recent_datapoints_from_rows(rows, limit=1)
    if not recent:
        return None, None
    return recent[0].date, recent[0].value


def format_datapoint_line(*, date: str | None, value: Any) -> str:
    """Format one datapoint as ``{date}: {value}``."""
    if date is None:
        return "(no datapoints)"
    return f"{date}: {value}"


def format_metric_recent_block(
    row: MetricRecentDatapointsRow,
    *,
    indent: str = "  ",
) -> list[str]:
    """Human-readable lines for one metric and its recent datapoints."""
    tag = "[automated]" if row.automated else "[manual]"
    description = (row.description or "").strip()
    name_and_description = f"{row.metric_name} - {description}" if description else row.metric_name
    header = f"{name_and_description} {tag}:"
    if row.error:
        return [header, f"{indent}(error: {row.error})"]
    if not row.recent:
        return [header, f"{indent}(no datapoints)"]
    lines = [header]
    lines.extend(f"{indent}{format_datapoint_line(date=p.date, value=p.value)}" for p in row.recent)
    return lines


def fetch_recent_datapoints_for_metric_id(
    metric_id: int,
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> tuple[tuple[DatapointValue, ...], str | None]:
    """Fetch the newest datapoints for one catalog id."""
    catalog_row = resolve_metric_catalog_row(metric_id, timeout_seconds=min(timeout_seconds, 30.0))
    sites = metric_requested_sites(catalog_row or {}, requested_sites)
    start_s, end_s = resolve_metric_datapoint_window(lookback_days=lookback_days)
    rows, err = fetch_metric_datapoints(
        metric_id,
        start_date=start_s,
        end_date=end_s,
        requested_sites=sites,
        timeout_seconds=timeout_seconds,
    )
    if err is not None:
        return (), str(err.get("error") or err)
    return recent_datapoints_from_rows(rows, limit=limit), None


def fetch_recent_datapoints_with_fallbacks(
    metric_ids: list[int],
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> tuple[tuple[DatapointValue, ...], str | None]:
    """Try each catalog id until datapoints are found or all attempts fail."""
    last_error: str | None = None
    for metric_id in metric_ids:
        recent, error = fetch_recent_datapoints_for_metric_id(
            metric_id,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            limit=limit,
        )
        if error is not None:
            last_error = error
            continue
        if recent:
            return recent, None
    return (), last_error


def fetch_latest_datapoint_for_metric_id(
    metric_id: int,
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
) -> tuple[str | None, Any, str | None]:
    """Fetch the newest datapoint for one catalog id. Returns ``(date, value, error)``."""
    recent, error = fetch_recent_datapoints_for_metric_id(
        metric_id,
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        limit=1,
    )
    if error is not None:
        return None, None, error
    if not recent:
        return None, None, None
    return recent[0].date, recent[0].value, None


def fetch_latest_datapoint_with_fallbacks(
    metric_ids: list[int],
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
) -> tuple[str | None, Any, str | None]:
    """Try each catalog id until a datapoint is found or all attempts fail."""
    recent, error = fetch_recent_datapoints_with_fallbacks(
        metric_ids,
        requested_sites=requested_sites,
        lookback_days=lookback_days,
        timeout_seconds=timeout_seconds,
        limit=1,
    )
    if error is not None and not recent:
        return None, None, error
    if not recent:
        return None, None, error
    return recent[0].date, recent[0].value, None


def fetch_registry_recent_datapoints(
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
    limit: int = DEFAULT_RECENT_DATAPOINT_COUNT,
) -> list[MetricRecentDatapointsRow]:
    """Recent datapoints for each ``my-metrics.yaml`` row with a ``metric-id``."""
    rows: list[MetricRecentDatapointsRow] = []
    for name, metric_id, entry in iter_metrics_with_id():
        recent, error = fetch_recent_datapoints_with_fallbacks(
            datapoint_metric_ids_for_entry(entry, metric_id),
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
            limit=limit,
        )
        rows.append(
            MetricRecentDatapointsRow(
                metric_name=name,
                metric_id=metric_id,
                recent=recent,
                error=error,
                automated=is_automated_metric(entry),
                description=registry_metric_description(entry),
            )
        )
    return rows


# Backward-compatible aliases for callers expecting the previous names.
LatestDatapointRow = MetricRecentDatapointsRow
format_latest_datapoint_line = format_datapoint_line
fetch_registry_latest_datapoints = fetch_registry_recent_datapoints
