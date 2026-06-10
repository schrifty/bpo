"""Latest MetricDataPoint per ``config/my-metrics.yaml`` registry row."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.leandna_metrics_client import (
    fetch_metric_datapoints,
    metric_requested_sites,
    resolve_metric_catalog_row,
    resolve_metric_datapoint_window,
)
from src.metrics_registry import datapoint_metric_ids_for_entry, iter_metrics_with_id


@dataclass(frozen=True)
class LatestDatapointRow:
    metric_name: str
    metric_id: int
    date: str | None
    value: Any
    error: str | None = None


def latest_datapoint_from_rows(rows: list[dict[str, Any]]) -> tuple[str | None, Any]:
    """Return ``(date, value)`` for the newest row by ``dataPointDate``."""
    if not rows:
        return None, None
    latest = rows[-1]
    raw_date = str(latest.get("dataPointDate") or "").strip()
    date = raw_date[:10] if raw_date else None
    return date, latest.get("value")


def format_latest_datapoint_line(*, date: str | None, value: Any) -> str:
    """Format one datapoint as ``{date}: {value}``."""
    if date is None:
        return "(no datapoints)"
    return f"{date}: {value}"


def fetch_latest_datapoint_for_metric_id(
    metric_id: int,
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
) -> tuple[str | None, Any, str | None]:
    """Fetch the newest datapoint for one catalog id. Returns ``(date, value, error)``."""
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
        msg = str(err.get("error") or err)
        return None, None, msg
    date, value = latest_datapoint_from_rows(rows)
    return date, value, None


def fetch_latest_datapoint_with_fallbacks(
    metric_ids: list[int],
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
) -> tuple[str | None, Any, str | None]:
    """Try each catalog id until a datapoint is found or all attempts fail."""
    last_error: str | None = None
    for metric_id in metric_ids:
        date, value, error = fetch_latest_datapoint_for_metric_id(
            metric_id,
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
        )
        if error is not None:
            last_error = error
            continue
        if date is not None:
            return date, value, None
    return None, None, last_error


def fetch_registry_latest_datapoints(
    *,
    requested_sites: str | None = None,
    lookback_days: int = 365,
    timeout_seconds: float = 60.0,
) -> list[LatestDatapointRow]:
    """Latest datapoint for each ``my-metrics.yaml`` row with a ``metric-id``."""
    rows: list[LatestDatapointRow] = []
    for name, metric_id, entry in iter_metrics_with_id():
        date, value, error = fetch_latest_datapoint_with_fallbacks(
            datapoint_metric_ids_for_entry(entry, metric_id),
            requested_sites=requested_sites,
            lookback_days=lookback_days,
            timeout_seconds=timeout_seconds,
        )
        rows.append(
            LatestDatapointRow(
                metric_name=name,
                metric_id=metric_id,
                date=date,
                value=value,
                error=error,
            )
        )
    return rows
