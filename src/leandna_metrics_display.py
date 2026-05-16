"""Human-readable CLI output for LeanDNA metric definitions + datapoint series."""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO


def metric_display_name(block: dict[str, Any]) -> str:
    return str(block.get("name") or block.get("crossSiteName") or block.get("id") or "").strip()


def metric_definition_for_json_display(block: dict[str, Any], *, strip_series_keys: bool = True) -> dict[str, Any]:
    """Catalog / metadata fields for JSON header (omit bulky series arrays by default)."""
    if not strip_series_keys:
        return dict(block)
    return {k: v for k, v in block.items() if k not in ("dataSeries", "values")}


def print_metric_datapoint_table(
    points: list[dict[str, Any]],
    *,
    out: TextIO | None = None,
    empty_message: str = "(no datapoints in window)",
) -> None:
    """Print ``dataPointDate`` / ``value`` rows (same layout as ``get-metrics-data`` brief)."""
    sink = out or sys.stdout
    if not points:
        sink.write(f"{empty_message}\n")
        return
    sink.write("dataPointDate\tvalue\n")
    for p in points:
        if not isinstance(p, dict):
            continue
        d = str(p.get("dataPointDate") or "")
        v = p.get("value")
        sink.write(f"{d}\t{v}\n")


def print_metric_block_display(
    block: dict[str, Any],
    *,
    values_key: str = "values",
    include_json_definition: bool = True,
    out: TextIO | None = None,
) -> None:
    """One metric: optional definition JSON, then datapoints in a brief table."""
    sink = out or sys.stdout
    mid = block.get("id", "")
    name = metric_display_name(block)
    sink.write(f"=== {name} (id={mid}) ===\n")
    if include_json_definition:
        sink.write(
            json.dumps(
                metric_definition_for_json_display(block),
                indent=2,
                default=str,
                ensure_ascii=False,
            )
            + "\n"
        )
    err = block.get("dataSeriesError")
    if isinstance(err, dict):
        sink.write(
            f"MetricDataPoint error: HTTP {err.get('status')} {err.get('error')!r}\n"
        )
        return
    series = block.get(values_key)
    pts = series if isinstance(series, list) else []
    sink.write("\n")
    print_metric_datapoint_table(pts, out=sink)
    sink.write("\n")


def print_metrics_grouped_display(
    blocks: list[dict[str, Any]],
    *,
    values_key: str = "values",
    include_json_definition: bool = True,
    out: TextIO | None = None,
) -> None:
    """Section per metric: optional JSON definition then human-readable datapoint table."""
    sink = out or sys.stdout
    for block in blocks:
        if isinstance(block, dict):
            print_metric_block_display(
                block,
                values_key=values_key,
                include_json_definition=include_json_definition,
                out=sink,
            )


def print_metrics_datapoint_table(
    blocks: list[dict[str, Any]],
    *,
    values_key: str = "values",
    out: TextIO | None = None,
) -> None:
    """One TSV row per datapoint across all metrics."""
    sink = out or sys.stdout
    sink.write("metric_id\tmetric_name\tdataPointDate\tvalue\n")
    for block in blocks:
        if not isinstance(block, dict):
            continue
        mid = block.get("id", "")
        name = metric_display_name(block).replace("\t", " ")
        series = block.get(values_key)
        pts = series if isinstance(series, list) else []
        for p in pts:
            if not isinstance(p, dict):
                continue
            d = str(p.get("dataPointDate") or "")
            v = p.get("value")
            sink.write(f"{mid}\t{name}\t{d}\t{v}\n")
