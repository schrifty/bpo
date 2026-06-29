"""Human-readable CLI output for LeanDNA metric definitions + datapoint series."""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO


def metric_display_name(block: dict[str, Any]) -> str:
    return str(
        block.get("name") or block.get("metricName") or block.get("crossSiteName") or block.get("id") or ""
    ).strip()


def extract_date_value_pairs(rows: list[dict[str, Any]]) -> list[tuple[str, Any, float | None]]:
    """``(dataPointDate, raw value, float value or None)`` per row (caller sorts by date)."""
    out: list[tuple[str, Any, float | None]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = str(r.get("dataPointDate") or "").strip()
        raw = r.get("value")
        fv: float | None
        try:
            fv = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            fv = None
        out.append((d, raw, fv))
    return out


def format_date_value_chart(
    pairs: list[tuple[str, Any, float | None]],
    *,
    bar_width: int = 36,
    heading: str | None = None,
) -> list[str]:
    """ASCII chart: one row per date with value and a horizontal bar."""
    floats = [p[2] for p in pairs if p[2] is not None]
    lines: list[str] = []
    if heading is not None:
        lines.append(heading)
    if not pairs:
        lines.append("(no datapoints)")
        return lines
    if not floats:
        lines.append("date            value")
        lines.append("----------------+-----------")
        for d, raw, _ in pairs:
            lines.append(f"{d.ljust(16)}| {str(raw).rjust(9)}")
        lines.append("(values are non-numeric — no bar scale)")
        return lines

    vmin, vmax = min(floats), max(floats)
    span = vmax - vmin
    lines.append("date            value       chart")
    lines.append("----------------+-----------+" + ("-" * bar_width))
    if span <= 0:
        bar_len = max(1, min(bar_width, bar_width // 2))
        for d, raw, _fv in pairs:
            bar = "█" * bar_len
            lines.append(f"{d.ljust(16)}| {str(raw).rjust(9)} | {bar}")
        lines.append(f"(flat series: value = {vmin})")
        return lines

    for d, raw, fv in pairs:
        if fv is None:
            bar = "(n/a)"
        else:
            frac = (fv - vmin) / span
            n = max(1, min(bar_width, int(round(frac * bar_width)) or 1))
            bar = "█" * n
        lines.append(f"{d.ljust(16)}| {str(raw).rjust(9)} | {bar}")
    lines.append(f"(bars scaled min={vmin} → max={vmax})")
    return lines


def print_metric_value_chart(
    metric: dict[str, Any],
    points: list[dict[str, Any]],
    *,
    out: TextIO | None = None,
    max_points: int = 10,
) -> None:
    """Print a KPI heading and ASCII chart for the last *max_points* datapoints."""
    sink = out or sys.stdout
    mid = metric.get("ndx", metric.get("id", ""))
    name = metric_display_name(metric)
    sink.write(f"\n=== {name} (id={mid}) ===\n")
    if not points:
        sink.write("(no datapoints in lookback window)\n")
        return
    tail = points[-max_points:] if len(points) > max_points else points
    pairs = extract_date_value_pairs(tail)
    for line in format_date_value_chart(pairs, heading=None):
        sink.write(line + "\n")


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
    """Print ``dataPointDate`` / ``value`` rows (same layout as ``metric-get-with-data`` brief)."""
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
