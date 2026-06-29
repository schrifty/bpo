"""Tests for LeanDNA metrics CLI display helpers."""

from __future__ import annotations

import io

from src.leandna_metrics_display import (
    format_date_value_chart,
    extract_date_value_pairs,
    metric_definition_for_json_display,
    print_metric_block_display,
    print_metrics_grouped_display,
)


def test_metric_definition_for_json_display_strips_series() -> None:
    block = {"id": 1, "name": "OTIF", "dataSeries": [{"dataPointDate": "x", "value": 1}]}
    assert metric_definition_for_json_display(block) == {"id": 1, "name": "OTIF"}


def test_print_metrics_grouped_display(capsys) -> None:
    buf = io.StringIO()
    print_metrics_grouped_display(
        [
            {
                "id": 99,
                "name": "Median TTR",
                "dataWindow": {"startDate": "2026-01-01", "endDate": "2026-03-31"},
                "dataSeries": [
                    {"dataPointDate": "2026-01-15", "value": 1.0},
                    {"dataPointDate": "2026-03-01", "value": 3.0},
                ],
            }
        ],
        values_key="dataSeries",
        out=buf,
    )
    text = buf.getvalue()
    assert "=== Median TTR (id=99) ===" in text
    assert '"name": "Median TTR"' in text
    assert "dataSeries" not in text
    assert "2026-01-15\t1.0" in text
    assert "2026-03-01\t3.0" in text


def test_format_date_value_chart_scales_bars() -> None:
    pairs = extract_date_value_pairs(
        [
            {"dataPointDate": "2026-01-01", "value": 1},
            {"dataPointDate": "2026-01-08", "value": 3},
            {"dataPointDate": "2026-01-15", "value": 2},
        ]
    )
    lines = format_date_value_chart(pairs, bar_width=10)
    assert lines[0] == "date            value       chart"
    assert "2026-01-01" in lines[2]
    assert "█" in "\n".join(lines)
    assert any("min=1" in line for line in lines)


def test_print_metric_block_display_shows_error() -> None:
    buf = io.StringIO()
    print_metric_block_display(
        {
            "id": 1,
            "name": "X",
            "dataSeries": [],
            "dataSeriesError": {"status": 403, "error": "forbidden"},
        },
        values_key="dataSeries",
        out=buf,
    )
    assert "MetricDataPoint error: HTTP 403" in buf.getvalue()
