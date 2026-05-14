"""Tests for LeanDNA Metrics Data API client."""

from __future__ import annotations

import sys
from typing import Any

from unittest.mock import MagicMock, patch


def test_displays_single_kpi_value_from_metric_report(monkeypatch, capsys) -> None:
    """HTTP is **mocked** — no LeanDNA call. Exercises ``format_first_kpi_line_from_metric_report`` + terminal display."""
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")
    report_body: dict[str, Any] = {
        "fiscalYear": 2026,
        "metrics": [{"id": 501, "name": "Supplier On-Time %", "siteId": 1}],
        "metricValues": [
            {
                "metricId": 501,
                "dataPointDate": "2026-04-01",
                "value": 94.25,
                "valueStreamId": 10,
            }
        ],
    }
    with patch("src.leandna_metrics_client.requests.get") as mock_get:
        mock_get.return_value = MagicMock()
        mock_get.return_value.json.return_value = report_body
        mock_get.return_value.raise_for_status = MagicMock()

        from src.leandna_metrics_client import fetch_metric_report, format_first_kpi_line_from_metric_report

        report = fetch_metric_report(2026, metric_ids=["501"])

    line = format_first_kpi_line_from_metric_report(report)
    assert line == "KPI: Supplier On-Time % = 94.25% (FY2026)"
    # Bypass pytest capture so the KPI line appears in the IDE / terminal without ``-s``.
    with capsys.disabled():
        sys.stdout.write("\n--- LeanDNA KPI (test display) ---\n")
        sys.stdout.write(f"{line}\n")
        sys.stdout.write("---------------------------------\n")
        sys.stdout.flush()


def test_list_metric_definitions_raw_list(monkeypatch):
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")
    with patch("src.leandna_metrics_client.requests.get") as mock_get:
        mock_get.return_value = MagicMock()
        mock_get.return_value.json.return_value = [{"id": "1", "name": "OTIF"}]
        mock_get.return_value.raise_for_status = MagicMock()

        from src.leandna_metrics_client import list_metric_definitions

        rows = list_metric_definitions()
        assert len(rows) == 1
        assert rows[0]["name"] == "OTIF"
        args, kwargs = mock_get.call_args
        assert args[0].endswith("/data/Metric")
        assert kwargs["headers"]["Authorization"] == "Bearer tok"


def test_list_metric_definitions_wrapped(monkeypatch):
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")
    with patch("src.leandna_metrics_client.requests.get") as mock_get:
        mock_get.return_value = MagicMock()
        mock_get.return_value.json.return_value = {"metrics": [{"id": "a"}]}
        mock_get.return_value.raise_for_status = MagicMock()

        from src.leandna_metrics_client import list_metric_definitions

        assert list_metric_definitions() == [{"id": "a"}]


def test_fetch_metric_report_query(monkeypatch):
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")
    with patch("src.leandna_metrics_client.requests.get") as mock_get:
        mock_get.return_value = MagicMock()
        mock_get.return_value.json.return_value = {
            "fiscalYear": 2026,
            "metrics": [],
            "metricValues": [],
        }
        mock_get.return_value.raise_for_status = MagicMock()

        from src.leandna_metrics_client import fetch_metric_report

        out = fetch_metric_report(
            2026,
            metric_ids=["m1", " m2 "],
            value_streams=["vsA"],
        )
        assert out["fiscalYear"] == 2026
        _args, kwargs = mock_get.call_args
        assert kwargs["params"]["fiscalYear"] == 2026
        assert kwargs["params"]["metrics"] == "m1,m2"
        assert kwargs["params"]["valueStreams"] == "vsA"


def test_missing_token_raises(monkeypatch):
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")
    from src.leandna_metrics_client import list_metric_definitions

    try:
        list_metric_definitions()
    except ValueError as e:
        assert "LEANDNA_DATA_API_COOKIE" in str(e) or "LEANDNA_DATA_API_BEARER_TOKEN" in str(e)
    else:
        raise AssertionError("expected ValueError")
