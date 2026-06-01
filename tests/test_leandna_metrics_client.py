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
        assert kwargs["timeout"] == (15.0, 120.0)


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
        assert kwargs["timeout"] == (15.0, 180.0)


def test_unwrap_metric_datapoint_rows_list_and_wrapped() -> None:
    from src.leandna_metrics_client import unwrap_metric_datapoint_rows

    assert unwrap_metric_datapoint_rows([{"dataPointDate": "2026-01-01", "value": 1}]) == [
        {"dataPointDate": "2026-01-01", "value": 1}
    ]
    assert unwrap_metric_datapoint_rows(
        {"metricDataPoints": [{"dataPointDate": "2026-02-01", "value": 2}]}
    ) == [{"dataPointDate": "2026-02-01", "value": 2}]
    assert unwrap_metric_datapoint_rows({}) == []


def test_resolve_metric_datapoint_window_explicit_and_lookback() -> None:
    from datetime import date, timedelta

    from src.leandna_metrics_client import resolve_metric_datapoint_window

    assert resolve_metric_datapoint_window(
        start_date="2026-01-01", end_date="2026-03-31", lookback_days=7
    ) == ("2026-01-01", "2026-03-31")

    start_s, end_s = resolve_metric_datapoint_window(
        lookback_days=10,
        end_date="2026-05-01",
    )
    assert end_s == "2026-05-01"
    assert start_s == (date.fromisoformat("2026-05-01") - timedelta(days=10)).isoformat()


def test_fetch_metric_datapoints_sorts_and_error(monkeypatch) -> None:
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", "")

    from src.leandna_metrics_client import fetch_metric_datapoints

    with patch("src.leandna_data_api_request.data_api_get_json") as mock_get:
        mock_get.return_value = {
            "ok": True,
            "body": [
                {"dataPointDate": "2026-03-01", "value": 3},
                {"dataPointDate": "2026-01-15", "value": 1},
            ],
        }
        rows, err = fetch_metric_datapoints(
            99,
            start_date="2026-01-01",
            end_date="2026-03-31",
            requested_sites="416",
        )
        assert err is None
        assert [r["dataPointDate"] for r in rows] == ["2026-01-15", "2026-03-01"]
        mock_get.assert_called_once()
        assert mock_get.call_args.args[0] == "Metric/99/MetricDataPoint"
        assert mock_get.call_args.kwargs["query"] == {
            "startDate": "2026-01-01",
            "endDate": "2026-03-31",
        }
        assert mock_get.call_args.kwargs["requested_sites"] == "416"

    with patch("src.leandna_data_api_request.data_api_get_json") as mock_get:
        mock_get.return_value = {"ok": False, "status": 403, "error": "forbidden"}
        rows, err = fetch_metric_datapoints(1, start_date="2026-01-01", end_date="2026-01-31")
        assert rows == []
        assert err == {"ok": False, "status": 403, "error": "forbidden"}


def test_slim_metric_datapoint_rows() -> None:
    from src.leandna_metrics_client import slim_metric_datapoint_rows

    assert slim_metric_datapoint_rows(
        [
            {"dataPointDate": "2026-01-01", "value": 1.5, "extra": "drop"},
            "skip",
        ]
    ) == [{"dataPointDate": "2026-01-01", "value": 1.5}]


def test_find_similar_metric_definitions_ranks_ttr() -> None:
    from src.leandna_metrics_client import find_similar_metric_definitions

    catalog = [
        {"id": 1, "name": "Job success rate"},
        {"id": 2, "name": "Time-To-Resolution (30d)"},
        {"id": 3, "name": "Time to first response"},
    ]
    hits = find_similar_metric_definitions(catalog, "time to resolution", window_days=30)
    assert hits
    assert hits[0]["id"] == 2
    assert hits[0]["match_score"] >= 0.9


def test_summarize_metric_datapoint_values() -> None:
    from src.leandna_metrics_client import summarize_metric_datapoint_values

    s = summarize_metric_datapoint_values(
        [
            {"dataPointDate": "2026-01-01", "value": 4.0},
            {"dataPointDate": "2026-01-15", "value": 8.0},
        ]
    )
    assert s["measured"] == 2
    assert s["median"] == 6.0
    assert s["latest"] == 8.0


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


def test_build_metric_datapoint_post_body_percentage() -> None:
    from src.leandna_metrics_client import build_metric_datapoint_post_body

    body = build_metric_datapoint_post_body(
        metric_id=2076,
        data_point_date="2026-05-22",
        numerator=1,
        denominator=100,
        category="Engineering",
    )
    assert body["value"] == 1.0
    assert body["numeratorValue"] == 1
    assert body["denominatorValue"] == 100
    assert body["metricId"] == 2076
    assert body["category"] == "Engineering"


def test_post_metric_datapoint_calls_mutate(monkeypatch) -> None:
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    with patch("src.leandna_data_api_request.data_api_mutate_json") as mock_mutate:
        mock_mutate.return_value = {"ok": True, "status": 201, "body": {"id": 1}}
        from src.leandna_metrics_client import post_metric_datapoint

        body = {"dataPointDate": "2026-05-22", "value": 1.0}
        env = post_metric_datapoint(2076, body, requested_sites="416")
        assert env["ok"] is True
        mock_mutate.assert_called_once()
        assert mock_mutate.call_args.args == ("POST", "Metric/2076/MetricDataPoint")
        assert mock_mutate.call_args.kwargs["json_body"] == body
        assert mock_mutate.call_args.kwargs["requested_sites"] == "416"


def test_delete_metric_datapoint_calls_mutate(monkeypatch) -> None:
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    with patch("src.leandna_data_api_request.data_api_mutate_json") as mock_mutate:
        mock_mutate.return_value = {"ok": True, "status": 200}
        from src.leandna_metrics_client import delete_metric_datapoint

        env = delete_metric_datapoint(2076, data_point_date="2026-05-22", requested_sites="416")
        assert env["ok"] is True
        mock_mutate.assert_called_once()
        assert mock_mutate.call_args.args == ("DELETE", "Metric/2076/MetricDataPoint")
        assert mock_mutate.call_args.kwargs["query"] == {
            "startDate": "2026-05-22",
            "endDate": "2026-05-22",
        }


def test_run_upsert_deletes_then_inserts(monkeypatch) -> None:
    monkeypatch.setattr("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", "tok")
    monkeypatch.setattr("src.leandna_metric_write_cli.leandna_data_api_credentials_configured", lambda: True)
    monkeypatch.setattr("src.leandna_metric_write_cli.leandna_app_session_configured", lambda: False)
    monkeypatch.setattr("src.leandna_metric_write_cli.misconfigured_app_session_message", lambda: None)
    monkeypatch.setattr("src.leandna_metric_write_cli._resolve_data_api_metadata", lambda _a: ("416", "", None))
    monkeypatch.setattr("src.leandna_metric_write_cli.metric_datapoint_exists_for_date", lambda *a, **k: True)

    from src.leandna_metric_write_cli import MetricWriteArgs, run_upsert

    calls: list[str] = []

    def _delete(_args, *, sites):
        calls.append("delete")
        return {"ok": True, "status": 200}

    def _insert(_args):
        calls.append("insert")
        return {"ok": True, "status": 201}

    monkeypatch.setattr("src.leandna_metric_write_cli._delete_via_data_api", _delete)
    monkeypatch.setattr("src.leandna_metric_write_cli._insert_via_data_api", _insert)

    args = MetricWriteArgs(
        metric_id=2076,
        entry_date="2026-05-22",
        numerator=1,
        denominator=100,
        requested_sites=None,
        category=None,
        skip_catalog=False,
        timeout_seconds=30,
        value_stream_ndx=None,
        factory_ndx=416,
        verbose=False,
    )
    code, env = run_upsert(args)
    assert code == 0
    assert env["deleted"] is True
    assert calls == ["delete", "insert"]
