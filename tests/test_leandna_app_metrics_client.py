"""Tests for LeanDNA classic app metrics client (session auth, no live HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_parse_ldna_session_id_from_cookie() -> None:
    from src.leandna_app_metrics_http import parse_ldna_session_id

    assert parse_ldna_session_id("LDNASESSIONID=abc123; other=x") == "abc123"
    assert parse_ldna_session_id("") is None


def test_normalize_metric_view_adds_id_alias() -> None:
    from src.leandna_app_metrics_client import normalize_metric_view_rows

    rows = normalize_metric_view_rows([{"ndx": 99, "metricName": "Job success rate"}])
    assert rows[0]["id"] == 99
    assert rows[0]["name"] == "Job success rate"


def test_build_metric_entry_put_body_percentage() -> None:
    from src.leandna_app_metrics_client import build_metric_entry_put_body

    body = build_metric_entry_put_body(
        metric_ndx=1,
        value_stream_ndx=2,
        entry_date="2026-05-12",
        numerator=1,
        denominator=4,
        factory_ndx=416,
    )
    assert body[0]["value"] == "25.0"
    assert body[0]["valueA"] == "1"
    assert body[0]["valueB"] == "4"
    assert body[0]["factoryNdx"] == 416


def test_put_metric_entries_blocked_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.delenv("BPO_ALLOW_PRODUCTION_MUTATIONS", raising=False)
    from src.leandna_app_metrics_client import put_metric_entries

    env = put_metric_entries("2026-05-12", [])
    assert env.get("ok") is False
    assert "mutations" in (env.get("error") or "").lower()


def test_append_metrics_view_query_param() -> None:
    from src.leandna_app_metrics_client import append_metrics_view_query_param

    q = append_metrics_view_query_param("entryType=Manual", "metricOwner", 42)
    assert "metricOwner=42" in q
    assert "entryType=Manual" in q
    q2 = append_metrics_view_query_param("metricOwner=1&entryType=Manual", "metricOwner", 99)
    assert "metricOwner=99" in q2
    assert "metricOwner=1" not in q2


def test_filter_metrics_owned_by_user() -> None:
    from src.leandna_app_metrics_client import filter_metrics_owned_by_user

    rows = [
        {"ndx": 1, "assignedUserNdx": 5},
        {"ndx": 2, "assignedUserNdx": 9},
    ]
    assert len(filter_metrics_owned_by_user(rows, 5)) == 1


def test_list_metrics_view_calls_switch_and_get(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.leandna_app_metrics_http.resolve_leandna_app_session_id", lambda: "sess")
    with patch("src.leandna_app_metrics_client._api_call") as mock_api:
        mock_resp = MagicMock()
        mock_resp.text = '[{"ndx": 5, "metricName": "A"}]'
        mock_resp.json.return_value = [{"ndx": 5, "metricName": "A"}]
        mock_api.return_value = mock_resp
        from src.leandna_app_metrics_client import list_metrics_view

        rows = list_metrics_view(switch_site_first=True, factory_ndx=416)
    assert len(rows) == 1
    assert rows[0]["id"] == 5
    assert mock_api.call_count >= 2
