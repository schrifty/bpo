"""Tests for LeanDNA generic Data API request helper and LangChain tools."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest


def test_normalize_data_api_relative_path_strips_data_prefix() -> None:
    from src.leandna_data_api_request import normalize_data_api_relative_path

    assert normalize_data_api_relative_path("ItemMasterData") == "ItemMasterData"
    assert normalize_data_api_relative_path("/data/Metric") == "Metric"
    assert normalize_data_api_relative_path("data/MaterialShortages/ShortagesByItem/Weekly") == (
        "MaterialShortages/ShortagesByItem/Weekly"
    )


def test_normalize_data_api_relative_path_rejects_traversal() -> None:
    from src.leandna_data_api_request import normalize_data_api_relative_path

    with pytest.raises(ValueError):
        normalize_data_api_relative_path("LeanProject/../evil")


def test_normalize_allows_lean_project_path_template_chars() -> None:
    from src.leandna_data_api_request import normalize_data_api_relative_path

    assert normalize_data_api_relative_path("LeanProject/1,2/Savings") == "LeanProject/1,2/Savings"


def test_build_leandna_data_api_headers_strips_redundant_bearer_prefix() -> None:
    from unittest.mock import patch

    from src.leandna_data_api_http import build_leandna_data_api_headers

    with patch.multiple(
        "src.leandna_data_api_http",
        LEANDNA_DATA_API_BEARER_TOKEN="Bearer  abc123",
        LEANDNA_DATA_API_COOKIE="",
    ):
        h = build_leandna_data_api_headers()
    assert h["Authorization"] == "Bearer abc123"


def test_format_data_api_error_envelope_parses_json_reason() -> None:
    from src.leandna_data_api_request import format_data_api_error_envelope

    msg = format_data_api_error_envelope(
        {
            "ok": False,
            "status": 401,
            "error": "Unauthorized",
            "body_preview": '{"status":401,"reason":"Session not found"}',
        },
        cred_prefix="PR_",
    )
    assert "401" in msg
    assert "Session not found" in msg
    assert "PR_LEANDNA_DATA_API_BEARER_TOKEN" in msg


def test_data_api_get_json_missing_credentials_envelope() -> None:
    from src.leandna_data_api_request import data_api_get_json

    with patch("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", ""), patch(
        "src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", ""
    ):
        out = data_api_get_json("Metric")
    assert out["ok"] is False
    assert "error" in out


def test_leandna_data_api_catalog_tool_json() -> None:
    from src.tools.leandna_data_api_tool import LeanDNADataApiCatalogTool

    raw = LeanDNADataApiCatalogTool()._run("")
    doc = json.loads(raw)
    assert doc["get_resources"]
    assert doc["mutation_operations"]
    assert "openapi_ui" in doc


def test_leandna_data_api_get_tool_invalid_json() -> None:
    from src.tools.leandna_data_api_tool import LeanDNADataApiGetTool

    raw = LeanDNADataApiGetTool()._run("not json")
    err = json.loads(raw)
    assert "error" in err


def test_data_api_mutate_json_invalid_method() -> None:
    from src.leandna_data_api_request import data_api_mutate_json

    out = data_api_mutate_json("PATCH", "Metric/1/MetricDataPoint")
    assert out["ok"] is False
    assert "POST" in out.get("error", "")


def test_data_api_mutate_json_missing_credentials_envelope() -> None:
    from src.leandna_data_api_request import data_api_mutate_json

    with patch("src.leandna_data_api_http.LEANDNA_DATA_API_BEARER_TOKEN", ""), patch(
        "src.leandna_data_api_http.LEANDNA_DATA_API_COOKIE", ""
    ):
        out = data_api_mutate_json("POST", "LeanProject", json_body={"name": "x"})
    assert out["ok"] is False
    assert "error" in out


def test_data_api_mutate_json_post_success_envelope() -> None:
    from unittest.mock import MagicMock, patch

    from src.leandna_data_api_request import data_api_mutate_json

    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.text = '{"created": true}'
    resp.reason = "OK"

    with patch("src.leandna_data_api_request.leandna_http_mutation_blocked_envelope", return_value=None), patch(
        "src.leandna_data_api_request.requests.request", return_value=resp
    ):
        out = data_api_mutate_json("POST", "LeanProject", json_body={"name": "Test"}, requested_sites="172")
    assert out["ok"] is True
    assert out["body"] == {"created": True}


def test_data_api_mutate_json_blocked_when_production_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg
    from src.leandna_data_api_request import data_api_mutate_json

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.delenv("BPO_ALLOW_PRODUCTION_MUTATIONS", raising=False)
    out = data_api_mutate_json("DELETE", "Metric/1/MetricDataPoint")
    assert out["ok"] is False
    assert "disabled" in out["error"].lower()
    assert out["method"] == "DELETE"


def test_leandna_data_api_mutate_tool_rejects_non_object_body() -> None:
    from src.tools.leandna_data_api_tool import LeanDNADataApiMutateTool

    raw = LeanDNADataApiMutateTool()._run('{"method":"POST","path":"LeanProject","body":"nope"}')
    err = json.loads(raw)
    assert "error" in err
