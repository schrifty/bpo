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
    assert "openapi_ui" in doc


def test_leandna_data_api_get_tool_invalid_json() -> None:
    from src.tools.leandna_data_api_tool import LeanDNADataApiGetTool

    raw = LeanDNADataApiGetTool()._run("not json")
    err = json.loads(raw)
    assert "error" in err
