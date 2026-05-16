"""Tests for production → staging LeanDNA metric copy."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.leandna_data_api_env import LeanDNAEnvConfig
from src.leandna_metrics_copy import (
    build_datapoint_post_body,
    build_metric_create_body,
    copy_metric_production_to_staging,
    find_metric_by_id,
)


def test_build_metric_create_body_strips_id_and_owner() -> None:
    src = {
        "id": 99,
        "ownerId": 1,
        "name": "Time-To-Resolution (30d)",
        "siteId": 416,
        "isCategorized": False,
    }
    body = build_metric_create_body(src, staging_site_id=500)
    assert "id" not in body
    assert "ownerId" not in body
    assert body["name"] == "Time-To-Resolution (30d)"
    assert body["siteId"] == 500
    assert body["metricType"] == "Manual"


def test_build_datapoint_post_body() -> None:
    body = build_datapoint_post_body(
        {"dataPointDate": "2026-01-01", "value": 4.5, "category": "All"},
        2001,
    )
    assert body["metricId"] == 2001
    assert body["dataPointDate"] == "2026-01-01"
    assert body["value"] == 4.5


@pytest.fixture
def env_configs() -> tuple[LeanDNAEnvConfig, LeanDNAEnvConfig]:
    prod = LeanDNAEnvConfig(
        bucket="production",
        base_url="https://prod.example/api",
        bearer_token="p",
        cookie="",
        origin="",
        referer="",
    )
    stg = LeanDNAEnvConfig(
        bucket="staging",
        base_url="https://stg.example/api",
        bearer_token="s",
        cookie="",
        origin="",
        referer="",
    )
    return prod, stg


def test_copy_metric_production_to_staging_happy_path(env_configs) -> None:
    prod, stg = env_configs
    source_metric = {
        "id": 100,
        "name": "Time-To-Resolution (30d)",
        "crossSiteName": "Time-To-Resolution (30d)",
        "siteId": 416,
        "isCategorized": False,
        "usesDenominatorValue": False,
    }
    datapoints = [{"dataPointDate": "2026-01-01", "value": 1.0, "category": ""}]

    with patch("src.leandna_metrics_copy.load_leandna_env_config") as load_env, patch(
        "src.leandna_metrics_copy.list_metrics_for_env"
    ) as list_metrics, patch(
        "src.leandna_metrics_copy.create_metric_definition"
    ) as create_def, patch(
        "src.leandna_metrics_copy.fetch_datapoints_for_env"
    ) as fetch_dp, patch(
        "src.leandna_metrics_copy.post_datapoint_for_env"
    ) as post_dp:
        load_env.side_effect = lambda b: prod if b == "production" else stg
        list_metrics.return_value = ([source_metric], None)
        create_def.return_value = (2001, {"ok": True, "status": 201})
        fetch_dp.return_value = (datapoints, None)
        post_dp.return_value = {"ok": True, "status": 201}

        out = copy_metric_production_to_staging(
            100,
            lookback_days=30,
            copy_datapoints=True,
        )

    assert out["ok"] is True
    assert out["production"]["metric_id"] == 100
    assert out["staging"]["metric_id"] == 2001
    assert out["datapoints"]["posted"] == 1
    create_def.assert_called_once()
    post_dp.assert_called_once()


def test_copy_metric_not_found(env_configs) -> None:
    prod, stg = env_configs
    with patch("src.leandna_metrics_copy.load_leandna_env_config") as load_env, patch(
        "src.leandna_metrics_copy.list_metrics_for_env",
        return_value=([], None),
    ):
        load_env.side_effect = lambda b: prod if b == "production" else stg
        out = copy_metric_production_to_staging(999)
    assert out["ok"] is False
    assert "not found" in out["error"]


def test_find_metric_by_id() -> None:
    catalog = [{"id": 10, "name": "A"}, {"id": "11", "name": "B"}]
    assert find_metric_by_id(catalog, 10)["name"] == "A"
    assert find_metric_by_id(catalog, "11")["name"] == "B"
