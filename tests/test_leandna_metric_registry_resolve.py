"""Tests for LeanDNA metric id lookup during metrics-upsert."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.leandna_metric_registry_resolve import (
    MetricRegistryResolveError,
    resolve_registry_metric_id,
    update_registry_metric_id_in_file,
)


def test_update_registry_metric_id_in_file() -> None:
    path = Path("/tmp/test-metrics.yaml")
    path.write_text(
        """
metrics:
  "Sprint Story Points Delivered":
    metric-id: null
    metric-generator: get_sprint_story_points_by_team
""".strip(),
        encoding="utf-8",
    )
    assert update_registry_metric_id_in_file("Sprint Story Points Delivered", 2099, path=path) is True
    text = path.read_text(encoding="utf-8")
    assert "metric-id: 2099" in text


def test_resolve_uses_registry_id_without_api_calls() -> None:
    entry = {"metric-id": 2086, "metric-generator": "get_sprint_delivery_by_team"}
    with patch("src.leandna_metric_registry_resolve.fetch_data_api_identity") as fetch_id:
        resolution = resolve_registry_metric_id(
            "Sprint Delivery %",
            entry,
            requested_sites=None,
            dry_run=False,
            timeout_seconds=30.0,
        )
    fetch_id.assert_not_called()
    assert resolution.metric_id == 2086
    assert resolution.source == "registry"


def test_resolve_site_id_prefers_portfolio_default_when_multiple_authorized() -> None:
    from src.leandna_metric_registry_resolve import _resolve_site_id

    body = {"authorizedSites": [{"siteId": 100}, {"siteId": 416}, {"siteId": 500}]}
    assert _resolve_site_id(requested_sites=None, identity_body=body) == 416


def test_resolve_site_id_uses_env_override_when_authorized() -> None:
    from src.leandna_metric_registry_resolve import _resolve_site_id

    body = {"authorizedSites": [{"siteId": 100}, {"siteId": 500}]}
    with patch.dict(os.environ, {"CORTEX_LEANDNA_METRICS_SITE_ID": "500"}):
        assert _resolve_site_id(requested_sites=None, identity_body=body) == 500


def test_resolve_missing_metric_requires_ui_create() -> None:
    entry = {"metric-id": None, "metric-generator": "get_sprint_story_points_by_team"}
    identity_body = {"userId": "42", "authorizedSites": [{"siteId": 416}]}
    with patch(
        "src.leandna_metric_registry_resolve.fetch_data_api_identity",
        return_value=type("I", (), {"user_id": "42", "body": identity_body})(),
    ), patch(
        "src.leandna_metrics_client.list_metric_definitions",
        return_value=[],
    ):
        with pytest.raises(MetricRegistryResolveError, match="LeanDNA app UI"):
            resolve_registry_metric_id(
                "Sprint Story Points Delivered",
                entry,
                requested_sites=None,
                dry_run=False,
                timeout_seconds=30.0,
            )


def test_resolve_dry_run_also_requires_catalog_match() -> None:
    entry = {"metric-id": None, "metric-generator": "get_sprint_story_points_by_team"}
    identity_body = {"userId": "42", "authorizedSites": [{"siteId": 416}]}
    with patch(
        "src.leandna_metric_registry_resolve.fetch_data_api_identity",
        return_value=type("I", (), {"user_id": "42", "body": identity_body})(),
    ), patch(
        "src.leandna_metrics_client.list_metric_definitions",
        return_value=[],
    ):
        with pytest.raises(MetricRegistryResolveError, match="LeanDNA app UI"):
            resolve_registry_metric_id(
                "Sprint Story Points Delivered",
                entry,
                requested_sites=None,
                dry_run=True,
                timeout_seconds=30.0,
            )


def test_resolve_discovers_owned_metric_from_catalog() -> None:
    entry = {"metric-id": None, "metric-generator": "get_sprint_story_points_by_team"}
    identity_body = {"userId": "42", "authorizedSites": [{"siteId": 416}]}
    catalog = [
        {
            "id": 2099,
            "name": "Sprint Story Points Delivered",
            "ownerId": "42",
            "siteId": 416,
        }
    ]
    with patch(
        "src.leandna_metric_registry_resolve.fetch_data_api_identity",
        return_value=type("I", (), {"user_id": "42", "body": identity_body})(),
    ), patch(
        "src.leandna_metrics_client.list_metric_definitions",
        return_value=catalog,
    ):
        resolution = resolve_registry_metric_id(
            "Sprint Story Points Delivered",
            entry,
            requested_sites=None,
            dry_run=False,
            timeout_seconds=30.0,
        )
    assert resolution.metric_id == 2099
    assert resolution.source == "catalog"
