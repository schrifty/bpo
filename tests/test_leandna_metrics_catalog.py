"""Tests for LeanDNA metric catalog helpers."""

from __future__ import annotations

from src.leandna_metrics_catalog import (
    filter_metric_catalog,
    format_metric_brief_lines,
    is_catalog_id_token,
)
from src.metrics_registry import is_fully_defined_metric


def test_is_catalog_id_token() -> None:
    assert is_catalog_id_token("638") is True
    assert is_catalog_id_token("job") is False


def test_filter_metric_catalog_by_id() -> None:
    catalog = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    rows = filter_metric_catalog(catalog, filter_token="2", use_all=False, max_metrics=50)
    assert len(rows) == 1
    assert rows[0]["id"] == 2


def test_format_metric_brief_lines() -> None:
    lines = format_metric_brief_lines([{"id": 1, "name": "OTIF", "metricType": "Manual", "siteId": 416}])
    assert len(lines) == 1
    assert "OTIF" in lines[0]


def test_registry_fully_defined() -> None:
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": "fn"}) is True
    assert is_fully_defined_metric({"metric-id": 1, "metric-generator": None}) is False


def test_resolve_effective_requested_sites() -> None:
    from src.leandna_metrics_catalog import resolve_effective_requested_sites

    assert resolve_effective_requested_sites("416") == "416"
    assert resolve_effective_requested_sites(None, identity_body={"authorizedSites": [{"siteId": 416}]}) == "416"
    assert resolve_effective_requested_sites(None, identity_body={"authorizedSites": []}) is None
    assert (
        resolve_effective_requested_sites(
            None,
            identity_body={"authorizedSites": [{"siteId": 416}, {"siteId": 99}]},
        )
        is None
    )


def test_fetch_my_metric_definitions_filters_by_owner(monkeypatch) -> None:
    from src.leandna_metrics_catalog import fetch_my_metric_definitions

    monkeypatch.setattr(
        "src.leandna_metrics_catalog.fetch_data_api_identity",
        lambda **_: type(
            "I",
            (),
            {
                "user_id": "42",
                "owner_label": "Marc",
                "body": {"userId": "42", "authorizedSites": [{"siteId": 416}]},
            },
        )(),
    )
    monkeypatch.setattr(
        "src.leandna_metrics_catalog.list_metric_definitions",
        lambda requested_sites=None, **__: [
            {"id": 1, "ownerId": "42", "siteId": 416},
            {"id": 2, "ownerId": "99", "siteId": 416},
        ],
    )
    rows, identity, sites = fetch_my_metric_definitions()
    assert identity.user_id == "42"
    assert sites == "416"
    assert [r["id"] for r in rows] == [1]
