"""Tests for Pendo preload disk cache."""

from __future__ import annotations

from unittest.mock import patch

from src.pendo_cache import (
    PRELOAD_KIND_VISITORS,
    clear_pendo_cache_for_tests,
    preload_cache_key,
    save_preload_payload,
    try_load_preload_payload,
)


def test_preload_cache_key_includes_days(monkeypatch, tmp_path) -> None:
    assert preload_cache_key(PRELOAD_KIND_VISITORS, 90) == "visitors_days90"
    assert preload_cache_key("page_catalog", None) == "page_catalog"


def test_pendo_disk_cache_round_trip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr("src.config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS", 12 * 3600)
    clear_pendo_cache_for_tests()
    payload = {"days": 30, "all_visitors": [], "all_customer_stats": {}}
    save_preload_payload(PRELOAD_KIND_VISITORS, 30, payload)
    assert try_load_preload_payload(PRELOAD_KIND_VISITORS, 30) == payload
    assert try_load_preload_payload(PRELOAD_KIND_VISITORS, 90) is None


def test_pendo_disk_cache_respects_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr("src.config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS", 0)
    clear_pendo_cache_for_tests()
    save_preload_payload(PRELOAD_KIND_VISITORS, 30, {"ok": True})
    assert try_load_preload_payload(PRELOAD_KIND_VISITORS, 30) is None


def test_visitor_partition_loads_from_disk_cache() -> None:
    from src.pendo_client import PendoClient

    blob = {
        "days": 30,
        "now_ms": 1,
        "all_visitors": [{"visitorId": "v1"}],
        "all_customer_stats": {"Acme": {"total": 1, "active_7d": 1}},
    }
    with patch("src.pendo_client.PendoClient.get_visitors") as get_visitors, patch(
        "src.pendo_cache.try_load_preload_payload",
        return_value=blob,
    ):
        out = PendoClient(integration_key="test-key")._get_visitor_partition(30)
    get_visitors.assert_not_called()
    assert out["days"] == 30
    assert len(out["all_visitors"]) == 1


def test_usage_by_site_entity_loads_from_disk_cache() -> None:
    from src.pendo_client import PendoClient

    blob = {"days": 30, "sites": [{"sitename": "Acme HQ"}]}
    with patch(
        "src.pendo_client.PendoClient.get_usage_by_site_and_entity"
    ) as live, patch(
        "src.pendo_cache.try_load_preload_payload",
        return_value=blob,
    ):
        out = PendoClient(integration_key="test-key")._get_usage_by_site_entity_cached(30)
    live.assert_not_called()
    assert out["days"] == 30
