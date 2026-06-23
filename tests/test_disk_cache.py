"""Tests for shared on-disk cache."""

from __future__ import annotations

from src.disk_cache import cache_get, cache_key, cache_set, clear_namespace_for_tests


def test_disk_cache_round_trip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_namespace_for_tests("ns")
    key = cache_key("demo", {"days": 7})
    cache_set("ns", key, {"ok": True}, 3600)
    assert cache_get("ns", key, 3600) == {"ok": True}
    assert cache_get("ns", key, 0) is None
