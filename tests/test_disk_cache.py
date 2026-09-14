"""Tests for shared on-disk cache."""

from __future__ import annotations

import json
import stat

from src.disk_cache import cache_get, cache_key, cache_path, cache_set, clear_namespace_for_tests


def test_disk_cache_round_trip(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_namespace_for_tests("ns")
    key = cache_key("demo", {"days": 7})
    cache_set("ns", key, {"ok": True}, 3600)
    assert cache_get("ns", key, 3600) == {"ok": True}
    assert cache_get("ns", key, 0) is None


def test_disk_cache_file_is_encrypted_and_private(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_namespace_for_tests("ns")
    secret = "secret-visitor@example.com"
    key = cache_key("demo", {"email": secret})
    cache_set("ns", key, {"email": secret}, 3600)
    path = cache_path("ns", key)
    raw = path.read_text(encoding="utf-8")
    assert secret not in raw
    outer = json.loads(raw)
    assert outer["v"] == 2
    assert outer["alg"] == "fernet"
    assert "data" not in outer
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600


def test_disk_cache_ignores_plaintext_legacy(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_namespace_for_tests("ns")
    key = cache_key("demo", {"days": 7})
    path = cache_path("ns", key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"ts": 9e12, "data": {"legacy": True}}), encoding="utf-8")
    assert cache_get("ns", key, 3600) is None


def test_disk_cache_skips_write_without_key(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.delenv("CORTEX_CACHE_FERNET_KEY", raising=False)
    clear_namespace_for_tests("ns")
    key = cache_key("demo", {"days": 7})
    cache_set("ns", key, {"ok": True}, 3600)
    assert not cache_path("ns", key).exists()
    assert cache_get("ns", key, 3600) is None
