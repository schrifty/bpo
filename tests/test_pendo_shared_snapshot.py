"""Tests for shared multi-window Pendo snapshot ingest/require."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.pendo_cache import (
    PRELOAD_KIND_FEATURE_CATALOG,
    PRELOAD_KIND_FEATURE_EVENTS,
    PRELOAD_KIND_GUIDE_CATALOG,
    PRELOAD_KIND_GUIDE_EVENTS,
    PRELOAD_KIND_PAGE_CATALOG,
    PRELOAD_KIND_PAGE_EVENTS,
    PRELOAD_KIND_TRACK_EVENTS,
    PRELOAD_KIND_USAGE_BY_SITE,
    PRELOAD_KIND_USAGE_BY_SITE_ENTITY,
    PRELOAD_KIND_VISITORS,
    clear_pendo_cache_for_tests,
    save_preload_payload,
    try_load_preload_payload,
)
from src.pendo_shared_snapshot import (
    PendoSnapshotError,
    check_shared_pendo_snapshot,
    ensure_shared_pendo_snapshot,
    refresh_shared_pendo_snapshot,
    write_manifest,
)


def _seed_window(days: int) -> None:
    for kind in (
        PRELOAD_KIND_VISITORS,
        PRELOAD_KIND_FEATURE_EVENTS,
        PRELOAD_KIND_PAGE_EVENTS,
        PRELOAD_KIND_TRACK_EVENTS,
        PRELOAD_KIND_GUIDE_EVENTS,
        PRELOAD_KIND_USAGE_BY_SITE,
        PRELOAD_KIND_USAGE_BY_SITE_ENTITY,
    ):
        save_preload_payload(kind, days, {"days": days, "ok": True})


def _seed_catalogs() -> None:
    for kind in (
        PRELOAD_KIND_PAGE_CATALOG,
        PRELOAD_KIND_FEATURE_CATALOG,
        PRELOAD_KIND_GUIDE_CATALOG,
    ):
        save_preload_payload(kind, None, {"ok": True})


@pytest.fixture()
def pendo_cache_root(monkeypatch, tmp_path):
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr("src.config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS", 24 * 3600)
    monkeypatch.setattr("src.pendo_shared_snapshot._config.CORTEX_CACHE_ROOT", tmp_path)
    monkeypatch.setattr(
        "src.pendo_shared_snapshot._config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS",
        24 * 3600,
    )
    clear_pendo_cache_for_tests()
    return tmp_path


def test_ensure_noop_when_require_disabled(pendo_cache_root, monkeypatch) -> None:
    monkeypatch.delenv("CORTEX_PENDO_SNAPSHOT_REQUIRE", raising=False)
    assert ensure_shared_pendo_snapshot(required_windows=[90]) is None


def test_check_fails_when_manifest_missing(pendo_cache_root, monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_PENDO_SNAPSHOT_REQUIRE", "1")
    with pytest.raises(PendoSnapshotError, match="missing manifest"):
        check_shared_pendo_snapshot(required_windows=[90])


def test_check_ok_when_manifest_and_keys_present(pendo_cache_root, monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_PENDO_SNAPSHOT_REQUIRE", "1")
    monkeypatch.setenv("CORTEX_PENDO_SNAPSHOT_MAX_AGE_HOURS", "18")
    _seed_catalogs()
    _seed_window(90)
    write_manifest(
        {
            "schema_version": 1,
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ts": time.time(),
            "windows": [90],
        }
    )
    out = check_shared_pendo_snapshot(required_windows=[90])
    assert out["ok"] is True
    assert out["windows"] == [90]


def test_check_fails_when_stale(pendo_cache_root, monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_PENDO_SNAPSHOT_MAX_AGE_HOURS", "1")
    _seed_catalogs()
    _seed_window(90)
    write_manifest(
        {
            "schema_version": 1,
            "saved_at": "2020-01-01T00:00:00Z",
            "ts": time.time() - 10 * 3600,
            "windows": [90],
        }
    )
    with pytest.raises(PendoSnapshotError, match="exceeds max"):
        check_shared_pendo_snapshot(required_windows=[90])


def test_refresh_writes_manifest_and_verifies_keys(pendo_cache_root, monkeypatch) -> None:
    pc = MagicMock()

    def _preload(days: int, **kwargs) -> None:
        _seed_catalogs()
        _seed_window(days)

    pc.preload.side_effect = _preload
    pc._get_usage_by_site_entity_cached.side_effect = lambda days: save_preload_payload(
        PRELOAD_KIND_USAGE_BY_SITE_ENTITY, days, {"days": days}
    )

    with patch(
        "src.pendo_portfolio_snapshot_drive.run_upload_portfolio_snapshot_cli",
        return_value={"file_id": "fid", "filename": "portfolio_snapshot_v1_days90_all.json", "customer_count": 3},
    ):
        manifest = refresh_shared_pendo_snapshot(windows=[7, 14], upload_portfolio_days=90, pc=pc)

    assert manifest["windows"] == [7, 14]
    assert manifest["portfolio"]["file_id"] == "fid"
    path = pendo_cache_root / "pendo" / "shared_snapshot_manifest_v1.json"
    assert path.is_file()
    disk = json.loads(path.read_text(encoding="utf-8"))
    assert disk["windows"] == [7, 14]
    assert pc.preload.call_count == 2
    # Nightly refresh must force fresh Pendo pulls (not soft disk hits).
    for call in pc.preload.call_args_list:
        assert call.kwargs.get("raise_on_error") is True


def test_refresh_clears_stale_disk_keys_before_warming(pendo_cache_root) -> None:
    """Near-TTL leftovers must not satisfy preload before a fresh rewrite."""
    _seed_catalogs()
    _seed_window(7)
    assert try_load_preload_payload(PRELOAD_KIND_VISITORS, 7) is not None

    pc = MagicMock()
    seen_cleared: list[bool] = []

    def _preload(days: int, **kwargs) -> None:
        # First warm after refresh starts: prior disk keys must already be gone.
        seen_cleared.append(try_load_preload_payload(PRELOAD_KIND_VISITORS, 7) is None)
        _seed_catalogs()
        _seed_window(days)

    pc.preload.side_effect = _preload
    pc._get_usage_by_site_entity_cached.side_effect = lambda days: save_preload_payload(
        PRELOAD_KIND_USAGE_BY_SITE_ENTITY, days, {"days": days}
    )

    refresh_shared_pendo_snapshot(windows=[7], upload_portfolio_days=None, pc=pc)
    assert seen_cleared == [True]
    assert try_load_preload_payload(PRELOAD_KIND_VISITORS, 7) is not None


def test_refresh_fails_loud_on_incomplete_preload(pendo_cache_root) -> None:
    pc = MagicMock()
    pc.preload.side_effect = lambda days, **kwargs: None
    pc._get_usage_by_site_entity_cached.side_effect = lambda days: None
    with pytest.raises(PendoSnapshotError, match="missing disk keys"):
        refresh_shared_pendo_snapshot(windows=[7], upload_portfolio_days=None, pc=pc)
