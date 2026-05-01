"""Drive JSON cache load counters (Pendo preload + integration)."""

from __future__ import annotations

from src.drive_cache_stats import (
    drive_cache_load_stats_snapshot,
    format_drive_cache_load_summary,
    record_integration_load_attempt,
    record_pendo_preload_load_attempt,
    reset_drive_cache_load_stats,
)


def test_reset_and_hit_miss_rates() -> None:
    reset_drive_cache_load_stats()
    assert format_drive_cache_load_summary() == "Drive JSON cache: no load attempts"

    record_pendo_preload_load_attempt(hit=True)
    record_pendo_preload_load_attempt(hit=False)
    snap = drive_cache_load_stats_snapshot()
    assert snap["pendo_preload"]["attempts"] == 2
    assert snap["pendo_preload"]["hits"] == 1

    record_integration_load_attempt(hit=False)
    snap2 = drive_cache_load_stats_snapshot()
    assert snap2["integration"]["attempts"] == 1
    assert snap2["integration"]["hits"] == 0

    line = format_drive_cache_load_summary()
    assert "pendo_preload" in line
    assert "integration" in line
    assert "50%" in line or "1/2" in line

    reset_drive_cache_load_stats()
    assert drive_cache_load_stats_snapshot()["pendo_preload"]["attempts"] == 0
