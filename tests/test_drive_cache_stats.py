"""Drive JSON cache load counters (integration payloads)."""

from __future__ import annotations

from src.drive_cache_stats import (
    drive_cache_breakdown_lines,
    drive_cache_load_stats_snapshot,
    format_drive_cache_load_summary,
    record_integration_load_attempt,
    reset_drive_cache_load_stats,
)


def test_reset_and_hit_miss_rates() -> None:
    reset_drive_cache_load_stats()
    assert format_drive_cache_load_summary() == "Drive JSON cache: no load attempts"

    record_integration_load_attempt(hit=True)
    record_integration_load_attempt(hit=False)
    snap = drive_cache_load_stats_snapshot()
    assert snap["integration"]["attempts"] == 2
    assert snap["integration"]["hits"] == 1

    line = format_drive_cache_load_summary()
    assert "integration" in line
    assert "50%" in line or "1/2" in line

    reset_drive_cache_load_stats()
    assert drive_cache_load_stats_snapshot()["integration"]["attempts"] == 0


def test_drive_cache_breakdown_lines() -> None:
    reset_drive_cache_load_stats()
    assert drive_cache_breakdown_lines() == []

    record_integration_load_attempt(hit=True)
    record_integration_load_attempt(hit=False)
    lines = drive_cache_breakdown_lines()
    assert "integration (Drive JSON)" in lines[0]
    assert "1 hit" in lines[0] and "1 miss" in lines[0]

    sf_lines = drive_cache_breakdown_lines(
        sf_comprehensive_summary={
            "customers_fetched": 10,
            "customers_drive_cache_hit": 8,
            "customers_salesforce_fetch": 2,
        },
    )
    assert any("salesforce_comprehensive" in ln for ln in sf_lines)
    assert any("8 hit" in ln and "2 miss" in ln for ln in sf_lines)
