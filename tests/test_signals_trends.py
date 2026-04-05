"""Unit tests for Notable Signals trend context (no live Pendo)."""

from __future__ import annotations

from src.signals_trends import finalize_signals_trends_banner


def test_finalize_banner_prefers_largest_delta():
    report = {
        "signals_trend_context": {
            "current_period": {"weekly_active_rate_pct": 20.0},
            "cohort": {"cohort_name": "Industrial", "cohort_median_weekly_active_rate_pct": 15.0},
            "wow": {
                "weekly_active_rate_pct_delta_pp": 0.5,
            },
            "prior_same_length": {
                "label": "QoQ",
                "weekly_active_rate_pct_delta_vs_current_pp": 4.0,
            },
        }
    }
    finalize_signals_trends_banner(report)
    assert "QoQ" in report["signals_trends_display"]
    assert "4.0" in report["signals_trends_display"] or "4" in report["signals_trends_display"]
    assert "Cohort" in report["signals_trends_display"]


def test_finalize_skips_when_display_preset():
    report = {"signals_trends_display": "Already set", "signals_trend_context": {}}
    finalize_signals_trends_banner(report)
    assert report["signals_trends_display"] == "Already set"
