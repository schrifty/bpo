"""Tests for portfolio Critical Signals ranking (read-heavy cap, keyword scoring)."""

from src.pendo_client import (
    PendoClient,
    _take_portfolio_signals_capping_read_heavy,
)


def test_take_portfolio_signals_caps_read_heavy():
    # Same severity sorts by ascending score, so lower scores win ties — use higher
    # severity for non–read-heavy rows so they sort first regardless of score.
    ranked: list[dict] = []
    for i in range(10):
        ranked.append(
            {
                "customer": f"G{i}",
                "signal": "Low guide reach — onboarding friction",
                "severity": 2,
                "score": 0,
            }
        )
    for i in range(15):
        ranked.append(
            {
                "customer": f"R{i}",
                "signal": f"Read-heavy usage: only {i}% write ratio",
                "severity": 1,
                "score": 100,
            }
        )
    ranked.sort(key=lambda x: (-x["severity"], x["score"]))
    out = _take_portfolio_signals_capping_read_heavy(ranked)
    rh = sum(1 for x in out if "read-heavy" in str(x.get("signal", "")).lower())
    assert rh == 4
    assert len(out) == 14
    assert all("Low guide" in str(x.get("signal", "")) for x in out[:10])


def test_compute_portfolio_signals_respects_max_lines_override():
    client = PendoClient(integration_key="test-key-for-unit-tests", base_url="https://example.invalid")
    summaries = []
    for i in range(35):
        summaries.append(
            {
                "customer": f"C{i}",
                "score": 0,
                "signals": [f"No Kei AI usage detected — customer {i}"],
            }
        )
    out_default = client._compute_portfolio_signals(summaries)
    out_big = client._compute_portfolio_signals(summaries, max_lines=100, max_read_heavy=100)
    assert len(out_default) <= 20
    assert len(out_big) == 35


def test_compute_portfolio_signals_prefers_higher_severity_and_caps_read_heavy():
    client = PendoClient(integration_key="test-key-for-unit-tests", base_url="https://example.invalid")
    summaries: list[dict] = []
    for i in range(12):
        summaries.append(
            {
                "customer": f"ReadHeavy{i}",
                "score": 0,
                "signals": ["Read-heavy usage: only 0.0% write ratio (may be dashboard-only)"],
            }
        )
    for i in range(8):
        summaries.append(
            {
                "customer": f"Decline{i}",
                "score": 50,
                "signals": ["Active users declining vs prior period"],
            },
        )
    out = client._compute_portfolio_signals(summaries)
    rh_lines = [x for x in out if "read-heavy" in str(x.get("signal", "")).lower()]
    assert len(rh_lines) <= 4
    assert any("declining" in str(x.get("signal", "")).lower() for x in out)
