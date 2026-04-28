"""Cross-source signals from poll_events and frustration on health reports."""

from src.cross_source_signals import (
    _collect_pendo_frustration_signals,
    _collect_pendo_poll_signals,
)


def test_poll_nps_alert():
    report = {
        "poll_events": {
            "response_count": 3,
            "nps": {"count": 3, "median": 5.0, "avg": 5.3},
        },
    }
    lines = _collect_pendo_poll_signals(report)
    assert lines and "NPS" in lines[0]
    assert "5" in lines[0] or "follow-up" in lines[0]


def test_frustration_rage_threshold():
    report = {
        "frustration": {
            "total_frustration_signals": 400,
            "totals": {"rageClickCount": 30, "deadClickCount": 1, "errorClickCount": 2, "uTurnCount": 3},
            "top_pages": [{"page": "Dashboard", "rageClickCount": 12}],
        },
    }
    lines = _collect_pendo_frustration_signals(report)
    assert lines and "rage-click" in lines[0].lower()


def test_empty_when_missing():
    assert _collect_pendo_poll_signals({}) == []
    assert _collect_pendo_frustration_signals({}) == []
