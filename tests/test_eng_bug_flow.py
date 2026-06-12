"""Tests for the bug inflow/outflow builder and slide."""

from __future__ import annotations

from datetime import datetime, timezone

from src.eng_bug_flow import build_eng_bug_flow
from src.slide_engineering_portfolio import eng_bug_flow_slide

_NOW = datetime(2026, 6, 11, tzinfo=timezone.utc)


def _bug(created: str, resolved: str | None = None) -> dict:
    return {
        "fields": {
            "created": created,
            "resolutiondate": resolved,
            "resolution": {"name": "Done"} if resolved else None,
        }
    }


class _FakeClient:
    def __init__(self, issues, open_count=42):
        self._issues = issues
        self._open = open_count

    def _search(self, *a, **k):
        return self._issues

    def jql_match_count(self, *a, **k):
        return self._open


def test_bug_flow_buckets_and_trend_growing() -> None:
    issues = [
        _bug("2026-06-08"),                # created this week, still open
        _bug("2026-06-09"),
        _bug("2026-06-01", "2026-06-02"),  # created & resolved prior week
        _bug("2026-05-04", "2026-06-10"),  # created out-of-window-ish, resolved this week
    ]
    flow = build_eng_bug_flow(_FakeClient(issues, open_count=99), window_days=28, now=_NOW)
    assert flow["error"] is None
    assert flow["weeks_count"] == 4
    assert len(flow["weeks"]) == 4
    # 3 created fall in window weeks; 2 resolved fall in window weeks.
    assert flow["created_total"] == 3
    assert flow["resolved_total"] == 2
    assert flow["net_total"] == 1
    assert flow["open_now"] == 99
    # Each week carries a net = created - resolved.
    assert all("net" in w for w in flow["weeks"])


def test_bug_flow_trend_shrinking() -> None:
    # Created well before the 84-day window (so they don't count as inflow), all
    # resolved this week -> pure outflow.
    issues = [_bug(f"2026-01-0{i}", "2026-06-10") for i in range(1, 9)]
    flow = build_eng_bug_flow(_FakeClient(issues), window_days=84, now=_NOW)
    assert flow["resolved_total"] == 8
    assert flow["net_total"] < -5
    assert flow["trend"] == "shrinking"


def test_bug_flow_slide_renders() -> None:
    report = {
        "eng_portfolio": {
            "bug_flow": {
                "window_days": 28,
                "weeks_count": 4,
                "weeks": [
                    {"label": "May 18", "created": 3, "resolved": 1, "net": 2},
                    {"label": "May 25", "created": 2, "resolved": 4, "net": -2},
                ],
                "created_total": 5,
                "resolved_total": 5,
                "net_total": 0,
                "open_now": 42,
                "trend": "flat",
            }
        }
    }
    reqs: list = []
    eng_bug_flow_slide(reqs, "sid_bf", report, 0)
    title = next(
        r["insertText"]["text"] for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_bf_ttl"
    )
    assert "Inflow" in title or "Backlog" in title


def test_bug_flow_slide_missing_data() -> None:
    reqs: list = []
    eng_bug_flow_slide(reqs, "sid_bf2", {"eng_portfolio": {"bug_flow": {"weeks": []}}}, 0)
    assert reqs
