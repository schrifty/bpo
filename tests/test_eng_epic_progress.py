"""Tests for the epic ("big rock") progress builder and slide."""

from __future__ import annotations

from src.eng_epic_progress import build_eng_epic_progress
from src.slide_engineering_portfolio import eng_epic_progress_slide


def _epic(key: str, summary: str, status: str = "In Progress") -> dict:
    return {
        "key": key,
        "fields": {
            "summary": summary,
            "status": {"name": status},
            "duedate": None,
            "assignee": {"displayName": "Owner"},
            "updated": "2026-06-10",
        },
    }


class _FakeClient:
    def __init__(self, epics, child_counts):
        self._epics = epics
        # child_counts: {key: (total, done)}
        self._counts = child_counts

    def _search(self, *a, **k):
        return self._epics

    def jql_match_count(self, jql, **k):
        # jql forms: "parent = KEY" or "parent = KEY AND statusCategory = Done"
        key = jql.split("=")[1].strip().split()[0]
        total, done = self._counts.get(key, (0, 0))
        return done if "Done" in jql else total


def test_epic_progress_filters_complete_and_sorts() -> None:
    epics = [
        _epic("LEAN-1", "Big active epic"),
        _epic("LEAN-2", "Catch-all done epic", status="Reopened"),
        _epic("LEAN-3", "Small active epic"),
    ]
    counts = {
        "LEAN-1": (24, 8),    # remaining 16 -> kept
        "LEAN-2": (764, 764),  # remaining 0 -> dropped (catch-all)
        "LEAN-3": (10, 9),     # remaining 1 -> kept
    }
    out = build_eng_epic_progress(_FakeClient(epics, counts), now=None)
    keys = [e["key"] for e in out["epics"]]
    assert keys == ["LEAN-1", "LEAN-3"]  # done catch-all dropped; sorted by total desc
    assert out["epic_count"] == 2
    by_key = {e["key"]: e for e in out["epics"]}
    assert by_key["LEAN-1"]["pct"] == 33
    assert by_key["LEAN-3"]["pct"] == 90
    assert by_key["LEAN-1"]["remaining"] == 16


def test_epic_progress_slide_renders() -> None:
    report = {
        "eng_portfolio": {
            "base_url": "https://x.atlassian.net",
            "epic_progress": {
                "epics": [
                    {"key": "LEAN-1", "summary": "Big rock", "status": "In Progress",
                     "pct": 33, "done": 8, "total": 24, "remaining": 16,
                     "overdue": False, "stale": False},
                ],
                "epic_count": 1,
                "median_pct": 33,
                "total_remaining": 16,
                "has_due_dates": False,
            }
        }
    }
    reqs: list = []
    eng_epic_progress_slide(reqs, "sid_ep", report, 0)
    title = next(
        r["insertText"]["text"] for r in reqs
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == "sid_ep_ttl"
    )
    assert "Initiative" in title


def test_epic_progress_slide_missing_data() -> None:
    reqs: list = []
    eng_epic_progress_slide(reqs, "sid_ep2", {"eng_portfolio": {"epic_progress": {"epics": []}}}, 0)
    assert reqs
