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
        # child_counts: {key: (total, done, active_30d)}
        self._counts = child_counts

    def _search(self, *a, **k):
        return self._epics

    def jql_match_count(self, jql, **k):
        # jql forms: "parent = KEY" | "... AND statusCategory = Done" | "... AND updated >= -30d"
        key = jql.split("=")[1].strip().split()[0]
        total, done, active = self._counts.get(key, (0, 0, 0))
        if "Done" in jql:
            return done
        if "updated" in jql:
            return active
        return total


def test_epic_progress_filters_and_ranks_by_remaining() -> None:
    epics = [
        _epic("LEAN-A", "Mid-progress active"),
        _epic("LEAN-B", "Early-stage active"),
        _epic("LEAN-C", "Oversized maintenance umbrella"),
        _epic("LEAN-D", "Almost done (few left)"),
        _epic("LEAN-E", "Near-complete by pct"),
        _epic("LEAN-F", "Stalled with open work", status="Reopened"),
    ]
    counts = {
        "LEAN-A": (50, 32, 44),    # rem 18, 64% -> kept
        "LEAN-B": (24, 8, 24),     # rem 16, 33% -> kept (early-stage)
        "LEAN-C": (200, 150, 30),  # total>120 umbrella -> dropped
        "LEAN-D": (46, 45, 10),    # rem 1 (<3) -> dropped
        "LEAN-E": (60, 57, 5),     # 95% near-done -> dropped
        "LEAN-F": (30, 10, 0),     # rem 20, 33%, no activity -> stalled, at risk
    }
    out = build_eng_epic_progress(_FakeClient(epics, counts), now=None)
    keys = [e["key"] for e in out["epics"]]
    # Sorted by remaining desc: F(20), A(18), B(16). Umbrella/near-done/tiny dropped.
    assert keys == ["LEAN-F", "LEAN-A", "LEAN-B"]
    assert out["epic_count"] == 3
    by_key = {e["key"]: e for e in out["epics"]}
    assert by_key["LEAN-F"]["stalled"] is True
    assert by_key["LEAN-F"]["at_risk"] is True
    assert by_key["LEAN-A"]["stalled"] is False
    assert by_key["LEAN-A"]["project"] == "LEAN"
    assert out["early_stage_count"] == 2   # F (33%) and B (33%)
    assert out["at_risk_count"] == 1       # F stalled
    assert out["median_pct"] == 33


def test_epic_progress_spans_both_projects() -> None:
    epics = [_epic("LEAN-1", "Lean rock"), _epic("CUSTOMER-9", "Customer rock")]
    counts = {"LEAN-1": (30, 10, 5), "CUSTOMER-9": (40, 10, 5)}  # rem 20 and 30
    out = build_eng_epic_progress(_FakeClient(epics, counts), now=None)
    assert [e["key"] for e in out["epics"]] == ["CUSTOMER-9", "LEAN-1"]  # by remaining
    assert {e["project"] for e in out["epics"]} == {"LEAN", "CUSTOMER"}
    assert out["projects"] == ["LEAN", "CUSTOMER"]


def test_epic_progress_slide_renders() -> None:
    report = {
        "eng_portfolio": {
            "base_url": "https://x.atlassian.net",
            "epic_progress": {
                "epics": [
                    {"key": "LEAN-1", "project": "LEAN", "summary": "Big rock", "status": "In Progress",
                     "owner": "Jane Doe", "pct": 33, "done": 8, "total": 24, "remaining": 16,
                     "active_30d": 24, "overdue": False, "stalled": False, "at_risk": False},
                ],
                "projects": ["LEAN", "CUSTOMER"],
                "epic_count": 1,
                "median_pct": 33,
                "total_remaining": 16,
                "early_stage_count": 1,
                "at_risk_count": 0,
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
