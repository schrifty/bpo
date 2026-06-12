"""Tests for the sprint story-point velocity series builder."""

from __future__ import annotations

from src.eng_sprint_velocity import build_sprint_velocity_series


def _board(team: str, board_id: int, sprints_newest_first: list[dict]) -> dict:
    return {"team": team, "board_id": board_id, "board_name": team, "sprints": sprints_newest_first}


def _sprint(name: str, sp: float, tickets: int, *, error: str | None = None) -> dict:
    if error:
        return {"error": error}
    return {
        "sprint": {"name": name},
        "story_points_delivered": sp,
        "delivered_issues": tickets,
    }


def test_builds_aligned_series_newest_to_right() -> None:
    history = {
        "boards": [
            # newest-first per list_board_sprints
            _board("LEAN Engineering", 44, [
                _sprint("Sprint 592", 30, 10),
                _sprint("Sprint 591", 20, 8),
                _sprint("Sprint 590", 25, 9),
            ]),
            _board("Data Integration", 46, [
                _sprint("DI 12", 12, 4),
                _sprint("DI 11", 8, 3),
            ]),
        ],
    }
    series = build_sprint_velocity_series(history, slots=6)

    assert series["error"] is None
    assert series["used_slots"] == 3
    # Labels prefer the primary (LEAN) board's sprint names, oldest -> newest.
    assert series["labels"] == ["Sprint 590", "Sprint 591", "Sprint 592"]
    assert series["teams"] == ["LEAN Engineering", "Data Integration"]
    # Latest sprint aligns to the rightmost slot for every board.
    assert series["sp_by_team"]["LEAN Engineering"] == [25.0, 20.0, 30.0]
    # Data Integration has only 2 sprints -> oldest slot is 0.
    assert series["sp_by_team"]["Data Integration"] == [0.0, 8.0, 12.0]
    # Totals combine boards per slot.
    assert series["sp_total"] == [25.0, 28.0, 42.0]
    assert series["tickets_total"] == [9, 11, 14]


def test_drops_zero_story_point_boards_but_keeps_labels() -> None:
    """A board that does not estimate in SP (e.g. LEAN) must not draw a flat-zero
    series, but its sprint names still label the recency axis."""
    history = {
        "boards": [
            _board("LEAN Engineering", 44, [
                _sprint("Sprint 595", 0, 59),
                _sprint("Sprint 594", 0, 50),
            ]),
            _board("CUSTOMER", 36, [
                _sprint("Wk Jun 1", 218, 34),
                _sprint("Wk May 25", 300, 40),
            ]),
        ],
    }
    series = build_sprint_velocity_series(history, slots=6)
    # LEAN (all-zero SP) dropped from SP bars, CUSTOMER kept.
    assert series["teams"] == ["CUSTOMER"]
    assert "LEAN Engineering" not in series["sp_by_team"]
    assert series["zero_sp_teams"] == ["LEAN Engineering"]
    # Labels still come from the primary (LEAN) board.
    assert series["labels"] == ["Sprint 594", "Sprint 595"]
    # Totals still account for all boards (LEAN contributes 0).
    assert series["sp_total"] == [300.0, 218.0]


def _sprint_with_id(name: str, sprint_id: int, sp: float, tickets: int) -> dict:
    return {
        "sprint": {"name": name, "id": sprint_id},
        "story_points_delivered": sp,
        "delivered_issues": tickets,
    }


def test_dedupes_boards_sharing_sprint_ids() -> None:
    """CUSTOMER Active Scrum and Data Integration are two boards over the same project,
    so they report the *same* Jira sprints. Each sprint must count once, not twice."""
    def shared() -> list[dict]:
        return [
            _sprint_with_id("Wk Jun 1", 2939, 239, 31),
            _sprint_with_id("Wk May 25", 2930, 176, 28),
        ]

    history = {
        "boards": [
            _board("CUSTOMER Active Scrum", 36, shared()),
            _board("Data Integration", 46, shared()),
        ],
    }
    series = build_sprint_velocity_series(history, slots=6)
    # The duplicate board is dropped from the bars (no two identical series).
    assert series["teams"] == ["CUSTOMER Active Scrum"]
    # Totals count each shared sprint once — not doubled.
    assert series["sp_total"] == [176.0, 239.0]
    assert series["tickets_total"] == [28, 31]


def test_respects_slot_cap() -> None:
    sprints = [_sprint(f"S{i}", float(i), i) for i in range(8, 0, -1)]
    history = {"boards": [_board("LEAN Engineering", 44, sprints)]}
    series = build_sprint_velocity_series(history, slots=4)
    assert series["used_slots"] == 4
    assert len(series["labels"]) == 4
    # Newest sprint (S8) sits at the rightmost slot.
    assert series["sp_by_team"]["LEAN Engineering"][-1] == 8.0


def test_drops_error_sprints_and_reports_when_empty() -> None:
    history = {
        "boards": [_board("LEAN Engineering", 44, [_sprint("x", 0, 0, error="boom")])],
        "error": None,
    }
    series = build_sprint_velocity_series(history, slots=6)
    assert series["used_slots"] == 0
    assert series["labels"] == []
    assert series["error"]


def test_handles_missing_payload() -> None:
    series = build_sprint_velocity_series(None, slots=6)
    assert series["error"]
    assert series["labels"] == []
