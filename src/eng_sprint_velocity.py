"""Shape per-board sprint story-point history into a portfolio velocity series.

The engineering portfolio velocity slide plots story points delivered per sprint
(the correct unit for sprint performance) rather than raw ticket counts.  Boards
run on independent sprint cadences, so we align each board's recent closed sprints
by *recency* (latest sprint to the right) into a fixed number of slots and label
the axis from the primary engineering board (LEAN) where available.
"""

from __future__ import annotations

from typing import Any

# Board whose sprint names label the shared velocity axis (engineering cadence).
PRIMARY_VELOCITY_BOARD_ID = 44


def _board_sprint_rows(board: dict[str, Any]) -> list[dict[str, Any]]:
    """Closed-sprint rows for one board, oldest→newest, error rows dropped."""
    rows = [
        s
        for s in (board.get("sprints") or [])
        if isinstance(s, dict) and not s.get("error")
    ]
    # ``list_board_sprints`` returns newest-first; reverse to oldest→newest.
    return list(reversed(rows))


def _sprint_label(sprint: dict[str, Any] | None) -> str:
    if not isinstance(sprint, dict):
        return ""
    inner = sprint.get("sprint")
    if isinstance(inner, dict):
        return str(inner.get("name") or "")
    return str(sprint.get("sprint_name") or sprint.get("name") or "")


def build_sprint_velocity_series(
    history: dict[str, Any] | None,
    *,
    slots: int = 6,
) -> dict[str, Any]:
    """Align per-board story-point history into ``slots`` recency columns.

    Returns a chart-ready payload::

        {
          "labels": [str, ...],                 # oldest→newest, len == used_slots
          "teams": [team_label, ...],           # board order, used for grouped bars
          "sp_by_team": {team_label: [float]},  # story points delivered per slot
          "tickets_total": [int],               # issues delivered per slot (line)
          "sp_total": [float],                  # total story points per slot
          "used_slots": int,
          "error": str | None,
        }

    When no usable history is present, ``labels``/``teams`` are empty and
    ``error`` carries the upstream reason.
    """
    slots = max(1, int(slots))
    if not isinstance(history, dict):
        return _empty_series(error="no sprint velocity data")

    boards = [b for b in (history.get("boards") or []) if isinstance(b, dict)]
    board_rows = {id(b): _board_sprint_rows(b) for b in boards}
    used_slots = min(slots, max((len(r) for r in board_rows.values()), default=0))
    if used_slots <= 0:
        err = history.get("error") or _first_board_error(boards) or "no closed sprints with story points"
        return _empty_series(error=str(err))

    teams: list[str] = []
    sp_by_team: dict[str, list[float]] = {}
    tickets_total = [0 for _ in range(used_slots)]
    sp_total = [0.0 for _ in range(used_slots)]
    label_sources: list[dict[str, Any] | None] = [None for _ in range(used_slots)]
    primary_labels: list[dict[str, Any] | None] = [None for _ in range(used_slots)]

    for board in boards:
        rows = board_rows[id(board)]
        if not rows:
            continue
        team = str(board.get("team") or board.get("board_name") or board.get("board_id") or "Team")
        # Align the latest sprint to the rightmost slot.
        per_slot = [0.0 for _ in range(used_slots)]
        recent = rows[-used_slots:]
        offset = used_slots - len(recent)
        is_primary = int(board.get("board_id") or -1) == PRIMARY_VELOCITY_BOARD_ID
        for i, sprint in enumerate(recent):
            slot = offset + i
            sp = float(sprint.get("story_points_delivered") or 0.0)
            tickets = int(sprint.get("delivered_issues") or 0)
            per_slot[slot] = sp
            sp_total[slot] += sp
            tickets_total[slot] += tickets
            if label_sources[slot] is None:
                label_sources[slot] = sprint
            if is_primary:
                primary_labels[slot] = sprint
        teams.append(team)
        sp_by_team[team] = per_slot

    if not teams:
        return _empty_series(error="no closed sprints with story points")

    labels: list[str] = []
    for slot in range(used_slots):
        chosen = primary_labels[slot] or label_sources[slot]
        labels.append(_sprint_label(chosen) or f"S-{used_slots - 1 - slot}")

    # Boards that do not estimate in story points (e.g. LEAN runs on ticket
    # throughput) contribute all-zero rows. Drop them from the SP bars — a flat-zero
    # series implies "delivered nothing" rather than "does not use story points" — but
    # keep their sprint names for the recency axis (computed above) and keep them in
    # ``tickets_total``/``sp_total`` accounting.
    sp_teams = [t for t in teams if any(v for v in sp_by_team.get(t, []))]
    zero_sp_teams = [t for t in teams if t not in sp_teams]
    sp_by_team_nonzero = {t: sp_by_team[t] for t in sp_teams}

    return {
        "labels": labels,
        "teams": sp_teams or teams,
        "sp_by_team": sp_by_team_nonzero or sp_by_team,
        "zero_sp_teams": zero_sp_teams,
        "tickets_total": tickets_total,
        "sp_total": [round(v, 1) for v in sp_total],
        "used_slots": used_slots,
        "error": None,
    }


def _empty_series(*, error: str | None) -> dict[str, Any]:
    return {
        "labels": [],
        "teams": [],
        "sp_by_team": {},
        "zero_sp_teams": [],
        "tickets_total": [],
        "sp_total": [],
        "used_slots": 0,
        "error": error,
    }


def _first_board_error(boards: list[dict[str, Any]]) -> str | None:
    for b in boards:
        if isinstance(b, dict) and b.get("error"):
            return str(b["error"])
    return None
