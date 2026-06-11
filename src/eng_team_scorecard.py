"""Multi-team development scorecard for the engineering portfolio deck."""

from __future__ import annotations

import statistics
from typing import Any

from .jira_client import JiraClient
from .jira_cycle_time import get_dev_team_cycle_times
from .jira_sprint_delivery import SPRINT_DELIVERY_BOARDS, get_sprint_delivery_by_team
from .jira_sprint_story_points import get_sprint_story_points_by_team

SCORECARD_BOARD_IDS: tuple[int, ...] = tuple(int(b["board_id"]) for b in SPRINT_DELIVERY_BOARDS)


def _index_teams_by_board(payload: dict[str, Any] | None) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    if not isinstance(payload, dict):
        return out
    for row in payload.get("teams") or []:
        if not isinstance(row, dict):
            continue
        bid = row.get("board_id")
        if bid is None:
            continue
        try:
            out[int(bid)] = row
        except (TypeError, ValueError):
            continue
    return out


def _dedupe_shared_sprint_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse boards that resolve to the same sprint into one row.

    Some boards (e.g. multiple CUSTOMER scrum boards) share a single sprint, so the
    Agile sprint-issue endpoint returns the same issues for each. Counting both rows
    would double-count delivery and story points, so we keep the first board per
    sprint id, fold the other board's team label into it, and drop the duplicate.
    """
    out: list[dict[str, Any]] = []
    by_sprint: dict[Any, dict[str, Any]] = {}
    for row in rows:
        sid = (row.get("sprint") or {}).get("id")
        if sid is None:
            out.append(row)
            continue
        keep = by_sprint.get(sid)
        if keep is None:
            by_sprint[sid] = row
            out.append(row)
            continue
        shared = keep.setdefault("shared_board_ids", [keep.get("board_id")])
        if row.get("board_id") not in shared:
            shared.append(row.get("board_id"))
        keep_team = str(keep.get("team") or "")
        row_team = str(row.get("team") or "")
        if row_team and row_team not in keep_team:
            keep["team"] = f"{keep_team} + {row_team}"
    return out


def merge_team_scorecard_rows(
    *,
    delivery: dict[str, Any] | None,
    story_points: dict[str, Any] | None,
    cycle_time: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Merge sprint delivery, story points, and cycle time by board id."""
    delivery_by_board = _index_teams_by_board(delivery)
    story_by_board = _index_teams_by_board(story_points)
    cycle_by_board = _index_teams_by_board(cycle_time)

    rows: list[dict[str, Any]] = []
    for board in SPRINT_DELIVERY_BOARDS:
        board_id = int(board["board_id"])
        delivery_row = delivery_by_board.get(board_id) or {}
        story_row = story_by_board.get(board_id) or {}
        cycle_row = cycle_by_board.get(board_id) or {}

        sprint = delivery_row.get("sprint") or story_row.get("sprint") or {}
        errors: list[str] = []
        for label, chunk in (
            ("delivery", delivery_row),
            ("story_points", story_row),
            ("cycle_time", cycle_row),
        ):
            err = chunk.get("error")
            if err:
                errors.append(f"{label}: {err}")

        rows.append(
            {
                "team": str(
                    delivery_row.get("team")
                    or story_row.get("team")
                    or cycle_row.get("team")
                    or board.get("team_label")
                    or board_id
                ),
                "board_id": board_id,
                "board_name": board.get("name"),
                "project_key": board.get("project_key"),
                "sprint": sprint,
                "sprint_name": (sprint.get("name") if isinstance(sprint, dict) else None),
                "delivery_pct": delivery_row.get("delivery_pct"),
                "delivered": delivery_row.get("delivered"),
                "committed": delivery_row.get("committed"),
                "story_points_delivered": story_row.get("story_points_delivered"),
                "story_points_committed": story_row.get("story_points_committed"),
                "median_cycle_days": cycle_row.get("median_days"),
                "median_lead_days": cycle_row.get("lead_time_median_days"),
                "measured_cycle_issues": cycle_row.get("measured"),
                "measured_lead_issues": cycle_row.get("lead_time_measured"),
                "errors": errors or None,
            }
        )
    return _dedupe_shared_sprint_rows(rows)


def summarize_team_scorecard(teams: list[dict[str, Any]]) -> dict[str, Any]:
    """Portfolio-level rollups for KPI tiles."""
    delivery_pcts = [
        float(row["delivery_pct"])
        for row in teams
        if row.get("delivery_pct") is not None
    ]
    median_cycles = [
        float(row["median_cycle_days"])
        for row in teams
        if row.get("median_cycle_days") is not None
    ]
    median_leads = [
        float(row["median_lead_days"])
        for row in teams
        if row.get("median_lead_days") is not None
    ]
    total_sp = sum(float(row.get("story_points_delivered") or 0) for row in teams)
    return {
        "average_delivery_pct": round(statistics.mean(delivery_pcts), 1) if delivery_pcts else None,
        "average_median_cycle_days": round(statistics.mean(median_cycles), 1) if median_cycles else None,
        "average_median_lead_days": round(statistics.mean(median_leads), 1) if median_leads else None,
        "total_story_points_delivered": round(total_sp, 1) if total_sp else None,
        "teams_with_delivery": len(delivery_pcts),
        "teams_with_cycle_time": len(median_cycles),
        "teams_with_lead_time": len(median_leads),
    }


def build_eng_team_scorecard(
    client: JiraClient,
    *,
    days: int = 30,
    timeout: float = 60.0,
    board_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Fetch and merge per-team sprint delivery, story points, and cycle time."""
    ids = board_ids if board_ids is not None else list(SCORECARD_BOARD_IDS)
    errors: list[str] = []

    delivery = get_sprint_delivery_by_team(
        client,
        board_ids=ids,
        timeout=timeout,
    )
    if delivery.get("error"):
        errors.append(str(delivery["error"]))

    story_points = get_sprint_story_points_by_team(
        client,
        board_ids=ids,
        timeout=timeout,
    )
    if story_points.get("error"):
        errors.append(str(story_points["error"]))

    cycle_time = get_dev_team_cycle_times(
        client,
        board_ids=ids,
        days=days,
        timeout=timeout,
    )
    if cycle_time.get("error"):
        errors.append(str(cycle_time["error"]))

    teams = merge_team_scorecard_rows(
        delivery=delivery,
        story_points=story_points,
        cycle_time=cycle_time,
    )
    for row in teams:
        shared = row.get("shared_board_ids")
        if shared and len(shared) > 1:
            errors.append(
                f"boards {shared} share one sprint; counted once as '{row.get('team')}'"
            )
    summary = summarize_team_scorecard(teams)

    return {
        "window_days": days,
        "board_ids": ids,
        "teams": teams,
        "summary": summary,
        "sources": {
            "delivery_definition": delivery.get("definition"),
            "story_points_definition": story_points.get("definition"),
            "cycle_time_mode": cycle_time.get("mode"),
        },
        "errors": errors or None,
    }
