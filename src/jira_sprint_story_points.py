"""Sprint story points delivered (Done) per development board from Jira Agile."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .jira_client import JiraClient, STORY_POINTS_FIELD
from .jira_cycle_time import _issue_excluded, load_status_category_map, parse_excluded_issue_types
from .jira_sprint_delivery import (
    SPRINT_DELIVERY_BOARDS,
    SprintSelector,
    _issue_is_done,
    _resolve_boards,
    list_board_sprints,
    resolve_board_sprint,
)

logger = logging.getLogger("cortex")

_AGILE = "/rest/agile/1.0"

SPRINT_STORY_POINTS_BOARDS = SPRINT_DELIVERY_BOARDS


def parse_story_points(fields: dict[str, Any]) -> float:
    """Story points from issue fields (missing or invalid → 0)."""
    raw = fields.get(STORY_POINTS_FIELD)
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _fetch_sprint_issues(
    client: JiraClient,
    sprint_id: int,
    *,
    max_issues: int,
    excluded_issue_types: tuple[str, ...],
    timeout: float,
) -> tuple[list[dict[str, Any]], int | None]:
    issues: list[dict[str, Any]] = []
    start_at = 0
    page_size = min(100, max(1, max_issues))
    reported_total: int | None = None
    fields_param = f"summary,status,issuetype,{STORY_POINTS_FIELD}"

    while len(issues) < max_issues:
        resp = requests.get(
            f"{client.base_url}{_AGILE}/sprint/{sprint_id}/issue",
            headers=client._headers,
            params={
                "startAt": start_at,
                "maxResults": min(page_size, max_issues - len(issues)),
                "fields": fields_param,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if reported_total is None and data.get("total") is not None:
            try:
                reported_total = int(data["total"])
            except (TypeError, ValueError):
                pass
        chunk = data.get("issues") or []
        for row in chunk:
            if not isinstance(row, dict):
                continue
            fields = row.get("fields") or {}
            if isinstance(fields, dict) and _issue_excluded(fields, excluded_issue_types):
                continue
            issues.append(row)
        if not chunk:
            break
        if data.get("isLast") is True:
            break
        if reported_total is not None and len(issues) >= reported_total:
            break
        start_at += len(chunk)

    return issues[:max_issues], reported_total


def _story_points_result(
    *,
    team_label: str,
    board: dict[str, Any],
    sprint: dict[str, Any],
    issues: list[dict[str, Any]],
    reported_total: int | None,
    status_map: dict[str, str],
    excluded_issue_types: tuple[str, ...],
    elapsed_seconds: float,
) -> dict[str, Any]:
    board_id = int(board["board_id"])
    committed_sp = 0.0
    delivered_sp = 0.0
    issues_with_points = 0
    delivered_issues = 0

    for issue in issues:
        fields = issue.get("fields")
        if not isinstance(fields, dict):
            continue
        sp = parse_story_points(fields)
        if sp > 0:
            issues_with_points += 1
        committed_sp += sp
        if _issue_is_done(fields, status_map):
            delivered_sp += sp
            delivered_issues += 1

    truncated = reported_total is not None and reported_total > len(issues)
    return {
        "team": team_label,
        "board_id": board_id,
        "board_name": board.get("name"),
        "project_key": board.get("project_key"),
        "sprint": sprint,
        "story_points_delivered": round(delivered_sp, 2),
        "story_points_committed": round(committed_sp, 2),
        "delivered_issues": delivered_issues,
        "committed_issues": len(issues),
        "issues_with_story_points": issues_with_points,
        "reported_total": reported_total,
        "truncated": truncated,
        "excluded_issue_types": list(excluded_issue_types),
        "elapsed_seconds": round(elapsed_seconds, 1),
    }


def board_sprint_story_points(
    client: JiraClient,
    board: dict[str, Any],
    *,
    status_map: dict[str, str],
    excluded_issue_types: tuple[str, ...],
    max_issues: int = 500,
    timeout: float = 60.0,
    sprint: dict[str, Any] | None = None,
    sprint_selector: SprintSelector | None = None,
) -> dict[str, Any]:
    """Story points on Done issues in one sprint (latest closed unless *sprint* / selector set)."""
    board_id = int(board["board_id"])
    team_label = str(board.get("team_label") or board.get("name") or board_id)
    t0 = time.time()

    if sprint is None:
        try:
            sprint = resolve_board_sprint(
                client,
                board_id,
                sprint_selector,
                timeout=timeout,
            )
        except requests.RequestException as e:
            return {
                "team": team_label,
                "board_id": board_id,
                "board_name": board.get("name"),
                "error": f"sprint list failed: {e}",
            }

    if sprint is None:
        hint = "no closed sprint on board"
        if sprint_selector and sprint_selector.is_set():
            hint = "no sprint matched selector on board"
        return {
            "team": team_label,
            "board_id": board_id,
            "board_name": board.get("name"),
            "error": hint,
        }

    sprint_id = int(sprint["id"])
    try:
        issues, reported_total = _fetch_sprint_issues(
            client,
            sprint_id,
            max_issues=max_issues,
            excluded_issue_types=excluded_issue_types,
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {
            "team": team_label,
            "board_id": board_id,
            "board_name": board.get("name"),
            "sprint": sprint,
            "error": f"sprint issues failed: {e}",
        }

    return _story_points_result(
        team_label=team_label,
        board=board,
        sprint=sprint,
        issues=issues,
        reported_total=reported_total,
        status_map=status_map,
        excluded_issue_types=excluded_issue_types,
        elapsed_seconds=time.time() - t0,
    )


def sum_delivered_story_points(teams: list[Any]) -> float:
    """Total story points delivered across boards (Done issues only)."""
    total = 0.0
    for team in teams:
        if not isinstance(team, dict) or team.get("error"):
            continue
        total += float(team.get("story_points_delivered") or 0)
    return round(total, 2)


def get_sprint_story_points_history(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    history_count: int = 10,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
) -> dict[str, Any]:
    """Per-board story points delivered for the last *history_count* closed sprints."""
    boards, err = _resolve_boards(board_ids)
    if err:
        return err

    excluded = (
        parse_excluded_issue_types(include_all=include_all_issue_types)
        if excluded_issue_types is None
        else excluded_issue_types
    )

    try:
        status_map = load_status_category_map(client, timeout=timeout)
    except Exception as e:
        return {"error": f"failed to load Jira statuses: {e}", "boards": []}

    n = max(1, int(history_count))
    board_rows: list[dict[str, Any]] = []
    for board in boards:
        board_id = int(board["board_id"])
        team_label = str(board.get("team_label") or board.get("name") or board_id)
        try:
            sprints = list_board_sprints(
                client,
                board_id,
                state="closed",
                max_sprints=n,
                timeout=timeout,
            )
        except requests.RequestException as e:
            board_rows.append(
                {
                    "team": team_label,
                    "board_id": board_id,
                    "board_name": board.get("name"),
                    "error": f"sprint list failed: {e}",
                    "sprints": [],
                }
            )
            continue

        sprint_rows: list[dict[str, Any]] = []
        for sprint in sprints:
            sprint_rows.append(
                board_sprint_story_points(
                    client,
                    board,
                    status_map=status_map,
                    excluded_issue_types=excluded,
                    max_issues=max_issues_per_board,
                    timeout=timeout,
                    sprint=sprint,
                )
            )

        board_rows.append(
            {
                "team": team_label,
                "board_id": board_id,
                "board_name": board.get("name"),
                "sprints": sprint_rows,
            }
        )

    return {
        "mode": "history",
        "history_count": n,
        "definition": (
            f"Story points on Done issues for the last {n} closed sprint(s) per board "
            f"({STORY_POINTS_FIELD})"
        ),
        "excluded_issue_types": list(excluded),
        "boards": board_rows,
    }


def get_sprint_story_points_by_team(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
    sprint_selector: SprintSelector | None = None,
) -> dict[str, Any]:
    """Story points delivered per configured scrum board."""
    boards, err = _resolve_boards(board_ids)
    if err:
        return err

    excluded = (
        parse_excluded_issue_types(include_all=include_all_issue_types)
        if excluded_issue_types is None
        else excluded_issue_types
    )

    try:
        status_map = load_status_category_map(client, timeout=timeout)
    except Exception as e:
        return {"error": f"failed to load Jira statuses: {e}", "teams": []}

    sel = sprint_selector or SprintSelector()
    if sel.active:
        definition = "Story points delivered (Done) for the active sprint on each board"
    elif sel.is_set():
        definition = "Story points delivered (Done) for the sprint matching selector on each board"
    else:
        definition = (
            "Story points on Done issues in the latest closed sprint on each board "
            f"({STORY_POINTS_FIELD})"
        )

    teams: list[dict[str, Any]] = []
    for board in boards:
        try:
            teams.append(
                board_sprint_story_points(
                    client,
                    board,
                    status_map=status_map,
                    excluded_issue_types=excluded,
                    max_issues=max_issues_per_board,
                    timeout=timeout,
                    sprint_selector=sel if sel.is_set() else None,
                )
            )
        except Exception as e:
            logger.exception("sprint story points board %s", board.get("board_id"))
            teams.append(
                {
                    "team": board.get("team_label"),
                    "board_id": board.get("board_id"),
                    "error": str(e),
                }
            )

    if not any(isinstance(t, dict) and not t.get("error") for t in teams):
        errors = [str(t.get("error")) for t in teams if isinstance(t, dict) and t.get("error")]
        detail = errors[0] if errors else "no sprint story point data"
        return {"error": f"sprint story points unavailable: {detail}", "teams": teams}

    return {
        "mode": "snapshot",
        "definition": definition,
        "sprint_selector": {
            "active": sel.active,
            "sprint_id": sel.sprint_id,
            "sprint_number": sel.sprint_number,
            "week": sel.week,
            "sprint_name": sel.sprint_name,
        }
        if sel.is_set()
        else None,
        "total_story_points_delivered": sum_delivered_story_points(teams),
        "excluded_issue_types": list(excluded),
        "boards": [b["board_id"] for b in boards],
        "teams": teams,
    }


def get_sprint_story_points_metric_value(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
) -> dict[str, Any]:
    """Minimal payload for ``metrics-upsert``: total story points delivered (``numerator`` / ``denominator`` = 1)."""
    payload = get_sprint_story_points_by_team(
        client,
        board_ids=board_ids,
        max_issues_per_board=max_issues_per_board,
        timeout=timeout,
        excluded_issue_types=excluded_issue_types,
        include_all_issue_types=include_all_issue_types,
    )
    if payload.get("error"):
        return {"error": payload["error"]}
    total = payload.get("total_story_points_delivered")
    if total is None:
        return {"error": "sprint story points unavailable: no total_story_points_delivered"}
    return {
        "numerator": float(total),
        "denominator": 1.0,
    }
