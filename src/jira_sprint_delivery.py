"""Sprint delivery % per development team from Jira Agile boards."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .jira_agile_discovery import _fetch_board_sprints
from .jira_client import JiraClient
from .jira_cycle_time import (
    DEV_CYCLE_TIME_BOARDS,
    _issue_excluded,
    load_status_category_map,
    parse_excluded_issue_types,
)

logger = logging.getLogger("bpo")

_AGILE = "/rest/agile/1.0"

# Scrum boards with sprint cadence (exclude kanban-style backlog boards).
SPRINT_DELIVERY_BOARDS: tuple[dict[str, Any], ...] = tuple(
    b for b in DEV_CYCLE_TIME_BOARDS if int(b["board_id"]) != 322
)

_PERCENT_DENOMINATOR = 100.0


def average_team_delivery_pct(teams: list[Any]) -> float:
    """Unweighted mean of per-board sprint delivery % (teams with committed work only)."""
    pcts: list[float] = []
    for team in teams:
        if not isinstance(team, dict) or team.get("error"):
            continue
        pct = team.get("delivery_pct")
        if pct is None:
            committed = team.get("committed")
            delivered = team.get("delivered")
            try:
                committed_f = float(committed)
                delivered_f = float(delivered)
            except (TypeError, ValueError):
                continue
            if committed_f <= 0:
                continue
            pct = (delivered_f / committed_f) * 100.0
        pcts.append(float(pct))
    if not pcts:
        errors = [str(t.get("error")) for t in teams if isinstance(t, dict) and t.get("error")]
        detail = errors[0] if errors else "no teams with committed sprint issues"
        raise ValueError(detail)
    return round(sum(pcts) / len(pcts), 3)


def _issue_is_done(fields: dict[str, Any], status_map: dict[str, str]) -> bool:
    status = fields.get("status")
    if isinstance(status, dict):
        cat = status.get("statusCategory")
        if isinstance(cat, dict) and str(cat.get("key") or "").strip().lower() == "done":
            return True
        name = str(status.get("name") or "").strip().lower()
        if name and status_map.get(name) == "done":
            return True
    return False


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

    while len(issues) < max_issues:
        resp = requests.get(
            f"{client.base_url}{_AGILE}/sprint/{sprint_id}/issue",
            headers=client._headers,
            params={
                "startAt": start_at,
                "maxResults": min(page_size, max_issues - len(issues)),
                "fields": "summary,status,issuetype",
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


def _pick_latest_closed_sprint(sprints: dict[str, Any]) -> dict[str, Any] | None:
    closed = sprints.get("recent_closed") or []
    if not isinstance(closed, list) or not closed:
        return None
    candidates = [s for s in closed if isinstance(s, dict) and s.get("id") is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda s: str(s.get("end") or ""))


def board_sprint_delivery(
    client: JiraClient,
    board: dict[str, Any],
    *,
    status_map: dict[str, str],
    excluded_issue_types: tuple[str, ...],
    max_issues: int = 500,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Delivery % for the board's most recently closed sprint."""
    board_id = int(board["board_id"])
    team_label = str(board.get("team_label") or board.get("name") or board_id)
    t0 = time.time()

    try:
        sprints = _fetch_board_sprints(client, board_id, timeout=timeout)
    except requests.RequestException as e:
        return {
            "team": team_label,
            "board_id": board_id,
            "board_name": board.get("name"),
            "error": f"sprint list failed: {e}",
        }

    sprint = _pick_latest_closed_sprint(sprints)
    if sprint is None:
        return {
            "team": team_label,
            "board_id": board_id,
            "board_name": board.get("name"),
            "error": "no closed sprint on board",
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

    committed = len(issues)
    delivered = 0
    for issue in issues:
        fields = issue.get("fields") or {}
        if isinstance(fields, dict) and _issue_is_done(fields, status_map):
            delivered += 1

    pct = round((delivered / committed) * 100, 3) if committed else None
    truncated = reported_total is not None and reported_total > len(issues)

    return {
        "team": team_label,
        "board_id": board_id,
        "board_name": board.get("name"),
        "project_key": board.get("project_key"),
        "sprint": sprint,
        "committed": committed,
        "delivered": delivered,
        "delivery_pct": pct,
        "reported_total": reported_total,
        "truncated": truncated,
        "excluded_issue_types": list(excluded_issue_types),
        "elapsed_seconds": round(time.time() - t0, 1),
    }


def get_sprint_delivery_by_team(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
) -> dict[str, Any]:
    """Sprint delivery % for each configured scrum board (latest closed sprint)."""
    boards = list(SPRINT_DELIVERY_BOARDS)
    if board_ids:
        wanted = {int(b) for b in board_ids}
        boards = [b for b in boards if int(b["board_id"]) in wanted]
        missing = wanted - {int(b["board_id"]) for b in boards}
        if missing:
            return {
                "error": f"unknown board id(s): {sorted(missing)}; "
                f"configured: {[b['board_id'] for b in SPRINT_DELIVERY_BOARDS]}",
                "teams": [],
            }
    if not boards:
        return {"error": "no boards selected", "teams": []}

    excluded = (
        parse_excluded_issue_types(include_all=include_all_issue_types)
        if excluded_issue_types is None
        else excluded_issue_types
    )

    try:
        status_map = load_status_category_map(client, timeout=timeout)
    except Exception as e:
        return {"error": f"failed to load Jira statuses: {e}", "teams": []}

    teams: list[dict[str, Any]] = []
    for board in boards:
        try:
            teams.append(
                board_sprint_delivery(
                    client,
                    board,
                    status_map=status_map,
                    excluded_issue_types=excluded,
                    max_issues=max_issues_per_board,
                    timeout=timeout,
                )
            )
        except Exception as e:
            logger.exception("sprint delivery board %s", board.get("board_id"))
            teams.append(
                {
                    "team": board.get("team_label"),
                    "board_id": board.get("board_id"),
                    "error": str(e),
                }
            )

    try:
        average_pct = average_team_delivery_pct(teams)
    except ValueError as e:
        return {"error": f"sprint delivery unavailable: {e}", "teams": teams}

    return {
        "metric": "Sprint Delivery %",
        "definition": (
            "Unweighted average of per-board sprint delivery % for the latest closed sprint "
            "on each board (Done / committed issues)"
        ),
        "average_delivery_pct": average_pct,
        "excluded_issue_types": list(excluded),
        "boards": [b["board_id"] for b in boards],
        "teams": teams,
    }


def get_sprint_delivery_metric_value(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
) -> dict[str, Any]:
    """Minimal payload for ``metrics-upsert``: average % only (``numerator`` / ``denominator`` = 100)."""
    payload = get_sprint_delivery_by_team(
        client,
        board_ids=board_ids,
        max_issues_per_board=max_issues_per_board,
        timeout=timeout,
        excluded_issue_types=excluded_issue_types,
        include_all_issue_types=include_all_issue_types,
    )
    if payload.get("error"):
        return {"error": payload["error"]}
    average_pct = payload.get("average_delivery_pct")
    if average_pct is None:
        return {"error": "sprint delivery unavailable: no average_delivery_pct"}
    return {
        "numerator": float(average_pct),
        "denominator": _PERCENT_DENOMINATOR,
    }
