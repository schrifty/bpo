"""Sprint delivery % per development team from Jira Agile boards."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from .jira_agile_discovery import _paginate_agile
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


@dataclass(frozen=True)
class SprintSelector:
    """Pick a sprint on a board by id, LEAN sprint number, week label, or name substring."""

    sprint_id: int | None = None
    sprint_number: int | None = None
    week: str | None = None
    sprint_name: str | None = None
    active: bool = False

    def is_set(self) -> bool:
        return self.active or any(
            v is not None and str(v).strip() != ""
            for v in (self.sprint_id, self.sprint_number, self.week, self.sprint_name)
        )


def normalize_sprint_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "state": raw.get("state"),
        "start": (raw.get("startDate") or raw.get("start") or "")[:10] or None,
        "end": (raw.get("endDate") or raw.get("end") or "")[:10] or None,
        "goal": (raw.get("goal") or "")[:120] or None,
    }


def sprint_matches_selector(sprint: dict[str, Any], selector: SprintSelector) -> bool:
    """True when *sprint* matches a non-empty *selector*."""
    if not selector.is_set():
        return False
    if selector.active:
        return str(sprint.get("state") or "").lower() == "active"
    sid = sprint.get("id")
    if selector.sprint_id is not None and sid is not None:
        try:
            return int(sid) == int(selector.sprint_id)
        except (TypeError, ValueError):
            pass
    name = str(sprint.get("name") or "")
    if selector.sprint_name:
        return selector.sprint_name.casefold() in name.casefold()
    if selector.sprint_number is not None:
        n = str(int(selector.sprint_number))
        return bool(re.search(rf"(?i)\bsprint\s*{re.escape(n)}\b", name))
    if selector.week is not None:
        w = str(selector.week).strip()
        if not w:
            return False
        if w.casefold() in name.casefold():
            return True
        return bool(re.search(rf"(?i)\bweek\s*#?\s*{re.escape(w)}\b", name))
    return False


def list_board_sprints(
    client: JiraClient,
    board_id: int,
    *,
    state: str = "closed",
    max_sprints: int = 50,
    timeout: float = 30.0,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    """Paginated sprints for one board (``active``, ``closed``, or ``future``).

    Jira often returns closed sprints oldest-first; results are sorted by end date
    descending so index ``0`` is the most recently closed.
    """
    want = max(1, max_sprints)
    pages = max_pages if max_pages is not None else max(10, (want + 49) // 50)
    raw = _paginate_agile(
        client,
        f"{_AGILE}/board/{board_id}/sprint",
        params={"state": state},
        max_pages=pages,
        timeout=timeout,
    )
    rows = [normalize_sprint_row(s) for s in raw if isinstance(s, dict)]
    rows.sort(key=lambda s: str(s.get("end") or s.get("start") or ""), reverse=True)
    return rows[:want]


def resolve_board_sprint(
    client: JiraClient,
    board_id: int,
    selector: SprintSelector | None,
    *,
    timeout: float = 30.0,
) -> dict[str, Any] | None:
    """Resolve one sprint on *board_id* (latest closed when *selector* is empty)."""
    sel = selector or SprintSelector()
    if sel.active or sel.is_set():
        states = ("active",) if sel.active else ("closed", "active", "future")
        for state in states:
            for sprint in list_board_sprints(
                client,
                board_id,
                state=state,
                max_sprints=100,
                timeout=timeout,
            ):
                if sprint_matches_selector(sprint, sel):
                    return sprint
        return None
    closed = list_board_sprints(client, board_id, state="closed", max_sprints=1, timeout=timeout)
    return closed[0] if closed else None


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


def _delivery_result(
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
    committed = len(issues)
    delivered = sum(
        1
        for issue in issues
        if isinstance(issue.get("fields"), dict) and _issue_is_done(issue["fields"], status_map)
    )
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
        "elapsed_seconds": round(elapsed_seconds, 1),
    }


def board_sprint_delivery(
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
    """Delivery % for one sprint on a board (latest closed unless *sprint* / *sprint_selector* set)."""
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

    return _delivery_result(
        team_label=team_label,
        board=board,
        sprint=sprint,
        issues=issues,
        reported_total=reported_total,
        status_map=status_map,
        excluded_issue_types=excluded_issue_types,
        elapsed_seconds=time.time() - t0,
    )


def _resolve_boards(board_ids: list[int] | None) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    boards = list(SPRINT_DELIVERY_BOARDS)
    if board_ids:
        wanted = {int(b) for b in board_ids}
        boards = [b for b in boards if int(b["board_id"]) in wanted]
        missing = wanted - {int(b["board_id"]) for b in boards}
        if missing:
            return [], {
                "error": f"unknown board id(s): {sorted(missing)}; "
                f"configured: {[b['board_id'] for b in SPRINT_DELIVERY_BOARDS]}",
                "teams": [],
            }
    if not boards:
        return [], {"error": "no boards selected", "teams": []}
    return boards, None


def get_sprint_delivery_history(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    history_count: int = 10,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
) -> dict[str, Any]:
    """Per-board delivery % for the last *history_count* closed sprints."""
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
            row = board_sprint_delivery(
                client,
                board,
                status_map=status_map,
                excluded_issue_types=excluded,
                max_issues=max_issues_per_board,
                timeout=timeout,
                sprint=sprint,
            )
            sprint_rows.append(row)

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
        "definition": f"Delivery % for the last {n} closed sprint(s) per board",
        "excluded_issue_types": list(excluded),
        "boards": board_rows,
    }


def get_sprint_delivery_by_team(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    max_issues_per_board: int = 500,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
    sprint_selector: SprintSelector | None = None,
) -> dict[str, Any]:
    """Sprint delivery % for each configured scrum board."""
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
        definition = "Delivery % for the active sprint on each board"
    elif sel.is_set():
        definition = "Delivery % for the sprint matching selector on each board"
    else:
        definition = (
            "Unweighted average of per-board sprint delivery % for the latest closed sprint "
            "on each board (Done / committed issues)"
        )

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
                    sprint_selector=sel if sel.is_set() else None,
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
        "mode": "snapshot",
        "metric": "Sprint Delivery %",
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
