"""Sprint delivery % per development team from Jira Agile boards."""

from __future__ import annotations

import logging
import re
import statistics
import time
from dataclasses import dataclass
from typing import Any

import requests

from .jira_agile_discovery import _paginate_agile
from .jira_client import JiraClient, _parse_jira_dt
from .jira_cycle_time import (
    DEV_CYCLE_TIME_BOARDS,
    _issue_excluded,
    load_status_category_map,
    parse_excluded_issue_types,
)

logger = logging.getLogger("cortex")

_AGILE = "/rest/agile/1.0"

# Scrum boards with sprint cadence (exclude kanban-style backlog boards).
SPRINT_DELIVERY_BOARDS: tuple[dict[str, Any], ...] = tuple(
    b for b in DEV_CYCLE_TIME_BOARDS if int(b["board_id"]) != 322
)

_PERCENT_DENOMINATOR = 100.0

# "Agile Team" custom field on the LEAN board — the LEAN engineering org runs many
# teams on a single board (44), distinguished by this field rather than by separate
# Jira boards. Used to segment the scorecard into per-team rows.
AGILE_TEAM_FIELD = "customfield_10633"
LEAN_SCORECARD_BOARD_ID = 44


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
    fields: str = "summary,status,issuetype",
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
                "fields": fields,
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


def _estimate_sum(node: Any) -> float | None:
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, (int, float)):
            return float(v)
    return None


def board_sprint_report(
    client: JiraClient,
    board_id: int,
    sprint_id: int,
    *,
    timeout: float = 60.0,
) -> dict[str, Any] | None:
    """Authoritative completed-vs-committed for one sprint, from Jira's sprint report.

    Uses the greenhopper sprint report that powers Jira's velocity chart — the only
    source that knows the sprint's state *at close* (what was completed, what was left
    incomplete, what was punted/removed). The agile sprint-issue endpoint only returns
    current membership, which over-counts delivery for boards that punt incomplete work
    out at close (e.g. LEAN reads as ~100%). Returns ``None`` on failure.

    ``committed`` is the count of issues present at close (completed + not completed);
    punted/removed issues are excluded from the say/do ratio.
    """
    try:
        resp = requests.get(
            f"{client.base_url}/rest/greenhopper/1.0/rapid/charts/sprintreport",
            headers=client._headers,
            params={"rapidViewId": int(board_id), "sprintId": int(sprint_id)},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("sprint report failed (board=%s sprint=%s): %s", board_id, sprint_id, e)
        return None

    contents = (resp.json() or {}).get("contents") or {}
    completed = len(contents.get("completedIssues") or [])
    not_completed = len(contents.get("issuesNotCompletedInCurrentSprint") or [])
    punted = len(contents.get("puntedIssues") or [])
    committed = completed + not_completed

    completed_sp = _estimate_sum(contents.get("completedIssuesEstimateSum"))
    not_completed_sp = _estimate_sum(contents.get("issuesNotCompletedEstimateSum"))
    committed_sp = None
    if completed_sp is not None or not_completed_sp is not None:
        committed_sp = (completed_sp or 0.0) + (not_completed_sp or 0.0)

    return {
        "completed": completed,
        "not_completed": not_completed,
        "punted": punted,
        "committed": committed,
        "delivery_pct": round(completed / committed * 100, 3) if committed else None,
        "completed_sp": completed_sp,
        "committed_sp": committed_sp,
    }


def _agile_team_name(value: Any) -> str | None:
    """Normalize the Agile Team field (select object, entity, list, or string)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return (value.get("value") or value.get("name") or value.get("title") or "").strip() or None
    if isinstance(value, list):
        for v in value:
            n = _agile_team_name(v)
            if n:
                return n
        return None
    s = str(value).strip()
    return s or None


def _lead_time_days(created: Any, resolved: Any) -> float | None:
    start = _parse_jira_dt(created)
    end = _parse_jira_dt(resolved)
    if not (start and end) or end < start:
        return None
    return (end - start).total_seconds() / 86400.0


def lean_sprint_delivery_by_agile_team(
    client: JiraClient,
    board: dict[str, Any],
    *,
    status_map: dict[str, str],
    excluded_issue_types: tuple[str, ...],
    max_issues: int = 1000,
    timeout: float = 60.0,
    sprint: dict[str, Any] | None = None,
    sprint_selector: SprintSelector | None = None,
    min_committed: int = 1,
) -> list[dict[str, Any]]:
    """Per-Agile-Team delivery rows for one board's sprint (latest closed by default).

    The LEAN board hosts several engineering teams on one board, tagged by the Agile
    Team field. This returns one scorecard row per team (ticket-count delivery + lead
    time; no story points, since LEAN does not estimate in SP), newest sprint resolved
    per ``sprint_selector``. Returns ``[]`` when no sprint or no issues are found.
    """
    board_id = int(board["board_id"])
    if sprint is None:
        try:
            sprint = resolve_board_sprint(client, board_id, sprint_selector, timeout=timeout)
        except requests.RequestException as e:
            logger.warning("LEAN agile-team sprint resolve failed: %s", e)
            return []
    if sprint is None:
        return []

    sprint_id = int(sprint["id"])
    win_start = _parse_jira_dt(sprint.get("start"))
    win_end = _parse_jira_dt(sprint.get("end"))
    # End date parses to midnight; include the whole final day of the sprint.
    if win_end is not None:
        from datetime import timedelta
        win_end = win_end + timedelta(days=1)

    def _resolved_in_window(resolved: Any) -> bool:
        rd = _parse_jira_dt(resolved)
        if rd is None:
            return False
        if win_start and rd < win_start:
            return False
        if win_end and rd > win_end:
            return False
        return True

    fields = f"status,issuetype,created,resolutiondate,{AGILE_TEAM_FIELD}"
    try:
        issues, _ = _fetch_sprint_issues(
            client, sprint_id,
            max_issues=max_issues,
            excluded_issue_types=excluded_issue_types,
            timeout=timeout,
            fields=fields,
        )
    except requests.RequestException as e:
        logger.warning("LEAN agile-team sprint issues failed: %s", e)
        return []

    groups: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        f = issue.get("fields") or {}
        if not isinstance(f, dict):
            continue
        team = _agile_team_name(f.get(AGILE_TEAM_FIELD)) or "Unassigned"
        groups.setdefault(team, []).append(f)

    rows: list[dict[str, Any]] = []
    for team, fs in groups.items():
        # LEAN punts incomplete work out of a sprint at close, so a "committed vs
        # delivered" ratio is not meaningful here — we report throughput instead:
        # issues actually resolved inside the sprint window. Fall back to done-status
        # count if no resolution dates are available.
        in_window = sum(1 for f in fs if _resolved_in_window(f.get("resolutiondate")))
        done_count = sum(1 for f in fs if _issue_is_done(f, status_map))
        throughput = in_window or done_count
        if throughput < min_committed:
            continue
        leads = [
            d for f in fs
            if _issue_is_done(f, status_map)
            and (d := _lead_time_days(f.get("created"), f.get("resolutiondate"))) is not None
        ]
        median_lead = round(statistics.median(leads), 2) if leads else None
        rows.append({
            "team": team,
            "board_id": board_id,
            "board_name": board.get("name"),
            "project_key": board.get("project_key"),
            "sprint": sprint,
            "sprint_name": sprint.get("name"),
            # Delivery % is intentionally absent — LEAN runs continuous flow, not
            # committed sprints (see board_sprint_report); throughput is the honest metric.
            "delivery_pct": None,
            "delivered": throughput,
            "committed": None,
            "throughput": throughput,
            "delivery_basis": "throughput",
            # LEAN does not estimate in story points.
            "story_points_delivered": None,
            "story_points_committed": None,
            "median_cycle_days": None,
            "median_lead_days": median_lead,
            "agile_team": True,
        })
    # "Unassigned" (no team tag) sinks to the bottom; real teams by throughput volume.
    rows.sort(key=lambda r: (r["team"] == "Unassigned", -r["throughput"]))
    return rows


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
