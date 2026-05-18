"""Discover Jira Software boards, sprints, and backlogs (Agile REST API)."""

from __future__ import annotations

import logging
from typing import Any

import requests

from .jira_client import JiraClient

logger = logging.getLogger("bpo")

_AGILE = "/rest/agile/1.0"


def _paginate_agile(
    client: JiraClient,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    max_pages: int = 50,
    timeout: float = 45.0,
) -> list[dict[str, Any]]:
    """Walk ``values`` pages from Jira Agile list endpoints."""
    out: list[dict[str, Any]] = []
    start_at = 0
    page_size = 50
    base = f"{client.base_url}{path}"
    for _ in range(max_pages):
        q: dict[str, Any] = {"startAt": start_at, "maxResults": page_size}
        if params:
            q.update(params)
        resp = requests.get(base, headers=client._headers, params=q, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        chunk = data.get("values") or []
        if not isinstance(chunk, list):
            break
        out.extend(x for x in chunk if isinstance(x, dict))
        if data.get("isLast", True):
            break
        start_at = int(data.get("startAt", start_at)) + len(chunk)
        if not chunk:
            break
    return out


def _board_project_key(board: dict[str, Any]) -> str | None:
    loc = board.get("location")
    if isinstance(loc, dict):
        for key in ("projectKey", "projectKeyOrId", "projectId"):
            val = loc.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return None


def _fetch_board_sprints(
    client: JiraClient,
    board_id: int,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    active: list[dict[str, Any]] = []
    recent_closed: list[dict[str, Any]] = []
    for state, bucket, max_results in (
        ("active", active, 5),
        ("closed", recent_closed, 3),
    ):
        url = f"{client.base_url}{_AGILE}/board/{board_id}/sprint"
        resp = requests.get(
            url,
            headers=client._headers,
            params={"state": state, "maxResults": max_results},
            timeout=timeout,
        )
        if not resp.ok:
            logger.debug("sprint %s board %s: %s", state, board_id, resp.status_code)
            continue
        vals = resp.json().get("values") or []
        for s in vals:
            if not isinstance(s, dict):
                continue
            bucket.append(
                {
                    "id": s.get("id"),
                    "name": s.get("name"),
                    "state": s.get("state"),
                    "start": (s.get("startDate") or "")[:10] or None,
                    "end": (s.get("endDate") or "")[:10] or None,
                    "goal": (s.get("goal") or "")[:120] or None,
                }
            )
    return {"active": active, "recent_closed": recent_closed}


def _fetch_backlog_summary(
    client: JiraClient,
    board_id: int,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Lightweight backlog probe (first page only)."""
    url = f"{client.base_url}{_AGILE}/board/{board_id}/backlog"
    try:
        resp = requests.get(
            url,
            headers=client._headers,
            params={"maxResults": 1, "fields": "key"},
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": str(e)}
    if resp.status_code == 404:
        return {"ok": False, "error": "no backlog (kanban or unsupported)"}
    if not resp.ok:
        return {"ok": False, "error": f"HTTP {resp.status_code}"}
    data = resp.json()
    issues = data.get("issues") or []
    total = data.get("total")
    if total is None:
        # Some responses omit total; approximate from first page flag
        total = len(issues) if data.get("isLast", True) else None
    return {
        "ok": True,
        "total": int(total) if total is not None else None,
        "sample_keys": [i.get("key") for i in issues[:3] if isinstance(i, dict)],
    }


def discover_development_boards(
    client: JiraClient,
    *,
    project_key: str | None = None,
    include_inactive: bool = False,
    sprint_probe: bool = True,
    backlog_probe: bool = True,
    timeout: float = 45.0,
) -> dict[str, Any]:
    """List boards (optionally per project) with sprint/backlog activity signals."""
    params: dict[str, Any] = {}
    if project_key:
        params["projectKeyOrId"] = project_key.strip()
    try:
        boards_raw = _paginate_agile(client, f"{_AGILE}/board", params=params, timeout=timeout)
    except requests.HTTPError as e:
        return {"error": str(e), "boards": []}

    boards_out: list[dict[str, Any]] = []
    for b in boards_raw:
        bid = b.get("id")
        if bid is None:
            continue
        try:
            board_id = int(bid)
        except (TypeError, ValueError):
            continue
        entry: dict[str, Any] = {
            "board_id": board_id,
            "name": b.get("name"),
            "type": b.get("type"),
            "project_key": _board_project_key(b),
            "self": b.get("self"),
        }
        if sprint_probe:
            entry["sprints"] = _fetch_board_sprints(client, board_id, timeout=timeout)
        if backlog_probe:
            entry["backlog"] = _fetch_backlog_summary(client, board_id, timeout=timeout)
        active_sprints = (entry.get("sprints") or {}).get("active") or []
        backlog_total = ((entry.get("backlog") or {}).get("total")) if entry.get("backlog", {}).get("ok") else None
        entry["active"] = bool(active_sprints) or (backlog_total or 0) > 0
        if include_inactive or entry["active"]:
            boards_out.append(entry)

    # Group by project for readability
    by_project: dict[str, list[dict[str, Any]]] = {}
    for row in boards_out:
        pk = row.get("project_key") or "(no project)"
        by_project.setdefault(str(pk), []).append(row)

    return {
        "jira_base": client.base_url,
        "project_filter": project_key,
        "board_count": len(boards_out),
        "boards": boards_out,
        "by_project": by_project,
    }


def try_discover_atlassian_teams(client: JiraClient, *, timeout: float = 20.0) -> dict[str, Any]:
    """Best-effort Teams API probe (often unavailable with basic Jira API token auth)."""
    candidates = (
        "/rest/teams/1.0/teams/find",
        "/gateway/api/public/teams/v1/teams",
    )
    results: list[dict[str, Any]] = []
    for path in candidates:
        url = f"{client.base_url}{path}"
        try:
            resp = requests.get(url, headers=client._headers, timeout=timeout)
            results.append(
                {
                    "path": path,
                    "status": resp.status_code,
                    "ok": resp.ok,
                    "note": (resp.text or "")[:200] if not resp.ok else "ok",
                }
            )
        except requests.RequestException as e:
            results.append({"path": path, "ok": False, "error": str(e)})
    return {"teams_api_probes": results}
