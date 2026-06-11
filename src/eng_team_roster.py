"""Engineering team roster — members per Agile Team — for the portfolio deck.

The LEAN engineering org runs several teams on a single Jira board, distinguished by
the "Agile Team" field (see ``jira_sprint_delivery.AGILE_TEAM_FIELD``). This module
builds a clean roster by assigning each engineer to their *home* team — the team where
they did the most work over a trailing window — so people who occasionally touch other
teams' tickets are not double-counted across rosters.

Jira has no team-lead field, so leads are read from ``config/engineering_team_leads.yaml``
(optional; blank when unknown). We never infer a lead from activity.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .jira_client import JiraClient
from .jira_sprint_delivery import AGILE_TEAM_FIELD

logger = logging.getLogger("bpo")

_LEADS_PATH = Path(__file__).resolve().parent.parent / "config" / "engineering_team_leads.yaml"


def _load_team_leads() -> dict[str, str]:
    """Load the optional Agile Team → lead-name map. Returns ``{}`` when absent/empty."""
    try:
        import yaml

        with open(_LEADS_PATH, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:  # noqa: BLE001 — config is optional, never fatal
        logger.warning("team leads config load failed: %s", e)
        return {}

    mapping = data.get("teams") if isinstance(data, dict) and isinstance(data.get("teams"), dict) else data
    out: dict[str, str] = {}
    if isinstance(mapping, dict):
        for key, value in mapping.items():
            if isinstance(value, str) and value.strip():
                out[str(key).strip()] = value.strip()
    return out


def _agile_team_value(value: Any) -> str | None:
    if isinstance(value, dict):
        return (value.get("value") or value.get("name") or "").strip() or None
    if isinstance(value, str):
        return value.strip() or None
    return None


# Atlassian Teams for the engineering org are named with this prefix (e.g.
# "Dev - Supply Insights"); we strip it for display.
_DEV_TEAM_PREFIX = "dev - "

# Friendlier display names for abbreviated Atlassian team names.
_TEAM_DISPLAY_ALIASES = {
    "IOP": "Inventory Optimization",
}


def _roster_from_atlassian_teams(
    client: JiraClient, leads: dict[str, str], *, window_days: int, timeout: float
) -> dict[str, Any] | None:
    """Build the roster from Atlassian Teams (authoritative). Returns None to fall back.

    Only teams whose name starts with ``Dev - `` are included (the engineering squads);
    the prefix is stripped for display. Membership and headcount come straight from the
    Teams API, so the slide stays accurate as the org maintains team membership.
    """
    if not getattr(client, "atlassian_org_id", None):
        return None
    try:
        payload = client.get_atlassian_teams(timeout=timeout)
    except Exception as e:  # noqa: BLE001
        logger.warning("Atlassian Teams fetch failed, falling back to activity roster: %s", e)
        return None
    if payload.get("error"):
        logger.warning("Atlassian Teams error, falling back to activity roster: %s", payload["error"])
        return None

    dev_teams = [
        t for t in (payload.get("teams") or [])
        if str(t.get("name") or "").lower().startswith(_DEV_TEAM_PREFIX)
    ]
    if not dev_teams:
        return None

    rows: list[dict[str, Any]] = []
    unique_members: set[str] = set()
    for team in dev_teams:
        name = str(team.get("name") or "")
        raw_display = name[len(_DEV_TEAM_PREFIX):].strip() or name
        display = _TEAM_DISPLAY_ALIASES.get(raw_display, raw_display)
        members = [str(m) for m in (team.get("members") or [])]
        unique_members.update(members)
        rows.append({
            "team": display,
            "headcount": int(team.get("member_count") or len(members)),
            "members": members,
            "lead": leads.get(display, "") or leads.get(raw_display, "") or leads.get(name, ""),
        })
    rows.sort(key=lambda r: -r["headcount"])
    return {
        "window_days": window_days,
        "total_engineers": len(unique_members),
        "teams": rows,
        "leads_configured": bool(leads),
        "source": "atlassian_teams",
        "error": None,
    }


def build_eng_team_roster(
    client: JiraClient,
    *,
    window_days: int = 90,
    timeout: float = 60.0,
    min_team_size: int = 1,
) -> dict[str, Any]:
    """Roster of engineers per engineering team.

    Prefers **Atlassian Teams** (the ``Dev - *`` squads) as the authoritative source of
    membership. If those are unavailable, falls back to a Jira-activity heuristic: each
    engineer is assigned to the Agile Team where they did the most work over the trailing
    window, so people who cross teams are not double-counted. Teams sort by headcount.
    """
    leads = _load_team_leads()

    from_teams = _roster_from_atlassian_teams(
        client, leads, window_days=window_days, timeout=timeout
    )
    if from_teams is not None:
        return from_teams

    try:
        issues = client._search(
            f"project = LEAN AND (statusCategory != Done OR updated >= -{window_days}d)",
            max_results=5000,
            fields=["assignee", AGILE_TEAM_FIELD, "status"],
            data_description=f"LEAN team roster (assignee × Agile Team, last {window_days}d)",
        )
    except Exception as e:  # noqa: BLE001 — surface as slide error, don't crash the deck
        logger.warning("Team roster fetch failed: %s", e)
        return {"error": str(e), "teams": [], "total_engineers": 0, "window_days": window_days}

    # person → team → issue count
    person_team: dict[str, Counter] = defaultdict(Counter)
    for issue in issues:
        fields = issue.get("fields") or {}
        if not isinstance(fields, dict):
            continue
        team = _agile_team_value(fields.get(AGILE_TEAM_FIELD))
        assignee = fields.get("assignee")
        if not team or not isinstance(assignee, dict):
            continue
        name = assignee.get("displayName") or assignee.get("name")
        if name:
            person_team[name][team] += 1

    # Home team = the team where the person did the most work.
    home: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for person, teams in person_team.items():
        team, _ = teams.most_common(1)[0]
        home[team].append((person, sum(teams.values())))

    rows: list[dict[str, Any]] = []
    for team, members in home.items():
        if len(members) < min_team_size:
            continue
        members.sort(key=lambda m: -m[1])
        rows.append({
            "team": team,
            "headcount": len(members),
            "members": [name for name, _ in members],
            "lead": leads.get(team, ""),
        })
    rows.sort(key=lambda r: -r["headcount"])

    return {
        "window_days": window_days,
        "total_engineers": sum(r["headcount"] for r in rows),
        "teams": rows,
        "leads_configured": bool(leads),
        "source": "jira_activity",
        "error": None,
    }
