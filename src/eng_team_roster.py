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

logger = logging.getLogger("cortex")

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
# "Dev - Supply Insights"); we strip it for display. Matching is case-insensitive.
_DEV_TEAM_PREFIX = "dev - "


def _is_dev_team(team_name: Any) -> bool:
    return str(team_name or "").lower().startswith(_DEV_TEAM_PREFIX)


def _normalize_person_name(name: Any) -> str | None:
    s = str(name or "").strip()
    return s.casefold() if s else None

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

    dev_teams = [t for t in (payload.get("teams") or []) if _is_dev_team(t.get("name"))]
    if not dev_teams:
        return None

    rows: list[dict[str, Any]] = []
    unique_members: set[str] = set()
    for team in dev_teams:
        name = str(team.get("name") or "")
        raw_display = name[len(_DEV_TEAM_PREFIX):].strip() or name
        display = _TEAM_DISPLAY_ALIASES.get(raw_display, raw_display)
        members = [str(m) for m in (team.get("members") or [])]
        lead = leads.get(display, "") or leads.get(raw_display, "") or leads.get(name, "")
        # A team's lead should sit on their own team. If the configured lead is not in the
        # Atlassian membership (config/membership drift), add them so the roster never shows
        # someone leading a team they are not listed on.
        if lead and not any(m.strip().casefold() == lead.casefold() for m in members):
            members.append(lead)
        unique_members.update(members)
        rows.append({
            "team": display,
            "headcount": len(members),
            "members": members,
            "lead": lead,
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


def build_engineer_audience_scope(
    client: JiraClient, *, timeout: float = 60.0
) -> dict[str, Any]:
    """Engineer vs non-engineer audience for Cursor slides.

    **Engineers** = unique display names on Atlassian ``dev-*`` teams (``Dev - *`` prefix).
    **Non-engineers** = unique display names on other teams that are not engineers.

    Cursor usage is joined by resolving those names to corporate email via the Atlassian
    Teams membership API. Returns normalized name sets plus email sets for filtering.
    """
    empty: dict[str, Any] = {
        "error": None,
        "engineer_names": set(),
        "non_engineer_names": set(),
        "emails": set(),
        "non_engineer_emails": set(),
        "headcount": 0,
        "non_engineer_headcount": 0,
    }
    if not getattr(client, "atlassian_org_id", None):
        return {**empty, "error": "ATLASSIAN_ORG_ID is not set"}
    try:
        payload = client.get_atlassian_teams(timeout=timeout)
    except Exception as e:  # noqa: BLE001
        return {**empty, "error": str(e)}
    if payload.get("error"):
        return {**empty, "error": str(payload["error"])}

    teams = payload.get("teams") or []
    dev_teams = [t for t in teams if _is_dev_team(t.get("name"))]
    if not dev_teams:
        return {**empty, "error": "no dev-* Atlassian teams found"}

    engineer_names: set[str] = set()
    all_names: set[str] = set()
    dev_account_ids: set[str] = set()
    all_account_ids: set[str] = set()

    for team in teams:
        ids = team.get("member_account_ids") or []
        all_account_ids.update(ids)
        if _is_dev_team(team.get("name")):
            dev_account_ids.update(ids)
            for raw in team.get("members") or []:
                norm = _normalize_person_name(raw)
                if norm:
                    engineer_names.add(norm)
        for raw in team.get("members") or []:
            norm = _normalize_person_name(raw)
            if norm:
                all_names.add(norm)

    id_to_name = client.resolve_account_names(all_account_ids, timeout=timeout)
    id_to_email = client.resolve_account_emails(all_account_ids, timeout=timeout)

    for aid in dev_account_ids:
        norm = _normalize_person_name(id_to_name.get(aid))
        if norm:
            engineer_names.add(norm)

    non_engineer_names = all_names - engineer_names

    engineer_emails: set[str] = set()
    for aid in dev_account_ids:
        email = id_to_email.get(aid)
        if email and str(email).strip():
            engineer_emails.add(str(email).strip().casefold())
    for aid, email in id_to_email.items():
        norm = _normalize_person_name(id_to_name.get(aid))
        if norm and norm in engineer_names and email and str(email).strip():
            engineer_emails.add(str(email).strip().casefold())

    non_engineer_emails = {
        str(email).strip().casefold()
        for aid, email in id_to_email.items()
        if email and str(email).strip()
        and (norm := _normalize_person_name(id_to_name.get(aid)))
        and norm in non_engineer_names
    }

    return {
        "error": None,
        "engineer_names": engineer_names,
        "non_engineer_names": non_engineer_names,
        "emails": engineer_emails,
        "non_engineer_emails": non_engineer_emails,
        "headcount": len(engineer_names),
        "non_engineer_headcount": len(non_engineer_names),
        "source": "atlassian_teams",
    }


def build_engineer_email_set(
    client: JiraClient, *, timeout: float = 60.0
) -> dict[str, Any]:
    """Legacy alias — prefer :func:`build_engineer_audience_scope`."""
    scope = build_engineer_audience_scope(client, timeout=timeout)
    if scope.get("error"):
        return {
            "error": scope["error"],
            "emails": set(),
            "headcount": 0,
        }
    return {
        "error": None,
        "emails": scope["emails"],
        "headcount": scope["headcount"],
        "engineer_names": scope["engineer_names"],
        "non_engineer_names": scope["non_engineer_names"],
        "source": scope.get("source"),
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
