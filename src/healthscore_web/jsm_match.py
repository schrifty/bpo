"""Match Salesforce Customer Entities to Jira Service Management organizations."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any


class JsmMatchError(RuntimeError):
    pass


def monthly_as_ofs(as_of: date, months: int) -> list[date]:
    """Newest ``months`` snapshot dates, oldest first.

    The newest date is ``as_of``. Each older date is the last day of the
    previous month. Monthly Health Score period keys are the calendar month
    before each of these dates.
    """
    if months < 1:
        raise JsmMatchError("months must be at least 1")
    days = [as_of]
    cursor = as_of.replace(day=1)
    for _ in range(months - 1):
        previous_end = cursor - timedelta(days=1)
        days.append(previous_end)
        cursor = previous_end.replace(day=1)
    days.reverse()
    return days


def match_terms(entity: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    for key in ("entity_name", "parent_name"):
        raw = str(entity.get(key) or "").strip()
        if raw:
            terms.append(raw)
    return terms


def resolve_organizations(
    entities: list[dict[str, Any]],
    *,
    client: Any | None = None,
) -> dict[str, list[str]]:
    """JSM organization names for each entity. One directory lookup is shared."""
    from src.jira_client import JiraClient

    jira = client if client is not None else JiraClient()
    resolved: dict[str, list[str]] = {}
    for entity in entities:
        entity_id = str(entity["id"])
        name = str(entity.get("name") or "").strip()
        try:
            resolved[entity_id] = jira.resolve_jsm_organizations(
                name, match_terms(entity) or None
            )
        except Exception as exc:  # noqa: BLE001
            raise JsmMatchError(
                f"JSM organization match failed for {name or entity_id}: {exc}"
            ) from exc
    return resolved


def issues_by_organization(issues: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        raw = issue.get("organizations") or []
        if isinstance(raw, str):
            raw = [raw]
        for name in raw:
            label = str(name).strip()
            if label:
                grouped.setdefault(label.casefold(), []).append(issue)
    return grouped


def matched_issues(
    organizations: list[str],
    by_org: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    matched: list[dict[str, Any]] = []
    for org in organizations:
        for issue in by_org.get(str(org).casefold(), []):
            key = str(issue.get("key") or "")
            token = key or str(id(issue))
            if token in seen:
                continue
            seen.add(token)
            matched.append(issue)
    return matched


def created_day(issue: dict[str, Any]) -> date | None:
    raw = str(issue.get("created") or "")[:10]
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None
