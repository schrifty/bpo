"""Jira HELP generator for Health Score ``escalation_rate``.

For the calendar month stored on the reading, counts the entity's HELP tickets
and the shares that are Bug Severity S1 or S2 or labeled ``jira_escalated``.
The reading is the higher of those two shares. Both shares at 0, including a
month with no tickets, scores 2. Any S1, S2, or escalated ticket scores 0.
Tickets are attributed by JSM organization. Outage and Healthcheck are
excluded. No organization match stays unscored.

A snapshot dated in a month records the previous calendar month, the same
period key the rest of the monthly Health Score uses.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from src.healthscore_web.jsm_match import (
    JsmMatchError,
    created_day,
    issues_by_organization,
    matched_issues,
    monthly_as_ofs,
    resolve_organizations,
)
from src.healthscore_web.store import connect, period_key_for, periods_for_metric, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "escalation_rate"
GRAIN = "monthly"
GENERATOR_NAME = "get_escalation_rate"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "support", "escalation"]
SEV_12 = frozenset({"s1", "s2", "sev-1", "sev-2", "sev1", "sev2"})
ESCALATED_LABEL = "jira_escalated"


class EscalationRateGeneratorError(RuntimeError):
    pass


def is_sev_12(severity: Any) -> bool:
    return str(severity or "").strip().casefold() in SEV_12


def is_escalated(issue: dict[str, Any]) -> bool:
    labels = issue.get("labels") or []
    return any(str(label).strip().casefold() == ESCALATED_LABEL for label in labels)


def escalation_value(tickets: int, sev: int, escalated: int) -> float:
    """Higher of the Sev-1/2 share and the escalated share. 0 when there are no tickets."""
    if tickets < 0 or sev < 0 or escalated < 0:
        raise EscalationRateGeneratorError(
            f"counts must be >= 0, got tickets={tickets} sev={sev} escalated={escalated}"
        )
    if tickets == 0:
        return 0.0
    sev_pct = 100 * sev / tickets
    escalated_pct = 100 * escalated / tickets
    return round(max(sev_pct, escalated_pct), 1)


def escalation_points(tickets: int, sev: int, escalated: int) -> int:
    """2 when neither share is above 0. 0 when any S1/S2 or escalated ticket is present."""
    if tickets < 0 or sev < 0 or escalated < 0:
        raise EscalationRateGeneratorError(
            f"counts must be >= 0, got tickets={tickets} sev={sev} escalated={escalated}"
        )
    if sev == 0 and escalated == 0:
        return 2
    return 0


def period_month(as_of: date) -> tuple[str, date, date]:
    """Calendar month named by this snapshot's period key, inclusive."""
    key = period_key_for(GRAIN, as_of)
    year_text, month_text = key.split("-")
    year, month = int(year_text), int(month_text)
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return key, start, end


def _tally(issues: list[dict[str, Any]], start: date, end: date) -> tuple[int, int, int]:
    tickets = sev = escalated = 0
    for issue in issues:
        day = created_day(issue)
        if day is None or day < start or day > end:
            continue
        tickets += 1
        if is_sev_12(issue.get("severity")):
            sev += 1
        if is_escalated(issue):
            escalated += 1
    return tickets, sev, escalated


def load_created_issues(
    *,
    start: date,
    end: date,
    client: Any | None = None,
) -> dict[str, Any]:
    from src.jira_client import JiraClient

    jira = client if client is not None else JiraClient()
    try:
        payload = jira.list_help_created_issues(start=start, end=end)
    except Exception as exc:  # noqa: BLE001
        raise EscalationRateGeneratorError(
            f"Jira HELP escalation-rate fetch failed: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise EscalationRateGeneratorError("Jira HELP escalation-rate fetch returned a non-object")
    if payload.get("error"):
        raise EscalationRateGeneratorError(
            f"Jira HELP escalation-rate fetch failed: {payload['error']}"
        )
    issues = payload.get("issues")
    if not isinstance(issues, list):
        raise EscalationRateGeneratorError("Jira HELP escalation-rate fetch returned no issue list")
    return payload


def _score_entities(
    *,
    entities: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    organizations_by_entity: dict[str, list[str]],
    as_of: date,
    truncated: bool,
    persist: bool,
    generator: str,
) -> dict[str, Any]:
    period_key, start, end = period_month(as_of)
    by_org = issues_by_organization(issues)
    if truncated:
        logger.warning(
            "Health Score escalation_rate: HELP created-ticket fetch hit its cap; "
            "scores use the fetched subset"
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            orgs = organizations_by_entity.get(entity_id) or []
            error = None
            value = None
            points = None
            tickets = sev = escalated = 0
            if not orgs:
                error = "no JSM organization match for this Customer Entity"
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            else:
                matched = matched_issues(orgs, by_org)
                tickets, sev, escalated = _tally(matched, start, end)
                value = escalation_value(tickets, sev, escalated)
                points = escalation_points(tickets, sev, escalated)
            meta = {
                "tickets": tickets,
                "sev_12": sev,
                "escalated": escalated,
                "month_start": start.isoformat(),
                "month_end": end.isoformat(),
                "jsm_organizations": list(orgs),
                "truncated": truncated,
                "owner": OWNER_EMAIL,
            }
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": period_key,
                        "as_of": as_of.isoformat(),
                        "value": value,
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
                        "error": error,
                    }
                )
                continue
            readings.append(
                upsert_reading(
                    conn,
                    entity_id=entity_id,
                    entity_name=str(entity.get("name") or ""),
                    metric_name=METRIC_NAME,
                    grain=GRAIN,
                    as_of=as_of,
                    value=value,
                    points=None if points is None else float(points),
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                    error=error,
                )
            )
    finally:
        if conn is not None:
            conn.close()
    scored = sum(1 for row in readings if row.get("points") is not None)
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "scored": scored,
        "unscored": len(readings) - scored,
        "truncated": truncated,
        "warnings": warnings,
        "readings": readings,
    }


def get_escalation_rate(
    *,
    entities: list[dict[str, Any]],
    issues: list[dict[str, Any]] | None = None,
    organizations_by_entity: dict[str, list[str]] | None = None,
    truncated: bool = False,
    client: Any | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score HELP escalation rate for each active Salesforce Customer Entity."""
    if not entities:
        raise EscalationRateGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate escalation_rate"
        )
    today = as_of or date.today()
    if issues is None:
        _, start, end = period_month(today)
        payload = load_created_issues(start=start, end=end, client=client)
        issues = payload["issues"]
        truncated = bool(payload.get("truncated"))
    if organizations_by_entity is None:
        try:
            organizations_by_entity = resolve_organizations(entities, client=client)
        except JsmMatchError as exc:
            raise EscalationRateGeneratorError(str(exc)) from exc
    return _score_entities(
        entities=entities,
        issues=issues,
        organizations_by_entity=organizations_by_entity,
        as_of=today,
        truncated=truncated,
        persist=persist,
        generator=generator,
    )


def backfill_escalation_rate(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    issues: list[dict[str, Any]] | None = None,
    organizations_by_entity: dict[str, list[str]] | None = None,
    client: Any | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score each of the newest ``months`` calendar months."""
    if not entities:
        raise EscalationRateGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate escalation_rate"
        )
    today = as_of or date.today()
    try:
        days = monthly_as_ofs(today, months)
    except JsmMatchError as exc:
        raise EscalationRateGeneratorError(str(exc)) from exc
    existing: set[str] = set()
    if only_missing and persist:
        conn = connect()
        try:
            existing = set(periods_for_metric(conn, METRIC_NAME))
        finally:
            conn.close()
    pending = [day for day in days if period_key_for(GRAIN, day) not in existing]
    skipped = [period_key_for(GRAIN, day) for day in days if period_key_for(GRAIN, day) in existing]
    truncated = False
    if pending and issues is None:
        _, start, _ = period_month(pending[0])
        _, _, end = period_month(pending[-1])
        payload = load_created_issues(start=start, end=end, client=client)
        issues = payload["issues"]
        truncated = bool(payload.get("truncated"))
    elif issues is None:
        issues = []
    if pending and organizations_by_entity is None:
        try:
            organizations_by_entity = resolve_organizations(entities, client=client)
        except JsmMatchError as exc:
            raise EscalationRateGeneratorError(str(exc)) from exc
    scored: list[dict[str, Any]] = []
    for day in pending:
        result = _score_entities(
            entities=entities,
            issues=issues,
            organizations_by_entity=organizations_by_entity or {},
            as_of=day,
            truncated=truncated,
            persist=persist,
            generator=generator,
        )
        scored.append(
            {
                "period_key": period_key_for(GRAIN, day),
                "as_of": day.isoformat(),
                "scored": result.get("scored"),
                "unscored": result.get("unscored"),
                "ok": result.get("ok"),
            }
        )
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "months_requested": months,
        "months_scored": len(scored),
        "months_skipped": skipped,
        "dry_run": not persist,
        "truncated": truncated,
        "months": scored,
    }
