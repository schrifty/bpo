"""Jira HELP generator for Health Score ``ticket_volume_trend``.

Counts tickets the entity opened in the trailing 90 days and compares that
count with the 90 days before. Tickets are attributed by JSM organization.
Outage and Healthcheck labels are excluded. Three times the prior window, or
any tickets when the prior window had none, scores 0. Any smaller change,
including no tickets in either window, scores 3. No organization match stays
unscored.
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

METRIC_NAME = "ticket_volume_trend"
GRAIN = "monthly"
GENERATOR_NAME = "get_ticket_volume_trend"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "support"]
WINDOW_DAYS = 90
SPIKE_MULTIPLE = 3


class TicketVolumeGeneratorError(RuntimeError):
    pass


def volume_change_pct(current: int, prior: int) -> float | None:
    """Percent change. None when the prior window is empty and the current one is not."""
    if prior < 0 or current < 0:
        raise TicketVolumeGeneratorError(
            f"ticket counts must be >= 0, got current={current} prior={prior}"
        )
    if prior == 0:
        return None if current > 0 else 0.0
    return round(100 * (current - prior) / prior, 1)


def ticket_volume_points(current: int, prior: int) -> int:
    """0 on a 3x spike or a new stream with no baseline. 3 otherwise."""
    if prior < 0 or current < 0:
        raise TicketVolumeGeneratorError(
            f"ticket counts must be >= 0, got current={current} prior={prior}"
        )
    if prior == 0:
        return 0 if current > 0 else 3
    if current >= SPIKE_MULTIPLE * prior:
        return 0
    return 3


def _window(as_of: date) -> tuple[date, date, date, date]:
    current_end = as_of
    current_start = as_of - timedelta(days=WINDOW_DAYS - 1)
    prior_end = current_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=WINDOW_DAYS - 1)
    return prior_start, prior_end, current_start, current_end


def _count_between(issues: list[dict[str, Any]], start: date, end: date) -> int:
    total = 0
    for issue in issues:
        day = created_day(issue)
        if day is not None and start <= day <= end:
            total += 1
    return total


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
        raise TicketVolumeGeneratorError(f"Jira HELP ticket-volume fetch failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise TicketVolumeGeneratorError("Jira HELP ticket-volume fetch returned a non-object")
    if payload.get("error"):
        raise TicketVolumeGeneratorError(
            f"Jira HELP ticket-volume fetch failed: {payload['error']}"
        )
    issues = payload.get("issues")
    if not isinstance(issues, list):
        raise TicketVolumeGeneratorError("Jira HELP ticket-volume fetch returned no issue list")
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
    prior_start, prior_end, current_start, current_end = _window(as_of)
    by_org = issues_by_organization(issues)
    if truncated:
        logger.warning(
            "Health Score ticket_volume_trend: HELP created-ticket fetch hit its cap; "
            "scores use the fetched subset"
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            orgs = organizations_by_entity.get(entity_id) or []
            matched = matched_issues(orgs, by_org)
            error = None
            pct = None
            points = None
            current = prior = 0
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
                current = _count_between(matched, current_start, current_end)
                prior = _count_between(matched, prior_start, prior_end)
                pct = volume_change_pct(current, prior)
                points = ticket_volume_points(current, prior)
            meta = {
                "current_count": current,
                "prior_count": prior,
                "change_pct": pct,
                "window_days": WINDOW_DAYS,
                "current_start": current_start.isoformat(),
                "current_end": current_end.isoformat(),
                "prior_start": prior_start.isoformat(),
                "prior_end": prior_end.isoformat(),
                "spike_multiple": SPIKE_MULTIPLE,
                "jsm_organizations": list(orgs),
                "truncated": truncated,
                "owner": OWNER_EMAIL,
            }
            row_points = None if points is None else float(points)
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": period_key_for(GRAIN, as_of),
                        "as_of": as_of.isoformat(),
                        "value": pct,
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
                    value=pct,
                    points=row_points,
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


def get_ticket_volume_trend(
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
    """Score HELP ticket-volume trend for each active Salesforce Customer Entity."""
    if not entities:
        raise TicketVolumeGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate ticket_volume_trend"
        )
    today = as_of or date.today()
    if issues is None:
        prior_start, _, _, current_end = _window(today)
        payload = load_created_issues(start=prior_start, end=current_end, client=client)
        issues = payload["issues"]
        truncated = bool(payload.get("truncated"))
    if organizations_by_entity is None:
        try:
            organizations_by_entity = resolve_organizations(entities, client=client)
        except JsmMatchError as exc:
            raise TicketVolumeGeneratorError(str(exc)) from exc
    return _score_entities(
        entities=entities,
        issues=issues,
        organizations_by_entity=organizations_by_entity,
        as_of=today,
        truncated=truncated,
        persist=persist,
        generator=generator,
    )


def backfill_ticket_volume_trend(
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
    """Score a trailing 90-day window ending on each of the newest ``months`` snapshot dates."""
    if not entities:
        raise TicketVolumeGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate ticket_volume_trend"
        )
    today = as_of or date.today()
    try:
        days = monthly_as_ofs(today, months)
    except JsmMatchError as exc:
        raise TicketVolumeGeneratorError(str(exc)) from exc
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
        prior_start, _, _, _ = _window(pending[0])
        payload = load_created_issues(start=prior_start, end=pending[-1], client=client)
        issues = payload["issues"]
        truncated = bool(payload.get("truncated"))
    elif issues is None:
        issues = []
    if pending and organizations_by_entity is None:
        try:
            organizations_by_entity = resolve_organizations(entities, client=client)
        except JsmMatchError as exc:
            raise TicketVolumeGeneratorError(str(exc)) from exc
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
