"""Jira Service Management generator for Health Score ``sla_adherence``.

Reads HELP tickets resolved in the trailing 30 days and the completed
time-to-first-response and time-to-resolution SLA cycles on those tickets.
The percent that did not breach any measured cycle is the same figure as the
KPI catalog metric ``SLA Adherence (30 Days)``. At least 90% scores 3 points;
below that scores 0. An entity with no JSM organization match, or no measured
cycle, stays unscored.

Tickets are attributed by JSM organization. Customer Entities that resolve to
the same organization share that organization's score.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "sla_adherence"
GRAIN = "monthly"
GENERATOR_NAME = "get_sla_adherence"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "support"]
WINDOW_DAYS = 30
TARGET_PCT = 90.0


class SlaAdherenceGeneratorError(RuntimeError):
    pass


def sla_adherence_points(pct: float | None) -> int | None:
    """3 points at or above 90%. 0 below that. No measured percent stays unscored."""
    if pct is None:
        return None
    if isinstance(pct, bool) or not isinstance(pct, (int, float)):
        raise SlaAdherenceGeneratorError(f"SLA percent must be a number, got {pct!r}")
    value = float(pct)
    if value < 0 or value > 100:
        raise SlaAdherenceGeneratorError(f"SLA percent must be from 0 to 100, got {value}")
    return 3 if value >= TARGET_PCT else 0


def _measured_outcome(issue: dict[str, Any]) -> bool | None:
    """True when every measured TTFR/TTR cycle was met. None when nothing was measured."""
    ttfr_measured = issue.get("ttfr_ms") is not None
    ttr_measured = issue.get("ttr_ms") is not None
    if not ttfr_measured and not ttr_measured:
        return None
    breached = (ttfr_measured and bool(issue.get("ttfr_breached"))) or (
        ttr_measured and bool(issue.get("ttr_breached"))
    )
    return not breached


def _issue_organizations(issue: dict[str, Any]) -> list[str]:
    raw = issue.get("organizations") or []
    if isinstance(raw, str):
        raw = [raw]
    return [str(name).strip() for name in raw if str(name).strip()]


def adherence_percent(issues: list[dict[str, Any]]) -> tuple[float | None, int, int]:
    """Return percent met, met count, and measured count. Percent is None when nothing was measured."""
    met = 0
    measured = 0
    seen: set[str] = set()
    for issue in issues:
        key = str(issue.get("key") or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        outcome = _measured_outcome(issue)
        if outcome is None:
            continue
        measured += 1
        if outcome:
            met += 1
    if measured < 1:
        return None, 0, 0
    return round(100 * met / measured, 1), met, measured


def _match_terms(entity: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    for key in ("entity_name", "parent_name"):
        raw = str(entity.get(key) or "").strip()
        if raw:
            terms.append(raw)
    return terms


def load_help_sla_issues(
    *,
    client: Any | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    from src.jira_client import JiraClient

    jira = client if client is not None else JiraClient()
    try:
        payload = jira.list_help_resolved_sla_issues(days=WINDOW_DAYS, as_of=as_of)
    except Exception as exc:  # noqa: BLE001
        raise SlaAdherenceGeneratorError(
            f"Jira HELP SLA adherence fetch failed: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise SlaAdherenceGeneratorError("Jira HELP SLA adherence fetch returned a non-object")
    if payload.get("error"):
        raise SlaAdherenceGeneratorError(
            f"Jira HELP SLA adherence fetch failed: {payload['error']}"
        )
    issues = payload.get("issues")
    if not isinstance(issues, list) or not issues:
        raise SlaAdherenceGeneratorError(
            "Jira HELP returned no resolved tickets in the trailing 30 days"
        )
    return payload


def get_sla_adherence(
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
    """Score HELP SLA adherence for each active Salesforce Customer Entity."""
    if not entities:
        raise SlaAdherenceGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate sla_adherence"
        )
    today = as_of or date.today()
    if issues is None:
        payload = load_help_sla_issues(client=client, as_of=today)
        issues = payload["issues"]
        truncated = bool(payload.get("truncated"))
    if organizations_by_entity is None:
        from src.jira_client import JiraClient

        jira = client if client is not None else JiraClient()
        organizations_by_entity = {}
        for entity in entities:
            entity_id = str(entity["id"])
            name = str(entity.get("name") or "").strip()
            try:
                organizations_by_entity[entity_id] = jira.resolve_jsm_organizations(
                    name, _match_terms(entity) or None
                )
            except Exception as exc:  # noqa: BLE001
                raise SlaAdherenceGeneratorError(
                    f"JSM organization match failed for {name or entity_id}: {exc}"
                ) from exc
    by_org: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        for org in _issue_organizations(issue):
            by_org.setdefault(org.casefold(), []).append(issue)
    if truncated:
        logger.warning(
            "Health Score sla_adherence: HELP resolved-ticket fetch hit its cap; "
            "scores use the fetched subset"
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            orgs = organizations_by_entity.get(entity_id) or []
            matched: list[dict[str, Any]] = []
            for org in orgs:
                matched.extend(by_org.get(org.casefold(), []))
            pct, met, measured = adherence_percent(matched)
            points = sla_adherence_points(pct)
            error = None
            if points is None:
                if not orgs:
                    error = "no JSM organization match for this Customer Entity"
                else:
                    error = "no completed HELP TTFR/TTR SLA cycles in the trailing 30 days"
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            meta = {
                "adherence_pct": pct,
                "met": met,
                "measured": measured,
                "window_days": WINDOW_DAYS,
                "target_pct": TARGET_PCT,
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
                        "period_key": period_key_for(GRAIN, today),
                        "as_of": today.isoformat(),
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
                    as_of=today,
                    value=pct,
                    points=None if points is None else float(points),
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                    error=error,
                )
            )
        unscored = sum(1 for row in readings if row.get("points") is None)
        if unscored:
            logger.info(
                "Health Score sla_adherence: %s entities have no measured HELP SLA cycles",
                unscored,
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
        "owner_display": OWNER_NAME,
        "scored": scored,
        "unscored": len(readings) - scored,
        "truncated": truncated,
        "warnings": warnings,
        "readings": readings,
    }
