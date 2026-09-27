"""Salesforce generator for Health Score ``champion_login_continuity``.

The named person is the Customer Entity Account executive sponsor
(``Executive_Sponsor__c``). Login time is ``Executive_Sponsor_Last_Login__c``.
A login in the trailing 7 days scores 2; a named sponsor with no login in that
window scores 0. An Account with no named sponsor stays unscored.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "champion_login_continuity"
GRAIN = "weekly"
GENERATOR_NAME = "get_champion_login_continuity"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "adoption"]
ACTIVE_WINDOW_DAYS = 7
SPONSOR_FIELDS = (
    "Id",
    "Executive_Sponsor__c",
    "Executive_Sponsor_Email__c",
    "Executive_Sponsor_First_Name__c",
    "Executive_Sponsor_Last_Login__c",
)


class ChampionLoginGeneratorError(RuntimeError):
    pass


def champion_login_points(*, named: bool, days_since_login: int | None) -> int | None:
    """2 when the sponsor logged in inside the window, 0 when they did not.

    ``None`` means the Account has no named executive sponsor and stays unscored.
    """
    if not named:
        return None
    if days_since_login is None:
        return 0
    if days_since_login < 0:
        raise ChampionLoginGeneratorError(
            f"days since executive sponsor login cannot be negative: {days_since_login}"
        )
    if days_since_login <= ACTIVE_WINDOW_DAYS:
        return 2
    return 0


def _named_sponsor(row: dict[str, Any]) -> bool:
    for key in (
        "Executive_Sponsor__c",
        "Executive_Sponsor_Email__c",
        "Executive_Sponsor_First_Name__c",
    ):
        if str(row.get(key) or "").strip():
            return True
    return False


def _parse_login(raw: Any, *, entity_id: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    elif len(normalized) >= 5 and normalized[-5] in "+-" and normalized[-3] != ":":
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ChampionLoginGeneratorError(
            f"Executive_Sponsor_Last_Login__c for {entity_id} is not a datetime: {text!r}"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def days_since_login(login: datetime | None, as_of: date) -> int | None:
    if login is None:
        return None
    elapsed = (as_of - login.astimezone(timezone.utc).date()).days
    return max(0, elapsed)


def load_executive_sponsor_rows() -> list[dict[str, Any]]:
    """Customer Entity Accounts with the executive-sponsor identity and last login."""
    from src.salesforce_client import SalesforceClient

    fields = ", ".join(SPONSOR_FIELDS)
    soql = f"SELECT {fields} FROM Account WHERE Type = 'Customer Entity'"
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ChampionLoginGeneratorError(
            f"Salesforce executive-sponsor query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ChampionLoginGeneratorError(
            "Salesforce executive-sponsor query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def get_champion_login_continuity(
    *,
    entities: list[dict[str, Any]],
    sponsor_rows: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score champion login continuity for each active Salesforce Customer Entity."""
    if not entities:
        raise ChampionLoginGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate "
            "champion_login_continuity"
        )
    try:
        rows = sponsor_rows if sponsor_rows is not None else load_executive_sponsor_rows()
    except ChampionLoginGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ChampionLoginGeneratorError(
            f"Salesforce executive-sponsor query failed: {exc}"
        ) from exc
    if not rows:
        raise ChampionLoginGeneratorError(
            "Salesforce returned no Customer Entity executive-sponsor rows"
        )
    by_id = {str(row.get("Id") or "").strip(): row for row in rows if row.get("Id")}
    today = as_of or date.today()
    warnings: list[dict[str, str]] = []
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            row = by_id.get(entity_id)
            if row is None:
                named = False
                elapsed = None
                error = "Salesforce Customer Entity missing from executive-sponsor query"
                sponsor_id = None
            else:
                named = _named_sponsor(row)
                login = _parse_login(
                    row.get("Executive_Sponsor_Last_Login__c"), entity_id=entity_id
                )
                elapsed = days_since_login(login, today)
                sponsor_id = str(row.get("Executive_Sponsor__c") or "").strip() or None
                error = None if named else "Salesforce Account has no executive sponsor"
            points = champion_login_points(named=named, days_since_login=elapsed)
            if error:
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
                logger.warning(
                    "Health Score champion_login_continuity: %s for Salesforce entity %s (%s)",
                    error,
                    entity.get("name"),
                    entity_id,
                )
            meta = {
                "source_fields": [
                    "Executive_Sponsor__c",
                    "Executive_Sponsor_Last_Login__c",
                ],
                "sponsor_contact_id": sponsor_id,
                "window_days": ACTIVE_WINDOW_DAYS,
                "owner": OWNER_EMAIL,
            }
            value = float(elapsed) if elapsed is not None else None
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": period_key_for(GRAIN, today),
                        "as_of": today.isoformat(),
                        "value": value,
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
                        "error": error,
                    }
                )
                continue
            saved = upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                value=value,
                points=None if points is None else float(points),
                generator=generator,
                tags=TAGS,
                meta=meta,
                error=error,
            )
            if saved.get("overridden"):
                overridden.append(entity_id)
            readings.append(saved)
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
        "overridden": len(overridden),
        "warnings": warnings,
        "readings": readings,
    }
