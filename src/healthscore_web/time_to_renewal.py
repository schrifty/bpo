"""Days until the Salesforce contract end, and whether that contract auto-renews.

Auto-renew is read from ``Contract_Renewal_Terms__c``. A term that starts with
"Auto-renewal" auto-renews. "Customer confirmation required" does not. Any
other wording stays unscored.

Auto-renew contracts 90–180 days out score 0. Non-auto-renew contracts 0–120
days out score 0. Every other counted window scores 3. A missing end date
stays unscored.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "time_to_renewal"
GRAIN = "weekly"
GENERATOR_NAME = "get_time_to_renewal"
TAGS = ["customer-success", "healthscore", "commercial"]
MAX_POINTS = 3
_ID_CHUNK = 150
_AUTO_PREFIX = "auto-renewal"
_MANUAL_PREFIX = "customer confirmation required"


class TimeToRenewalGeneratorError(RuntimeError):
    """Salesforce could not produce a renewal-window score."""


def auto_renew_status(terms: str | None) -> bool | None:
    """True when the contract auto-renews, False when it needs confirmation."""
    text = " ".join(str(terms or "").split()).casefold()
    if text.startswith(_AUTO_PREFIX):
        return True
    if text.startswith(_MANUAL_PREFIX):
        return False
    return None


def renewal_points(days: int | None, auto_renew: bool | None) -> int | None:
    """Map days-until-end and auto-renew onto the framework bands."""
    if days is None or auto_renew is None:
        return None
    if auto_renew and 90 <= days <= 180:
        return 0
    if not auto_renew and 0 <= days <= 120:
        return 0
    return MAX_POINTS


def get_time_to_renewal(
    *,
    entities: list[dict[str, Any]],
    contracts: dict[str, dict[str, Any]] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score each entity's renewal window as of this week."""
    if not entities:
        raise TimeToRenewalGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate time_to_renewal"
        )
    today = as_of or date.today()
    if contracts is None:
        contracts = load_contract_facts([str(entity["id"]) for entity in entities])
    if not contracts:
        raise TimeToRenewalGeneratorError(
            "Salesforce returned no contract end dates for time to renewal."
        )
    key = period_key or period_key_for(GRAIN, today)
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            fact = contracts.get(entity_id) or {}
            end = _as_date(fact.get("end_date"))
            auto = auto_renew_status(fact.get("renewal_terms"))
            days = (end - today).days if end is not None else None
            error = None
            if end is None:
                error = "Salesforce contract end date is missing."
            elif auto is None:
                error = "Renewal terms do not say whether the contract auto-renews."
            points = None if error else renewal_points(days, auto)
            if error:
                warnings.append(
                    {"id": entity_id, "name": str(entity.get("name") or ""), "warning": error}
                )
            meta = {
                "contract_end_date": end.isoformat() if end else None,
                "days_until_end": days,
                "auto_renew": auto,
                "renewal_terms": fact.get("renewal_terms"),
            }
            payload = {
                "entity_id": entity_id,
                "entity_name": entity.get("name"),
                "metric_name": METRIC_NAME,
                "grain": GRAIN,
                "period_key": key,
                "as_of": today.isoformat(),
                "value": float(days) if points is not None and days is not None else None,
                "points": points,
                "generator": generator,
                "tags": list(TAGS),
                "meta": meta,
                "error": error,
            }
            if conn is None:
                readings.append(payload)
                continue
            readings.append(
                upsert_reading(
                    conn,
                    entity_id=entity_id,
                    entity_name=str(entity.get("name") or ""),
                    metric_name=METRIC_NAME,
                    grain=GRAIN,
                    as_of=today,
                    period_key=key,
                    value=payload["value"],
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
        "scored": scored,
        "unscored": len(readings) - scored,
        "warnings": warnings,
        "readings": readings,
        "dry_run": not persist,
    }


def load_contract_facts(entity_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Contract end date and renewal terms for the given Customer Entity ids."""
    from src.salesforce_client import SalesforceClient

    ids = [entity_id for entity_id in dict.fromkeys(entity_ids) if entity_id]
    if not ids:
        return {}
    facts: dict[str, dict[str, Any]] = {}
    client = SalesforceClient()
    try:
        for chunk in _chunks(ids):
            quoted = ", ".join(f"'{_soql_literal(entity_id)}'" for entity_id in chunk)
            soql = (
                "SELECT Id, Contract_Contract_End_Date__c, Contract_Renewal_Terms__c "
                f"FROM Account WHERE Id IN ({quoted})"
            )
            for row in client.query_soql(soql):
                entity_id = str(row.get("Id") or "").strip()
                if not entity_id:
                    continue
                facts[entity_id] = {
                    "end_date": row.get("Contract_Contract_End_Date__c"),
                    "renewal_terms": row.get("Contract_Renewal_Terms__c"),
                }
    except TimeToRenewalGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise TimeToRenewalGeneratorError(
            f"Salesforce contract query failed for time_to_renewal: {exc}"
        ) from exc
    return facts


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()[:10]
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _chunks(values: list[str]) -> list[list[str]]:
    return [values[index : index + _ID_CHUNK] for index in range(0, len(values), _ID_CHUNK)]


def _soql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
