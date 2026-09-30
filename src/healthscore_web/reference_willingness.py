"""Salesforce generator for Health Score ``reference_willingness``.

There is no reference, case-study, or testimonial willingness field. The
agreement flags on Account are Logo Use Approved and Press Approved. Yes is
either flag on the Customer Entity or on its parent Customer account. No is
neither. Last Marketing Response is stored with the reading and does not
change the score. Those booleans have no field history, so each month
repeats the current flags.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from src.healthscore_web.jsm_match import JsmMatchError, monthly_as_ofs
from src.healthscore_web.store import connect, period_key_for, periods_for_metric, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "reference_willingness"
GRAIN = "monthly"
GENERATOR_NAME = "get_reference_willingness"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "advocacy"]
LOGO_FIELD = "Logo_Use_Approved__c"
PRESS_FIELD = "Press_Approved__c"
ACTIVITY_FIELD = "Last_Marketing_Response__c"
_SF_ID = re.compile(r"^[A-Za-z0-9]{15,18}$")
_CHUNK = 100


class ReferenceWillingnessGeneratorError(RuntimeError):
    pass


def reference_willingness_points(willing: bool) -> int:
    """Yes scores 2. No scores 0."""
    if not isinstance(willing, bool):
        raise ReferenceWillingnessGeneratorError(
            f"willingness must be true or false, got {willing!r}"
        )
    return 2 if willing else 0


def _quote_ids(ids: list[str]) -> str:
    cleaned: list[str] = []
    for raw in ids:
        text = str(raw or "").strip()
        if not _SF_ID.fullmatch(text):
            raise ReferenceWillingnessGeneratorError(f"invalid Salesforce Account id: {raw!r}")
        cleaned.append(text)
    if not cleaned:
        raise ReferenceWillingnessGeneratorError("Salesforce id list is empty")
    return ", ".join(f"'{item}'" for item in cleaned)


def _flag(row: dict[str, Any], field: str) -> bool:
    return row.get(field) is True


def load_account_flags(account_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Logo, press, parent, and last marketing response for each Account id."""
    from src.salesforce_client import SalesforceClient

    found: dict[str, dict[str, Any]] = {}
    client = SalesforceClient()
    unique = list(dict.fromkeys(str(item).strip() for item in account_ids if str(item).strip()))
    for offset in range(0, len(unique), _CHUNK):
        chunk = unique[offset : offset + _CHUNK]
        soql = (
            f"SELECT Id, ParentId, {LOGO_FIELD}, {PRESS_FIELD}, {ACTIVITY_FIELD} "
            f"FROM Account WHERE Id IN ({_quote_ids(chunk)})"
        )
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise ReferenceWillingnessGeneratorError(
                f"Salesforce reference willingness query failed: {exc}"
            ) from exc
        for row in rows:
            if not isinstance(row, dict) or not row.get("Id"):
                continue
            found[str(row["Id"])] = row
    missing = [account_id for account_id in unique if account_id not in found]
    if missing:
        raise ReferenceWillingnessGeneratorError(
            "Salesforce reference willingness query missed "
            f"{len(missing)} accounts, including {missing[0]}"
        )
    return found


def willingness_for_entity(
    entity_id: str,
    accounts: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Yes when the entity or its parent has logo or press approval."""
    entity = accounts.get(entity_id)
    if entity is None:
        raise ReferenceWillingnessGeneratorError(
            f"Salesforce Account {entity_id} was not loaded"
        )
    parent_id = str(entity.get("ParentId") or "").strip()
    parent = accounts.get(parent_id) if parent_id else None
    own = _flag(entity, LOGO_FIELD) or _flag(entity, PRESS_FIELD)
    inherited = bool(parent) and (_flag(parent, LOGO_FIELD) or _flag(parent, PRESS_FIELD))
    activity = entity.get(ACTIVITY_FIELD) or (parent or {}).get(ACTIVITY_FIELD)
    return {
        "willing": own or inherited,
        "logo_approved": _flag(entity, LOGO_FIELD) or bool(parent and _flag(parent, LOGO_FIELD)),
        "press_approved": _flag(entity, PRESS_FIELD) or bool(parent and _flag(parent, PRESS_FIELD)),
        "inherited_from_parent": (not own) and inherited,
        "last_marketing_response": str(activity)[:10] if activity else None,
    }


def get_reference_willingness(
    *,
    entities: list[dict[str, Any]],
    accounts: dict[str, dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score reference willingness for each active Customer Entity."""
    if not entities:
        raise ReferenceWillingnessGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate reference_willingness"
        )
    today = as_of or date.today()
    if accounts is None:
        entity_ids = [str(entity["id"]) for entity in entities]
        loaded = load_account_flags(entity_ids)
        parent_ids = [
            str(row.get("ParentId") or "").strip()
            for row in loaded.values()
            if str(row.get("ParentId") or "").strip() and str(row.get("ParentId")) not in loaded
        ]
        if parent_ids:
            loaded.update(load_account_flags(parent_ids))
        accounts = loaded
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    key = period_key_for(GRAIN, today)
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            detail = willingness_for_entity(entity_id, accounts)
            points = reference_willingness_points(bool(detail["willing"]))
            meta = {
                **detail,
                "window_end": today.isoformat(),
                "owner": OWNER_EMAIL,
            }
            value = 1 if detail["willing"] else 0
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": key,
                        "as_of": today.isoformat(),
                        "value": value,
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
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
                    period_key=key,
                    value=float(value),
                    points=float(points),
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                )
            )
    finally:
        if conn is not None:
            conn.close()
    willing = sum(1 for row in readings if row.get("points") == 2)
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "willing": willing,
        "unwilling": len(readings) - willing,
        "readings": readings,
    }


def backfill_reference_willingness(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    accounts: dict[str, dict[str, Any]] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Write the current willingness flag onto each of the newest ``months`` dates."""
    if not entities:
        raise ReferenceWillingnessGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate reference_willingness"
        )
    today = as_of or date.today()
    try:
        days = monthly_as_ofs(today, months)
    except JsmMatchError as exc:
        raise ReferenceWillingnessGeneratorError(str(exc)) from exc
    existing: set[str] = set()
    if only_missing and persist:
        conn = connect()
        try:
            existing = set(periods_for_metric(conn, METRIC_NAME))
        finally:
            conn.close()
    pending = [day for day in days if period_key_for(GRAIN, day) not in existing]
    skipped_periods = [
        period_key_for(GRAIN, day) for day in days if period_key_for(GRAIN, day) in existing
    ]
    scored: list[dict[str, Any]] = []
    for day in pending:
        result = get_reference_willingness(
            entities=entities,
            accounts=accounts,
            as_of=day,
            persist=persist,
            generator=generator,
        )
        scored.append(
            {
                "period_key": period_key_for(GRAIN, day),
                "as_of": day.isoformat(),
                "willing": result.get("willing"),
                "unwilling": result.get("unwilling"),
                "ok": result.get("ok"),
            }
        )
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "months": months,
        "scored_periods": scored,
        "skipped_periods": skipped_periods,
    }
