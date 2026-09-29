"""Salesforce generator for Health Score ``champion_turnover``.

A primary champion is the Account executive sponsor (``Executive_Sponsor__c``),
or a Contact whose buying role (``Buyer_Role_HubSpot_Sync__c``) is Champion,
Technical Buyer, or Executive Sponsor, or whose user role (``User_Role__c``)
includes Buyer. An economic buyer is a Contact whose buying role is Budget
Holder or Decision Maker.

The week runs from Monday through the run date. A tracked person turned over
when Salesforce Contact history in that week shows they moved to a different
Account, or their title changed beyond punctuation and filler words, or
UserGems recorded ``UserGem__JobStartedDate__c`` or
``UserGem__RoleStartedDate__c`` in that week. Either event scores 0. Tracked
people with no such change score 4. An Account with none of these people stays
unscored.

``UserGem__NoLongerAtCompany__c`` is not a weekly signal. It is a standing
boolean, Contact history does not track it, and it has no departure date.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.healthscore_web.champion_login import (
    load_executive_sponsor_rows,
    user_role_includes_buyer,
)
from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "champion_turnover"
GRAIN = "weekly"
GENERATOR_NAME = "get_champion_turnover"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "relationship"]
FULL_POINTS = 4
CHAMPION_BUYING_ROLES = ("Champion", "Technical Buyer", "Executive Sponsor")
ECONOMIC_BUYER_ROLES = ("Budget Holder", "Decision Maker")
CONTACT_FIELDS = (
    "Id",
    "AccountId",
    "Buyer_Role_HubSpot_Sync__c",
    "User_Role__c",
    "UserGem__JobStartedDate__c",
    "UserGem__RoleStartedDate__c",
)
NO_TRACKED_ERROR = "Salesforce Account has no primary champion or economic buyer"
MISSING_SPONSOR_ROW_ERROR = "Salesforce Customer Entity missing from executive-sponsor query"
_TITLE_STOPWORDS = frozenset({"a", "an", "and", "of", "the"})
_SALESFORCE_ID_RE = re.compile(r"^[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?$")
_ID_CHUNK = 100


class ChampionTurnoverGeneratorError(RuntimeError):
    pass


def champion_turnover_points(*, tracked: bool, turned_over: bool) -> int | None:
    """0 when a tracked person left or changed role, 4 when none did.

    ``None`` means the Account has no primary champion or economic buyer, and
    stays unscored.
    """
    if not tracked:
        return None
    if turned_over:
        return 0
    return FULL_POINTS


def week_bounds(as_of: date) -> tuple[date, date]:
    """Monday of the ISO week through ``as_of``."""
    start = as_of - timedelta(days=as_of.weekday())
    return start, as_of


def title_key(value: Any) -> str:
    """Title text with case, punctuation, and filler words removed."""
    text = str(value or "").casefold().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    parts = sorted(part for part in text.split() if part not in _TITLE_STOPWORDS)
    return " ".join(parts)


def title_changed(old: Any, new: Any) -> bool:
    """True when both titles are present and differ after normalization."""
    old_key = title_key(old)
    new_key = title_key(new)
    if not old_key or not new_key:
        return False
    return old_key != new_key


def contact_turnover_roles(contact: dict[str, Any]) -> set[str]:
    """``champion``, ``economic_buyer``, or both, for one Contact."""
    roles: set[str] = set()
    buying = str(contact.get("Buyer_Role_HubSpot_Sync__c") or "").strip().casefold()
    if buying in {item.casefold() for item in CHAMPION_BUYING_ROLES}:
        roles.add("champion")
    if buying in {item.casefold() for item in ECONOMIC_BUYER_ROLES}:
        roles.add("economic_buyer")
    if user_role_includes_buyer(contact.get("User_Role__c")):
        roles.add("champion")
    return roles


def history_value_matches_entity(value: Any, entity: dict[str, Any]) -> bool:
    """True when a Contact history value is this Account's id or name."""
    text = str(value or "").strip()
    if not text:
        return False
    if _same_id(text, str(entity.get("id") or "")):
        return True
    name = str(entity.get("name") or "").strip()
    return bool(name) and text.casefold() == name.casefold()


def load_turnover_contacts() -> list[dict[str, Any]]:
    """Qualifying Contacts currently on Customer Entity accounts."""
    from src.salesforce_client import SalesforceClient

    roles = ", ".join(
        f"'{role}'" for role in (*CHAMPION_BUYING_ROLES, *ECONOMIC_BUYER_ROLES)
    )
    fields = ", ".join(CONTACT_FIELDS)
    soql = (
        f"SELECT {fields} FROM Contact WHERE Account.Type = 'Customer Entity' AND ("
        f"Buyer_Role_HubSpot_Sync__c IN ({roles}) "
        "OR User_Role__c = 'Buyer' "
        "OR User_Role__c LIKE 'Buyer,%' "
        "OR User_Role__c LIKE '%,Buyer' "
        "OR User_Role__c LIKE '%,Buyer,%'"
        ")"
    )
    return _query_contacts(SalesforceClient(), soql, "champion-turnover contact")


def load_contacts_by_ids(contact_ids: list[str]) -> list[dict[str, Any]]:
    """Contacts by id, including people who left a Customer Entity this week."""
    from src.salesforce_client import SalesforceClient, _soql_quoted_id_list

    if not contact_ids:
        return []
    client = SalesforceClient()
    fields = ", ".join(CONTACT_FIELDS)
    rows: list[dict[str, Any]] = []
    for start in range(0, len(contact_ids), _ID_CHUNK):
        chunk = contact_ids[start : start + _ID_CHUNK]
        try:
            quoted = _soql_quoted_id_list(chunk)
        except ValueError as exc:
            raise ChampionTurnoverGeneratorError(
                f"Salesforce champion-turnover contact id is not queryable: {exc}"
            ) from exc
        soql = f"SELECT {fields} FROM Contact WHERE Id IN ({quoted})"
        rows.extend(_query_contacts(client, soql, "champion-turnover contact"))
    return rows


def load_contact_history(week_start: date, week_end: date) -> list[dict[str, Any]]:
    """Title and Account changes in the week, inclusive of both dates."""
    from src.salesforce_client import SalesforceClient

    start = f"{week_start.isoformat()}T00:00:00Z"
    end = f"{week_end.isoformat()}T23:59:59Z"
    soql = (
        "SELECT ContactId, Field, OldValue, NewValue, CreatedDate FROM ContactHistory "
        "WHERE Field IN ('Title', 'Account') "
        f"AND CreatedDate >= {start} AND CreatedDate <= {end}"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ChampionTurnoverGeneratorError(
            f"Salesforce champion-turnover history query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ChampionTurnoverGeneratorError(
            "Salesforce champion-turnover history query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def get_champion_turnover(
    *,
    entities: list[dict[str, Any]],
    sponsor_rows: list[dict[str, Any]] | None = None,
    contacts: list[dict[str, Any]] | None = None,
    history: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score champion and economic-buyer turnover for each Customer Entity."""
    if not entities:
        raise ChampionTurnoverGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate champion_turnover"
        )
    today = as_of or date.today()
    week_start, week_end = week_bounds(today)
    try:
        rows = sponsor_rows if sponsor_rows is not None else load_executive_sponsor_rows()
    except ChampionTurnoverGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ChampionTurnoverGeneratorError(
            f"Salesforce executive-sponsor query failed: {exc}"
        ) from exc
    if not rows:
        raise ChampionTurnoverGeneratorError(
            "Salesforce returned no Customer Entity executive-sponsor rows"
        )
    loaded_contacts = contacts is None
    if contacts is None:
        contacts = load_turnover_contacts()
    if history is None:
        history = load_contact_history(week_start, week_end)
    if loaded_contacts:
        contacts = _with_related_contacts(contacts, rows, history)
    week_history = [row for row in history if week_start <= _history_date(row) <= week_end]
    by_account = _contacts_by_account(contacts)
    by_contact = _contacts_by_id(contacts)
    qualifying = {
        contact_id: roles
        for contact_id, contact in by_contact.items()
        if (roles := contact_turnover_roles(contact))
    }
    history_by_contact = _history_by_contact(week_history)
    sponsors = _rows_by_id(rows)
    warnings: list[dict[str, str]] = []
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            sponsor = _lookup_id(sponsors, entity_id)
            people = _lookup_account_contacts(by_account, entity_id)
            departed = _departed_contacts(entity, week_history, qualifying)
            if sponsor is None and not people and not departed:
                tracked: dict[str, set[str]] = {}
                events: list[dict[str, Any]] = []
                error = MISSING_SPONSOR_ROW_ERROR
            else:
                sponsor_id = str((sponsor or {}).get("Executive_Sponsor__c") or "").strip()
                tracked = _tracked_people(entity_id, people, sponsor_id, departed, qualifying)
                events = _turnover_events(
                    entity,
                    tracked,
                    by_contact,
                    history_by_contact,
                    sponsor_id,
                    week_start,
                    week_end,
                )
                error = None if tracked else NO_TRACKED_ERROR
            points = champion_turnover_points(tracked=bool(tracked), turned_over=bool(events))
            if error:
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
                logger.warning(
                    "Health Score champion_turnover: %s for Salesforce entity %s (%s)",
                    error,
                    entity.get("name"),
                    entity_id,
                )
            reading = _reading(
                entity=entity,
                today=today,
                week_start=week_start,
                week_end=week_end,
                tracked=tracked,
                events=events,
                points=points,
                error=error,
                generator=generator,
            )
            if conn is None:
                readings.append(reading)
                continue
            saved = upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                value=reading["value"],
                points=None if points is None else float(points),
                generator=generator,
                tags=TAGS,
                meta=reading["meta"],
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
        "period_key": period_key_for(GRAIN, today),
        "scored": scored,
        "unscored": len(readings) - scored,
        "overridden": len(overridden),
        "warnings": warnings,
        "readings": readings,
    }


def _reading(
    *,
    entity: dict[str, Any],
    today: date,
    week_start: date,
    week_end: date,
    tracked: dict[str, set[str]],
    events: list[dict[str, Any]],
    points: int | None,
    error: str | None,
    generator: str,
) -> dict[str, Any]:
    return {
        "entity_id": str(entity["id"]),
        "entity_name": entity.get("name"),
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "period_key": period_key_for(GRAIN, today),
        "as_of": today.isoformat(),
        "value": None if points is None else (1.0 if events else 0.0),
        "points": points,
        "generator": generator,
        "tags": list(TAGS),
        "meta": {
            "source_fields": [
                "Executive_Sponsor__c",
                "Contact.Buyer_Role_HubSpot_Sync__c",
                "Contact.User_Role__c",
                "Contact.UserGem__JobStartedDate__c",
                "Contact.UserGem__RoleStartedDate__c",
                "ContactHistory.Title",
                "ContactHistory.Account",
            ],
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "tracked_contacts": len(tracked),
            "events": events,
            "owner": OWNER_EMAIL,
        },
        "error": error,
    }


def _query_contacts(client: Any, soql: str, label: str) -> list[dict[str, Any]]:
    try:
        rows = client.query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ChampionTurnoverGeneratorError(
            f"Salesforce {label} query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ChampionTurnoverGeneratorError(
            f"Salesforce {label} query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def _with_related_contacts(
    contacts: list[dict[str, Any]],
    sponsor_rows: list[dict[str, Any]],
    history: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    known = {_contact_id(contact) for contact in contacts}
    known.discard("")
    extra: list[str] = []
    seen = set(known)
    for row in sponsor_rows:
        sponsor_id = str(row.get("Executive_Sponsor__c") or "").strip()
        if sponsor_id and sponsor_id not in seen:
            seen.add(sponsor_id)
            extra.append(sponsor_id)
    for row in history:
        contact_id = str(row.get("ContactId") or "").strip()
        if contact_id and contact_id not in seen:
            seen.add(contact_id)
            extra.append(contact_id)
    if not extra:
        return contacts
    return [*contacts, *load_contacts_by_ids(extra)]


def _tracked_people(
    entity_id: str,
    people: list[dict[str, Any]],
    sponsor_id: str,
    departed: dict[str, set[str]],
    qualifying: dict[str, set[str]],
) -> dict[str, set[str]]:
    tracked: dict[str, set[str]] = {}
    for contact in people:
        contact_id = _contact_id(contact)
        roles = qualifying.get(contact_id) or set()
        if contact_id and roles and _same_id(str(contact.get("AccountId") or ""), entity_id):
            tracked.setdefault(contact_id, set()).update(roles)
    if sponsor_id:
        tracked.setdefault(sponsor_id, set()).add("champion")
    for contact_id, roles in departed.items():
        tracked.setdefault(contact_id, set()).update(roles)
    return tracked


def _departed_contacts(
    entity: dict[str, Any],
    history: list[dict[str, Any]],
    qualifying: dict[str, set[str]],
) -> dict[str, set[str]]:
    departed: dict[str, set[str]] = {}
    for row in history:
        if str(row.get("Field") or "") != "Account":
            continue
        if not history_value_matches_entity(row.get("OldValue"), entity):
            continue
        contact_id = str(row.get("ContactId") or "").strip()
        roles = _roles_for(qualifying, contact_id)
        if contact_id and roles:
            departed.setdefault(contact_id, set()).update(roles)
    return departed


def _turnover_events(
    entity: dict[str, Any],
    tracked: dict[str, set[str]],
    contacts: dict[str, dict[str, Any]],
    history_by_contact: dict[str, list[dict[str, Any]]],
    sponsor_id: str,
    week_start: date,
    week_end: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    entity_id = str(entity.get("id") or "")
    for contact_id, roles in tracked.items():
        is_sponsor = bool(sponsor_id) and _same_id(contact_id, sponsor_id)
        for row in _lookup_history(history_by_contact, contact_id):
            kind = _history_kind(row, entity, sponsor=is_sponsor)
            if kind is None:
                continue
            _add_event(events, seen, contact_id, kind, roles, _history_detail(row, kind))
        contact = _lookup_id(contacts, contact_id)
        if not isinstance(contact, dict):
            continue
        on_entity = _same_id(str(contact.get("AccountId") or ""), entity_id) or is_sponsor
        if not on_entity:
            continue
        for field, kind in (
            ("UserGem__JobStartedDate__c", "job_started"),
            ("UserGem__RoleStartedDate__c", "role_started"),
        ):
            started = _optional_date(contact.get(field), field=field)
            if started is not None and week_start <= started <= week_end:
                _add_event(events, seen, contact_id, kind, roles, started.isoformat())
    events.sort(key=lambda item: (item["contact_id"], item["kind"]))
    return events


def _history_kind(row: dict[str, Any], entity: dict[str, Any], *, sponsor: bool) -> str | None:
    field = str(row.get("Field") or "")
    if field == "Title":
        return "title_change" if title_changed(row.get("OldValue"), row.get("NewValue")) else None
    if field != "Account":
        return None
    if sponsor or history_value_matches_entity(row.get("OldValue"), entity):
        return "left_account"
    return None


def _history_detail(row: dict[str, Any], kind: str) -> str:
    if kind == "title_change":
        old = _short(row.get("OldValue"))
        new = _short(row.get("NewValue"))
        return f"{old} → {new}"
    if kind == "left_account":
        return "moved to a different Account"
    return ""


def _add_event(
    events: list[dict[str, Any]],
    seen: set[tuple[str, str]],
    contact_id: str,
    kind: str,
    roles: set[str],
    detail: str,
) -> None:
    key = (contact_id, kind)
    if key in seen:
        return
    seen.add(key)
    events.append(
        {
            "contact_id": contact_id,
            "kind": kind,
            "roles": sorted(roles),
            "detail": detail,
        }
    )


def _contacts_by_account(contacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        account_id = str(contact.get("AccountId") or "").strip()
        if not account_id:
            continue
        grouped.setdefault(account_id, []).append(contact)
        if _SALESFORCE_ID_RE.fullmatch(account_id):
            grouped.setdefault(account_id[:15], []).append(contact)
    return grouped


def _contacts_by_id(contacts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return _rows_by_id(contacts)


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = str(row.get("Id") or "").strip()
        if not value:
            continue
        indexed[value] = row
        if _SALESFORCE_ID_RE.fullmatch(value):
            indexed.setdefault(value[:15], row)
    return indexed


def _history_by_contact(history: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in history:
        contact_id = str(row.get("ContactId") or "").strip()
        if not contact_id:
            raise ChampionTurnoverGeneratorError(
                "Salesforce Contact history row is missing ContactId"
            )
        grouped.setdefault(contact_id, []).append(row)
        if _SALESFORCE_ID_RE.fullmatch(contact_id):
            grouped.setdefault(contact_id[:15], []).append(row)
    return grouped


def _lookup_account_contacts(
    grouped: dict[str, list[dict[str, Any]]], entity_id: str
) -> list[dict[str, Any]]:
    found = list(grouped.get(entity_id, []))
    if _SALESFORCE_ID_RE.fullmatch(entity_id):
        for contact in grouped.get(entity_id[:15], []):
            if contact not in found:
                found.append(contact)
    return found


def _lookup_history(
    grouped: dict[str, list[dict[str, Any]]], contact_id: str
) -> list[dict[str, Any]]:
    found = list(grouped.get(contact_id, []))
    if _SALESFORCE_ID_RE.fullmatch(contact_id):
        for row in grouped.get(contact_id[:15], []):
            if row not in found:
                found.append(row)
    return found


def _lookup_id(indexed: dict[str, Any], value: str) -> Any:
    if value in indexed:
        return indexed[value]
    if _SALESFORCE_ID_RE.fullmatch(value):
        return indexed.get(value[:15])
    return None


def _roles_for(qualifying: dict[str, set[str]], contact_id: str) -> set[str]:
    roles = qualifying.get(contact_id)
    if roles:
        return roles
    if _SALESFORCE_ID_RE.fullmatch(contact_id):
        return qualifying.get(contact_id[:15]) or set()
    return set()


def _contact_id(contact: dict[str, Any]) -> str:
    return str(contact.get("Id") or "").strip()


def _same_id(left: str, right: str) -> bool:
    a = str(left or "").strip()
    b = str(right or "").strip()
    if not a or not b:
        return False
    if a == b:
        return True
    if _SALESFORCE_ID_RE.fullmatch(a) and _SALESFORCE_ID_RE.fullmatch(b) and a[:15] == b[:15]:
        return True
    return False


def _history_date(row: dict[str, Any]) -> date:
    return _required_instant(row.get("CreatedDate"), field="ContactHistory.CreatedDate").date()


def _required_instant(raw: Any, *, field: str) -> datetime:
    text = str(raw or "").strip()
    if not text:
        raise ChampionTurnoverGeneratorError(f"{field} is blank")
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    elif len(normalized) >= 5 and normalized[-5] in "+-" and normalized[-3] != ":":
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ChampionTurnoverGeneratorError(f"{field} is not a datetime: {text!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_date(raw: Any, *, field: str) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if "T" in text:
        return _required_instant(text, field=field).date()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ChampionTurnoverGeneratorError(f"{field} is not a date: {text!r}") from exc


def _short(value: Any, limit: int = 80) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"
