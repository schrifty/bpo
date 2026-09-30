"""Salesforce and Chorus generator for Health Score ``executive_sponsorship``.

A senior executive is a Contact on the Customer Entity Account, or a Chorus
participant who is not a LeanDNA rep, whose title is Senior Director or
above. That includes Senior Director, Vice President, SVP, EVP, Managing
Director, President, and chief officers. Director alone does not count.
An executive assistant, or an assistant to someone, does not count.

A touch is a Salesforce Task or Event with that Contact in the trailing two
quarters, or a finished Chorus meeting or dial in that window whose
participant title is senior. The same person on the same day counts once.
Parent-account contacts are not included. A Chorus call on a parent account
credits a child only when a participant email matches a senior Contact on
that child.

No touches scores 0. A last touch older than 90 days scores 1. A last touch
within 90 days scores 2 for one touch and 4 for two or more. An Account with
no senior Contact and no senior Chorus participant stays unscored.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.call_sentiment import RECORDED_TYPES, is_recorded_call
from src.healthscore_web.meeting_cadence import engagement_day, window_start
from src.healthscore_web.meeting_cadence import _sf_key as salesforce_key
from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "executive_sponsorship"
GRAIN = "monthly"
GENERATOR_NAME = "get_executive_sponsorship"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "relationship"]
RECENT_DAYS = 90
MAX_POINTS = 4
_ID_CHUNK = 150
_EXCLUDE = re.compile(r"\b(executive assistant|administrative assistant|assistant to)\b")
_SENIOR_PHRASE = re.compile(
    r"\b(senior director|sr director|vice president|managing director|president)\b"
)
_SENIOR_TOKEN = re.compile(
    r"\b(vp|svp|evp|ceo|cfo|coo|cto|cio|cpo|cro|ciso|cmo|chro|cdo|cso)\b"
)
_CHIEF_OFFICER = re.compile(r"\bchief\b.+\bofficer\b")
_NON_WORD = re.compile(r"[^a-z0-9]+")
NO_SENIOR_ERROR = (
    "no Senior Director or above contact, and no senior Chorus participant, "
    "in the trailing two quarters"
)


class ExecutiveSponsorshipGeneratorError(RuntimeError):
    pass


def is_senior_title(title: str | None) -> bool:
    """True when ``title`` is Senior Director or above, and not an assistant."""
    text = _NON_WORD.sub(" ", str(title or "").casefold()).strip()
    if not text or _EXCLUDE.search(text):
        return False
    if _SENIOR_PHRASE.search(text) or _SENIOR_TOKEN.search(text):
        return True
    return bool(_CHIEF_OFFICER.search(text))


def sponsorship_points(
    *, senior_count: int, touch_count: int, days_since: int | None
) -> int | None:
    """4, 2, 1, or 0. None when nobody senior was found."""
    if senior_count < 1 and touch_count < 1:
        return None
    if touch_count < 1:
        return 0
    if days_since is None:
        raise ExecutiveSponsorshipGeneratorError(
            "days since last touch is required when a touch was recorded"
        )
    if isinstance(days_since, bool) or not isinstance(days_since, int) or days_since < 0:
        raise ExecutiveSponsorshipGeneratorError(
            f"days since last touch must be a non-negative integer, got {days_since!r}"
        )
    if days_since > RECENT_DAYS:
        return 1
    if touch_count >= 2:
        return MAX_POINTS
    return 2


def get_executive_sponsorship(
    *,
    entities: list[dict[str, Any]],
    contacts: list[dict[str, Any]] | None = None,
    tasks: list[dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
    engagements: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score senior-executive touches for each active Customer Entity."""
    if not entities:
        raise ExecutiveSponsorshipGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate executive_sponsorship"
        )
    today = as_of or date.today()
    start = window_start(today)
    if contacts is None:
        contacts = load_titled_contacts()
    senior = [row for row in contacts if isinstance(row, dict) and is_senior_title(row.get("Title"))]
    contact_ids = _contact_ids(senior)
    if tasks is None:
        tasks = load_tasks(contact_ids, start, today) if contact_ids else []
    if events is None:
        events = load_events(contact_ids, start, today) if contact_ids else []
    if engagements is None:
        engagements = load_chorus_calls(start, today)
    by_account = _seniors_by_account(senior)
    by_email = _seniors_by_email(senior)
    touches = _collect_touches(
        entities,
        by_account,
        by_email,
        tasks,
        events,
        engagements,
        start=start,
        end=today,
    )
    key = period_key or period_key_for(GRAIN, today)
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            people = by_account.get(salesforce_key(entity_id), [])
            found = touches.get(entity_id, set())
            days_since = None
            if found:
                last = max(day for _person, day in found)
                days_since = (today - last).days
            points = sponsorship_points(
                senior_count=len(people),
                touch_count=len(found),
                days_since=days_since,
            )
            error = None
            if points is None:
                error = NO_SENIOR_ERROR
                warnings.append(
                    {"id": entity_id, "name": str(entity.get("name") or ""), "warning": error}
                )
            meta = {
                "senior_contacts": len(people),
                "touches": len(found),
                "days_since_last": days_since,
                "window_start": start.isoformat(),
                "window_end": today.isoformat(),
                "recent_days": RECENT_DAYS,
                "owner": OWNER_EMAIL,
            }
            payload = {
                "entity_id": entity_id,
                "entity_name": entity.get("name"),
                "metric_name": METRIC_NAME,
                "grain": GRAIN,
                "period_key": key,
                "as_of": today.isoformat(),
                "value": float(len(found)) if points is not None else None,
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
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "scored": scored,
        "unscored": len(readings) - scored,
        "warnings": warnings,
        "readings": readings,
        "dry_run": not persist,
    }


def load_titled_contacts() -> list[dict[str, Any]]:
    """Contacts on Customer Entity accounts that have a title."""
    from src.salesforce_client import SalesforceClient

    soql = (
        "SELECT Id, AccountId, Email, Title FROM Contact "
        "WHERE Account.Type = 'Customer Entity' AND Title != null"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ExecutiveSponsorshipGeneratorError(
            f"Salesforce senior-contact query failed: {exc}"
        ) from exc
    if not isinstance(rows, list) or not rows:
        raise ExecutiveSponsorshipGeneratorError(
            "Salesforce returned no titled contacts on Customer Entity accounts"
        )
    return [row for row in rows if isinstance(row, dict)]


def load_tasks(contact_ids: list[str], start: date, end: date) -> list[dict[str, Any]]:
    """Tasks linked to a senior contact in the window.

    This org does not expose ``Task.WhoId`` or ``Task.ActivityDate``. The contact
    is ``TaskWhoRelation.RelationId``. The day is ``CompletedDateTime``, or
    ``CreatedDate`` when the task is not completed.
    """
    start_ts = f"{start.isoformat()}T00:00:00Z"
    end_ts = f"{end.isoformat()}T23:59:59Z"
    window = (
        f"((Task.CompletedDateTime >= {start_ts} AND Task.CompletedDateTime <= {end_ts}) "
        f"OR (Task.CompletedDateTime = null AND Task.CreatedDate >= {start_ts} "
        f"AND Task.CreatedDate <= {end_ts}))"
    )
    rows = _load_relations(
        "TaskWhoRelation",
        "TaskId, RelationId, Task.CompletedDateTime, Task.CreatedDate",
        window,
        contact_ids,
    )
    return [_task_touch(row) for row in rows]


def load_events(contact_ids: list[str], start: date, end: date) -> list[dict[str, Any]]:
    """Events linked to a senior contact whose start falls in the window."""
    start_ts = f"{start.isoformat()}T00:00:00Z"
    end_ts = f"{end.isoformat()}T23:59:59Z"
    window = (
        f"((Event.StartDateTime >= {start_ts} AND Event.StartDateTime <= {end_ts}) "
        f"OR (Event.StartDateTime = null AND Event.ActivityDate >= {start.isoformat()} "
        f"AND Event.ActivityDate <= {end.isoformat()}))"
    )
    rows = _load_relations(
        "EventWhoRelation",
        "EventId, RelationId, Event.StartDateTime, Event.ActivityDate",
        window,
        contact_ids,
    )
    return [_event_touch(row) for row in rows]


def _parent(row: dict[str, Any], key: str) -> dict[str, Any]:
    value = row.get(key)
    return value if isinstance(value, dict) else {}


def _task_touch(row: dict[str, Any]) -> dict[str, Any]:
    parent = _parent(row, "Task")
    return {
        "Id": row.get("TaskId"),
        "WhoId": row.get("RelationId"),
        "ActivityDate": parent.get("CompletedDateTime") or parent.get("CreatedDate"),
    }


def _event_touch(row: dict[str, Any]) -> dict[str, Any]:
    parent = _parent(row, "Event")
    return {
        "Id": row.get("EventId"),
        "WhoId": row.get("RelationId"),
        "StartDateTime": parent.get("StartDateTime") or parent.get("ActivityDate"),
    }


def load_chorus_calls(start: date, end: date) -> list[dict[str, Any]]:
    """Finished Chorus meetings and dials in the window. An empty list is a real result."""
    from src.chorus_client import ChorusClient, ChorusClientError, chorus_configured

    if not chorus_configured():
        raise ExecutiveSponsorshipGeneratorError("Chorus is not configured (set CHORUS_API_KEY)")
    min_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    max_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    try:
        client = ChorusClient()
        rows: list[Any] = []
        for kind in sorted(RECORDED_TYPES):
            rows.extend(
                client.list_engagements(min_date=min_ts, max_date=max_ts, engagement_type=kind)
            )
    except ChorusClientError as exc:
        raise ExecutiveSponsorshipGeneratorError(f"Chorus sponsorship load failed: {exc}") from exc
    if not isinstance(rows, list):
        raise ExecutiveSponsorshipGeneratorError("Chorus engagements payload was not a list")
    recorded = [row for row in rows if isinstance(row, dict) and is_recorded_call(row)]
    if rows and not recorded:
        logger.warning(
            "Health Score executive_sponsorship: Chorus returned engagements but none were finished calls"
        )
    missing_participants = sum(1 for row in recorded if not row.get("participants"))
    if recorded and missing_participants == len(recorded):
        logger.warning(
            "Health Score executive_sponsorship: Chorus calls have no participants, so none can show a senior title"
        )
    return recorded


def _load_relations(
    sobject: str, fields: str, window: str, contact_ids: list[str]
) -> list[dict[str, Any]]:
    from src.salesforce_client import SalesforceClient

    if not contact_ids:
        return []
    client = SalesforceClient()
    found: list[dict[str, Any]] = []
    for chunk in _chunks(contact_ids, _ID_CHUNK):
        quoted = ", ".join(f"'{_soql_id(item)}'" for item in chunk)
        soql = (
            f"SELECT {fields} FROM {sobject} "
            f"WHERE {window} AND Type = 'Contact' AND RelationId IN ({quoted})"
        )
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise ExecutiveSponsorshipGeneratorError(
                f"Salesforce {sobject} query failed: {exc}"
            ) from exc
        if not isinstance(rows, list):
            raise ExecutiveSponsorshipGeneratorError(
                f"Salesforce {sobject} query returned a non-list payload"
            )
        found.extend(row for row in rows if isinstance(row, dict))
    return found


def _collect_touches(
    entities: list[dict[str, Any]],
    by_account: dict[str, list[dict[str, Any]]],
    by_email: dict[str, list[tuple[str, str]]],
    tasks: list[dict[str, Any]],
    events: list[dict[str, Any]],
    engagements: list[dict[str, Any]],
    *,
    start: date,
    end: date,
) -> dict[str, set[tuple[str, date]]]:
    entity_ids = {str(entity["id"]) for entity in entities}
    entity_by_key = {salesforce_key(entity_id): entity_id for entity_id in entity_ids}
    contact_entity = {
        salesforce_key(str(contact.get("Id") or "")): entity_by_key.get(
            salesforce_key(str(contact.get("AccountId") or ""))
        )
        for people in by_account.values()
        for contact in people
    }
    touches: dict[str, set[tuple[str, date]]] = {entity_id: set() for entity_id in entity_ids}
    for row in tasks:
        _add_salesforce_touch(touches, contact_entity, row, _activity_day(row, "ActivityDate"), start, end)
    for row in events:
        _add_salesforce_touch(touches, contact_entity, row, _activity_day(row, "StartDateTime"), start, end)
    for engagement in engagements:
        if not isinstance(engagement, dict) or not is_recorded_call(engagement):
            continue
        day = engagement_day(engagement)
        if day < start or day > end:
            continue
        account_entity = entity_by_key.get(salesforce_key(str(engagement.get("account_id") or "")))
        for person in engagement.get("participants") or []:
            if not isinstance(person, dict):
                continue
            if str(person.get("type") or "").casefold() == "rep":
                continue
            if not is_senior_title(person.get("title")):
                continue
            email = str(person.get("email") or "").strip().casefold()
            name = str(person.get("name") or "").strip().casefold()
            matched = by_email.get(email, []) if email else []
            credited = False
            for account_id, person_key in matched:
                entity_id = entity_by_key.get(salesforce_key(account_id))
                if entity_id:
                    touches[entity_id].add((person_key, day))
                    credited = True
            if credited:
                continue
            if account_entity and (email or name):
                touches[account_entity].add((f"chorus:{email or name}", day))
    return touches


def _add_salesforce_touch(
    touches: dict[str, set[tuple[str, date]]],
    contact_entity: dict[str, str | None],
    row: dict[str, Any],
    day: date,
    start: date,
    end: date,
) -> None:
    if day < start or day > end:
        return
    entity_id = contact_entity.get(salesforce_key(str(row.get("WhoId") or "")))
    person = salesforce_key(str(row.get("WhoId") or ""))
    if entity_id and person:
        touches[entity_id].add((person, day))


def _activity_day(row: dict[str, Any], field: str) -> date:
    raw = row.get(field)
    text = str(raw or "").strip()
    if len(text) < 10:
        reference = str(row.get("Id") or field)
        raise ExecutiveSponsorshipGeneratorError(
            f"Salesforce activity {reference} has no {field}"
        )
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ExecutiveSponsorshipGeneratorError(
            f"Salesforce activity {row.get('Id') or field} {field} is not a date: {raw!r}"
        ) from exc


def _seniors_by_account(contacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        account = salesforce_key(str(contact.get("AccountId") or ""))
        if account:
            grouped.setdefault(account, []).append(contact)
    return grouped


def _seniors_by_email(contacts: list[dict[str, Any]]) -> dict[str, list[tuple[str, str]]]:
    grouped: dict[str, list[tuple[str, str]]] = {}
    for contact in contacts:
        email = str(contact.get("Email") or "").strip().casefold()
        account = str(contact.get("AccountId") or "").strip()
        person = salesforce_key(str(contact.get("Id") or ""))
        if email and account and person:
            grouped.setdefault(email, []).append((account, person))
    return grouped


def _contact_ids(contacts: list[dict[str, Any]]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for contact in contacts:
        text = str(contact.get("Id") or "").strip()
        key = salesforce_key(text)
        if key and key not in seen:
            seen.add(key)
            found.append(text)
    return found


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _soql_id(value: str) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9]{15,18}", text):
        raise ExecutiveSponsorshipGeneratorError(f"invalid Salesforce id: {value!r}")
    return text
