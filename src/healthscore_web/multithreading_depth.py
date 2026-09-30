"""Salesforce and Chorus generator for Health Score ``multithreading_depth``.

A director is a Contact on the Customer Entity Account, or a Chorus
participant who is not a LeanDNA rep, whose title is Director or above.
That includes Director, Senior Director, Vice President, SVP, EVP, Managing
Director, President, and chief officers. An executive assistant, or an
assistant to someone, does not count.

Engaged means a Salesforce Task or Event with that Contact, or a finished
Chorus meeting or dial whose participant title is Director or above. The
current window is the trailing 90 days. The prior window is the 90 days
before that. A person counts once in a window. Parent-account contacts are
not included. A Chorus call on a parent account credits a child only when a
participant email matches a Director-or-above Contact on that child.

Zero people engaged in the current window scores 0. One person scores 1, or
2 when that count is higher than the prior window. Two score 2, or 3 when
higher. Three or more score 3, or 4 when higher. An Account with no
Director-or-above Contact and no Director-or-above Chorus participant in
either window stays unscored.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.healthscore_web.call_sentiment import RECORDED_TYPES, is_recorded_call
from src.healthscore_web.meeting_cadence import MeetingCadenceGeneratorError, engagement_day
from src.healthscore_web.meeting_cadence import _sf_key as salesforce_key
from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "multithreading_depth"
GRAIN = "monthly"
GENERATOR_NAME = "get_multithreading_depth"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "relationship"]
WINDOW_DAYS = 90
MAX_POINTS = 4
_ID_CHUNK = 150
_EXCLUDE = re.compile(r"\b(executive assistant|administrative assistant|assistant to)\b")
_DIRECTOR_PHRASE = re.compile(
    r"\b(director|vice president|managing director|president)\b"
)
_DIRECTOR_TOKEN = re.compile(
    r"\b(vp|svp|evp|ceo|cfo|coo|cto|cio|cpo|cro|ciso|cmo|chro|cdo|cso)\b"
)
_CHIEF_OFFICER = re.compile(r"\bchief\b.+\bofficer\b")
_NON_WORD = re.compile(r"[^a-z0-9]+")
NO_DIRECTOR_ERROR = (
    "no Director or above contact, and no Director or above Chorus participant, "
    "in the trailing 180 days"
)


class MultithreadingDepthGeneratorError(RuntimeError):
    pass


def is_director_title(title: str | None) -> bool:
    """True when ``title`` is Director or above, and not an assistant."""
    text = _NON_WORD.sub(" ", str(title or "").casefold()).strip()
    if not text or _EXCLUDE.search(text):
        return False
    if _DIRECTOR_PHRASE.search(text) or _DIRECTOR_TOKEN.search(text):
        return True
    return bool(_CHIEF_OFFICER.search(text))


def multithreading_points(*, director_count: int, engaged: int, prior_engaged: int) -> int | None:
    """0–4 from the current Director+ count and whether it beat the prior window.

    None when nobody at Director or above was found in either window.
    """
    _count(director_count, "director count")
    _count(engaged, "engaged count")
    _count(prior_engaged, "prior engaged count")
    if director_count < 1 and engaged < 1 and prior_engaged < 1:
        return None
    growing = engaged > prior_engaged
    if engaged <= 0:
        return 0
    if engaged == 1:
        return 2 if growing else 1
    if engaged == 2:
        return 3 if growing else 2
    return MAX_POINTS if growing else 3


def get_multithreading_depth(
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
    """Score distinct Director-or-above people engaged in the trailing 90 days."""
    if not entities:
        raise MultithreadingDepthGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate multithreading_depth"
        )
    today = as_of or date.today()
    current_start = today - timedelta(days=WINDOW_DAYS - 1)
    prior_end = current_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=WINDOW_DAYS - 1)
    if contacts is None:
        contacts = load_titled_contacts()
    directors = [row for row in contacts if isinstance(row, dict) and is_director_title(row.get("Title"))]
    contact_ids = _contact_ids(directors)
    if tasks is None:
        tasks = load_tasks(contact_ids, prior_start, today) if contact_ids else []
    if events is None:
        events = load_events(contact_ids, prior_start, today) if contact_ids else []
    if engagements is None:
        engagements = load_chorus_calls(prior_start, today)
    by_account = _directors_by_account(directors)
    by_email = _directors_by_email(directors)
    people = _collect_people(
        entities,
        by_account,
        by_email,
        tasks,
        events,
        engagements,
        current_start=current_start,
        prior_start=prior_start,
        prior_end=prior_end,
        end=today,
    )
    key = period_key or period_key_for(GRAIN, today)
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            found = people.get(entity_id, {"current": set(), "prior": set()})
            engaged = len(found["current"])
            prior = len(found["prior"])
            known = by_account.get(salesforce_key(entity_id), [])
            points = multithreading_points(
                director_count=len(known),
                engaged=engaged,
                prior_engaged=prior,
            )
            error = None
            if points is None:
                error = NO_DIRECTOR_ERROR
                warnings.append(
                    {"id": entity_id, "name": str(entity.get("name") or ""), "warning": error}
                )
            meta = {
                "director_contacts": len(known),
                "engaged": engaged,
                "prior_engaged": prior,
                "growing": engaged > prior,
                "window_start": current_start.isoformat(),
                "prior_window_start": prior_start.isoformat(),
                "window_end": today.isoformat(),
                "owner": OWNER_EMAIL,
            }
            payload = {
                "entity_id": entity_id,
                "entity_name": entity.get("name"),
                "metric_name": METRIC_NAME,
                "grain": GRAIN,
                "period_key": key,
                "as_of": today.isoformat(),
                "value": float(engaged) if points is not None else None,
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
        raise MultithreadingDepthGeneratorError(
            f"Salesforce director-contact query failed: {exc}"
        ) from exc
    if not isinstance(rows, list) or not rows:
        raise MultithreadingDepthGeneratorError(
            "Salesforce returned no titled contacts on Customer Entity accounts"
        )
    return [row for row in rows if isinstance(row, dict)]


def load_tasks(contact_ids: list[str], start: date, end: date) -> list[dict[str, Any]]:
    """Tasks whose WhoId is a director contact and whose ActivityDate is in the window."""
    return _load_activity(
        "Task",
        "Id, ActivityDate, WhoId",
        f"ActivityDate >= {start.isoformat()} AND ActivityDate <= {end.isoformat()}",
        contact_ids,
    )


def load_events(contact_ids: list[str], start: date, end: date) -> list[dict[str, Any]]:
    """Events whose WhoId is a director contact and whose start falls in the window."""
    start_ts = f"{start.isoformat()}T00:00:00Z"
    end_ts = f"{end.isoformat()}T23:59:59Z"
    return _load_activity(
        "Event",
        "Id, StartDateTime, WhoId",
        f"StartDateTime >= {start_ts} AND StartDateTime <= {end_ts}",
        contact_ids,
    )


def load_chorus_calls(start: date, end: date) -> list[dict[str, Any]]:
    """Finished Chorus meetings and dials in the window. An empty list is a real result."""
    from src.chorus_client import ChorusClient, ChorusClientError, chorus_configured

    if not chorus_configured():
        raise MultithreadingDepthGeneratorError("Chorus is not configured (set CHORUS_API_KEY)")
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
        raise MultithreadingDepthGeneratorError(f"Chorus multithreading load failed: {exc}") from exc
    if not isinstance(rows, list):
        raise MultithreadingDepthGeneratorError("Chorus engagements payload was not a list")
    recorded = [row for row in rows if isinstance(row, dict) and is_recorded_call(row)]
    if rows and not recorded:
        logger.warning(
            "Health Score multithreading_depth: Chorus returned engagements but none were finished calls"
        )
    missing_participants = sum(1 for row in recorded if not row.get("participants"))
    if recorded and missing_participants == len(recorded):
        logger.warning(
            "Health Score multithreading_depth: Chorus calls have no participants, so none can show a director title"
        )
    return recorded


def _load_activity(
    sobject: str, fields: str, window: str, contact_ids: list[str]
) -> list[dict[str, Any]]:
    from src.salesforce_client import SalesforceClient

    if not contact_ids:
        return []
    client = SalesforceClient()
    found: list[dict[str, Any]] = []
    for chunk in _chunks(contact_ids, _ID_CHUNK):
        quoted = ", ".join(f"'{_soql_id(item)}'" for item in chunk)
        soql = f"SELECT {fields} FROM {sobject} WHERE {window} AND WhoId IN ({quoted})"
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise MultithreadingDepthGeneratorError(
                f"Salesforce {sobject} query failed: {exc}"
            ) from exc
        if not isinstance(rows, list):
            raise MultithreadingDepthGeneratorError(
                f"Salesforce {sobject} query returned a non-list payload"
            )
        found.extend(row for row in rows if isinstance(row, dict))
    return found


def _collect_people(
    entities: list[dict[str, Any]],
    by_account: dict[str, list[dict[str, Any]]],
    by_email: dict[str, list[tuple[str, str]]],
    tasks: list[dict[str, Any]],
    events: list[dict[str, Any]],
    engagements: list[dict[str, Any]],
    *,
    current_start: date,
    prior_start: date,
    prior_end: date,
    end: date,
) -> dict[str, dict[str, set[str]]]:
    entity_ids = {str(entity["id"]) for entity in entities}
    entity_by_key = {salesforce_key(entity_id): entity_id for entity_id in entity_ids}
    contact_entity = {
        salesforce_key(str(contact.get("Id") or "")): entity_by_key.get(
            salesforce_key(str(contact.get("AccountId") or ""))
        )
        for people in by_account.values()
        for contact in people
    }
    found = {entity_id: {"current": set(), "prior": set()} for entity_id in entity_ids}

    def add(entity_id: str | None, person: str, day: date) -> None:
        bucket = _bucket(day, current_start, prior_start, prior_end, end)
        if entity_id and person and bucket:
            found[entity_id][bucket].add(person)

    for row in tasks:
        add(
            contact_entity.get(salesforce_key(str(row.get("WhoId") or ""))),
            salesforce_key(str(row.get("WhoId") or "")),
            _activity_day(row, "ActivityDate"),
        )
    for row in events:
        add(
            contact_entity.get(salesforce_key(str(row.get("WhoId") or ""))),
            salesforce_key(str(row.get("WhoId") or "")),
            _activity_day(row, "StartDateTime"),
        )
    for engagement in engagements:
        if not isinstance(engagement, dict) or not is_recorded_call(engagement):
            continue
        try:
            day = engagement_day(engagement)
        except MeetingCadenceGeneratorError as exc:
            raise MultithreadingDepthGeneratorError(str(exc)) from exc
        account_entity = entity_by_key.get(salesforce_key(str(engagement.get("account_id") or "")))
        for person in engagement.get("participants") or []:
            if not isinstance(person, dict):
                continue
            if str(person.get("type") or "").casefold() == "rep":
                continue
            if not is_director_title(person.get("title")):
                continue
            email = str(person.get("email") or "").strip().casefold()
            name = str(person.get("name") or "").strip().casefold()
            matched = by_email.get(email, []) if email else []
            credited = False
            for account_id, person_key in matched:
                entity_id = entity_by_key.get(salesforce_key(account_id))
                if entity_id:
                    add(entity_id, person_key, day)
                    credited = True
            if credited or not account_entity or not (email or name):
                continue
            add(account_entity, f"chorus:{email or name}", day)
    return found


def _bucket(day: date, current_start: date, prior_start: date, prior_end: date, end: date) -> str | None:
    if current_start <= day <= end:
        return "current"
    if prior_start <= day <= prior_end:
        return "prior"
    return None


def _activity_day(row: dict[str, Any], field: str) -> date:
    raw = row.get(field)
    text = str(raw or "").strip()
    if len(text) < 10:
        reference = str(row.get("Id") or field)
        raise MultithreadingDepthGeneratorError(
            f"Salesforce activity {reference} has no {field}"
        )
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise MultithreadingDepthGeneratorError(
            f"Salesforce activity {row.get('Id') or field} {field} is not a date: {raw!r}"
        ) from exc


def _directors_by_account(contacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        account = salesforce_key(str(contact.get("AccountId") or ""))
        if account:
            grouped.setdefault(account, []).append(contact)
    return grouped


def _directors_by_email(contacts: list[dict[str, Any]]) -> dict[str, list[tuple[str, str]]]:
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
        raise MultithreadingDepthGeneratorError(f"invalid Salesforce id: {value!r}")
    return text


def _count(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MultithreadingDepthGeneratorError(
            f"{label} must be a non-negative integer, got {value!r}"
        )
