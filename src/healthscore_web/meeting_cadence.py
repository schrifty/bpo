"""Chorus generator for Health Score ``meeting_cadence``.

The trailing two quarters are the six calendar months ending on ``as_of``.
A scheduled meeting is a Chorus engagement of type ``meeting`` or
``unrecorded_meeting`` whose owner role is CSM or CS Director, or whose
subject is a QBR, EBR, or business review. Held means the engagement type is
``meeting`` and ``no_show`` is false. The percent held is the score: 80% or
more is 2 points, less is 0.

Chorus ``account_id`` is a Salesforce Account id. It credits that Customer
Entity, and every active Customer Entity whose parent is that account.
An entity with no scheduled meeting in the window stays unscored.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "meeting_cadence"
GRAIN = "monthly"
GENERATOR_NAME = "get_meeting_cadence"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "relationship"]
TARGET_PCT = 80.0
CS_ROLES = frozenset({"CSM", "CS Director"})
MEETING_TYPES = frozenset({"meeting", "unrecorded_meeting"})
HELD_TYPE = "meeting"
QBR_TERMS = ("qbr", "ebr", "business review")
_SF_ID = re.compile(r"^[A-Za-z0-9]{15,18}$")


class MeetingCadenceGeneratorError(RuntimeError):
    pass


def window_start(as_of: date) -> date:
    """First calendar day of the trailing two quarters, inclusive."""
    month = as_of.month - 6
    year = as_of.year
    if month <= 0:
        month += 12
        year -= 1
    day = min(as_of.day, _month_length(year, month))
    return date(year, month, day)


def meeting_cadence_points(pct: float | None) -> int | None:
    """2 points at or above 80%. 0 below that. No scheduled meetings stays unscored."""
    if pct is None:
        return None
    if isinstance(pct, bool) or not isinstance(pct, (int, float)):
        raise MeetingCadenceGeneratorError(f"held percent must be a number, got {pct!r}")
    value = float(pct)
    if value < 0 or value > 100:
        raise MeetingCadenceGeneratorError(f"held percent must be from 0 to 100, got {value}")
    return 2 if value >= TARGET_PCT else 0


def held_percent(scheduled: int, held: int) -> float | None:
    """Percent of scheduled meetings that were held. None when nothing was scheduled."""
    _count(scheduled, "scheduled")
    _count(held, "held")
    if held > scheduled:
        raise MeetingCadenceGeneratorError(
            f"held count {held} is not a subset of scheduled count {scheduled}"
        )
    if scheduled < 1:
        return None
    return round(100 * held / scheduled, 1)


def is_cadence_meeting(engagement: dict[str, Any], roles: dict[str, str]) -> bool:
    """True for a CSM-owned meeting or a QBR, EBR, or business review."""
    kind = str(engagement.get("engagement_type") or "").strip().casefold()
    if kind not in MEETING_TYPES:
        return False
    subject = str(engagement.get("subject") or "").casefold()
    if any(term in subject for term in QBR_TERMS):
        return True
    role = roles.get(str(engagement.get("user_id") or "").strip(), "")
    return role in CS_ROLES


def engagement_day(engagement: dict[str, Any]) -> date:
    """UTC calendar day of a Chorus ``date_time`` epoch or ISO timestamp."""
    raw = engagement.get("date_time")
    reference = str(engagement.get("engagement_id") or engagement.get("id") or "engagement")
    if isinstance(raw, bool) or raw is None:
        raise MeetingCadenceGeneratorError(
            f"Chorus engagement {reference} has no date_time"
        )
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=timezone.utc).date()
    text = str(raw).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return date.fromisoformat(text[:10])
        except ValueError as exc:
            raise MeetingCadenceGeneratorError(
                f"Chorus engagement {reference} date_time is not a date: {raw!r}"
            ) from exc
    raise MeetingCadenceGeneratorError(
        f"Chorus engagement {reference} date_time is not a date: {raw!r}"
    )


def load_chorus_records(as_of: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Engagements in the trailing two quarters, and the Chorus user roster."""
    from src.chorus_client import ChorusClient, ChorusClientError, chorus_configured

    if not chorus_configured():
        raise MeetingCadenceGeneratorError("Chorus is not configured (set CHORUS_API_KEY)")
    start = window_start(as_of)
    min_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    max_ts = int(
        datetime(as_of.year, as_of.month, as_of.day, 23, 59, 59, tzinfo=timezone.utc).timestamp()
    )
    try:
        client = ChorusClient()
        engagements = client.list_engagements(min_date=min_ts, max_date=max_ts)
        users = client.list_users()
    except ChorusClientError as exc:
        raise MeetingCadenceGeneratorError(
            f"Chorus meeting cadence load failed: {exc}"
        ) from exc
    if not isinstance(engagements, list) or not engagements:
        raise MeetingCadenceGeneratorError(
            "Chorus returned no engagements in the trailing two quarters"
        )
    if not isinstance(users, list) or not users:
        raise MeetingCadenceGeneratorError("Chorus returned no users")
    return engagements, users


def load_entity_parent_ids(entity_ids: list[str]) -> dict[str, str]:
    """Direct ParentId for each Customer Entity. Missing rows fail the run."""
    from src.salesforce_client import SalesforceClient

    found: dict[str, str] = {}
    client = SalesforceClient()
    for offset in range(0, len(entity_ids), 100):
        chunk = entity_ids[offset : offset + 100]
        soql = f"SELECT Id, ParentId FROM Account WHERE Id IN ({_quote_salesforce_ids(chunk)})"
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise MeetingCadenceGeneratorError(
                f"Salesforce Customer Entity parent query failed: {exc}"
            ) from exc
        for row in rows:
            if not isinstance(row, dict) or not row.get("Id"):
                continue
            found[str(row["Id"])] = str(row.get("ParentId") or "").strip()
    missing = [entity_id for entity_id in entity_ids if entity_id not in found]
    if missing:
        raise MeetingCadenceGeneratorError(
            "Salesforce Customer Entity parent query missed "
            f"{len(missing)} accounts, including {missing[0]}"
        )
    return found


def get_meeting_cadence(
    *,
    entities: list[dict[str, Any]],
    engagements: list[dict[str, Any]] | None = None,
    users: list[dict[str, Any]] | None = None,
    parent_ids: dict[str, str] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score CSM and QBR meeting cadence for each active Customer Entity."""
    if not entities:
        raise MeetingCadenceGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate meeting_cadence"
        )
    today = as_of or date.today()
    if engagements is None or users is None:
        engagements, users = load_chorus_records(today)
    if parent_ids is None:
        parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    roles = _user_roles(users)
    tallies, skipped = _tally_meetings(engagements, roles, entities, parent_ids, as_of=today)
    if skipped["unmatched_account"]:
        logger.warning(
            "Health Score meeting_cadence: %s CSM/QBR meetings did not match an active Customer Entity",
            skipped["unmatched_account"],
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    start = window_start(today)
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            tally = tallies[entity_id]
            scheduled = tally["scheduled"]
            held = tally["held"]
            pct = held_percent(scheduled, held)
            points = meeting_cadence_points(pct)
            error = None
            if points is None:
                error = "no scheduled CSM or QBR meetings in the trailing two quarters"
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            meta = {
                "held": held,
                "scheduled": scheduled,
                "held_pct": pct,
                "window_start": start.isoformat(),
                "window_end": today.isoformat(),
                "target_pct": TARGET_PCT,
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
                "Health Score meeting_cadence: %s entities have no scheduled CSM or QBR meetings",
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
        "skipped": skipped,
        "warnings": warnings,
        "readings": readings,
    }


def _tally_meetings(
    engagements: list[dict[str, Any]],
    roles: dict[str, str],
    entities: list[dict[str, Any]],
    parent_ids: dict[str, str],
    *,
    as_of: date,
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    start = window_start(as_of)
    children = _children_by_parent(entities, parent_ids)
    entity_ids = {str(entity["id"]) for entity in entities}
    entity_by_key = {_sf_key(entity_id): entity_id for entity_id in entity_ids}
    buckets = {entity_id: {"scheduled": set(), "held": set()} for entity_id in entity_ids}
    skipped = {"not_cadence": 0, "outside_window": 0, "no_account": 0, "unmatched_account": 0}
    seen: set[str] = set()
    in_scope = 0
    for engagement in engagements:
        if not isinstance(engagement, dict):
            raise MeetingCadenceGeneratorError("Chorus engagement was not an object")
        if not is_cadence_meeting(engagement, roles):
            skipped["not_cadence"] += 1
            continue
        day = engagement_day(engagement)
        if day < start or day > as_of:
            skipped["outside_window"] += 1
            continue
        engagement_id = str(engagement.get("engagement_id") or engagement.get("id") or "").strip()
        if not engagement_id:
            raise MeetingCadenceGeneratorError("Chorus cadence meeting is missing an engagement_id")
        if engagement_id in seen:
            continue
        seen.add(engagement_id)
        in_scope += 1
        account = _sf_key(str(engagement.get("account_id") or ""))
        if not account:
            skipped["no_account"] += 1
            continue
        credited = _credited_entities(account, entity_by_key, children)
        if not credited:
            skipped["unmatched_account"] += 1
            continue
        held = _is_held(engagement)
        for entity_id in credited:
            buckets[entity_id]["scheduled"].add(engagement_id)
            if held:
                buckets[entity_id]["held"].add(engagement_id)
    if in_scope < 1:
        raise MeetingCadenceGeneratorError(
            "Chorus returned no CSM or QBR meetings in the trailing two quarters"
        )
    tallies = {
        entity_id: {
            "scheduled": len(bucket["scheduled"]),
            "held": len(bucket["held"]),
        }
        for entity_id, bucket in buckets.items()
    }
    return tallies, skipped


def _credited_entities(
    account_key: str,
    entity_by_key: dict[str, str],
    children: dict[str, set[str]],
) -> set[str]:
    credited: set[str] = set()
    direct = entity_by_key.get(account_key)
    if direct:
        credited.add(direct)
    credited.update(children.get(account_key, ()))
    return credited


def _children_by_parent(
    entities: list[dict[str, Any]],
    parent_ids: dict[str, str],
) -> dict[str, set[str]]:
    children: dict[str, set[str]] = {}
    for entity in entities:
        entity_id = str(entity["id"])
        parent = _sf_key(str(parent_ids.get(entity_id) or entity.get("parent_id") or ""))
        if parent:
            children.setdefault(parent, set()).add(entity_id)
    return children


def _is_held(engagement: dict[str, Any]) -> bool:
    kind = str(engagement.get("engagement_type") or "").strip().casefold()
    return kind == HELD_TYPE and not _no_show(engagement)


def _no_show(engagement: dict[str, Any]) -> bool:
    raw = engagement.get("no_show")
    if isinstance(raw, bool):
        return raw
    reference = str(engagement.get("engagement_id") or engagement.get("id") or "engagement")
    raise MeetingCadenceGeneratorError(
        f"Chorus engagement {reference} no_show is not true or false: {raw!r}"
    )


def _user_roles(users: list[dict[str, Any]]) -> dict[str, str]:
    roles: dict[str, str] = {}
    for user in users:
        if not isinstance(user, dict):
            continue
        user_id = str(user.get("id") or "").strip()
        if user_id:
            roles[user_id] = str(user.get("role") or "").strip()
    if not roles:
        raise MeetingCadenceGeneratorError("Chorus user roster has no user ids")
    return roles


def _sf_key(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 15:
        return text[:15]
    return text


def _count(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MeetingCadenceGeneratorError(f"{name} count must be an integer, got {value!r}")
    if value < 0:
        raise MeetingCadenceGeneratorError(f"{name} count cannot be negative: {value}")


def _month_length(year: int, month: int) -> int:
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    return 30 if month in (4, 6, 9, 11) else 31


def _quote_salesforce_ids(ids: list[str]) -> str:
    quoted: list[str] = []
    for raw in ids:
        text = str(raw or "").strip()
        if not _SF_ID.match(text):
            raise MeetingCadenceGeneratorError(f"invalid Salesforce Account id: {raw!r}")
        quoted.append(f"'{text}'")
    if not quoted:
        raise MeetingCadenceGeneratorError("Salesforce id list is empty")
    return ", ".join(quoted)
