"""Chorus generator for Health Score ``call_sentiment``.

Chorus does not return a numeric sentiment field. Each recorded meeting's
summary contains the sentence Chorus writes, "overall sentiment was …".
That clause is the score for the call: positive language is +1, negative
language is -1, and a clause with both or neither is 0.

The reading is the mean of those call scores over the trailing 30 days, from
-1 to +1. A positive mean scores 2, a negative mean scores 0, and a mean of
exactly 0 scores 1. An entity with no recorded call in that window scores 0.
Calls whose summary has no sentiment sentence stay out of the average, and an
entity whose calls all lack that sentence stays unscored.

Chorus ``account_id`` is a Salesforce Account id. It credits that Customer
Entity, and every active Customer Entity whose parent is that account.
"""

from __future__ import annotations

import logging
import re
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any

from src.healthscore_web.meeting_cadence import engagement_day, load_entity_parent_ids
from src.healthscore_web.store import connect, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "call_sentiment"
GRAIN = "monthly"
GENERATOR_NAME = "get_call_sentiment"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "sentiment"]
RECORDED_TYPES = frozenset({"meeting", "dial"})
FETCH_WORKERS = 8
WINDOW_DAYS = 30

_CLAUSE = re.compile(r"overall sentiment was\s+([^.]*)", re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")
POSITIVE_TERMS = (
    "positive",
    "collaborative",
    "constructive",
    "enthusiastic",
    "solution-oriented",
    "solution-focused",
    "forward-looking",
    "pragmatic",
    "appreciat",
)
NEGATIVE_TERMS = (
    "negative",
    "frustrat",
    "hostile",
    "adversarial",
    "dissatisf",
    "unhappy",
    "tense",
    "concerned",
    "contentious",
)


class CallSentimentGeneratorError(RuntimeError):
    pass


def month_bounds(day: date) -> tuple[date, date]:
    """First and last calendar day of ``day``'s month."""
    last = monthrange(day.year, day.month)[1]
    return date(day.year, day.month, 1), date(day.year, day.month, last)


def window_bounds(as_of: date) -> tuple[date, date]:
    """The 30 calendar days ending on ``as_of``, inclusive."""
    return as_of - timedelta(days=WINDOW_DAYS - 1), as_of


def month_key(day: date) -> str:
    """Period key for the calendar month the calls happened in."""
    return f"{day.year:04d}-{day.month:02d}"


def trailing_months(as_of: date, months: int) -> list[date]:
    """First day of each of the newest ``months`` calendar months, oldest first."""
    if months < 1:
        raise CallSentimentGeneratorError("history months must be at least 1")
    year, month = as_of.year, as_of.month
    found: list[date] = []
    for _ in range(months):
        found.append(date(year, month, 1))
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    found.reverse()
    return found


def sentiment_clause_score(text: str | None) -> float | None:
    """+1, 0, or -1 from Chorus's "overall sentiment was" clause. None when absent."""
    if not text or not str(text).strip():
        return None
    plain = _TAG.sub(" ", str(text))
    match = _CLAUSE.search(plain)
    if not match:
        return None
    clause = match.group(1).casefold()
    positive = any(term in clause for term in POSITIVE_TERMS)
    negative = any(term in clause for term in NEGATIVE_TERMS)
    if positive and negative:
        return 0.0
    if negative:
        return -1.0
    if positive:
        return 1.0
    return 0.0


def call_sentiment_points(value: float | None) -> int | None:
    """2 when the month leans positive, 0 when it leans negative, 1 at exactly 0."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CallSentimentGeneratorError(f"sentiment value must be a number, got {value!r}")
    number = float(value)
    if number < -1 or number > 1:
        raise CallSentimentGeneratorError(f"sentiment value must be from -1 to +1, got {number}")
    if number > 0:
        return 2
    if number < 0:
        return 0
    return 1


def summary_text(conversation: Any) -> str:
    """Meeting-summary prose from a Chorus conversation payload or a plain string."""
    if isinstance(conversation, str):
        return conversation
    if not isinstance(conversation, dict):
        raise CallSentimentGeneratorError(
            f"Chorus conversation was {type(conversation).__name__}, expected an object"
        )
    data = conversation.get("data") if isinstance(conversation.get("data"), dict) else conversation
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else data
    parts: list[str] = []
    questions = attrs.get("custom_questions") or []
    if isinstance(questions, list):
        for item in questions:
            if isinstance(item, dict) and item.get("answer"):
                parts.append(str(item["answer"]))
    summary = attrs.get("summary")
    if summary:
        parts.append(str(summary))
    return "\n".join(parts)


def is_recorded_call(engagement: dict[str, Any]) -> bool:
    """A finished Chorus meeting or dial that was not a no-show."""
    kind = str(engagement.get("engagement_type") or "").strip().casefold()
    if kind not in RECORDED_TYPES:
        return False
    if engagement.get("no_show") is True:
        return False
    state = str(engagement.get("processing_state") or "").strip().casefold()
    if state and state != "done":
        return False
    try:
        duration = float(engagement.get("duration") or 0)
    except (TypeError, ValueError) as exc:
        reference = str(engagement.get("engagement_id") or "engagement")
        raise CallSentimentGeneratorError(
            f"Chorus engagement {reference} duration is not a number"
        ) from exc
    return duration > 0


def get_call_sentiment(
    *,
    entities: list[dict[str, Any]],
    engagements: list[dict[str, Any]] | None = None,
    summaries: dict[str, str] | None = None,
    parent_ids: dict[str, str] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score call sentiment for the 30 days ending on ``as_of``."""
    if not entities:
        raise CallSentimentGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate call_sentiment"
        )
    today = as_of or date.today()
    start, end = window_bounds(today)
    key = period_key or month_key(today)
    if engagements is None:
        engagements = load_recorded_calls(start, end)
    if parent_ids is None:
        parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    grouped, skipped = _calls_by_entity(engagements, entities, parent_ids, start=start, end=end)
    texts = summaries if summaries is not None else _fetch_summaries(_pending_ids(grouped))
    return _score_month(
        entities,
        grouped,
        texts,
        skipped,
        start=start,
        end=end,
        period_key=key,
        as_of=today,
        persist=persist,
        generator=generator,
    )


def backfill_call_sentiment(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    engagements: list[dict[str, Any]] | None = None,
    summaries: dict[str, str] | None = None,
    parent_ids: dict[str, str] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score a trailing 30-day window ending on each of the newest ``months`` month-ends."""
    if not entities:
        raise CallSentimentGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate call_sentiment"
        )
    today = as_of or date.today()
    month_starts = trailing_months(today, months)
    existing = _stored_periods() if only_missing and persist else set()
    pending = [day for day in month_starts if month_key(day) not in existing]
    skipped_months = [month_key(day) for day in month_starts if month_key(day) in existing]
    if engagements is None and pending:
        engagements = load_recorded_calls(month_starts[0], today)
    elif engagements is None:
        engagements = []
    if parent_ids is None and pending:
        parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    elif parent_ids is None:
        parent_ids = {}
    scored: list[dict[str, Any]] = []
    for day in pending:
        _, month_end = month_bounds(day)
        window_end = min(month_end, today)
        start, _ = window_bounds(window_end)
        grouped, skipped = _calls_by_entity(
            engagements, entities, parent_ids, start=start, end=window_end
        )
        texts = summaries if summaries is not None else _fetch_summaries(_pending_ids(grouped))
        result = _score_month(
            entities,
            grouped,
            texts,
            skipped,
            start=start,
            end=window_end,
            period_key=month_key(day),
            as_of=window_end,
            persist=persist,
            generator=generator,
        )
        scored.append(
            {
                "period_key": month_key(day),
                "as_of": window_end.isoformat(),
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
        "months_skipped": skipped_months,
        "dry_run": not persist,
        "months": scored,
    }


def load_recorded_calls(start: date, end: date) -> list[dict[str, Any]]:
    """Finished Chorus meetings and dials between ``start`` and ``end``, inclusive."""
    from datetime import datetime, timezone

    from src.chorus_client import ChorusClient, ChorusClientError, chorus_configured

    if not chorus_configured():
        raise CallSentimentGeneratorError("Chorus is not configured (set CHORUS_API_KEY)")
    min_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    max_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    try:
        client = ChorusClient()
        rows: list[Any] = []
        for kind in sorted(RECORDED_TYPES):
            rows.extend(client.list_engagements(min_date=min_ts, max_date=max_ts, engagement_type=kind))
    except ChorusClientError as exc:
        raise CallSentimentGeneratorError(f"Chorus call sentiment load failed: {exc}") from exc
    if not isinstance(rows, list) or not rows:
        raise CallSentimentGeneratorError(
            f"Chorus returned no meetings or dials from {start.isoformat()} to {end.isoformat()}"
        )
    recorded = [row for row in rows if isinstance(row, dict) and is_recorded_call(row)]
    if not recorded:
        raise CallSentimentGeneratorError(
            f"Chorus returned no finished recordings from {start.isoformat()} to {end.isoformat()}"
        )
    return recorded


def _fetch_summaries(engagement_ids: list[str]) -> dict[str, str]:
    if not engagement_ids:
        return {}
    from src.chorus_client import ChorusClient, ChorusClientError

    failures: list[str] = []
    texts: dict[str, str] = {}

    def _one(engagement_id: str) -> tuple[str, str]:
        body = ChorusClient().get_conversation(engagement_id)
        return engagement_id, summary_text(body)

    workers = min(FETCH_WORKERS, len(engagement_ids))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, engagement_id): engagement_id for engagement_id in engagement_ids}
        done = 0
        for future in as_completed(futures):
            engagement_id = futures[future]
            done += 1
            if done % 50 == 0 or done == len(engagement_ids):
                logger.info(
                    "Health Score call_sentiment: loaded %s/%s Chorus summaries",
                    done,
                    len(engagement_ids),
                )
            try:
                found_id, text = future.result()
            except ChorusClientError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            except CallSentimentGeneratorError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            texts[found_id] = text
    if failures and len(failures) == len(engagement_ids):
        raise CallSentimentGeneratorError(
            "Chorus conversation summaries all failed, including " + failures[0]
        )
    if failures:
        logger.warning(
            "Health Score call_sentiment: %s Chorus conversation summaries failed, including %s",
            len(failures),
            failures[0],
        )
    return texts


def _score_month(
    entities: list[dict[str, Any]],
    grouped: dict[str, list[str]],
    summaries: dict[str, str],
    skipped: dict[str, int],
    *,
    start: date,
    end: date,
    period_key: str,
    as_of: date,
    persist: bool,
    generator: str,
) -> dict[str, Any]:
    if skipped.get("unmatched_account"):
        logger.warning(
            "Health Score call_sentiment: %s recordings did not match an active Customer Entity",
            skipped["unmatched_account"],
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            call_ids = grouped.get(entity_id) or []
            scores = [
                score
                for call_id in call_ids
                if (score := sentiment_clause_score(summaries.get(call_id))) is not None
            ]
            value = round(sum(scores) / len(scores), 4) if scores else None
            points = 0 if not call_ids else call_sentiment_points(value)
            error = None
            if call_ids and points is None:
                error = (
                    "no Chorus meeting summary with an overall-sentiment sentence "
                    f"in the {WINDOW_DAYS} days ending {end.isoformat()}"
                )
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            counts = _band_counts(scores)
            meta = {
                "calls": len(call_ids),
                "scored_calls": len(scores),
                **counts,
                "window_days": WINDOW_DAYS,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "owner": OWNER_EMAIL,
            }
            payload = {
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
                    as_of=as_of,
                    period_key=period_key,
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
        "owner_display": OWNER_NAME,
        "period_key": period_key,
        "scored": scored,
        "unscored": len(readings) - scored,
        "skipped": skipped,
        "warnings": warnings,
        "readings": readings,
    }


def _band_counts(scores: list[float]) -> dict[str, int]:
    return {
        "positive": sum(1 for score in scores if score > 0),
        "neutral": sum(1 for score in scores if score == 0),
        "negative": sum(1 for score in scores if score < 0),
    }


def _calls_by_entity(
    engagements: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    parent_ids: dict[str, str],
    *,
    start: date,
    end: date,
) -> tuple[dict[str, list[str]], dict[str, int]]:
    children = _children_by_parent(entities, parent_ids)
    entity_ids = {str(entity["id"]) for entity in entities}
    entity_by_key = {_sf_key(entity_id): entity_id for entity_id in entity_ids}
    grouped = {entity_id: [] for entity_id in entity_ids}
    skipped = {"not_recorded": 0, "outside_window": 0, "no_account": 0, "unmatched_account": 0}
    seen: set[str] = set()
    for engagement in engagements:
        if not isinstance(engagement, dict):
            raise CallSentimentGeneratorError("Chorus engagement was not an object")
        if not is_recorded_call(engagement):
            skipped["not_recorded"] += 1
            continue
        day = engagement_day(engagement)
        if day < start or day > end:
            skipped["outside_window"] += 1
            continue
        engagement_id = str(engagement.get("engagement_id") or engagement.get("id") or "").strip()
        if not engagement_id:
            raise CallSentimentGeneratorError("Chorus recording is missing an engagement_id")
        if engagement_id in seen:
            continue
        seen.add(engagement_id)
        account = _sf_key(str(engagement.get("account_id") or ""))
        if not account:
            skipped["no_account"] += 1
            continue
        credited = _credited_entities(account, entity_by_key, children)
        if not credited:
            skipped["unmatched_account"] += 1
            continue
        for entity_id in credited:
            grouped[entity_id].append(engagement_id)
    return grouped, skipped


def _pending_ids(grouped: dict[str, list[str]]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for call_ids in grouped.values():
        for call_id in call_ids:
            if call_id in seen:
                continue
            seen.add(call_id)
            found.append(call_id)
    return found


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


def _sf_key(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 15:
        return text[:15]
    return text


def _stored_periods() -> set[str]:
    from src.healthscore_web.store import periods_for_metric

    conn = connect()
    try:
        return set(periods_for_metric(conn, METRIC_NAME))
    finally:
        conn.close()
