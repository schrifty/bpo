"""Chorus generator for Health Score ``call_talk_ratio``.

Each finished Chorus meeting or dial carries a talk-time split. Rep clusters
are LeanDNA staff. Customer and unidentified speakers are everyone else.
Chorus's own ``rep_talk_percent`` is rep talk divided by every speaker's talk,
and the cluster ``ranges[].talk_time`` values are that same split in seconds.

The reading is that share across the trailing 30 days, as a percent:
``100 * rep seconds / all speaker seconds``. At or under 50% scores 2. Over
50% scores 0. An entity with no recorded call in the window stays unscored.
A recording with no talk-time split stays out of the total, and an entity
whose calls all lack that split stays unscored.

Chorus ``account_id`` is a Salesforce Account id. It credits that Customer
Entity, and every active Customer Entity whose parent is that account.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

from src.healthscore_web.call_sentiment import (
    _calls_by_entity,
    _pending_ids,
    is_recorded_call,
    month_bounds,
    month_key,
    trailing_months,
    window_bounds,
)
from src.healthscore_web.store import connect, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "call_talk_ratio"
GRAIN = "monthly"
GENERATOR_NAME = "get_call_talk_ratio"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "sentiment"]
FETCH_WORKERS = 8
WINDOW_DAYS = 30
FULL_POINTS_AT_OR_BELOW = 50.0


class CallTalkRatioGeneratorError(RuntimeError):
    pass


def call_talk_ratio_points(value: float | None) -> int | None:
    """2 at or under 50 percent rep talk, 0 above it. None when there is no reading."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CallTalkRatioGeneratorError(f"talk ratio must be a number, got {value!r}")
    number = float(value)
    if number < 0 or number > 100:
        raise CallTalkRatioGeneratorError(f"talk ratio must be from 0 to 100, got {number}")
    if number <= FULL_POINTS_AT_OR_BELOW:
        return 2
    return 0


def talk_seconds(conversation: Any) -> tuple[float, float] | None:
    """``(rep_seconds, all_speaker_seconds)`` for one Chorus conversation.

    Cluster talk time is preferred. When a recording has none, Chorus's
    ``rep_talk_percent`` is used with a weight of 100 so that one call still
    contributes its published percent. None when neither split is present.
    """
    attrs = _attributes(conversation)
    recording = attrs.get("recording") if isinstance(attrs.get("recording"), dict) else {}
    rep = 0.0
    total = 0.0
    for cluster in recording.get("clusters") or []:
        if not isinstance(cluster, dict):
            raise CallTalkRatioGeneratorError("Chorus talk-time cluster was not an object")
        staff = str(cluster.get("type") or "").strip().casefold() == "rep"
        for span in cluster.get("ranges") or []:
            if not isinstance(span, dict) or span.get("talk_time") is None:
                continue
            seconds = _nonnegative("talk_time", span.get("talk_time"))
            total += seconds
            if staff:
                rep += seconds
    if total > 0:
        return rep, total
    percent = _metric_percent(attrs)
    if percent is None:
        return None
    return percent, 100.0


def get_call_talk_ratio(
    *,
    entities: list[dict[str, Any]],
    engagements: list[dict[str, Any]] | None = None,
    shares: dict[str, Any] | None = None,
    parent_ids: dict[str, str] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score rep talk-time share for the 30 days ending on ``as_of``."""
    if not entities:
        raise CallTalkRatioGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate call_talk_ratio"
        )
    today = as_of or date.today()
    start, end = window_bounds(today)
    key = period_key or month_key(today)
    if engagements is None:
        engagements = load_recorded_calls(start, end)
    if parent_ids is None:
        from src.healthscore_web.meeting_cadence import load_entity_parent_ids

        parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    grouped, skipped = _calls_by_entity(engagements, entities, parent_ids, start=start, end=end)
    resolved = shares if shares is not None else _fetch_shares(_pending_ids(grouped))
    return _score_month(
        entities,
        grouped,
        resolved,
        skipped,
        start=start,
        end=end,
        period_key=key,
        as_of=today,
        persist=persist,
        generator=generator,
    )


def backfill_call_talk_ratio(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    engagements: list[dict[str, Any]] | None = None,
    shares: dict[str, Any] | None = None,
    parent_ids: dict[str, str] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score a trailing 30-day window ending on each of the newest ``months`` month-ends."""
    if not entities:
        raise CallTalkRatioGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate call_talk_ratio"
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
        from src.healthscore_web.meeting_cadence import load_entity_parent_ids

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
        resolved = shares if shares is not None else _fetch_shares(_pending_ids(grouped))
        result = _score_month(
            entities,
            grouped,
            resolved,
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
    from src.healthscore_web.call_sentiment import RECORDED_TYPES

    if not chorus_configured():
        raise CallTalkRatioGeneratorError("Chorus is not configured (set CHORUS_API_KEY)")
    min_ts = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    max_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    try:
        client = ChorusClient()
        rows: list[Any] = []
        for kind in sorted(RECORDED_TYPES):
            rows.extend(client.list_engagements(min_date=min_ts, max_date=max_ts, engagement_type=kind))
    except ChorusClientError as exc:
        raise CallTalkRatioGeneratorError(f"Chorus talk-time load failed: {exc}") from exc
    if not isinstance(rows, list) or not rows:
        raise CallTalkRatioGeneratorError(
            f"Chorus returned no meetings or dials from {start.isoformat()} to {end.isoformat()}"
        )
    recorded = [row for row in rows if isinstance(row, dict) and is_recorded_call(row)]
    if not recorded:
        raise CallTalkRatioGeneratorError(
            f"Chorus returned no finished recordings from {start.isoformat()} to {end.isoformat()}"
        )
    return recorded


def _fetch_shares(engagement_ids: list[str]) -> dict[str, tuple[float, float] | None]:
    if not engagement_ids:
        return {}
    from src.chorus_client import ChorusClient, ChorusClientError

    failures: list[str] = []
    shares: dict[str, tuple[float, float] | None] = {}

    def _one(engagement_id: str) -> tuple[str, tuple[float, float] | None]:
        body = ChorusClient().get_conversation(engagement_id)
        return engagement_id, talk_seconds(body)

    workers = min(FETCH_WORKERS, len(engagement_ids))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, engagement_id): engagement_id for engagement_id in engagement_ids}
        done = 0
        for future in as_completed(futures):
            engagement_id = futures[future]
            done += 1
            if done % 50 == 0 or done == len(engagement_ids):
                logger.info(
                    "Health Score call_talk_ratio: loaded %s/%s Chorus talk-time splits",
                    done,
                    len(engagement_ids),
                )
            try:
                found_id, share = future.result()
            except ChorusClientError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            except CallTalkRatioGeneratorError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            shares[found_id] = share
    if failures and len(failures) == len(engagement_ids):
        raise CallTalkRatioGeneratorError(
            "Chorus talk-time splits all failed, including " + failures[0]
        )
    if failures:
        logger.warning(
            "Health Score call_talk_ratio: %s Chorus talk-time splits failed, including %s",
            len(failures),
            failures[0],
        )
    return shares


def _score_month(
    entities: list[dict[str, Any]],
    grouped: dict[str, list[str]],
    shares: dict[str, Any],
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
            "Health Score call_talk_ratio: %s recordings did not match an active Customer Entity",
            skipped["unmatched_account"],
        )
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            call_ids = grouped.get(entity_id) or []
            pairs = [
                pair
                for call_id in call_ids
                if (pair := _as_share(shares.get(call_id))) is not None
            ]
            rep = sum(pair[0] for pair in pairs)
            total = sum(pair[1] for pair in pairs)
            value = round(100.0 * rep / total, 4) if total > 0 else None
            points = call_talk_ratio_points(value) if call_ids else None
            error = None
            if call_ids and points is None:
                error = (
                    "no Chorus talk-time split "
                    f"in the {WINDOW_DAYS} days ending {end.isoformat()}"
                )
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            meta = {
                "calls": len(call_ids),
                "scored_calls": len(pairs),
                "rep_seconds": round(rep, 4),
                "talk_seconds": round(total, 4),
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


def _as_share(raw: Any) -> tuple[float, float] | None:
    if raw is None:
        return None
    if isinstance(raw, tuple) and len(raw) == 2:
        rep = _nonnegative("rep seconds", raw[0])
        total = _nonnegative("talk seconds", raw[1])
        if total <= 0:
            return None
        if rep > total:
            raise CallTalkRatioGeneratorError(
                f"rep talk time {rep} exceeds total talk time {total}"
            )
        return rep, total
    return talk_seconds(raw)


def _attributes(conversation: Any) -> dict[str, Any]:
    if not isinstance(conversation, dict):
        raise CallTalkRatioGeneratorError(
            f"Chorus conversation was {type(conversation).__name__}, expected an object"
        )
    data = conversation.get("data") if isinstance(conversation.get("data"), dict) else conversation
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else data
    if not isinstance(attrs, dict):
        raise CallTalkRatioGeneratorError("Chorus conversation had no attributes")
    return attrs


def _metric_percent(attrs: dict[str, Any]) -> float | None:
    for metric in attrs.get("metrics") or []:
        if not isinstance(metric, dict):
            continue
        if str(metric.get("name") or "").strip() != "rep_talk_percent":
            continue
        if metric.get("value") is None:
            return None
        return _percent(metric.get("value"))
    return None


def _percent(value: Any) -> float:
    number = _nonnegative("rep_talk_percent", value)
    if number > 100:
        raise CallTalkRatioGeneratorError(f"rep_talk_percent must be from 0 to 100, got {number}")
    return number


def _nonnegative(label: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CallTalkRatioGeneratorError(f"Chorus {label} is not a number: {value!r}")
    number = float(value)
    if number < 0:
        raise CallTalkRatioGeneratorError(f"Chorus {label} cannot be negative: {number}")
    return number


def _stored_periods() -> set[str]:
    from src.healthscore_web.store import periods_for_metric

    conn = connect()
    try:
        return set(periods_for_metric(conn, METRIC_NAME))
    finally:
        conn.close()
