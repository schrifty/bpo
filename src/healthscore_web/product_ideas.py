"""Pendo generator for Health Score ``product_ideas``.

A submission is a poll response on a Pendo guide with ``autoCreateFeedback``
set, which is how this subscription sends responses into the feedback portal.
The trailing 12 months are measured from ``as_of``. Tone is the
PositiveNegative answer: 1 is positive and 0 is negative. One or more
submissions scores 2. None scores 0.

The Pendo account name is matched to a Customer Entity name, or to every
active entity under one matching parent. An account that matches more than
one parent is skipped and warned.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.enhancement_engagement import window_start
from src.healthscore_web.jsm_match import JsmMatchError, monthly_as_ofs
from src.healthscore_web.store import connect, period_key_for, periods_for_metric, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "product_ideas"
GRAIN = "monthly"
GENERATOR_NAME = "get_product_ideas"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "advocacy"]
_SUFFIXES = frozenset(
    {"corporation", "corp", "incorporated", "inc", "llc", "ltd", "limited", "company", "co"}
)
_NON_WORD = re.compile(r"[^a-z0-9]+")


class ProductIdeasGeneratorError(RuntimeError):
    pass


def product_idea_points(count: int) -> int:
    """2 when the customer submitted at least one idea. 0 when they submitted none."""
    if isinstance(count, bool) or not isinstance(count, int):
        raise ProductIdeasGeneratorError(f"idea count must be an integer, got {count!r}")
    if count < 0:
        raise ProductIdeasGeneratorError(f"idea count cannot be negative: {count}")
    return 2 if count >= 1 else 0


def normalize_customer_name(value: str | None) -> str:
    """Lowercase name with punctuation and legal suffixes removed."""
    parts = [
        part
        for part in _NON_WORD.sub(" ", str(value or "").casefold()).split()
        if part not in _SUFFIXES
    ]
    return " ".join(parts)


def submission_tone(poll_type: str | None, response: Any) -> str:
    """Positive, negative, or other. PositiveNegative 1 is positive and 0 is negative."""
    if str(poll_type or "").strip() != "PositiveNegative":
        return "other"
    if response in (1, True, "1"):
        return "positive"
    if response in (0, False, "0"):
        return "negative"
    return "other"


def submission_id(event: dict[str, Any]) -> str:
    """Stable id. Pendo leaves ``eventId`` blank on these poll responses."""
    raw = str(event.get("eventId") or "").strip()
    if raw:
        return raw
    parts = [
        str(event.get("guideId") or "").strip(),
        str(event.get("pollId") or "").strip(),
        str(event.get("visitorId") or "").strip(),
        str(event.get("browserTime") or "").strip(),
        str(event.get("guideStepId") or "").strip(),
    ]
    if not any(parts):
        raise ProductIdeasGeneratorError("Pendo feedback poll has no identity fields")
    return "|".join(parts)


def submission_day(event: dict[str, Any]) -> date:
    """UTC day of a Pendo poll ``browserTime`` in milliseconds."""
    raw = event.get("browserTime")
    reference = str(event.get("eventId") or event.get("guideId") or "poll")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ProductIdeasGeneratorError(
            f"Pendo poll {reference} has no browserTime: {raw!r}"
        )
    return datetime.fromtimestamp(float(raw) / 1000, timezone.utc).date()


def credited_entity_ids(pendo_name: str, entities: list[dict[str, Any]]) -> list[str]:
    """Entities for one Pendo account name.

    An exact entity name wins. Otherwise entities whose names start with the
    Pendo name credit when they share one parent. Otherwise every entity under
    the one parent whose name matches. More than one parent is no match.
    """
    needle = normalize_customer_name(pendo_name)
    if not needle:
        return []
    exact = [
        str(entity["id"])
        for entity in entities
        if normalize_customer_name(str(entity.get("name") or "")) == needle
    ]
    if exact:
        return exact
    prefixed = [
        entity
        for entity in entities
        if normalize_customer_name(str(entity.get("name") or "")).startswith(needle + " ")
    ]
    parents = {
        normalize_customer_name(str(entity.get("parent_name") or ""))
        for entity in prefixed
        if normalize_customer_name(str(entity.get("parent_name") or ""))
    }
    if prefixed and len(parents) <= 1:
        return [str(entity["id"]) for entity in prefixed]
    grouped: dict[str, list[str]] = {}
    for entity in entities:
        parent = normalize_customer_name(str(entity.get("parent_name") or ""))
        if parent == needle or parent.startswith(needle + " "):
            grouped.setdefault(parent, []).append(str(entity["id"]))
    if len(grouped) == 1:
        return next(iter(grouped.values()))
    return []


def load_feedback_guide_ids() -> list[str]:
    """Guides that create a Pendo feedback item from a response."""
    from src.pendo_client import PendoClient

    client = PendoClient()
    try:
        response = client._http_session().get(
            f"{client.base_url}/guide",
            headers=client._headers(),
            timeout=60,
        )
        response.raise_for_status()
        guides = response.json()
    except Exception as exc:  # noqa: BLE001
        raise ProductIdeasGeneratorError(f"Pendo guide catalog failed: {exc}") from exc
    if not isinstance(guides, list):
        raise ProductIdeasGeneratorError("Pendo guide catalog was not a list")
    ids = [
        str(guide.get("id") or "").strip()
        for guide in guides
        if isinstance(guide, dict) and guide.get("autoCreateFeedback") and guide.get("id")
    ]
    if not ids:
        raise ProductIdeasGeneratorError(
            "Pendo has no guides with autoCreateFeedback, so the feedback portal has no submissions"
        )
    return ids


def load_poll_events(*, start: date, end: date) -> list[dict[str, Any]]:
    """Poll responses from ``start`` through ``end``, inclusive."""
    from src.pendo_client import PendoClient

    span = max(1, (date.today() - start).days + 2)
    client = PendoClient()
    try:
        raw = client.aggregate(
            [
                {
                    "source": {
                        "pollEvents": None,
                        "timeSeries": {
                            "period": "dayRange",
                            "first": "now()",
                            "count": -span,
                        },
                    }
                }
            ],
            label="product ideas polls",
            read_timeout_days=span,
        )
    except Exception as exc:  # noqa: BLE001
        raise ProductIdeasGeneratorError(f"Pendo poll event load failed: {exc}") from exc
    rows = raw.get("results") if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        raise ProductIdeasGeneratorError("Pendo poll event load returned no results list")
    return [row for row in rows if isinstance(row, dict)]


def load_account_names(account_ids: list[str]) -> dict[str, str]:
    """Pendo account id to the agent account name."""
    from src.pendo_client import PendoClient

    client = PendoClient()
    names: dict[str, str] = {}
    for account_id in dict.fromkeys(str(item).strip() for item in account_ids if str(item).strip()):
        try:
            info = client.get_account_info(account_id)
        except Exception as exc:  # noqa: BLE001
            raise ProductIdeasGeneratorError(
                f"Pendo account {account_id} lookup failed: {exc}"
            ) from exc
        if not isinstance(info, dict) or info.get("error"):
            continue
        agent = (info.get("metadata") or {}).get("agent") or {}
        name = str(agent.get("name") or "").strip()
        if name:
            names[account_id] = name
    return names


def tally_submissions(
    events: list[dict[str, Any]],
    *,
    guide_ids: set[str],
    account_names: dict[str, str],
    entities: list[dict[str, Any]],
    as_of: date,
) -> tuple[dict[str, dict[str, int]], list[str]]:
    """Count submissions and tone per entity. Unmatched accounts are warned."""
    start = window_start(as_of)
    buckets: dict[str, dict[str, set[str]]] = {
        str(entity["id"]): {"all": set(), "positive": set(), "negative": set(), "other": set()}
        for entity in entities
    }
    warnings: list[str] = []
    seen_unmatched: set[str] = set()
    seen_events: set[str] = set()
    for event in events:
        if str(event.get("guideId") or "") not in guide_ids:
            continue
        day = submission_day(event)
        if day < start or day > as_of:
            continue
        event_id = submission_id(event)
        if event_id in seen_events:
            continue
        seen_events.add(event_id)
        account_id = str(event.get("accountId") or "").strip()
        pendo_name = account_names.get(account_id, "")
        if not pendo_name:
            if account_id not in seen_unmatched:
                seen_unmatched.add(account_id)
                warnings.append(f"Pendo account {account_id or '(blank)'} has no name")
            continue
        credited = credited_entity_ids(pendo_name, entities)
        if not credited:
            if pendo_name not in seen_unmatched:
                seen_unmatched.add(pendo_name)
                warnings.append(
                    f"Pendo account {pendo_name!r} did not match one Salesforce customer"
                )
            continue
        tone = submission_tone(str(event.get("pollType") or ""), event.get("pollResponse"))
        for entity_id in credited:
            if entity_id not in buckets:
                continue
            buckets[entity_id]["all"].add(event_id)
            buckets[entity_id][tone].add(event_id)
    tallies = {
        entity_id: {tone: len(ids) for tone, ids in tones.items()}
        for entity_id, tones in buckets.items()
    }
    return tallies, warnings


def get_product_ideas(
    *,
    entities: list[dict[str, Any]],
    events: list[dict[str, Any]] | None = None,
    guide_ids: list[str] | None = None,
    account_names: dict[str, str] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score Pendo feedback submissions for each active Customer Entity."""
    if not entities:
        raise ProductIdeasGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate product_ideas"
        )
    today = as_of or date.today()
    ids = guide_ids if guide_ids is not None else load_feedback_guide_ids()
    guide_set = {str(item) for item in ids}
    if events is None:
        events = load_poll_events(start=window_start(today), end=today)
    if account_names is None:
        account_ids = [
            str(event.get("accountId") or "").strip()
            for event in events
            if str(event.get("guideId") or "") in guide_set
        ]
        account_names = load_account_names(account_ids)
    tallies, warnings = tally_submissions(
        events,
        guide_ids=guide_set,
        account_names=account_names,
        entities=entities,
        as_of=today,
    )
    for warning in warnings:
        logger.warning("Health Score product_ideas: %s", warning)
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    key = period_key_for(GRAIN, today)
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            tally = tallies[entity_id]
            count = int(tally["all"])
            points = product_idea_points(count)
            meta = {
                "ideas": count,
                "positive": tally["positive"],
                "negative": tally["negative"],
                "other": tally["other"],
                "window_start": window_start(today).isoformat(),
                "window_end": today.isoformat(),
                "owner": OWNER_EMAIL,
            }
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": key,
                        "as_of": today.isoformat(),
                        "value": count,
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
                    value=float(count),
                    points=float(points),
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                )
            )
    finally:
        if conn is not None:
            conn.close()
    with_ideas = sum(1 for row in readings if row.get("points") == 2)
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "with_ideas": with_ideas,
        "without_ideas": len(readings) - with_ideas,
        "warnings": warnings,
        "readings": readings,
    }


def backfill_product_ideas(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    events: list[dict[str, Any]] | None = None,
    guide_ids: list[str] | None = None,
    account_names: dict[str, str] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score a trailing 12-month window ending on each of the newest ``months`` dates."""
    if not entities:
        raise ProductIdeasGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate product_ideas"
        )
    today = as_of or date.today()
    try:
        days = monthly_as_ofs(today, months)
    except JsmMatchError as exc:
        raise ProductIdeasGeneratorError(str(exc)) from exc
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
    if pending:
        ids = guide_ids if guide_ids is not None else load_feedback_guide_ids()
        if events is None:
            events = load_poll_events(start=window_start(pending[0]), end=pending[-1])
        if account_names is None:
            guide_set = {str(item) for item in ids}
            account_ids = [
                str(event.get("accountId") or "").strip()
                for event in events
                if str(event.get("guideId") or "") in guide_set
            ]
            account_names = load_account_names(account_ids)
        guide_ids = ids
    scored: list[dict[str, Any]] = []
    warnings: list[str] = []
    for day in pending:
        result = get_product_ideas(
            entities=entities,
            events=events,
            guide_ids=guide_ids,
            account_names=account_names,
            as_of=day,
            persist=persist,
            generator=generator,
        )
        for warning in result.get("warnings") or []:
            if warning not in warnings:
                warnings.append(warning)
        scored.append(
            {
                "period_key": period_key_for(GRAIN, day),
                "as_of": day.isoformat(),
                "with_ideas": result.get("with_ideas"),
                "without_ideas": result.get("without_ideas"),
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
        "warnings": warnings,
    }
