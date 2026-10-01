"""Chorus generator for the ``competitive_mentions`` override flag.

The flag is raised when a finished Chorus meeting or dial in the calendar
month has a transcript that names a LeanDNA competitor. Summaries, email,
and the web are not scanned. A month with no such mention leaves the flag
off, including when the entity had no recorded call.

Chorus ``account_id`` is a Salesforce Account id. It credits that Customer
Entity, and every active Customer Entity whose parent is that account.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

from src.healthscore_web.call_sentiment import (
    CallSentimentGeneratorError,
    _calls_by_entity,
    _pending_ids,
    load_recorded_calls,
    month_bounds,
    month_key,
    trailing_months,
)
from src.healthscore_web.meeting_cadence import load_entity_parent_ids
from src.healthscore_web.store import connect, periods_for_metric, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "competitive_mentions"
GRAIN = "monthly"
GENERATOR_NAME = "get_competitive_mentions"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "override"]
FETCH_WORKERS = 8

# Display name, then phrases matched on the folded transcript. Short or
# ordinary words are not bare tokens: "GAINS" must be the company styling or
# "GAINS Systems", and "Board" ignores board meetings and similar phrases.
COMPETITORS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Kinaxis", ("kinaxis",)),
    ("o9 Solutions", ("o9 solutions", "o9")),
    ("Blue Yonder", ("blue yonder",)),
    ("SAP Integrated Business Planning (SAP IBP)", ("sap integrated business planning", "sap ibp")),
    (
        "Oracle Fusion Cloud Supply Chain Planning",
        ("oracle fusion cloud supply chain planning", "fusion cloud supply chain planning"),
    ),
    ("ToolsGroup", ("toolsgroup", "tools group")),
    ("OMP", ("omp",)),
    ("Logility", ("logility",)),
    ("E2open", ("e2open", "e2 open")),
    ("Infor", ("infor",)),
    ("Coupa", ("coupa",)),
    ("Dassault Systèmes", ("dassault",)),
    ("Anaplan", ("anaplan",)),
    ("John Galt Solutions", ("john galt",)),
    ("Slimstock", ("slimstock",)),
    ("ICRON", ("icron",)),
    ("Eyelit Technologies", ("eyelit",)),
    ("PTC", ("ptc",)),
    ("QAD", ("qad",)),
    ("Arkieva", ("arkieva",)),
    ("AIMMS", ("aimms",)),
    ("AspenTech", ("aspentech", "aspen tech", "aspen technology")),
    ("RELEX Solutions", ("relex",)),
    ("Factory Physics", ("factory physics",)),
    ("Plex Systems", ("plex systems",)),
    ("Silkline", ("silkline",)),
    ("Swarm Engineering", ("swarm engineering",)),
    ("Agillence", ("agillence",)),
    ("Lhotse Analytics", ("lhotse",)),
    ("Statii", ("statii",)),
    ("Iptor", ("iptor",)),
    ("Acumatica", ("acumatica",)),
    ("Epicor", ("epicor",)),
    ("SAP S/4HANA", ("sap s/4hana", "sap s4hana", "s/4hana", "s4hana", "s/4 hana")),
    ("SAP Ariba", ("sap ariba", "ariba")),
    ("Oracle ERP/PeopleSoft", ("oracle erp", "peoplesoft", "people soft")),
    (
        "Microsoft Dynamics 365 Supply Chain Management",
        ("microsoft dynamics 365 supply chain", "dynamics 365 supply chain"),
    ),
    ("Descartes Systems Group", ("descartes",)),
    ("Baxter Planning", ("baxter planning",)),
)

_BOARD_SKIP = re.compile(
    r"(?:on board|onboard|dashboard|keyboard|whiteboard|circuit board|"
    r"motherboard|billboard|board of|board meeting|board member|board pack|"
    r"board review|board deck|board minutes|boardroom|board room)"
)


class CompetitiveMentionsGeneratorError(RuntimeError):
    pass


def _phrase(body: str) -> re.Pattern[str]:
    return re.compile(r"(?<![a-z0-9])" + re.escape(body) + r"(?![a-z0-9])")


_PATTERNS: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = tuple(
    (name, tuple(_phrase(phrase) for phrase in phrases)) for name, phrases in COMPETITORS
)
_GAINS_PHRASE = _phrase("gains systems")
_GAINS_CAPS = re.compile(r"(?<![A-Za-z0-9])GAINS(?![A-Za-z0-9])")
_BOARD = _phrase("board")


def fold_transcript(text: str) -> str:
    """Lowercase transcript text with accents removed, for phrase matching."""
    normalized = unicodedata.normalize("NFKD", text)
    stripped = "".join(char for char in normalized if not unicodedata.combining(char))
    return stripped.casefold()


def mentioned_competitors(text: str | None) -> list[str]:
    """Competitor display names found in one transcript, in list order."""
    if not text or not str(text).strip():
        return []
    original = str(text)
    folded = fold_transcript(original)
    found: list[str] = []
    for name, patterns in _PATTERNS:
        if any(pattern.search(folded) for pattern in patterns):
            found.append(name)
    if _GAINS_PHRASE.search(folded) or _GAINS_CAPS.search(original):
        found.append("GAINS")
    if _board_mentioned(folded):
        found.append("Board")
    return found


def _board_mentioned(folded: str) -> bool:
    for match in _BOARD.finditer(folded):
        start = max(0, match.start() - 24)
        end = min(len(folded), match.end() + 24)
        if _BOARD_SKIP.search(folded[start:end]):
            continue
        return True
    return False


def transcript_text(conversation: Any) -> str:
    """Joined utterance snippets from a Chorus conversation. Summaries are ignored."""
    if isinstance(conversation, str):
        return conversation
    if not isinstance(conversation, dict):
        raise CompetitiveMentionsGeneratorError(
            f"Chorus conversation was {type(conversation).__name__}, expected an object"
        )
    data = conversation.get("data") if isinstance(conversation.get("data"), dict) else conversation
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else data
    recording = attrs.get("recording") if isinstance(attrs.get("recording"), dict) else {}
    utterances = recording.get("utterances") or []
    if not isinstance(utterances, list):
        raise CompetitiveMentionsGeneratorError("Chorus transcript utterances were not a list")
    snippets: list[str] = []
    for item in utterances:
        if isinstance(item, dict) and item.get("snippet"):
            snippets.append(str(item["snippet"]))
    return "\n".join(snippets)


def get_competitive_mentions(
    *,
    entities: list[dict[str, Any]],
    engagements: list[dict[str, Any]] | None = None,
    transcripts: dict[str, str] | None = None,
    parent_ids: dict[str, str] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Raise the flag for entities whose month transcripts name a competitor."""
    if not entities:
        raise CompetitiveMentionsGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate competitive_mentions"
        )
    today = as_of or date.today()
    start, month_end = month_bounds(today)
    end = min(month_end, today)
    key = period_key or month_key(today)
    try:
        if engagements is None:
            engagements = load_recorded_calls(start, end)
        if parent_ids is None:
            parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
        grouped, skipped = _calls_by_entity(engagements, entities, parent_ids, start=start, end=end)
    except CallSentimentGeneratorError as exc:
        raise CompetitiveMentionsGeneratorError(str(exc)) from exc
    texts, fetched_links = (
        (transcripts, {})
        if transcripts is not None
        else _fetch_transcripts(_pending_ids(grouped))
    )
    return _score_month(
        entities,
        grouped,
        texts,
        skipped,
        _conversation_links(engagements, fetched_links),
        start=start,
        end=end,
        period_key=key,
        as_of=end,
        persist=persist,
        generator=generator,
    )


def backfill_competitive_mentions(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    engagements: list[dict[str, Any]] | None = None,
    transcripts: dict[str, str] | None = None,
    parent_ids: dict[str, str] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Scan each of the newest ``months`` calendar months."""
    if not entities:
        raise CompetitiveMentionsGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate competitive_mentions"
        )
    today = as_of or date.today()
    month_starts = trailing_months(today, months)
    existing = _stored_periods() if only_missing and persist else set()
    pending = [day for day in month_starts if month_key(day) not in existing]
    skipped_months = [month_key(day) for day in month_starts if month_key(day) in existing]
    try:
        if engagements is None and pending:
            engagements = load_recorded_calls(month_starts[0], today)
        elif engagements is None:
            engagements = []
        if parent_ids is None and pending:
            parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
        elif parent_ids is None:
            parent_ids = {}
    except CallSentimentGeneratorError as exc:
        raise CompetitiveMentionsGeneratorError(str(exc)) from exc
    scored: list[dict[str, Any]] = []
    for day in pending:
        _, month_end = month_bounds(day)
        window_end = min(month_end, today)
        try:
            grouped, skipped = _calls_by_entity(
                engagements, entities, parent_ids, start=day, end=window_end
            )
        except CallSentimentGeneratorError as exc:
            raise CompetitiveMentionsGeneratorError(str(exc)) from exc
        texts, fetched_links = (
            (transcripts, {})
            if transcripts is not None
            else _fetch_transcripts(_pending_ids(grouped))
        )
        result = _score_month(
            entities,
            grouped,
            texts,
            skipped,
            _conversation_links(engagements, fetched_links),
            start=day,
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
                "raised": result.get("raised"),
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


def _http_url(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if text.lower().startswith(("http://", "https://")):
        return text
    return None


def conversation_link(conversation: Any) -> str | None:
    """A Chorus conversation page, when the payload includes one."""
    if not isinstance(conversation, dict):
        return None
    data = conversation.get("data") if isinstance(conversation.get("data"), dict) else conversation
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else data
    recording = attrs.get("recording") if isinstance(attrs.get("recording"), dict) else {}
    for source in (conversation, data, attrs, recording):
        if not isinstance(source, dict):
            continue
        for key in ("url", "conversation_url", "recording_url", "share_url"):
            found = _http_url(source.get(key))
            if found:
                return found
    return None


def _conversation_links(
    engagements: list[dict[str, Any]] | None, fetched: dict[str, str]
) -> dict[str, str]:
    """Engagement links, with a link from the conversation payload taking precedence."""
    links: dict[str, str] = {}
    for engagement in engagements or []:
        if not isinstance(engagement, dict):
            continue
        engagement_id = str(engagement.get("engagement_id") or engagement.get("id") or "").strip()
        found = conversation_link(engagement) or _http_url(engagement.get("url"))
        if engagement_id and found:
            links[engagement_id] = found
    links.update({key: value for key, value in fetched.items() if value})
    return links


def _fetch_transcripts(engagement_ids: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    if not engagement_ids:
        return {}, {}
    from src.chorus_client import ChorusClient, ChorusClientError

    failures: list[str] = []
    texts: dict[str, str] = {}
    links: dict[str, str] = {}

    def _one(engagement_id: str) -> tuple[str, str, str | None]:
        body = ChorusClient().get_conversation(engagement_id)
        return engagement_id, transcript_text(body), conversation_link(body)

    workers = min(FETCH_WORKERS, len(engagement_ids))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, engagement_id): engagement_id for engagement_id in engagement_ids}
        done = 0
        for future in as_completed(futures):
            engagement_id = futures[future]
            done += 1
            if done % 50 == 0 or done == len(engagement_ids):
                logger.info(
                    "Health Score competitive_mentions: loaded %s/%s Chorus transcripts",
                    done,
                    len(engagement_ids),
                )
            try:
                found_id, text, link = future.result()
            except ChorusClientError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            except CompetitiveMentionsGeneratorError as exc:
                failures.append(f"{engagement_id}: {exc}")
                continue
            texts[found_id] = text
            if link:
                links[found_id] = link
    if failures and len(failures) == len(engagement_ids):
        raise CompetitiveMentionsGeneratorError(
            "Chorus transcripts all failed, including " + failures[0]
        )
    if failures:
        logger.warning(
            "Health Score competitive_mentions: %s Chorus transcripts failed, including %s",
            len(failures),
            failures[0],
        )
    return texts, links


def _score_month(
    entities: list[dict[str, Any]],
    grouped: dict[str, list[str]],
    transcripts: dict[str, str],
    skipped: dict[str, int],
    links: dict[str, str] | None = None,
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
            "Health Score competitive_mentions: %s recordings did not match an active Customer Entity",
            skipped["unmatched_account"],
        )
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            hits: list[dict[str, str]] = []
            seen: set[tuple[str, str]] = set()
            for call_id in grouped.get(entity_id) or []:
                for name in mentioned_competitors(transcripts.get(call_id)):
                    pair = (name, call_id)
                    if pair in seen:
                        continue
                    seen.add(pair)
                    hit = {"competitor": name, "engagement_id": call_id}
                    link = (links or {}).get(call_id)
                    if link:
                        hit["url"] = link
                    hits.append(hit)
            raised = bool(hits)
            meta = {
                "calls": len(grouped.get(entity_id) or []),
                "raised": raised,
                "competitors": sorted({hit["competitor"] for hit in hits}),
                "mentions": hits[:20],
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
                "value": 1.0 if raised else 0.0,
                "points": None,
                "generator": generator,
                "tags": list(TAGS),
                "meta": meta,
                "error": None,
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
                    value=payload["value"],
                    points=None,
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                    error=None,
                )
            )
    finally:
        if conn is not None:
            conn.close()
    raised_count = sum(1 for row in readings if row.get("value") not in (None, 0, 0.0))
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "period_key": period_key,
        "raised": raised_count,
        "clear": len(readings) - raised_count,
        "skipped": skipped,
        "readings": readings,
    }


def _stored_periods() -> set[str]:
    conn = connect()
    try:
        return set(periods_for_metric(conn, METRIC_NAME))
    finally:
        conn.close()
