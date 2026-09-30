"""Use case / anchor expansion vs. contract for each Customer Entity.

Pendo is the source. A premium module is Inventory Optimization (IOP), plus AIOP
or DSM when those names appear in the feature or page catalog. Any use during
the calendar month of ``as_of`` scores 3. A matched account with no use scores
0. An entity with no Pendo account stays unscored.

Pendo stores this usage on a customer account, not on a Salesforce site. An
account credits every active Customer Entity whose name or parent contains
that account name as a whole label.
"""

from __future__ import annotations

import calendar
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading
from src.pendo_client import PendoClient
from src.salesforce_client import _customer_label_matches_text

logger = logging.getLogger(__name__)

METRIC_NAME = "premium_anchor_expansion"
GRAIN = "monthly"
GENERATOR_NAME = "get_premium_anchor_expansion"
TAGS = ["customer-success", "healthscore", "value"]
MAX_POINTS = 3
NO_ACCOUNT_ERROR = "No Pendo account matches this Customer Entity."
_PREMIUM_NAME = re.compile(
    r"\baiop\b|\bdsm\b|\biop\b|inventory optimization",
    re.IGNORECASE,
)


class PremiumAnchorGeneratorError(RuntimeError):
    """Pendo or the Customer Entity inventory could not produce this score."""


def is_premium_name(name: str) -> bool:
    """True when a Pendo feature or page is a premium module."""
    return bool(_PREMIUM_NAME.search(name or ""))


def premium_points(events: float) -> int:
    """Any premium use scores 3. No use scores 0."""
    return MAX_POINTS if events > 0 else 0


def get_premium_anchor_expansion(
    *,
    entities: list[dict[str, Any]],
    accounts: list[dict[str, str]] | None = None,
    events: dict[str, dict[tuple[int, int], float]] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score whether each entity used a premium module during the month."""
    if not entities:
        raise PremiumAnchorGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate premium_anchor_expansion"
        )
    today = as_of or date.today()
    if accounts is None or events is None:
        loaded_accounts, loaded_events = load_premium_events(
            date(today.year, today.month, 1), today
        )
        accounts = loaded_accounts if accounts is None else accounts
        events = loaded_events if events is None else events
    if not accounts:
        raise PremiumAnchorGeneratorError(
            "Pendo returned no accounts for premium anchor expansion."
        )
    key = period_key or period_key_for(GRAIN, today)
    month = (today.year, today.month)
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            account_ids = accounts_for_entity(entity, accounts)
            error = None
            value: float | None
            points: int | None
            if not account_ids:
                value = None
                points = None
                error = NO_ACCOUNT_ERROR
                warnings.append(
                    {"id": entity_id, "name": str(entity.get("name") or ""), "warning": error}
                )
                total = 0.0
            else:
                total = sum(
                    float((events.get(account_id) or {}).get(month) or 0)
                    for account_id in account_ids
                )
                value = total
                points = premium_points(total)
            meta = {
                "pendo_accounts": account_ids,
                "premium_events": total,
                "month": f"{month[0]:04d}-{month[1]:02d}",
            }
            payload = {
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
        "scored": scored,
        "unscored": len(readings) - scored,
        "warnings": warnings,
        "readings": readings,
        "dry_run": not persist,
    }


def accounts_for_entity(
    entity: dict[str, Any], accounts: list[dict[str, str]]
) -> list[str]:
    """Pendo accounts whose name is the entity or its parent."""
    found: list[str] = []
    for account in accounts:
        label = str(account.get("name") or "").strip().upper()
        account_id = str(account.get("id") or "").strip()
        if not label or not account_id:
            continue
        if any(
            _customer_label_matches_text(label, str(entity.get(key) or ""))
            for key in ("name", "entity_name", "parent_name", "ultimate_parent_name")
        ):
            found.append(account_id)
    return found


def load_premium_events(
    start: date, end: date
) -> tuple[list[dict[str, str]], dict[str, dict[tuple[int, int], float]]]:
    """Load Pendo accounts and premium feature/page events in the date window."""
    try:
        client = PendoClient()
        feature_ids = _premium_ids(client.get_feature_catalog())
        page_ids = _premium_ids(client.get_page_catalog())
        accounts = _accounts(client.list_accounts())
        logger.info(
            "Health Score premium_anchor_expansion: %s premium features, %s premium pages, %s Pendo accounts",
            len(feature_ids),
            len(page_ids),
            len(accounts),
        )
        events: dict[str, dict[tuple[int, int], float]] = {}
        days = _trailing_days(start)
        if feature_ids:
            _add_events(
                events,
                _grouped(client, "featureEvents", "featureId", days),
                feature_ids,
                start,
                end,
            )
        if page_ids:
            _add_events(
                events,
                _grouped(client, "pageEvents", "pageId", days),
                page_ids,
                start,
                end,
            )
    except PremiumAnchorGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise PremiumAnchorGeneratorError(
            f"Pendo premium-usage query failed: {exc}"
        ) from exc
    if not feature_ids and not page_ids:
        raise PremiumAnchorGeneratorError(
            "Pendo has no Inventory Optimization, AIOP, or DSM features or pages."
        )
    return accounts, events


def _premium_ids(payload: Any) -> set[str]:
    """Catalog is ``{id: name}``. A list of ``{id, name}`` rows is accepted too."""
    found: set[str] = set()
    if isinstance(payload, dict):
        pairs = (
            (key, value)
            for key, value in payload.items()
            if key != "results" and isinstance(value, str)
        )
    elif isinstance(payload, list):
        pairs = (
            (row.get("id"), row.get("name"))
            for row in payload
            if isinstance(row, dict)
        )
    else:
        pairs = ()
    for item_id, name in pairs:
        if is_premium_name(str(name or "")) and str(item_id or "").strip():
            found.add(str(item_id).strip())
    return found


def _accounts(payload: Any) -> list[dict[str, str]]:
    rows = payload.get("results") if isinstance(payload, dict) else None
    accounts: list[dict[str, str]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(((row.get("metadata") or {}).get("agent") or {}).get("name") or "").strip()
        account_id = str(row.get("accountId") or "").strip()
        if name and account_id:
            accounts.append({"id": account_id, "name": name})
    return accounts


def _grouped(client: PendoClient, source: str, item_key: str, days: int) -> list[dict[str, Any]]:
    pipeline = [
        {"source": {source: None, "timeSeries": _day_range(days)}},
        {
            "group": {
                "group": ["accountId", item_key, "month"],
                "fields": [{"numEvents": {"sum": "numEvents"}}],
            }
        },
    ]
    payload = client.aggregate(
        pipeline,
        label=f"premium-{source}",
        read_timeout_days=days,
    )
    rows = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise PremiumAnchorGeneratorError(f"Pendo {source} aggregation returned no rows.")
    return [row for row in rows if isinstance(row, dict)]


def _add_events(
    events: dict[str, dict[tuple[int, int], float]],
    rows: list[dict[str, Any]],
    premium_ids: set[str],
    start: date,
    end: date,
) -> None:
    for row in rows:
        item_id = str(row.get("featureId") or row.get("pageId") or "")
        if item_id not in premium_ids:
            continue
        month = _month_key(row.get("month"))
        if month is None:
            continue
        month_start = date(month[0], month[1], 1)
        month_end = date(month[0], month[1], calendar.monthrange(month[0], month[1])[1])
        if month_end < start or month_start > end:
            continue
        account_id = str(row.get("accountId") or "").strip()
        if not account_id:
            continue
        bucket = events.setdefault(account_id, {})
        bucket[month] = bucket.get(month, 0.0) + float(row.get("numEvents") or 0)


def _month_key(value: Any) -> tuple[int, int] | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        stamp = datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        return stamp.year, stamp.month
    text = str(value or "").strip()
    if len(text) >= 7 and text[4] == "-":
        try:
            return int(text[:4]), int(text[5:7])
        except ValueError:
            return None
    return None


def _trailing_days(start: date) -> int:
    days = (date.today() - start).days + 1
    if days < 1:
        raise PremiumAnchorGeneratorError(
            f"Premium usage window {start.isoformat()} is in the future."
        )
    return days


def _day_range(days: int) -> dict[str, Any]:
    return {"period": "dayRange", "first": "now()", "count": -max(int(days), 1)}
