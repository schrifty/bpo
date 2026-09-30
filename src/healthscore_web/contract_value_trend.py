"""Percent change in annual contract value at a Salesforce renewal.

A renewal is a Closed Won Opportunity of type Renewal or Invoice Only Renewal.
Its ARR is one year of the new term. The prior term is the previous Closed Won
New Business, Renewal, or Invoice Only Renewal on that same account. The stored
value is the percent change. Growth of 5% or more scores 5, then 4, 3, and 2
for each lower whole percent, 0% up to 2% scores 1, and a contraction scores 0.

An opportunity on the Customer Entity scores that entity. An opportunity on
the parent scores the child only when that parent has one active Customer
Entity. A month with no renewal stays unscored.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "contract_value_trend"
GRAIN = "monthly"
GENERATOR_NAME = "get_contract_value_trend"
TAGS = ["customer-success", "healthscore", "commercial"]
MAX_POINTS = 5
_ID_CHUNK = 150
RENEWAL_TYPES = frozenset({"Renewal", "Invoice Only Renewal"})
TERM_TYPES = frozenset({"New Business", "Renewal", "Invoice Only Renewal"})
NO_RENEWAL_ERROR = "No Salesforce renewal closed for this Customer Entity during the month."
NO_PRIOR_ERROR = "No prior-term ARR to compare with this renewal."


class ContractValueTrendGeneratorError(RuntimeError):
    """Salesforce could not produce a renewal ACV change."""


def contract_value_points(change_pct: float | None) -> int | None:
    """Map a percent ACV change onto the framework bands."""
    if change_pct is None:
        return None
    if change_pct < 0:
        return 0
    if change_pct >= 5:
        return MAX_POINTS
    if change_pct >= 4:
        return 4
    if change_pct >= 3:
        return 3
    if change_pct >= 2:
        return 2
    return 1


def get_contract_value_trend(
    *,
    entities: list[dict[str, Any]],
    opportunities: list[dict[str, Any]] | None = None,
    parents: dict[str, str] | None = None,
    as_of: date | None = None,
    period_key: str | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score the renewal that closed during the calendar month of ``as_of``."""
    if not entities:
        raise ContractValueTrendGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate contract_value_trend"
        )
    today = as_of or date.today()
    entity_ids = [str(entity["id"]) for entity in entities]
    if parents is None:
        parents = load_parent_ids(entity_ids)
    if opportunities is None:
        opportunities = load_term_opportunities()
    if not opportunities:
        raise ContractValueTrendGeneratorError(
            "Salesforce returned no won new-business or renewal ARR for contract value trend."
        )
    month_start = date(today.year, today.month, 1)
    chosen = _renewals_in_month(entities, parents, opportunities, month_start, today)
    key = period_key or period_key_for(GRAIN, today)
    warnings: list[dict[str, str]] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            renewal = chosen.get(entity_id)
            error = None
            change: float | None = None
            prior: float | None = None
            current: float | None = None
            if renewal is None:
                error = NO_RENEWAL_ERROR
            else:
                current = float(renewal["ARR__c"])
                prior = _prior_arr(opportunities, str(renewal["AccountId"]), renewal)
                if prior is None or prior <= 0:
                    error = NO_PRIOR_ERROR
                else:
                    change = round((current - prior) / prior * 100.0, 2)
            points = None if error else contract_value_points(change)
            if error:
                warnings.append(
                    {"id": entity_id, "name": str(entity.get("name") or ""), "warning": error}
                )
            meta = {
                "opportunity_id": None if renewal is None else renewal.get("Id"),
                "close_date": None if renewal is None else renewal.get("CloseDate"),
                "current_arr": current,
                "prior_arr": prior,
                "change_pct": change,
                "month": f"{today.year:04d}-{today.month:02d}",
            }
            payload = {
                "entity_id": entity_id,
                "entity_name": entity.get("name"),
                "metric_name": METRIC_NAME,
                "grain": GRAIN,
                "period_key": key,
                "as_of": today.isoformat(),
                "value": change,
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
                    value=change,
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


def load_parent_ids(entity_ids: list[str]) -> dict[str, str]:
    """ParentId for each Customer Entity."""
    from src.salesforce_client import SalesforceClient

    ids = [entity_id for entity_id in dict.fromkeys(entity_ids) if entity_id]
    parents: dict[str, str] = {}
    if not ids:
        return parents
    client = SalesforceClient()
    try:
        for chunk in _chunks(ids):
            quoted = ", ".join(f"'{_soql_literal(entity_id)}'" for entity_id in chunk)
            rows = client.query_soql(f"SELECT Id, ParentId FROM Account WHERE Id IN ({quoted})")
            for row in rows:
                entity_id = str(row.get("Id") or "").strip()
                parent_id = str(row.get("ParentId") or "").strip()
                if entity_id and parent_id:
                    parents[entity_id] = parent_id
    except Exception as exc:  # noqa: BLE001
        raise ContractValueTrendGeneratorError(
            f"Salesforce parent-account query failed for contract_value_trend: {exc}"
        ) from exc
    return parents


def load_term_opportunities() -> list[dict[str, Any]]:
    """Closed Won new-business and renewal opportunities that carry ARR."""
    from src.salesforce_client import SalesforceClient

    soql = (
        "SELECT Id, AccountId, ARR__c, CloseDate, Type FROM Opportunity "
        "WHERE IsWon = true AND ARR__c != null AND CloseDate != null "
        "AND Type IN ('New Business', 'Renewal', 'Invoice Only Renewal')"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ContractValueTrendGeneratorError(
            f"Salesforce renewal query failed for contract_value_trend: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ContractValueTrendGeneratorError(
            "Salesforce renewal query returned no rows for contract_value_trend."
        )
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get("AccountId") or "").strip()
        opp_id = str(row.get("Id") or "").strip()
        close_date = str(row.get("CloseDate") or "").strip()[:10]
        opp_type = str(row.get("Type") or "").strip()
        if not account_id or not opp_id or len(close_date) < 10 or opp_type not in TERM_TYPES:
            continue
        try:
            arr = float(row.get("ARR__c"))
        except (TypeError, ValueError):
            continue
        if arr <= 0:
            continue
        cleaned.append(
            {
                "Id": opp_id,
                "AccountId": account_id,
                "ARR__c": arr,
                "CloseDate": close_date,
                "Type": opp_type,
            }
        )
    return cleaned


def _renewals_in_month(
    entities: list[dict[str, Any]],
    parents: dict[str, str],
    opportunities: list[dict[str, Any]],
    month_start: date,
    month_end: date,
) -> dict[str, dict[str, Any]]:
    start = month_start.isoformat()
    end = month_end.isoformat()
    month_opps = [
        opp
        for opp in opportunities
        if opp["Type"] in RENEWAL_TYPES and start <= opp["CloseDate"] <= end
    ]
    entity_ids = {str(entity["id"]) for entity in entities}
    children: dict[str, list[str]] = defaultdict(list)
    for entity in entities:
        parent_id = parents.get(str(entity["id"]))
        if parent_id:
            children[parent_id].append(str(entity["id"]))
    chosen: dict[str, dict[str, Any]] = {}

    def consider(entity_id: str, opp: dict[str, Any]) -> None:
        current = chosen.get(entity_id)
        if current is None or (opp["CloseDate"], opp["Id"]) > (current["CloseDate"], current["Id"]):
            chosen[entity_id] = opp

    for opp in month_opps:
        if opp["AccountId"] in entity_ids:
            consider(opp["AccountId"], opp)
    for opp in month_opps:
        if opp["AccountId"] in entity_ids:
            continue
        kids = children.get(opp["AccountId"]) or []
        if len(kids) == 1 and kids[0] not in chosen:
            consider(kids[0], opp)
    return chosen


def _prior_arr(
    opportunities: list[dict[str, Any]], account_id: str, renewal: dict[str, Any]
) -> float | None:
    stamp = (str(renewal["CloseDate"]), str(renewal["Id"]))
    earlier = [
        opp
        for opp in opportunities
        if opp["AccountId"] == account_id
        and opp["Type"] in TERM_TYPES
        and (opp["CloseDate"], opp["Id"]) < stamp
    ]
    if not earlier:
        return None
    earlier.sort(key=lambda opp: (opp["CloseDate"], opp["Id"]))
    return float(earlier[-1]["ARR__c"])


def _chunks(values: list[str]) -> list[list[str]]:
    return [values[index : index + _ID_CHUNK] for index in range(0, len(values), _ID_CHUNK)]


def _soql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
