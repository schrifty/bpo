"""Salesforce generator for Health Score ``summit_attendance``.

LeanDNA Summit is the Manufacturing Excellence Summit campaign. Before the
campaign start date, a Campaign Member status of Registered counts. On and
after that date, only Attended counts. Campaign end dates are ignored; they
stay open for months after the event.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from src.healthscore_web.jsm_match import JsmMatchError, monthly_as_ofs
from src.healthscore_web.store import connect, period_key_for, periods_for_metric, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "summit_attendance"
GRAIN = "monthly"
GENERATOR_NAME = "get_summit_attendance"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "advocacy"]
WINDOW_DAYS = 365
SUMMIT_NAME = "manufacturing excellence summit"
EXCLUDED_NAME_PARTS = ("dinner", "takeaway", "outreach")


class SummitAttendanceGeneratorError(RuntimeError):
    pass


def summit_attendance_points(attended: bool) -> int:
    """Yes scores 1. No scores 0."""
    return 1 if attended else 0


def _parse_day(raw: Any, *, label: str) -> date | None:
    text = str(raw or "").strip()[:10]
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise SummitAttendanceGeneratorError(
            f"{label} is not a date: {raw!r}"
        ) from exc


def is_summit_campaign(name: str, campaign_type: str | None = None) -> bool:
    folded = " ".join(str(name or "").casefold().split())
    if SUMMIT_NAME not in folded:
        return False
    if any(part in folded for part in EXCLUDED_NAME_PARTS):
        return False
    if campaign_type and str(campaign_type).strip().casefold() not in ("", "event"):
        return False
    return True


def member_counts(status: str, *, mode: str) -> bool:
    """Registered counts only before the event. Attended counts in both modes."""
    folded = " ".join(str(status or "").casefold().split())
    attended = folded == "attended" or folded.startswith("attended ")
    registered = folded == "registered" or folded.startswith("registered ")
    if mode == "attended":
        return attended
    if mode == "registered":
        return registered or attended
    raise SummitAttendanceGeneratorError(f"unknown summit member mode {mode!r}")


def campaign_mode(start: date, as_of: date) -> str:
    return "registered" if as_of < start else "attended"


def _campaigns_in_window(campaigns: list[dict[str, Any]], as_of: date) -> list[dict[str, Any]]:
    earliest = as_of - timedelta(days=WINDOW_DAYS)
    chosen: list[dict[str, Any]] = []
    for row in campaigns:
        name = str(row.get("Name") or row.get("name") or "")
        campaign_type = row.get("Type") if "Type" in row else row.get("type")
        if not is_summit_campaign(name, None if campaign_type is None else str(campaign_type)):
            continue
        start = _parse_day(row.get("StartDate") or row.get("start_date"), label=f"{name} StartDate")
        if start is None:
            raise SummitAttendanceGeneratorError(
                f"Manufacturing Excellence Summit campaign {name!r} has no start date"
            )
        if start < earliest:
            continue
        chosen.append({**row, "start_date": start, "mode": campaign_mode(start, as_of)})
    return chosen


def _account_of(member: dict[str, Any]) -> tuple[str, str]:
    contact = member.get("Contact") if isinstance(member.get("Contact"), dict) else {}
    account = contact.get("Account") if isinstance(contact.get("Account"), dict) else {}
    account_id = str(
        member.get("account_id")
        or contact.get("AccountId")
        or account.get("Id")
        or ""
    ).strip()
    account_type = str(member.get("account_type") or account.get("Type") or "").strip()
    return account_id, account_type


def _parent_id(entity: dict[str, Any]) -> str:
    return str(entity.get("parent_id") if "parent_id" in entity else entity.get("ParentId") or "").strip()


def load_summit_campaigns() -> list[dict[str, Any]]:
    from src.salesforce_client import SalesforceClient

    soql = (
        "SELECT Id, Name, Type, StartDate, EndDate FROM Campaign "
        "WHERE Name LIKE '%Manufacturing Excellence Summit%'"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise SummitAttendanceGeneratorError(
            f"Salesforce Summit campaign query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise SummitAttendanceGeneratorError(
            "Salesforce Summit campaign query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def load_campaign_members(campaign_ids: list[str]) -> list[dict[str, Any]]:
    from src.salesforce_client import SalesforceClient, _soql_quoted_id_list

    if not campaign_ids:
        return []
    ids = _soql_quoted_id_list(campaign_ids)
    soql = (
        "SELECT CampaignId, Status, ContactId, Contact.AccountId, "
        "Contact.Account.Type, Contact.Account.ParentId "
        f"FROM CampaignMember WHERE ContactId != null AND CampaignId IN ({ids})"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise SummitAttendanceGeneratorError(
            f"Salesforce Summit campaign member query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise SummitAttendanceGeneratorError(
            "Salesforce Summit campaign member query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def load_entity_parent_ids(entity_ids: list[str]) -> dict[str, str]:
    from src.salesforce_client import SalesforceClient, _soql_quoted_id_list

    found: dict[str, str] = {}
    client = SalesforceClient()
    for offset in range(0, len(entity_ids), 100):
        chunk = entity_ids[offset : offset + 100]
        ids = _soql_quoted_id_list(chunk)
        soql = f"SELECT Id, ParentId FROM Account WHERE Id IN ({ids})"
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise SummitAttendanceGeneratorError(
                f"Salesforce Customer Entity parent query failed: {exc}"
            ) from exc
        for row in rows:
            if not isinstance(row, dict) or not row.get("Id"):
                continue
            found[str(row["Id"])] = str(row.get("ParentId") or "").strip()
    missing = [entity_id for entity_id in entity_ids if entity_id not in found]
    if missing:
        raise SummitAttendanceGeneratorError(
            "Salesforce Customer Entity parent query missed "
            f"{len(missing)} accounts, including {missing[0]}"
        )
    return found


def _entities_with_parents(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if all("parent_id" in entity or "ParentId" in entity for entity in entities):
        return entities
    parents = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    return [{**entity, "parent_id": parents.get(str(entity["id"]), "")} for entity in entities]


def _credited_entity_ids(
    entities: list[dict[str, Any]],
    members: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
) -> tuple[set[str], list[str]]:
    by_id = {str(campaign.get("Id") or campaign.get("id")): campaign for campaign in campaigns}
    entity_ids = {str(entity["id"]) for entity in entities}
    children_by_parent: dict[str, set[str]] = {}
    for entity in entities:
        parent = _parent_id(entity)
        if parent:
            children_by_parent.setdefault(parent, set()).add(str(entity["id"]))
    credited: set[str] = set()
    warnings: list[str] = []
    for campaign in campaigns:
        if campaign["mode"] != "attended":
            continue
        campaign_id = str(campaign.get("Id") or campaign.get("id"))
        statuses = [
            str(member.get("Status") or member.get("status") or "")
            for member in members
            if str(member.get("CampaignId") or member.get("campaign_id") or "") == campaign_id
        ]
        attended = sum(1 for status in statuses if member_counts(status, mode="attended"))
        registered = sum(1 for status in statuses if member_counts(status, mode="registered") and not member_counts(status, mode="attended"))
        if attended == 0 and registered:
            warnings.append(
                f"{campaign.get('Name') or campaign.get('name')} started "
                f"{campaign['start_date'].isoformat()} and has {registered} "
                "registrations but no Attended members"
            )
    for member in members:
        campaign_id = str(member.get("CampaignId") or member.get("campaign_id") or "")
        campaign = by_id.get(campaign_id)
        if campaign is None:
            continue
        if not member_counts(str(member.get("Status") or member.get("status") or ""), mode=campaign["mode"]):
            continue
        account_id, account_type = _account_of(member)
        if not account_id:
            continue
        if account_id in entity_ids and account_type.casefold() != "customer":
            credited.add(account_id)
        if account_type.casefold() == "customer" or (
            not account_type and account_id in children_by_parent
        ):
            credited.update(children_by_parent.get(account_id, set()))
    return credited, warnings


def get_summit_attendance(
    *,
    entities: list[dict[str, Any]],
    campaigns: list[dict[str, Any]] | None = None,
    members: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score Summit attendance for each active Salesforce Customer Entity."""
    if not entities:
        raise SummitAttendanceGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate summit_attendance"
        )
    today = as_of or date.today()
    try:
        raw_campaigns = campaigns if campaigns is not None else load_summit_campaigns()
    except SummitAttendanceGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SummitAttendanceGeneratorError(
            f"Salesforce Summit campaign query failed: {exc}"
        ) from exc
    chosen = _campaigns_in_window(raw_campaigns, today)
    if not chosen:
        raise SummitAttendanceGeneratorError(
            "no Manufacturing Excellence Summit campaign in the trailing 12 months or upcoming"
        )
    campaign_ids = [str(row.get("Id") or row.get("id")) for row in chosen]
    try:
        raw_members = members if members is not None else load_campaign_members(campaign_ids)
    except SummitAttendanceGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SummitAttendanceGeneratorError(
            f"Salesforce Summit campaign member query failed: {exc}"
        ) from exc
    scoped = _entities_with_parents(entities)
    credited, campaign_warnings = _credited_entity_ids(scoped, raw_members, chosen)
    warnings: list[dict[str, str]] = [
        {"id": "", "name": text, "warning": text} for text in campaign_warnings
    ]
    for text in campaign_warnings:
        logger.warning("Health Score summit_attendance: %s", text)
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in scoped:
            entity_id = str(entity["id"])
            present = entity_id in credited
            points = summit_attendance_points(present)
            meta = {
                "source_fields": ["Campaign", "CampaignMember.Status", "Contact.AccountId"],
                "campaigns": [
                    {
                        "id": str(row.get("Id") or row.get("id")),
                        "name": row.get("Name") or row.get("name"),
                        "start_date": row["start_date"].isoformat(),
                        "mode": row["mode"],
                    }
                    for row in chosen
                ],
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
                        "value": float(points),
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
                        "error": None,
                    }
                )
                continue
            saved = upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                value=float(points),
                points=float(points),
                generator=generator,
                tags=TAGS,
                meta=meta,
                error=None,
            )
            if saved.get("overridden"):
                overridden.append(entity_id)
            readings.append(saved)
    finally:
        if conn is not None:
            conn.close()
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "scored": len(readings),
        "attended": sum(1 for row in readings if row.get("points") == 1 or row.get("points") == 1.0),
        "overridden": len(overridden),
        "warnings": warnings,
        "readings": readings,
    }


def backfill_summit_attendance(
    *,
    months: int,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    campaigns: list[dict[str, Any]] | None = None,
    members: list[dict[str, Any]] | None = None,
    persist: bool = False,
    only_missing: bool = True,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score summit attendance as of each of the newest ``months`` dates."""
    if not entities:
        raise SummitAttendanceGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate summit_attendance"
        )
    today = as_of or date.today()
    try:
        days = monthly_as_ofs(today, months)
    except JsmMatchError as exc:
        raise SummitAttendanceGeneratorError(str(exc)) from exc
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
    if pending and campaigns is None:
        campaigns = load_summit_campaigns()
    if pending and members is None:
        campaign_ids = [str(row.get("Id") or row.get("id") or "") for row in campaigns or []]
        members = load_campaign_members([item for item in campaign_ids if item])
    scored: list[dict[str, Any]] = []
    warnings: list[str] = []
    for day in pending:
        result = get_summit_attendance(
            entities=entities,
            campaigns=campaigns,
            members=members,
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
                "attended": result.get("attended"),
                "scored": result.get("scored"),
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
