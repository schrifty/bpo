"""Salesforce generator for Health Score ``champion_login_continuity``.

A champion is any of these people on the Customer Entity Account:

* the executive sponsor (``Executive_Sponsor__c``), whose login is
  ``Executive_Sponsor_Last_Login__c``
* a Contact whose buying role (``Buyer_Role_HubSpot_Sync__c``) is Champion,
  Technical Buyer, or Executive Sponsor
* a Contact whose user role (``User_Role__c``) includes Buyer

Contact login time is ``Pendo_Last_Visit__c``. The reading is days since the
most recent of those logins. A login in the trailing 7 days scores 2. Named
people with no login in that window score 0. An Account with none of these
people stays unscored.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "champion_login_continuity"
GRAIN = "weekly"
GENERATOR_NAME = "get_champion_login_continuity"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "adoption"]
ACTIVE_WINDOW_DAYS = 7
BUYING_ROLES = ("Champion", "Technical Buyer", "Executive Sponsor")
SPONSOR_FIELDS = (
    "Id",
    "Executive_Sponsor__c",
    "Executive_Sponsor_Email__c",
    "Executive_Sponsor_First_Name__c",
    "Executive_Sponsor_Last_Login__c",
)
CONTACT_FIELDS = (
    "Id",
    "AccountId",
    "Buyer_Role_HubSpot_Sync__c",
    "User_Role__c",
    "Pendo_Last_Visit__c",
)
NO_CHAMPION_ERROR = (
    "Salesforce Account has no executive sponsor, champion, technical buyer, or buyer"
)


class ChampionLoginGeneratorError(RuntimeError):
    pass


def champion_login_points(*, named: bool, days_since_login: int | None) -> int | None:
    """2 when a champion logged in inside the window, 0 when they did not.

    ``None`` means the Account has no executive sponsor, champion, technical
    buyer, or buyer, and stays unscored.
    """
    if not named:
        return None
    if days_since_login is None:
        return 0
    if days_since_login < 0:
        raise ChampionLoginGeneratorError(
            f"days since executive sponsor login cannot be negative: {days_since_login}"
        )
    if days_since_login <= ACTIVE_WINDOW_DAYS:
        return 2
    return 0


def _named_sponsor(row: dict[str, Any]) -> bool:
    for key in (
        "Executive_Sponsor__c",
        "Executive_Sponsor_Email__c",
        "Executive_Sponsor_First_Name__c",
    ):
        if str(row.get(key) or "").strip():
            return True
    return False


def user_role_includes_buyer(value: Any) -> bool:
    """True when ``User_Role__c`` lists Buyer as its own comma-separated role."""
    parts = [part.strip().casefold() for part in str(value or "").split(",")]
    return "buyer" in parts


def contact_is_champion(contact: dict[str, Any]) -> bool:
    """Buying role Champion, Technical Buyer, or Executive Sponsor, or user role Buyer."""
    role = str(contact.get("Buyer_Role_HubSpot_Sync__c") or "").strip().casefold()
    if role in {item.casefold() for item in BUYING_ROLES}:
        return True
    return user_role_includes_buyer(contact.get("User_Role__c"))


def _parse_login(raw: Any, *, entity_id: str, field: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    elif len(normalized) >= 5 and normalized[-5] in "+-" and normalized[-3] != ":":
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ChampionLoginGeneratorError(
            f"{field} for {entity_id} is not a datetime: {text!r}"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def days_since_login(login: datetime | None, as_of: date) -> int | None:
    if login is None:
        return None
    elapsed = (as_of - login.astimezone(timezone.utc).date()).days
    return max(0, elapsed)


def load_executive_sponsor_rows() -> list[dict[str, Any]]:
    """Customer Entity Accounts with the executive-sponsor identity and last login."""
    from src.salesforce_client import SalesforceClient

    fields = ", ".join(SPONSOR_FIELDS)
    soql = f"SELECT {fields} FROM Account WHERE Type = 'Customer Entity'"
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ChampionLoginGeneratorError(
            f"Salesforce executive-sponsor query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ChampionLoginGeneratorError(
            "Salesforce executive-sponsor query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict)]


def load_champion_contacts() -> list[dict[str, Any]]:
    """Contacts on Customer Entity accounts who count as a champion.

    Buying role Champion, Technical Buyer, or Executive Sponsor, or a user role
    that lists Buyer. The caller still checks the role so a loose SOQL match
    cannot score a different role.
    """
    from src.salesforce_client import SalesforceClient

    roles = ", ".join(f"'{role}'" for role in BUYING_ROLES)
    fields = ", ".join(CONTACT_FIELDS)
    soql = (
        f"SELECT {fields} FROM Contact WHERE Account.Type = 'Customer Entity' AND ("
        f"Buyer_Role_HubSpot_Sync__c IN ({roles}) "
        "OR User_Role__c = 'Buyer' "
        "OR User_Role__c LIKE 'Buyer,%' "
        "OR User_Role__c LIKE '%,Buyer' "
        "OR User_Role__c LIKE '%,Buyer,%'"
        ")"
    )
    try:
        rows = SalesforceClient().query_soql(soql)
    except Exception as exc:  # noqa: BLE001
        raise ChampionLoginGeneratorError(
            f"Salesforce champion-contact query failed: {exc}"
        ) from exc
    if not isinstance(rows, list):
        raise ChampionLoginGeneratorError(
            "Salesforce champion-contact query returned a non-list payload"
        )
    return [row for row in rows if isinstance(row, dict) and contact_is_champion(row)]


def _contacts_by_account(contacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for contact in contacts:
        account_id = str(contact.get("AccountId") or "").strip()
        if not account_id or not contact_is_champion(contact):
            continue
        grouped.setdefault(account_id, []).append(contact)
    return grouped


def _champion_login(
    entity_id: str,
    sponsor: dict[str, Any] | None,
    contacts: list[dict[str, Any]],
) -> tuple[bool, datetime | None, str | None, list[str]]:
    """Whether anyone qualifies, their most recent login, that contact id, and roles."""
    logins: list[tuple[datetime, str | None]] = []
    roles: list[str] = []
    named = False
    if sponsor is not None and _named_sponsor(sponsor):
        named = True
        roles.append("executive sponsor")
        login = _parse_login(
            sponsor.get("Executive_Sponsor_Last_Login__c"),
            entity_id=entity_id,
            field="Executive_Sponsor_Last_Login__c",
        )
        if login is not None:
            logins.append((login, str(sponsor.get("Executive_Sponsor__c") or "").strip() or None))
    for contact in contacts:
        if not contact_is_champion(contact):
            continue
        named = True
        role = str(contact.get("Buyer_Role_HubSpot_Sync__c") or "").strip()
        if role.casefold() in {item.casefold() for item in BUYING_ROLES}:
            roles.append(role)
        if user_role_includes_buyer(contact.get("User_Role__c")):
            roles.append("Buyer")
        login = _parse_login(
            contact.get("Pendo_Last_Visit__c"),
            entity_id=entity_id,
            field="Pendo_Last_Visit__c",
        )
        if login is not None:
            logins.append((login, str(contact.get("Id") or "").strip() or None))
    if not logins:
        return named, None, None, _unique(roles)
    latest, contact_id = max(logins, key=lambda item: item[0])
    return named, latest, contact_id, _unique(roles)


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def get_champion_login_continuity(
    *,
    entities: list[dict[str, Any]],
    sponsor_rows: list[dict[str, Any]] | None = None,
    champion_contacts: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score champion login continuity for each active Salesforce Customer Entity."""
    if not entities:
        raise ChampionLoginGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate "
            "champion_login_continuity"
        )
    try:
        rows = sponsor_rows if sponsor_rows is not None else load_executive_sponsor_rows()
    except ChampionLoginGeneratorError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ChampionLoginGeneratorError(
            f"Salesforce executive-sponsor query failed: {exc}"
        ) from exc
    if not rows:
        raise ChampionLoginGeneratorError(
            "Salesforce returned no Customer Entity executive-sponsor rows"
        )
    contacts = (
        champion_contacts if champion_contacts is not None else load_champion_contacts()
    )
    by_account = _contacts_by_account(contacts)
    by_id = {str(row.get("Id") or "").strip(): row for row in rows if row.get("Id")}
    today = as_of or date.today()
    warnings: list[dict[str, str]] = []
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            row = by_id.get(entity_id)
            people = by_account.get(entity_id, [])
            if row is None and not people:
                named = False
                elapsed = None
                error = "Salesforce Customer Entity missing from executive-sponsor query"
                sponsor_id = None
                login_contact_id = None
                roles: list[str] = []
            else:
                named, login, login_contact_id, roles = _champion_login(entity_id, row, people)
                elapsed = days_since_login(login, today)
                sponsor_id = (
                    str((row or {}).get("Executive_Sponsor__c") or "").strip() or None
                )
                error = None if named else NO_CHAMPION_ERROR
            points = champion_login_points(named=named, days_since_login=elapsed)
            if error:
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
                logger.warning(
                    "Health Score champion_login_continuity: %s for Salesforce entity %s (%s)",
                    error,
                    entity.get("name"),
                    entity_id,
                )
            meta = {
                "source_fields": [
                    "Executive_Sponsor__c",
                    "Executive_Sponsor_Last_Login__c",
                    "Contact.Buyer_Role_HubSpot_Sync__c",
                    "Contact.User_Role__c",
                    "Contact.Pendo_Last_Visit__c",
                ],
                "sponsor_contact_id": sponsor_id,
                "login_contact_id": login_contact_id,
                "champion_roles": roles,
                "qualifying_contacts": len(people),
                "window_days": ACTIVE_WINDOW_DAYS,
                "owner": OWNER_EMAIL,
            }
            value = float(elapsed) if elapsed is not None else None
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": period_key_for(GRAIN, today),
                        "as_of": today.isoformat(),
                        "value": value,
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
                        "error": error,
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
                value=value,
                points=None if points is None else float(points),
                generator=generator,
                tags=TAGS,
                meta=meta,
                error=error,
            )
            if saved.get("overridden"):
                overridden.append(entity_id)
            readings.append(saved)
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
        "overridden": len(overridden),
        "warnings": warnings,
        "readings": readings,
    }
