"""Aha generator for Health Score ``enhancement_engagement``.

R is the number of Aha Product Ideas submitted in the trailing 12 months by a
customer portal user. D is how many of those ideas have workflow status
Shipped. The submitter's email domain matches an Aha idea organization's
``email_domains``. That organization's Salesforce Account id is either a
Customer Entity or the parent Customer account; every active Customer Entity
under that parent receives the same R and D.

Ideas with no requests in the window stay unscored. The component has no
weight yet, so the reading does not enter the weighted Health Score.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "enhancement_engagement"
GRAIN = "monthly"
GENERATOR_NAME = "get_enhancement_engagement"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "relationship"]
PRODUCT_NAME = "product ideas"
DELIVERED_STATUS = "shipped"
INTERNAL_DOMAINS = frozenset({"leandna.com"})
_SF_ID = re.compile(r"^[A-Za-z0-9]{15,18}$")
_DOMAIN_SPLIT = re.compile(r"[\s,;]+")


class EnhancementEngagementGeneratorError(RuntimeError):
    pass


def enhancement_engagement_points(requests: int, delivered: int) -> float | None:
    """Score 0–10 from request count R and delivered count D.

    No requests is N/A. Up to 3 points come from submitting requests, capped
    at 5 requests. Up to 7 points come from D / R. D must be a subset of R.
    """
    if isinstance(requests, bool) or isinstance(delivered, bool):
        raise EnhancementEngagementGeneratorError(
            f"request and delivered counts must be integers, got {requests!r}, {delivered!r}"
        )
    if not isinstance(requests, int) or not isinstance(delivered, int):
        raise EnhancementEngagementGeneratorError(
            f"request and delivered counts must be integers, got {requests!r}, {delivered!r}"
        )
    if requests < 0 or delivered < 0:
        raise EnhancementEngagementGeneratorError(
            f"request and delivered counts cannot be negative: R={requests}, D={delivered}"
        )
    if delivered > requests:
        raise EnhancementEngagementGeneratorError(
            f"delivered count {delivered} is not a subset of request count {requests}"
        )
    if requests == 0:
        return None
    request_count = Decimal(requests)
    delivered_count = Decimal(delivered)
    raw = (
        Decimal(3) * min(request_count, Decimal(5)) / Decimal(5)
        + Decimal(7) * delivered_count / request_count
    )
    clamped = min(Decimal(10), max(Decimal(0), raw))
    return float(clamped.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def window_start(as_of: date) -> date:
    """Calendar date one year before ``as_of``, inclusive."""
    try:
        return as_of.replace(year=as_of.year - 1)
    except ValueError:
        return as_of.replace(year=as_of.year - 1, day=28)


def email_domain(email: Any) -> str:
    text = str(email or "").strip().casefold()
    if "@" not in text:
        return ""
    return text.rsplit("@", 1)[-1].strip().rstrip(".")


def split_email_domains(raw: Any) -> list[str]:
    domains: list[str] = []
    seen: set[str] = set()
    for part in _DOMAIN_SPLIT.split(str(raw or "").strip().casefold()):
        domain = part.lstrip("@").rstrip(".")
        if not domain or domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return domains


def salesforce_account_id(organization: dict[str, Any]) -> str:
    for field in organization.get("integration_fields") or []:
        if not isinstance(field, dict):
            continue
        if str(field.get("service_name") or "").strip().casefold() != "salesforce":
            continue
        if str(field.get("name") or "").strip() != "Id":
            continue
        return str(field.get("value") or "").strip()
    return ""


def _created_on(idea: dict[str, Any]) -> date:
    raw = idea.get("created_at") or idea.get("created_on")
    text = str(raw or "").strip()[:10]
    reference = str(idea.get("reference_num") or idea.get("id") or "idea")
    if len(text) != 10:
        raise EnhancementEngagementGeneratorError(
            f"Aha idea {reference} has no created_at date"
        )
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise EnhancementEngagementGeneratorError(
            f"Aha idea {reference} created_at is not a date: {raw!r}"
        ) from exc


def _status_name(idea: dict[str, Any]) -> str:
    status = idea.get("workflow_status")
    if isinstance(status, dict):
        return str(status.get("name") or "").strip()
    return str(idea.get("status") or status or "").strip()


def _portal_email(idea: dict[str, Any]) -> str:
    user = idea.get("created_by_portal_user")
    if isinstance(user, dict):
        return str(user.get("email") or "").strip()
    return str(idea.get("email") or "").strip()


def _quote_salesforce_ids(ids: list[str]) -> str:
    quoted: list[str] = []
    for raw in ids:
        text = str(raw or "").strip()
        if not _SF_ID.match(text):
            raise EnhancementEngagementGeneratorError(
                f"invalid Salesforce Account id: {raw!r}"
            )
        quoted.append(f"'{text}'")
    if not quoted:
        raise EnhancementEngagementGeneratorError("Salesforce id list is empty")
    return ", ".join(quoted)


def load_entity_parent_ids(entity_ids: list[str]) -> dict[str, str]:
    """Direct ParentId for each Customer Entity. Missing rows fail the run."""
    from src.salesforce_client import SalesforceClient

    found: dict[str, str] = {}
    client = SalesforceClient()
    for offset in range(0, len(entity_ids), 100):
        chunk = entity_ids[offset : offset + 100]
        soql = (
            "SELECT Id, ParentId FROM Account WHERE Id IN "
            f"({_quote_salesforce_ids(chunk)})"
        )
        try:
            rows = client.query_soql(soql)
        except Exception as exc:  # noqa: BLE001
            raise EnhancementEngagementGeneratorError(
                f"Salesforce Customer Entity parent query failed: {exc}"
            ) from exc
        for row in rows:
            if not isinstance(row, dict) or not row.get("Id"):
                continue
            found[str(row["Id"])] = str(row.get("ParentId") or "").strip()
    missing = [entity_id for entity_id in entity_ids if entity_id not in found]
    if missing:
        raise EnhancementEngagementGeneratorError(
            "Salesforce Customer Entity parent query missed "
            f"{len(missing)} accounts, including {missing[0]}"
        )
    return found


def load_aha_records() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Product Ideas, plus idea organizations with email domains and Salesforce ids."""
    from src.aha_client import AhaClient, AhaClientError, aha_configured

    if not aha_configured():
        raise EnhancementEngagementGeneratorError(
            "Aha is not configured (set CORTEX_AHA_API_KEY)"
        )
    try:
        client = AhaClient()
        products = client.list_products()
        matches = [
            product
            for product in products
            if isinstance(product, dict)
            and str(product.get("name") or "").strip().casefold() == PRODUCT_NAME
        ]
        if len(matches) != 1:
            raise EnhancementEngagementGeneratorError(
                f"expected one Aha product named 'Product Ideas', found {len(matches)}"
            )
        product_id = str(matches[0].get("id") or "").strip()
        if not product_id:
            raise EnhancementEngagementGeneratorError("Aha Product Ideas record has no id")
        ideas = client.list_all(
            f"/products/{product_id}/ideas",
            params={
                "fields": "created_by_portal_user,workflow_status,created_at,reference_num"
            },
        )
        organizations = client.list_all(
            "/idea_organizations",
            params={"fields": "email_domains,integration_fields"},
        )
    except EnhancementEngagementGeneratorError:
        raise
    except AhaClientError as exc:
        raise EnhancementEngagementGeneratorError(f"Aha enhancement request load failed: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise EnhancementEngagementGeneratorError(
            f"Aha enhancement request load failed: {exc}"
        ) from exc
    idea_rows = [row for row in ideas if isinstance(row, dict)]
    org_rows = [row for row in organizations if isinstance(row, dict)]
    if not idea_rows:
        raise EnhancementEngagementGeneratorError(
            "Aha Product Ideas returned no ideas"
        )
    if not org_rows:
        raise EnhancementEngagementGeneratorError(
            "Aha returned no idea organizations"
        )
    return idea_rows, org_rows


def _accounts_by_domain(
    organizations: list[dict[str, Any]],
) -> tuple[dict[str, set[str]], list[str]]:
    """Map email domain to Salesforce Account ids. Domains with no Account id warn."""
    accounts: dict[str, set[str]] = {}
    warnings: list[str] = []
    for organization in organizations:
        domains = split_email_domains(organization.get("email_domains"))
        account_id = salesforce_account_id(organization)
        if not domains:
            continue
        if not account_id:
            warnings.append(
                "Aha idea organization has email domains "
                f"{', '.join(domains)} and no Salesforce Account id"
            )
            continue
        for domain in domains:
            accounts.setdefault(domain, set()).add(account_id)
    return accounts, warnings


def count_enhancement_requests(
    ideas: list[dict[str, Any]],
    organizations: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    parent_ids: dict[str, str],
    *,
    as_of: date,
) -> tuple[dict[str, dict[str, int]], list[str], dict[str, int]]:
    """Count R and D per active entity. D is the Shipped subset of that entity's R."""
    start = window_start(as_of)
    entity_ids = {str(entity["id"]) for entity in entities}
    children_by_parent: dict[str, set[str]] = {}
    for entity in entities:
        entity_id = str(entity["id"])
        parent = str(parent_ids.get(entity_id) or "").strip()
        if parent:
            children_by_parent.setdefault(parent, set()).add(entity_id)
    accounts_by_domain, warnings = _accounts_by_domain(organizations)
    counts: dict[str, dict[str, set[str]]] = {
        entity_id: {"requests": set(), "delivered": set()} for entity_id in entity_ids
    }
    skipped = {"internal": 0, "no_portal_user": 0, "outside_window": 0}
    unmatched: set[str] = set()
    for idea in ideas:
        created = _created_on(idea)
        if created < start or created > as_of:
            skipped["outside_window"] += 1
            continue
        idea_id = str(idea.get("id") or idea.get("reference_num") or "").strip()
        if not idea_id:
            raise EnhancementEngagementGeneratorError("Aha idea is missing an id")
        domain = email_domain(_portal_email(idea))
        if not domain:
            skipped["no_portal_user"] += 1
            continue
        if domain in INTERNAL_DOMAINS:
            skipped["internal"] += 1
            continue
        account_ids = accounts_by_domain.get(domain) or set()
        if not account_ids:
            unmatched.add(domain)
            continue
        targets: set[str] = set()
        for account_id in account_ids:
            if account_id in entity_ids:
                targets.add(account_id)
            targets.update(children_by_parent.get(account_id, set()))
        if not targets:
            unmatched.add(domain)
            continue
        delivered = _status_name(idea).casefold() == DELIVERED_STATUS
        for entity_id in targets:
            counts[entity_id]["requests"].add(idea_id)
            if delivered:
                counts[entity_id]["delivered"].add(idea_id)
    if unmatched:
        listed = ", ".join(sorted(unmatched))
        warnings.append(
            "Aha enhancement requests from "
            f"{len(unmatched)} email domains did not match an active Customer Entity: {listed}"
        )
        logger.warning(
            "Health Score enhancement_engagement: %s email domains did not match an active Customer Entity",
            len(unmatched),
        )
    tallies = {
        entity_id: {
            "requests": len(bucket["requests"]),
            "delivered": len(bucket["delivered"]),
        }
        for entity_id, bucket in counts.items()
    }
    return tallies, warnings, skipped


def get_enhancement_engagement(
    *,
    entities: list[dict[str, Any]],
    ideas: list[dict[str, Any]] | None = None,
    organizations: list[dict[str, Any]] | None = None,
    parent_ids: dict[str, str] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score enhancement engagement for each active Salesforce Customer Entity."""
    if not entities:
        raise EnhancementEngagementGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate enhancement_engagement"
        )
    today = as_of or date.today()
    if ideas is None or organizations is None:
        ideas, organizations = load_aha_records()
    if parent_ids is None:
        parent_ids = load_entity_parent_ids([str(entity["id"]) for entity in entities])
    tallies, data_warnings, skipped = count_enhancement_requests(
        ideas,
        organizations,
        entities,
        parent_ids,
        as_of=today,
    )
    if skipped["no_portal_user"] or skipped["internal"]:
        logger.info(
            "Health Score enhancement_engagement skipped %s ideas with no portal user and %s internal ideas",
            skipped["no_portal_user"],
            skipped["internal"],
        )
    warnings: list[dict[str, str]] = [
        {"id": "", "name": "", "warning": text} for text in data_warnings
    ]
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            tally = tallies[entity_id]
            requests = tally["requests"]
            delivered = tally["delivered"]
            points = enhancement_engagement_points(requests, delivered)
            error = None
            if points is None:
                error = "no Product Ideas enhancement requests in the trailing 12 months"
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            meta = {
                "request_count": requests,
                "delivered_count": delivered,
                "window_start": window_start(today).isoformat(),
                "window_end": today.isoformat(),
                "product": "Product Ideas",
                "delivered_status": "Shipped",
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
                        "value": points,
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
                    value=points,
                    points=points,
                    generator=generator,
                    tags=TAGS,
                    meta=meta,
                    error=error,
                )
            )
        unscored_now = sum(1 for row in readings if row.get("points") is None)
        if unscored_now:
            logger.info(
                "Health Score enhancement_engagement: %s entities have no requests in the trailing 12 months",
                unscored_now,
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
