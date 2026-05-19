"""Salesforce API client using JWT Bearer Flow (OAuth 2.0).

Authenticates with a Connected App via JWT signed by a private key.
Queries Account (Entity Contract) and Opportunity (creation count, pipeline ARR).
Also exposes SOQL helpers for common standard objects (see MAINSTREAM_OBJECT_FIELDS)
and ``get_customer_salesforce_comprehensive`` for deck-sized multi-object exports.

Read responses (SOQL record lists, global sObject describe, COUNT totals) are cached
in-process for ``BPO_SALESFORCE_CACHE_TTL_HOURS`` (default 48). JWT tokens are not cached here.
"""

from __future__ import annotations

import copy
import datetime
import hashlib
import threading
import time
from pathlib import Path
from typing import Any

import requests

from .config import (
    BPO_SALESFORCE_CACHE_TTL_SECONDS,
    SF_ACCOUNT_FACTORY_START_DATE_FIELD,
    SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP,
    SF_LOGIN_URL,
    SF_CONSUMER_KEY,
    SF_USERNAME,
    SF_PRIVATE_KEY,
    SF_PRIVATE_KEY_PATH,
    logger,
)

_SF_READ_CACHE_LOCK = threading.Lock()
_sf_read_cache: dict[str, tuple[float, Any]] = {}


def clear_salesforce_read_cache() -> None:
    """Drop cached Salesforce read responses (SOQL, describe, counts). For tests and debugging."""
    with _SF_READ_CACHE_LOCK:
        _sf_read_cache.clear()


def reset_for_tests() -> None:
    """Reset module-level Salesforce state that can leak between tests."""
    clear_salesforce_read_cache()


def _sf_cache_key(kind: str, payload: str) -> str:
    return f"sf:{kind}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def _sf_read_cache_get(key: str) -> Any | None:
    if BPO_SALESFORCE_CACHE_TTL_SECONDS <= 0:
        return None
    now = time.time()
    with _SF_READ_CACHE_LOCK:
        hit = _sf_read_cache.get(key)
        if not hit:
            return None
        ts, val = hit
        if now - ts > BPO_SALESFORCE_CACHE_TTL_SECONDS:
            del _sf_read_cache[key]
            return None
        return copy.deepcopy(val)


def _sf_read_cache_set(key: str, val: Any) -> None:
    if BPO_SALESFORCE_CACHE_TTL_SECONDS <= 0:
        return
    with _SF_READ_CACHE_LOCK:
        _sf_read_cache[key] = (time.time(), copy.deepcopy(val))

# Account (Entity Contract): Type = 'Customer Entity'
# Base fields; Parent + optional Ultimate Parent are added by ``_entity_account_select_field_names``.
_ACCOUNT_ENTITY_CORE_FIELDS = (
    "Id",
    "Name",
    "LeanDNA_Entity_Name__c",
    "US_Persons_Only_Customer__c",
    "Contract_Status__c",
    "Contract_Contract_Start_Date__c",
    "Contract_Contract_End_Date__c",
    "ARR__c",
    "ParentId",
    "Parent.Name",
)


def _relationship_json_key_for_lookup(lookup_field: str) -> str:
    """Custom lookup API name -> REST expand key (e.g. Ultimate_Parent_Account__c -> Ultimate_Parent_Account__r)."""
    lf = (lookup_field or "").strip()
    if lf.endswith("__c"):
        return lf[:-3] + "__r"
    return lf + "__r"


def _entity_account_select_field_names() -> tuple[str, ...]:
    """Columns for Customer Entity accounts: core + Parent + optional Ultimate Parent + factory-start date."""
    cols = list(_ACCOUNT_ENTITY_CORE_FIELDS)
    ult = (SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP or "").strip()
    if ult:
        if ult.endswith("__c"):
            cols.extend([ult, ult[:-3] + "__r.Name"])
        else:
            cols.extend([ult, ult + ".Name"])
    fs = (SF_ACCOUNT_FACTORY_START_DATE_FIELD or "").strip()
    if fs and fs not in cols:
        cols.append(fs)
    return tuple(cols)


def _normalize_entity_account_row(r: dict[str, Any]) -> dict[str, Any]:
    """Flatten Parent / Ultimate Parent names for matching and slides."""
    parent = r.get("Parent") if isinstance(r.get("Parent"), dict) else {}
    parent_name = (parent.get("Name") or "").strip()
    ult_name = ""
    lf = (SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP or "").strip()
    if lf:
        uo = r.get(_relationship_json_key_for_lookup(lf))
        if isinstance(uo, dict):
            ult_name = (uo.get("Name") or "").strip()
    fs_field = (SF_ACCOUNT_FACTORY_START_DATE_FIELD or "").strip()
    factory_start = r.get(fs_field) if fs_field else None
    return {
        "Id": r.get("Id"),
        "Name": r.get("Name"),
        "LeanDNA_Entity_Name__c": r.get("LeanDNA_Entity_Name__c"),
        "US_Persons_Only_Customer__c": r.get("US_Persons_Only_Customer__c"),
        "Contract_Status__c": r.get("Contract_Status__c"),
        "Contract_Contract_Start_Date__c": r.get("Contract_Contract_Start_Date__c"),
        "Contract_Contract_End_Date__c": r.get("Contract_Contract_End_Date__c"),
        "factory_start_date": factory_start,
        "ARR__c": r.get("ARR__c"),
        "ParentId": r.get("ParentId"),
        "parent_name": parent_name,
        "ultimate_parent_name": ult_name,
    }


def _customer_name_matches_entity_account(name_upper: str, a: dict[str, Any]) -> bool:
    """Match Pendo/customer label to Entity Account Name, entity name, Parent, or Ultimate Parent."""
    if not name_upper:
        return False
    if name_upper in (a.get("Name") or "").upper():
        return True
    if name_upper in (a.get("LeanDNA_Entity_Name__c") or "").upper():
        return True
    pn = (a.get("parent_name") or "").strip()
    if pn and name_upper in pn.upper():
        return True
    un = (a.get("ultimate_parent_name") or "").strip()
    if un and name_upper in un.upper():
        return True
    return False


# Align with SalesforceClient._CHURNED_STATUSES for renewal windows (module-level for helpers).
_CHURNED_CONTRACT_STATUS_LOWER = frozenset({"churned", "cancelled", "terminated", "expired", "closed"})


def _parse_sf_contract_date(raw: Any) -> datetime.date | None:
    if raw is None:
        return None
    s = str(raw).strip()[:10]
    if len(s) < 10:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _renewal_roll_up_fields(matching: list[dict[str, Any]]) -> dict[str, Any]:
    """Contract status + end/start band across matched Customer Entity rows (one Pendo customer label)."""
    today = datetime.date.today()
    statuses: set[str] = set()
    ends_active: list[datetime.date] = []
    ends_all: list[datetime.date] = []
    starts_active: list[datetime.date] = []
    for a in matching:
        st = (a.get("Contract_Status__c") or "").strip()
        if st:
            statuses.add(st)
        churned = st.lower() in _CHURNED_CONTRACT_STATUS_LOWER if st else False
        ed = _parse_sf_contract_date(a.get("Contract_Contract_End_Date__c"))
        if ed:
            ends_all.append(ed)
            if not churned:
                ends_active.append(ed)
        sd = _parse_sf_contract_date(a.get("Contract_Contract_Start_Date__c"))
        if sd and not churned:
            starts_active.append(sd)
    ends_use = ends_active or ends_all
    nearest = min(ends_use) if ends_use else None
    farthest = max(ends_use) if ends_use else None
    days_nearest = (nearest - today).days if nearest else None
    status_list = sorted(statuses, key=lambda x: x.lower())[:16]
    out: dict[str, Any] = {
        "contract_statuses_distinct": status_list,
        "entity_row_count": len(matching),
        "contract_end_date_nearest": nearest.isoformat() if nearest else None,
        "contract_end_date_farthest": farthest.isoformat() if farthest else None,
        "days_until_contract_end_nearest": days_nearest,
        "contract_start_date_earliest_active": min(starts_active).isoformat() if starts_active else None,
        "contract_start_date_latest_active": max(starts_active).isoformat() if starts_active else None,
    }
    return out


def opportunity_account_scope_ids(entity_rows: list[dict[str, Any]]) -> list[str]:
    """Account Ids for opportunity SOQL: Customer Entity rows plus distinct ParentId values."""
    out: list[str] = []
    seen: set[str] = set()
    for row in entity_rows:
        if not isinstance(row, dict):
            continue
        for raw in (row.get("Id"), row.get("ParentId")):
            aid = (raw or "").strip()
            if len(aid) >= 15 and aid not in seen:
                seen.add(aid)
                out.append(aid)
    return out


def opportunity_account_scope_ids_from_entity_ids(
    entity_ids: list[str],
    account_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    """Expand deduplicated entity Ids to include parent accounts for portfolio pipeline SOQL."""
    rows = [account_by_id[eid] for eid in entity_ids if eid in account_by_id]
    return opportunity_account_scope_ids(rows)


# Opportunity types for creation and pipeline (include Renewal for entity-churn / parent-opp cases)
OPP_TYPES = ("New Business", "New Expansion Business", "Expansion Business", "POC", "Renewal")
PIPELINE_STAGES = ("3-Business Validation", "4-Proposal", "5-Contracts")
# Closed Won motion (calendar year on CloseDate); Expansion Business + New Expansion Business count as expansion.
_EXPANSION_CLOSED_WON_TYPES = frozenset({"Expansion Business", "New Expansion Business"})
_NEW_LOGO_CLOSED_WON_TYPES = frozenset({"New Business"})

# REST API version used for query + sObject metadata (keep in sync across methods).
SF_REST_API_VERSION = "v59.0"

# Default SELECT columns for mainstream standard objects (org must expose these fields).
# If a field is missing or renamed in your org, pass ``fields=`` to ``query_mainstream_object``
# or use ``query_soql`` with your own SOQL.
MAINSTREAM_OBJECT_FIELDS: dict[str, tuple[str, ...]] = {
    "Lead": (
        "Id", "FirstName", "LastName", "Company", "Email", "Phone", "Status", "LeadSource",
        "OwnerId", "CreatedDate", "LastModifiedDate", "IsConverted",
    ),
    "Account": (
        "Id", "Name", "Type", "Industry", "BillingCity", "BillingState", "BillingCountry",
        "Phone", "Website", "OwnerId", "CreatedDate",
    ),
    "Contact": (
        "Id", "FirstName", "LastName", "Email", "Phone", "AccountId", "Title",
        "MailingCity", "MailingState", "MailingCountry", "OwnerId", "CreatedDate",
    ),
    "Opportunity": (
        "Id", "Name", "AccountId", "StageName", "Amount", "Probability", "CloseDate", "Type",
        "ForecastCategoryName", "OwnerId", "CreatedDate", "LastModifiedDate",
    ),
    "Case": (
        "Id", "CaseNumber", "Subject", "Status", "Priority", "Origin",
        "AccountId", "ContactId", "OwnerId", "CreatedDate", "ClosedDate",
    ),
    "Task": (
        "Id", "Subject", "Status", "Priority", "ActivityDate", "WhoId", "WhatId",
        "OwnerId", "IsClosed", "CreatedDate",
    ),
    "Event": (
        "Id", "Subject", "StartDateTime", "EndDateTime", "Location", "WhoId", "WhatId",
        "OwnerId", "CreatedDate",
    ),
    "Campaign": (
        "Id", "Name", "Status", "Type", "StartDate", "EndDate", "BudgetedCost", "ActualCost",
        "OwnerId",
    ),
    "CampaignMember": (
        "Id", "CampaignId", "LeadId", "ContactId", "Status", "CreatedDate",
    ),
    "User": (
        "Id", "Name", "Username", "Email", "IsActive", "ProfileId", "UserType",
    ),
    "Product2": (
        "Id", "Name", "ProductCode", "Description", "IsActive", "Family", "CreatedDate",
    ),
    "Pricebook2": (
        "Id", "Name", "IsActive", "IsStandard", "Description",
    ),
    "Contract": (
        "Id", "ContractNumber", "AccountId", "Status", "StartDate", "EndDate",
        "ContractTerm", "OwnerId", "CreatedDate",
    ),
    "Order": (
        "Id", "OrderNumber", "AccountId", "EffectiveDate", "Status", "TotalAmount",
        "Type", "OwnerId", "CreatedDate",
    ),
    "Quote": (
        "Id", "QuoteNumber", "Name", "OpportunityId", "AccountId", "Status",
        "ExpirationDate", "GrandTotal", "OwnerId", "CreatedDate",
    ),
    "Asset": (
        "Id", "Name", "AccountId", "SerialNumber", "Status", "Product2Id",
        "InstallDate", "OwnerId",
    ),
    "OpportunityLineItem": (
        "Id", "OpportunityId", "Product2Id", "Quantity", "UnitPrice", "TotalPrice",
        "ServiceDate",
    ),
}

MAINSTREAM_OBJECT_NAMES: tuple[str, ...] = tuple(sorted(MAINSTREAM_OBJECT_FIELDS.keys()))

# Narrower SELECT lists when the default set hits INVALID_FIELD / unsupported columns in some orgs.
MAINSTREAM_OBJECT_FALLBACK_FIELDS: dict[str, tuple[str, ...]] = {
    "Case": (
        "Id", "CaseNumber", "Subject", "Status", "AccountId", "OwnerId", "CreatedDate",
    ),
    "Task": (
        "Id", "Subject", "Status", "OwnerId", "CreatedDate",
    ),
    "Event": (
        "Id", "Subject", "StartDateTime", "EndDateTime", "OwnerId", "CreatedDate",
    ),
    "Order": (
        "Id", "OrderNumber", "AccountId", "EffectiveDate", "Status", "OwnerId", "CreatedDate",
    ),
    "Quote": (
        "Id", "Name", "OpportunityId", "AccountId", "Status", "OwnerId", "CreatedDate",
    ),
    "Asset": (
        "Id", "Name", "AccountId", "Status", "OwnerId",
    ),
    "OpportunityLineItem": (
        "Id", "OpportunityId", "Product2Id", "Quantity",
    ),
    "User": (
        "Id", "Name", "Username", "Email", "IsActive",
    ),
    "Lead": (
        "Id", "FirstName", "LastName", "Company", "Email", "Status", "OwnerId", "CreatedDate",
    ),
    "Product2": (
        "Id", "Name", "IsActive", "CreatedDate",
    ),
    "Pricebook2": (
        "Id", "Name", "IsActive",
    ),
}


def _soql_string_escape(s: str) -> str:
    """Escape a value for use inside a SOQL single-quoted string (double single-quotes)."""
    return (s or "").replace("'", "''")


def _soql_like_literal(s: str) -> str:
    """Sanitize for LIKE pattern body: escape %, _, and \\ per SOQL rules."""
    t = (s or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return _soql_string_escape(t)


def _strip_sf_attributes(rec: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in rec.items() if k != "attributes"}


def _chunk_list(items: list[str], size: int) -> list[list[str]]:
    if size < 1:
        size = 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_salesforce_rest_errors(resp: requests.Response | None) -> str:
    """Best-effort parse of Salesforce REST error JSON (query, describe, etc.)."""
    if resp is None:
        return "no response"
    raw = (resp.text or "").strip()
    if not raw:
        return (getattr(resp, "reason", None) or f"HTTP {resp.status_code}")[:500]
    try:
        body = resp.json()
    except Exception:
        return raw[:500]
    parts: list[str] = []
    if isinstance(body, list):
        for item in body:
            if isinstance(item, dict):
                code = item.get("errorCode") or item.get("error")
                msg = item.get("message") or item.get("error_description")
                if code and msg:
                    parts.append(f"{code}: {msg}")
                elif msg:
                    parts.append(str(msg))
    elif isinstance(body, dict):
        if "message" in body:
            parts.append(str(body["message"]))
        for key in ("error_description", "error"):
            if body.get(key):
                parts.append(str(body[key]))
    return "; ".join(parts)[:800] if parts else raw[:500]


def _parse_oauth_error(resp: requests.Response | None) -> str:
    """Extract error message from Salesforce OAuth token error (JSON or form-encoded)."""
    if resp is None:
        return "no response"
    raw = (resp.text or "").strip() or (getattr(resp, "reason", None) or "unknown")
    try:
        err = resp.json()
        return (err.get("error_description") or err.get("error") or raw)[:500]
    except Exception:
        pass
    if "error_description=" in raw or "error=" in raw:
        from urllib.parse import parse_qs
        parsed = parse_qs(raw, keep_blank_values=True)
        desc = (parsed.get("error_description") or [""])[0]
        err = (parsed.get("error") or [""])[0]
        return (desc or err or raw)[:500]
    return raw[:500]


def _load_private_key() -> str | None:
    """Return PEM private key from env or file."""
    if SF_PRIVATE_KEY:
        return SF_PRIVATE_KEY
    if SF_PRIVATE_KEY_PATH:
        path = Path(SF_PRIVATE_KEY_PATH).expanduser()
        if path.exists():
            return path.read_text()
        logger.warning("SF_PRIVATE_KEY_PATH file not found: %s", path)
    return None


class SalesforceClient:
    """Salesforce API client with JWT auth and SOQL helpers."""

    def __init__(self):
        self._token: str | None = None
        self._instance_url: str | None = None

    def _ensure_token(self) -> None:
        if self._token is not None:
            return
        try:
            import jwt as pyjwt
            private_key = _load_private_key()
            if not all([SF_LOGIN_URL, SF_CONSUMER_KEY, SF_USERNAME, private_key]):
                raise ValueError("Salesforce not configured")
            now = int(time.time())
            payload = {
                "iss": SF_CONSUMER_KEY,
                "sub": SF_USERNAME,
                "aud": SF_LOGIN_URL.rstrip("/"),
                "exp": now + 300,
                "iat": now,
            }
            assertion = pyjwt.encode(
                payload, private_key, algorithm="RS256", headers={"alg": "RS256"}
            )
            if hasattr(assertion, "decode"):
                assertion = assertion.decode("utf-8")
            resp = requests.post(
                f"{SF_LOGIN_URL.rstrip('/')}/services/oauth2/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            if not resp.ok:
                detail = _parse_oauth_error(resp)
                raise ValueError(f"Salesforce OAuth ({resp.status_code}): {detail}")
            data = resp.json()
        except ValueError:
            raise
        except Exception as e:
            # If this is (or wraps) a requests HTTP error, pull out the response body so the user sees Salesforce's message
            resp = getattr(e, "response", None)
            if resp is not None:
                detail = _parse_oauth_error(resp)
                logger.warning("Salesforce JWT token request failed: %s", e)
                raise ValueError(f"Salesforce OAuth ({getattr(resp, 'status_code', '')}): {detail}") from e
            logger.warning("Salesforce JWT token request failed: %s", e)
            raise
        self._token = data["access_token"]
        self._instance_url = data["instance_url"].rstrip("/")

    def _query_uncached(self, soql: str) -> list[dict]:
        """Run SOQL query and return list of records (no read cache)."""
        self._ensure_token()
        url = f"{self._instance_url}/services/data/{SF_REST_API_VERSION}/query"
        params = {"q": soql}
        headers = {"Authorization": f"Bearer {self._token}"}
        out: list[dict] = []
        req_url: str | None = url
        req_params: dict | None = params
        while req_url:
            resp = requests.get(req_url, params=req_params, headers=headers, timeout=30)
            if not resp.ok:
                detail = _parse_salesforce_rest_errors(resp)
                raise requests.HTTPError(
                    f"{resp.status_code} Salesforce error: {detail}",
                    response=resp,
                )
            data = resp.json()
            out.extend(data.get("records", []))
            next_path = data.get("nextRecordsUrl")
            if next_path:
                req_url = f"{self._instance_url}{next_path}"
                req_params = None
            else:
                req_url = None
        return out

    def _query(self, soql: str) -> list[dict]:
        """Run SOQL query and return list of records (cached per ``BPO_SALESFORCE_CACHE_TTL_*``)."""
        key = _sf_cache_key("rec", soql)
        cached = _sf_read_cache_get(key)
        if cached is not None:
            return cached
        out = self._query_uncached(soql)
        _sf_read_cache_set(key, out)
        return out

    def query_soql(self, soql: str) -> list[dict[str, Any]]:
        """Public: run arbitrary SOQL (same as internal query runner)."""
        return self._query(soql)

    def list_sobject_types(self) -> list[dict[str, Any]]:
        """Return global describe list (``sobjects``) — name, label, custom flags, etc."""
        key = _sf_cache_key("describe", f"{SF_REST_API_VERSION}/sobjects/")
        cached = _sf_read_cache_get(key)
        if cached is not None:
            return cached
        self._ensure_token()
        url = f"{self._instance_url}/services/data/{SF_REST_API_VERSION}/sobjects/"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30,
        )
        resp.raise_for_status()
        objs = resp.json().get("sobjects", [])
        _sf_read_cache_set(key, objs)
        return objs

    def describe_sobject(self, object_api_name: str) -> dict[str, Any]:
        """Return field/object metadata for one sObject."""
        obj = object_api_name.strip()
        key = _sf_cache_key("describe_sobject", f"{SF_REST_API_VERSION}/sobjects/{obj}/describe")
        cached = _sf_read_cache_get(key)
        if cached is not None:
            return cached
        self._ensure_token()
        url = f"{self._instance_url}/services/data/{SF_REST_API_VERSION}/sobjects/{obj}/describe"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        _sf_read_cache_set(key, data)
        return data

    def get_sobject_field_names(self, object_api_name: str) -> frozenset[str]:
        """Field API names visible to this integration user for one sObject."""
        cached = getattr(self, "_sobject_field_names_cache", None)
        if cached is None:
            cached = {}
            self._sobject_field_names_cache = cached
        if object_api_name in cached:
            return cached[object_api_name]
        desc = self.describe_sobject(object_api_name)
        names = frozenset(
            str(f["name"])
            for f in desc.get("fields", [])
            if isinstance(f, dict) and f.get("name")
        )
        cached[object_api_name] = names
        return names

    def get_queryable_sobject_names(self) -> frozenset[str]:
        """API names of sObjects the current user may query (``queryable`` on global describe).

        Cached per client instance for the lifetime of the token/session.
        """
        cached = getattr(self, "_queryable_sobject_names_cache", None)
        if cached is not None:
            return cached
        rows = self.list_sobject_types()
        names = frozenset(
            str(o["name"])
            for o in rows
            if isinstance(o, dict) and o.get("name") and o.get("queryable") is True
        )
        self._queryable_sobject_names_cache = names
        return names

    def query_mainstream_object(
        self,
        object_api_name: str,
        *,
        fields: tuple[str, ...] | None = None,
        where: str | None = None,
        limit: int | None = 2000,
    ) -> list[dict[str, Any]]:
        """SELECT default field sets for a known standard object (see MAINSTREAM_OBJECT_FIELDS).

        ``where`` is the SOQL condition only (no leading ``WHERE``). Use bind-safe literals;
        this does not escape user input.

        Raises ``ValueError`` if ``object_api_name`` is unknown and ``fields`` is omitted.
        """
        if fields is not None:
            cols = fields
        elif object_api_name in MAINSTREAM_OBJECT_FIELDS:
            cols = MAINSTREAM_OBJECT_FIELDS[object_api_name]
        else:
            raise ValueError(
                f"Unknown mainstream object {object_api_name!r}; pass fields=(...) or use one "
                f"of: {', '.join(MAINSTREAM_OBJECT_NAMES)}"
            )
        field_list = ", ".join(cols)
        soql = f"SELECT {field_list} FROM {object_api_name}"
        if where:
            soql += f" WHERE {where}"
        if limit is not None:
            cap = max(1, min(int(limit), 2000))
            soql += f" LIMIT {cap}"
        try:
            return self._query(soql)
        except requests.HTTPError as e:
            if (
                e.response is not None
                and e.response.status_code == 400
                and fields is None
            ):
                fb = MAINSTREAM_OBJECT_FALLBACK_FIELDS.get(object_api_name)
                if fb is not None:
                    return self.query_mainstream_object(
                        object_api_name, fields=fb, where=where, limit=limit
                    )
            raise

    def query_leads(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Lead", where=where, limit=limit)

    def query_accounts(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Account", where=where, limit=limit)

    def query_contacts(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Contact", where=where, limit=limit)

    def query_opportunities(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Opportunity", where=where, limit=limit)

    def query_cases(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Case", where=where, limit=limit)

    def query_tasks(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Task", where=where, limit=limit)

    def query_events(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Event", where=where, limit=limit)

    def query_campaigns(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Campaign", where=where, limit=limit)

    def query_campaign_members(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("CampaignMember", where=where, limit=limit)

    def query_users(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("User", where=where, limit=limit)

    def query_products(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Product2", where=where, limit=limit)

    def query_pricebooks(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Pricebook2", where=where, limit=limit)

    def query_contracts(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Contract", where=where, limit=limit)

    def query_orders(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Order", where=where, limit=limit)

    def query_quotes(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Quote", where=where, limit=limit)

    def query_assets(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("Asset", where=where, limit=limit)

    def query_opportunity_line_items(
        self, *, where: str | None = None, limit: int | None = 2000
    ) -> list[dict[str, Any]]:
        return self.query_mainstream_object("OpportunityLineItem", where=where, limit=limit)

    def get_entity_accounts(self) -> list[dict[str, Any]]:
        """All Account records with Type = 'Customer Entity' (contract, ARR, Parent / Ultimate Parent)."""
        fields = ", ".join(_entity_account_select_field_names())
        soql = f"SELECT {fields} FROM Account WHERE Type = 'Customer Entity'"
        raw = self._query(soql)
        return [_normalize_entity_account_row(r) for r in raw]

    def get_opportunity_creation_this_year(self, account_ids: list[str]) -> int:
        """Count Opportunities (Type in OPP_TYPES, CreatedDate = THIS YEAR) for given Account IDs."""
        if not account_ids:
            return 0
        ids_comma = ", ".join(f"'{aid}'" for aid in account_ids)
        types_comma = ", ".join(f"'{t}'" for t in OPP_TYPES)
        soql = (
            f"SELECT COUNT() FROM Opportunity "
            f"WHERE AccountId IN ({ids_comma}) AND Type IN ({types_comma}) "
            f"AND CALENDAR_YEAR(CreatedDate) = {time.gmtime().tm_year}"
        )
        key = _sf_cache_key("cnt", soql)
        cached = _sf_read_cache_get(key)
        if cached is not None:
            return int(cached)
        self._ensure_token()
        url = f"{self._instance_url}/services/data/{SF_REST_API_VERSION}/query"
        resp = requests.get(
            url, params={"q": soql}, headers={"Authorization": f"Bearer {self._token}"}, timeout=30
        )
        resp.raise_for_status()
        n = resp.json().get("totalSize", 0)
        _sf_read_cache_set(key, n)
        return n

    def get_advanced_pipeline_arr(self, account_ids: list[str], *, open_only: bool = True) -> float:
        """Sum ARR__c for Opportunities in pipeline stages for given Account IDs."""
        if not account_ids:
            return 0.0
        ids_comma = ", ".join(f"'{aid}'" for aid in account_ids)
        types_comma = ", ".join(f"'{t}'" for t in OPP_TYPES)
        stages_comma = ", ".join(f"'{s}'" for s in PIPELINE_STAGES)
        closed = " AND IsClosed = false" if open_only else ""
        soql = (
            f"SELECT SUM(ARR__c) total FROM Opportunity "
            f"WHERE AccountId IN ({ids_comma}) AND Type IN ({types_comma}) "
            f"AND StageName IN ({stages_comma}){closed}"
        )
        raw = self._query(soql)
        if not raw:
            return 0.0
        total = raw[0].get("total")
        return float(total) if total is not None else 0.0

    def get_open_pipeline_opportunities(
        self,
        account_ids: list[str],
        *,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        """Open Opportunities in pipeline stages (for renewal-in-flight on churned entities)."""
        if not account_ids or limit <= 0:
            return []
        ids_comma = ", ".join(f"'{aid}'" for aid in account_ids)
        types_comma = ", ".join(f"'{t}'" for t in OPP_TYPES)
        stages_comma = ", ".join(f"'{s}'" for s in PIPELINE_STAGES)
        soql = (
            f"SELECT Id, Name, StageName, Type, ARR__c, CloseDate, AccountId "
            f"FROM Opportunity WHERE AccountId IN ({ids_comma}) AND Type IN ({types_comma}) "
            f"AND StageName IN ({stages_comma}) AND IsClosed = false "
            f"ORDER BY ARR__c DESC NULLS LAST LIMIT {max(1, min(int(limit), 25))}"
        )
        rows = self._query(soql)
        out: list[dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            out.append(
                {
                    "name": r.get("Name"),
                    "stage": r.get("StageName"),
                    "type": r.get("Type"),
                    "arr": r.get("ARR__c"),
                    "close_date": r.get("CloseDate"),
                    "account_id": r.get("AccountId"),
                }
            )
        return out

    def renewal_in_flight_fields_for_entities(
        self,
        matching: list[dict[str, Any]],
        *,
        all_matched_churned: bool,
    ) -> dict[str, Any]:
        """Signals when entity contracts are churned but parent-account pipeline opps are open."""
        if not all_matched_churned or not matching:
            return {"renewal_in_flight": False, "churn_risk": False}
        entity_ids = [a["Id"] for a in matching if isinstance(a, dict) and a.get("Id")]
        scope = opportunity_account_scope_ids(matching)
        pipe_entity = self.get_advanced_pipeline_arr(entity_ids) if entity_ids else 0.0
        pipe_total = self.get_advanced_pipeline_arr(scope) if scope else 0.0
        opps = self.get_open_pipeline_opportunities(scope, limit=6)
        in_flight = pipe_total > 0 or bool(opps)
        fields: dict[str, Any] = {
            "renewal_in_flight": in_flight,
            "pipeline_arr_entity_accounts": round(pipe_entity, 2),
            "pipeline_arr_including_parent_accounts": round(pipe_total, 2),
        }
        if in_flight:
            fields["renewal_in_flight_note"] = (
                "Customer Entity contract status is churned/expired, but open Opportunities exist "
                "on parent account(s) in pipeline stages (often renewal negotiation)."
            )
            fields["open_pipeline_opportunities_sample"] = opps
            fields["churn_risk"] = False
        else:
            fields["churn_risk"] = True
        return fields

    _CHURNED_STATUSES = frozenset({"churned", "cancelled", "terminated", "expired", "closed"})

    def get_arr_by_customer_names(self, customer_names: list[str]) -> dict[str, float]:
        """Return ``{customer_name: ARR}`` for all matching Entity accounts in one query.

        Names are matched case-insensitively against Account ``Name``,
        ``LeanDNA_Entity_Name__c``, Parent Account name, and (when configured)
        Ultimate Parent Account name. When multiple Account rows match the same
        customer, ARR values are summed.
        """
        if not customer_names:
            return {}
        accounts = self.get_entity_accounts()
        lookup: dict[str, float] = {}
        for name in customer_names:
            upper = (name or "").strip().upper()
            if not upper:
                continue
            total_arr = 0.0
            for a in accounts:
                if not _customer_name_matches_entity_account(upper, a):
                    continue
                try:
                    total_arr += float(a.get("ARR__c") or 0)
                except (TypeError, ValueError):
                    continue
            if total_arr:
                lookup[name] = total_arr
        return lookup

    def _portfolio_closed_won_opportunity_rows_cy(
        self, account_ids: list[str], calendar_year: int
    ) -> list[dict[str, Any]]:
        """Return Opportunity rows (AccountId, Type, Amount) Won in *calendar_year*.

        Uses ``IsWon`` and ``CloseDate`` (standard Salesforce). Types include expansion
        motion (see ``_EXPANSION_CLOSED_WON_TYPES`` / ``_NEW_LOGO_CLOSED_WON_TYPES``).
        Chunked IN lists to stay under REST query length limits.
        """
        if not account_ids:
            return []
        motion_types = tuple(sorted(_EXPANSION_CLOSED_WON_TYPES | _NEW_LOGO_CLOSED_WON_TYPES))
        types_in = ", ".join(f"'{t}'" for t in motion_types)
        out: list[dict[str, Any]] = []
        chunk_size = 60
        for i in range(0, len(account_ids), chunk_size):
            chunk = account_ids[i : i + chunk_size]
            ids_in = ", ".join(f"'{aid}'" for aid in chunk)
            soql = (
                f"SELECT AccountId, Type, Amount FROM Opportunity "
                f"WHERE AccountId IN ({ids_in}) "
                f"AND IsWon = true "
                f"AND CALENDAR_YEAR(CloseDate) = {int(calendar_year)} "
                f"AND Type IN ({types_in})"
            )
            out.extend(self._query(soql))
        return out

    @staticmethod
    def _expansion_kpis_from_opportunities(
        *,
        per_name: dict[str, list[dict[str, Any]]],
        names_clean: list[str],
        closed_won_rows: list[dict[str, Any]],
        calendar_year: int,
    ) -> dict[str, Any]:
        """Derive portfolio expansion / new-logo KPIs from closed-won opps + entity rollups."""
        expansion_accounts: set[str] = set()
        new_biz_accounts: set[str] = set()
        expansion_amount = 0.0
        expansion_opp_count = 0
        for r in closed_won_rows:
            if not isinstance(r, dict):
                continue
            aid = r.get("AccountId")
            if not isinstance(aid, str) or len(aid.strip()) < 15:
                continue
            typ = str(r.get("Type") or "")
            try:
                amt = float(r.get("Amount") or 0)
            except (TypeError, ValueError):
                amt = 0.0
            if typ in _EXPANSION_CLOSED_WON_TYPES:
                expansion_accounts.add(aid)
                expansion_amount += amt
                expansion_opp_count += 1
            elif typ in _NEW_LOGO_CLOSED_WON_TYPES:
                new_biz_accounts.add(aid)

        active_labels: list[str] = []
        for name in names_clean:
            matching = per_name.get(name) or []
            if not matching:
                continue
            has_active = False
            for a in matching:
                st = (a.get("Contract_Status__c") or "").strip().lower()
                if st not in SalesforceClient._CHURNED_STATUSES:
                    has_active = True
                    break
            if has_active:
                active_labels.append(name)

        def _label_account_ids(label: str) -> set[str]:
            return {
                str(a.get("Id")).strip()
                for a in (per_name.get(label) or [])
                if isinstance(a.get("Id"), str) and len(str(a.get("Id")).strip()) >= 15
            }

        expanding_labels: list[str] = []
        for label in active_labels:
            if _label_account_ids(label) & expansion_accounts:
                expanding_labels.append(label)

        new_logo_labels: list[str] = []
        for label in active_labels:
            if _label_account_ids(label) & new_biz_accounts:
                new_logo_labels.append(label)

        denom = len(active_labels)
        numer = len(expanding_labels)
        pct = round(100.0 * numer / denom, 1) if denom else 0.0

        return {
            "configured": True,
            "empty": False,
            "calendar_year": int(calendar_year),
            "eligible_active_customer_count": denom,
            "active_customers_with_expansion_wins_cy": numer,
            "pct_active_customers_expanding_cy": pct,
            "closed_won_expansion_deal_count_cy": expansion_opp_count,
            "closed_won_expansion_amount_sum_cy": round(expansion_amount, 2),
            "distinct_accounts_expansion_win_cy": len(expansion_accounts),
            "active_customers_with_new_business_won_cy": len(new_logo_labels),
            "distinct_accounts_new_business_win_cy": len(new_biz_accounts),
            "expanding_customer_labels_sample": sorted(expanding_labels)[:12],
        }

    def get_active_customer_names(self, customer_names: list[str]) -> set[str]:
        """Return the subset of *customer_names* whose Contract_Status__c is NOT churned.

        A customer is considered **active** when at least one matched Entity
        account has a ``Contract_Status__c`` that is *not* in
        ``_CHURNED_STATUSES`` (case-insensitive).  Customers with no Salesforce
        match are assumed active (we lack data to exclude them).
        """
        if not customer_names:
            return set()
        accounts = self.get_entity_accounts()
        active: set[str] = set()
        for name in customer_names:
            upper = (name or "").strip().upper()
            if not upper:
                continue
            matched_any = False
            has_active_contract = False
            for a in accounts:
                if not _customer_name_matches_entity_account(upper, a):
                    continue
                matched_any = True
                status = (a.get("Contract_Status__c") or "").strip().lower()
                if status not in self._CHURNED_STATUSES:
                    has_active_contract = True
                    break
            if has_active_contract or not matched_any:
                active.add(name)
        return active

    def get_portfolio_revenue_book_metrics(self, customer_names: list[str]) -> dict[str, Any]:
        """Aggregate ARR, contract status, pipeline, and opps across portfolio customer labels.

        Loads entity accounts once; name matching matches :meth:`get_arr_by_customer_names`.
        Account Ids are deduplicated before pipeline / opportunity SOQL.
        """
        names_clean = [(n or "").strip() for n in customer_names if (n or "").strip()]
        empty_out: dict[str, Any] = {
            "configured": True,
            "empty": True,
            "pendo_customers": 0,
            "salesforce_matched_customers": 0,
            "salesforce_unmatched_customers": 0,
            "total_arr": 0.0,
            "active_installed_base_arr": 0.0,
            "churned_contract_arr": 0.0,
            "pipeline_arr": 0.0,
            "opportunity_count_this_year": 0,
            "active_customer_count": 0,
            "churned_customer_count": 0,
            "top_customers_by_arr": [],
            "matched_customer_contract_rollups": [],
            "churned_customer_names_sample": [],
            "expansion_kpis": {
                "configured": True,
                "empty": True,
                "calendar_year": time.gmtime().tm_year,
            },
        }
        if not names_clean:
            return empty_out

        accounts = self.get_entity_accounts()
        account_by_id = {
            a["Id"]: a for a in accounts if isinstance(a, dict) and a.get("Id")
        }
        from .portfolio_salesforce_allowlist import matching_entity_accounts_for_customer_label

        per_name: dict[str, list[dict[str, Any]]] = {
            n: matching_entity_accounts_for_customer_label(n, accounts) for n in names_clean
        }

        matched_names = [n for n in names_clean if per_name[n]]
        unmatched = len(names_clean) - len(matched_names)
        seen_ids: set[str] = set()
        dedup_ids: list[str] = []
        for n in matched_names:
            for a in per_name[n]:
                aid = a.get("Id")
                if isinstance(aid, str) and len(aid.strip()) >= 15 and aid not in seen_ids:
                    seen_ids.add(aid)
                    dedup_ids.append(aid)

        top_rows: list[dict[str, Any]] = []
        total_arr = 0.0
        active_arr = 0.0
        churned_arr = 0.0
        churned_names: list[str] = []
        active_cust = 0
        churned_cust = 0
        for name in names_clean:
            matching = per_name[name]
            if not matching:
                continue
            arr_sum = 0.0
            for a in matching:
                try:
                    arr_sum += float(a.get("ARR__c") or 0)
                except (TypeError, ValueError):
                    pass
            has_active_contract = False
            for a in matching:
                status = (a.get("Contract_Status__c") or "").strip().lower()
                if status not in self._CHURNED_STATUSES:
                    has_active_contract = True
                    break
            all_matched_churned = not has_active_contract
            row = {
                "customer": name,
                "arr": round(arr_sum, 2),
                "active": not all_matched_churned,
            }
            row.update(_renewal_roll_up_fields(matching))
            row.update(
                self.renewal_in_flight_fields_for_entities(
                    matching, all_matched_churned=all_matched_churned
                )
            )
            top_rows.append(row)
            total_arr += arr_sum
            if all_matched_churned:
                churned_arr += arr_sum
                churned_cust += 1
                if len(churned_names) < 12:
                    churned_names.append(name)
            else:
                active_arr += arr_sum
                active_cust += 1

        top_rows.sort(key=lambda r: (-(float(r.get("arr") or 0)), str(r.get("customer") or "")))
        top10 = top_rows[:10]
        matched_customer_contract_rollups = list(top_rows)
        dedup_scope = (
            opportunity_account_scope_ids_from_entity_ids(dedup_ids, account_by_id)
            if dedup_ids
            else []
        )
        pipeline = self.get_advanced_pipeline_arr(dedup_scope) if dedup_scope else 0.0
        opps = self.get_opportunity_creation_this_year(dedup_scope) if dedup_scope else 0

        calendar_year = time.gmtime().tm_year
        expansion_kpis: dict[str, Any]
        if not dedup_ids:
            expansion_kpis = {"configured": True, "empty": True, "calendar_year": calendar_year}
        else:
            try:
                won_rows = self._portfolio_closed_won_opportunity_rows_cy(dedup_scope, calendar_year)
                expansion_kpis = self._expansion_kpis_from_opportunities(
                    per_name=per_name,
                    names_clean=names_clean,
                    closed_won_rows=won_rows,
                    calendar_year=calendar_year,
                )
            except Exception as e:
                logger.warning("Salesforce: portfolio expansion KPI rollup failed: %s", e)
                expansion_kpis = {
                    "configured": True,
                    "calendar_year": calendar_year,
                    "error": str(e)[:420],
                }

        return {
            "configured": True,
            "empty": False,
            "pendo_customers": len(names_clean),
            "salesforce_matched_customers": len(matched_names),
            "salesforce_unmatched_customers": unmatched,
            "total_arr": round(total_arr, 2),
            "active_installed_base_arr": round(active_arr, 2),
            "churned_contract_arr": round(churned_arr, 2),
            "pipeline_arr": round(pipeline, 2),
            "opportunity_count_this_year": int(opps),
            "active_customer_count": active_cust,
            "churned_customer_count": churned_cust,
            "top_customers_by_arr": top10,
            "matched_customer_contract_rollups": matched_customer_contract_rollups,
            "churned_customer_names_sample": churned_names,
            "expansion_kpis": expansion_kpis,
        }

    def _get_customer_salesforce_by_account_ids(
        self, customer_name: str, account_ids: list[str]
    ) -> dict[str, Any]:
        """Load Customer Entity accounts by explicit Ids (no name scan)."""
        seen: list[str] = []
        for x in account_ids:
            s = (x or "").strip()
            if len(s) in (15, 18) and s not in seen:
                seen.append(s)
        if not seen:
            return {
                "customer": customer_name,
                "accounts": [],
                "account_ids": [],
                "opportunity_count_this_year": 0,
                "pipeline_arr": 0.0,
                "matched": False,
            }
        fields = ", ".join(_entity_account_select_field_names())
        matching: list[dict[str, Any]] = []
        for chunk in _chunk_list(seen, 50):
            ids_in = ", ".join(f"'{x}'" for x in chunk)
            soql = (
                f"SELECT {fields} FROM Account "
                f"WHERE Id IN ({ids_in}) AND Type = 'Customer Entity'"
            )
            try:
                raw = self._query(soql)
            except Exception as e:
                logger.warning("Salesforce account Id lookup failed: %s", e)
                return {
                    "customer": customer_name,
                    "accounts": [],
                    "account_ids": [],
                    "opportunity_count_this_year": 0,
                    "pipeline_arr": 0.0,
                    "matched": False,
                }
            for r in raw:
                matching.append(_normalize_entity_account_row(r))
        if not matching:
            return {
                "customer": customer_name,
                "accounts": [],
                "account_ids": [],
                "opportunity_count_this_year": 0,
                "pipeline_arr": 0.0,
                "matched": False,
            }
        out_ids = [a["Id"] for a in matching if a.get("Id")]
        return {
            "customer": customer_name,
            "accounts": matching,
            "account_ids": out_ids,
            "opportunity_count_this_year": self.get_opportunity_creation_this_year(out_ids),
            "pipeline_arr": self.get_advanced_pipeline_arr(out_ids),
            "matched": True,
        }

    def get_customer_salesforce(
        self,
        customer_name: str,
        *,
        preferred_account_ids: list[str] | None = None,
        primary_account_id: str | None = None,
    ) -> dict[str, Any]:
        """Contract info, opportunity count (this year), and pipeline ARR for a customer.

        When ``preferred_account_ids`` is set (e.g. from ``customer_identity_map.yaml``), resolves
        those Customer Entity accounts **by Id** first. Otherwise matches Entity Account by
        ``Name``, ``LeanDNA_Entity_Name__c``, Parent name, or Ultimate Parent name (case-insensitive
        substring; Ultimate Parent requires ``SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP``).

        Response includes ``resolution`` — ``salesforce_account_id``, ``name``, or ``none`` — and
        ``primary_account_id`` when it can be determined.
        """
        pref = [x for x in (preferred_account_ids or []) if (x or "").strip()]
        if pref:
            by_id = self._get_customer_salesforce_by_account_ids(customer_name, pref)
            if by_id.get("matched"):
                ids_list = by_id.get("account_ids") or []
                prim = (primary_account_id or "").strip()
                if prim and prim in ids_list:
                    by_id["primary_account_id"] = prim
                elif len(ids_list) == 1:
                    by_id["primary_account_id"] = ids_list[0]
                else:
                    by_id["primary_account_id"] = None
                by_id["resolution"] = "salesforce_account_id"
                return by_id
            logger.info(
                "Salesforce: mapped Account Id(s) did not resolve Customer Entity rows; "
                "falling back to name match for %r",
                customer_name,
            )

        accounts = self.get_entity_accounts()
        name_upper = (customer_name or "").strip().upper()
        matching = [a for a in accounts if _customer_name_matches_entity_account(name_upper, a)]
        if not matching:
            return {
                "customer": customer_name,
                "accounts": [],
                "account_ids": [],
                "opportunity_count_this_year": 0,
                "pipeline_arr": 0.0,
                "matched": False,
                "resolution": "none",
                "primary_account_id": None,
            }
        account_ids = [a["Id"] for a in matching if a.get("Id")]
        prim2 = (primary_account_id or "").strip()
        primary_out = (
            prim2 if prim2 and prim2 in account_ids else (account_ids[0] if len(account_ids) == 1 else None)
        )
        return {
            "customer": customer_name,
            "accounts": matching,
            "account_ids": account_ids,
            "opportunity_count_this_year": self.get_opportunity_creation_this_year(account_ids),
            "pipeline_arr": self.get_advanced_pipeline_arr(account_ids),
            "matched": True,
            "resolution": "name",
            "primary_account_id": primary_out,
        }

    def expand_descendant_account_ids(
        self,
        seed_ids: list[str],
        *,
        max_depth: int = 25,
        max_total_accounts: int = 2000,
        chunk_size: int = 100,
    ) -> list[str]:
        """Return ``seed_ids`` plus every Account Id reachable via ``ParentId`` (children), breadth-first.

        Does not follow partner or other relationships—only the standard Account hierarchy. Stops at
        ``max_depth`` or ``max_total_accounts`` to bound API cost and SOQL ``IN`` size.
        """
        seeds = [x for x in seed_ids if x]
        if not seeds:
            return []
        ordered: list[str] = []
        seen: set[str] = set()
        for s in seeds:
            if s not in seen:
                seen.add(s)
                ordered.append(s)
        frontier = list(seeds)
        depth = 0
        while frontier and depth < max_depth and len(seen) < max_total_accounts:
            next_frontier: list[str] = []
            for chunk in _chunk_list(frontier, chunk_size):
                ids_in = ", ".join(f"'{x}'" for x in chunk)
                soql = f"SELECT Id FROM Account WHERE ParentId IN ({ids_in})"
                rows = self._query(soql)
                for r in rows:
                    cid = r.get("Id")
                    if not cid or cid in seen:
                        continue
                    if len(seen) >= max_total_accounts:
                        break
                    seen.add(cid)
                    ordered.append(cid)
                    next_frontier.append(cid)
                if len(seen) >= max_total_accounts:
                    break
            frontier = next_frontier
            depth += 1
        return ordered

    def get_customer_salesforce_comprehensive(
        self,
        customer_name: str,
        *,
        row_limit: int = 75,
        preferred_account_ids: list[str] | None = None,
        primary_account_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a wide slice of mainstream Salesforce objects scoped to matched Customer Entity accounts.

        Reuses ``get_customer_salesforce`` matching — optional ``preferred_account_ids`` / ``primary_account_id``
        (same as Id-first resolution). Otherwise Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent.
        Child accounts in the standard hierarchy (``ParentId``) are included via ``expand_descendant_account_ids``;
        all SOQL filters use that expanded Id set. Each object query is isolated: failures are recorded in
        ``category_errors`` without failing the whole call.
        ``products_org_sample`` and ``pricebooks_org_sample`` are org-wide samples (not account-filtered).
        """
        base = self.get_customer_salesforce(
            customer_name,
            preferred_account_ids=preferred_account_ids,
            primary_account_id=primary_account_id,
        )
        out: dict[str, Any] = {
            **base,
            "row_limit": max(1, min(int(row_limit), 500)),
            "categories": {},
            "category_errors": {},
        }
        account_ids = base.get("account_ids") or []
        if not base.get("matched") or not account_ids:
            return out

        try:
            expanded = self.expand_descendant_account_ids(account_ids)
        except Exception as e:
            logger.warning("Salesforce account hierarchy expansion failed: %s", e)
            expanded = list(account_ids)
            out["category_errors"]["account_hierarchy"] = str(e)[:500]
        out["account_ids_expanded"] = expanded
        opp_scope = opportunity_account_scope_ids(matching)
        out["opportunity_account_scope_ids"] = opp_scope
        out["opportunity_count_this_year"] = self.get_opportunity_creation_this_year(opp_scope)
        out["pipeline_arr"] = self.get_advanced_pipeline_arr(opp_scope)
        has_active_entity = False
        for a in matching:
            st = (a.get("Contract_Status__c") or "").strip().lower()
            if st and st not in self._CHURNED_STATUSES:
                has_active_entity = True
                break
        out.update(
            self.renewal_in_flight_fields_for_entities(
                matching,
                all_matched_churned=bool(matching) and not has_active_entity,
            )
        )

        ids_in = ", ".join(f"'{aid}'" for aid in expanded)
        cap = out["row_limit"]

        try:
            queryable = self.get_queryable_sobject_names()
        except Exception as e:
            logger.warning(
                "Salesforce global sObject describe failed; comprehensive queries will not be pre-skipped: %s",
                e,
            )
            queryable = None

        def _run(label: str, fetcher, *, sobject: str | None = None):
            if queryable is not None and sobject is not None and sobject not in queryable:
                out["categories"][label] = []
                out["category_errors"][label] = (
                    f"SObject {sobject!r} is not API-queryable for this integration user. "
                    "In Salesforce: open the Connected App user's Profile or Permission Set and enable "
                    "object access (at least Read) for SOQL, or use a user with broader API rights."
                )[:500]
                logger.info(
                    "Salesforce comprehensive skip %s: %s not in queryable sObject set for this user",
                    label,
                    sobject,
                )
                return
            try:
                raw = fetcher()
                out["categories"][label] = [_strip_sf_attributes(r) for r in raw]
            except Exception as e:
                out["category_errors"][label] = str(e)[:500]
                logger.warning("Salesforce comprehensive %s failed: %s", label, e)
                out["categories"][label] = []

        def _account_activity_where(sobject: str) -> str | None:
            """Best available relationship filter for Task/Event in orgs with restricted fields."""
            try:
                fields = self.get_sobject_field_names(sobject)
            except Exception as e:
                logger.debug(
                    "Salesforce %s describe failed; using WhatId relationship filter: %s",
                    sobject,
                    e,
                )
                return f"WhatId IN ({ids_in})"
            if "WhatId" in fields:
                return f"WhatId IN ({ids_in})"
            if "AccountId" in fields:
                return f"AccountId IN ({ids_in})"
            out["category_errors"][sobject.lower() + "s"] = (
                f"SObject {sobject!r} is queryable, but neither WhatId nor AccountId is visible "
                "to this integration user for account-scoped activity export."
            )[:500]
            out["categories"][sobject.lower() + "s"] = []
            logger.info(
                "Salesforce comprehensive skip %ss: no visible WhatId/AccountId field for account-scoped query",
                sobject.lower(),
            )
            return None

        _run(
            "contacts",
            lambda: self.query_contacts(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Contact",
        )
        _run(
            "opportunities",
            lambda: self.query_opportunities(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Opportunity",
        )
        _run(
            "opportunity_line_items",
            lambda: self.query_opportunity_line_items(
                where=f"OpportunityId IN (SELECT Id FROM Opportunity WHERE AccountId IN ({ids_in}))",
                limit=cap,
            ),
            sobject="OpportunityLineItem",
        )
        _run(
            "cases",
            lambda: self.query_cases(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Case",
        )
        task_where = _account_activity_where("Task")
        if task_where is not None:
            _run(
                "tasks",
                lambda: self.query_tasks(where=task_where, limit=cap),
                sobject="Task",
            )
        event_where = _account_activity_where("Event")
        if event_where is not None:
            _run(
                "events",
                lambda: self.query_events(where=event_where, limit=cap),
                sobject="Event",
            )
        _run(
            "contracts",
            lambda: self.query_contracts(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Contract",
        )
        _run(
            "orders",
            lambda: self.query_orders(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Order",
        )
        _run(
            "quotes",
            lambda: self.query_quotes(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Quote",
        )
        _run(
            "assets",
            lambda: self.query_assets(where=f"AccountId IN ({ids_in})", limit=cap),
            sobject="Asset",
        )
        _run(
            "owners_sample",
            lambda: self.query_users(
                where=f"Id IN (SELECT OwnerId FROM Account WHERE Id IN ({ids_in}))",
                limit=min(40, cap),
            ),
            sobject="User",
        )

        contacts = out["categories"].get("contacts") or []
        cids = [c["Id"] for c in contacts if c.get("Id")]
        if cids:
            # Long ContactId IN lists make the REST query string huge; chunk to avoid 400/URI limits.
            contact_in_chunk = 25

            def fetch_campaign_members_chunked() -> list[dict[str, Any]]:
                seen: set[str] = set()
                acc: list[dict[str, Any]] = []
                for ch in _chunk_list(cids, contact_in_chunk):
                    c_in = ", ".join(f"'{x}'" for x in ch)
                    batch = self.query_campaign_members(
                        where=f"ContactId IN ({c_in})", limit=cap
                    )
                    for r in batch:
                        rid = r.get("Id")
                        if rid:
                            if rid in seen:
                                continue
                            seen.add(rid)
                        acc.append(r)
                        if len(acc) >= cap:
                            return acc
                return acc

            def fetch_campaigns_chunked() -> list[dict[str, Any]]:
                seen_camp: set[str] = set()
                acc: list[dict[str, Any]] = []
                for ch in _chunk_list(cids, contact_in_chunk):
                    c_in = ", ".join(f"'{x}'" for x in ch)
                    batch = self.query_campaigns(
                        where=(
                            "Id IN (SELECT CampaignId FROM CampaignMember "
                            f"WHERE ContactId IN ({c_in}))"
                        ),
                        limit=cap,
                    )
                    for r in batch:
                        rid = r.get("Id")
                        if not rid or rid in seen_camp:
                            continue
                        seen_camp.add(rid)
                        acc.append(r)
                        if len(acc) >= cap:
                            return acc
                return acc

            _run("campaign_members", fetch_campaign_members_chunked, sobject="CampaignMember")
            _run("campaigns_related", fetch_campaigns_chunked, sobject="Campaign")
        else:
            out["categories"]["campaign_members"] = []
            out["categories"]["campaigns_related"] = []

        frag = _soql_like_literal((customer_name or "").strip()[:120])
        if frag:
            _run(
                "leads_name_match",
                lambda: self.query_leads(
                    where=f"(Company LIKE '%{frag}%' OR LastName LIKE '%{frag}%')",
                    limit=min(40, cap),
                ),
                sobject="Lead",
            )
        else:
            out["categories"]["leads_name_match"] = []

        _run(
            "products_org_sample",
            lambda: self.query_products(where="IsActive = true", limit=min(40, cap)),
            sobject="Product2",
        )
        _run(
            "pricebooks_org_sample",
            lambda: self.query_pricebooks(limit=min(25, cap)),
            sobject="Pricebook2",
        )

        return out
