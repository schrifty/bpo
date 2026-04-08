"""Salesforce API client using JWT Bearer Flow (OAuth 2.0).

Authenticates with a Connected App via JWT signed by a private key.
Queries Account (Entity Contract) and Opportunity (creation count, pipeline ARR).
Also exposes SOQL helpers for common standard objects (see MAINSTREAM_OBJECT_FIELDS)
and ``get_customer_salesforce_comprehensive`` for deck-sized multi-object exports.

Read responses (SOQL record lists, global sObject describe, COUNT totals) are cached
in-process for ``BPO_SALESFORCE_CACHE_TTL_HOURS`` (default 48). JWT tokens are not cached here.
"""

import copy
import hashlib
import threading
import time
from pathlib import Path
from typing import Any

import requests

from .config import (
    BPO_SALESFORCE_CACHE_FORCE_REFRESH,
    BPO_SALESFORCE_CACHE_TTL_SECONDS,
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


def _sf_cache_key(kind: str, payload: str) -> str:
    return f"sf:{kind}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def _sf_read_cache_get(key: str) -> Any | None:
    if BPO_SALESFORCE_CACHE_FORCE_REFRESH or BPO_SALESFORCE_CACHE_TTL_SECONDS <= 0:
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
    if BPO_SALESFORCE_CACHE_FORCE_REFRESH or BPO_SALESFORCE_CACHE_TTL_SECONDS <= 0:
        return
    with _SF_READ_CACHE_LOCK:
        _sf_read_cache[key] = (time.time(), copy.deepcopy(val))

# Account (Entity Contract): Type = 'Customer Entity'
ACCOUNT_FIELDS = (
    "Id", "Name", "LeanDNA_Entity_Name__c", "US_Persons_Only_Customer__c",
    "Contract_Status__c", "Contract_Contract_Start_Date__c", "Contract_Contract_End_Date__c", "ARR__c",
)
# Opportunity types for creation and pipeline
OPP_TYPES = ("New Business", "New Expansion Business", "Expansion Business", "POC")
PIPELINE_STAGES = ("3-Business Validation", "4-Proposal", "5-Contracts")

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
        "Id", "Subject", "Status", "ActivityDate", "WhatId", "OwnerId", "CreatedDate",
    ),
    "Event": (
        "Id", "Subject", "StartDateTime", "EndDateTime", "WhatId", "OwnerId", "CreatedDate",
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
        """All Account records with Type = 'Customer Entity' (contract info)."""
        fields = ", ".join(ACCOUNT_FIELDS)
        soql = f"SELECT {fields} FROM Account WHERE Type = 'Customer Entity'"
        raw = self._query(soql)
        return [
            {
                "Id": r.get("Id"),
                "Name": r.get("Name"),
                "LeanDNA_Entity_Name__c": r.get("LeanDNA_Entity_Name__c"),
                "US_Persons_Only_Customer__c": r.get("US_Persons_Only_Customer__c"),
                "Contract_Status__c": r.get("Contract_Status__c"),
                "Contract_Contract_Start_Date__c": r.get("Contract_Contract_Start_Date__c"),
                "Contract_Contract_End_Date__c": r.get("Contract_Contract_End_Date__c"),
                "ARR__c": r.get("ARR__c"),
            }
            for r in raw
        ]

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

    def get_advanced_pipeline_arr(self, account_ids: list[str]) -> float:
        """Sum ARR__c for Opportunities in pipeline stages for given Account IDs."""
        if not account_ids:
            return 0.0
        ids_comma = ", ".join(f"'{aid}'" for aid in account_ids)
        types_comma = ", ".join(f"'{t}'" for t in OPP_TYPES)
        stages_comma = ", ".join(f"'{s}'" for s in PIPELINE_STAGES)
        soql = (
            f"SELECT SUM(ARR__c) total FROM Opportunity "
            f"WHERE AccountId IN ({ids_comma}) AND Type IN ({types_comma}) "
            f"AND StageName IN ({stages_comma})"
        )
        raw = self._query(soql)
        if not raw:
            return 0.0
        total = raw[0].get("total")
        return float(total) if total is not None else 0.0

    _CHURNED_STATUSES = frozenset({"churned", "cancelled", "terminated", "expired", "closed"})

    def get_arr_by_customer_names(self, customer_names: list[str]) -> dict[str, float]:
        """Return ``{customer_name: ARR}`` for all matching Entity accounts in one query.

        Names are matched case-insensitively against Account.Name and
        Account.LeanDNA_Entity_Name__c.  When multiple Account rows match
        the same customer, ARR values are summed.
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
                a_name = (a.get("Name") or "").upper()
                a_entity = (a.get("LeanDNA_Entity_Name__c") or "").upper()
                if upper in a_name or upper in a_entity:
                    total_arr += float(a.get("ARR__c") or 0)
            if total_arr:
                lookup[name] = total_arr
        return lookup

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
                a_name = (a.get("Name") or "").upper()
                a_entity = (a.get("LeanDNA_Entity_Name__c") or "").upper()
                if upper in a_name or upper in a_entity:
                    matched_any = True
                    status = (a.get("Contract_Status__c") or "").strip().lower()
                    if status not in self._CHURNED_STATUSES:
                        has_active_contract = True
                        break
            if has_active_contract or not matched_any:
                active.add(name)
        return active

    def get_customer_salesforce(self, customer_name: str) -> dict[str, Any]:
        """Contract info, opportunity count (this year), and pipeline ARR for a customer.

        Matches Account by Name or LeanDNA_Entity_Name__c (case-insensitive contains).
        """
        accounts = self.get_entity_accounts()
        name_upper = (customer_name or "").strip().upper()
        matching = [
            a for a in accounts
            if name_upper in (a.get("Name") or "").upper()
            or name_upper in (a.get("LeanDNA_Entity_Name__c") or "").upper()
        ]
        if not matching:
            return {
                "customer": customer_name,
                "accounts": [],
                "account_ids": [],
                "opportunity_count_this_year": 0,
                "pipeline_arr": 0.0,
                "matched": False,
            }
        account_ids = [a["Id"] for a in matching if a.get("Id")]
        return {
            "customer": customer_name,
            "accounts": matching,
            "account_ids": account_ids,
            "opportunity_count_this_year": self.get_opportunity_creation_this_year(account_ids),
            "pipeline_arr": self.get_advanced_pipeline_arr(account_ids),
            "matched": True,
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
    ) -> dict[str, Any]:
        """Fetch a wide slice of mainstream Salesforce objects scoped to matched Customer Entity accounts.

        Reuses ``get_customer_salesforce`` matching (Name / LeanDNA_Entity_Name__c). Child accounts in the
        standard hierarchy (``ParentId``) are included via ``expand_descendant_account_ids``; all SOQL
        filters use that expanded Id set. Each object query is isolated: failures are recorded in
        ``category_errors`` without failing the whole call.
        ``products_org_sample`` and ``pricebooks_org_sample`` are org-wide samples (not account-filtered).
        """
        base = self.get_customer_salesforce(customer_name)
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
        out["opportunity_count_this_year"] = self.get_opportunity_creation_this_year(expanded)
        out["pipeline_arr"] = self.get_advanced_pipeline_arr(expanded)

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
        _run(
            "tasks",
            lambda: self.query_tasks(where=f"WhatId IN ({ids_in})", limit=cap),
            sobject="Task",
        )
        _run(
            "events",
            lambda: self.query_events(where=f"WhatId IN ({ids_in})", limit=cap),
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
