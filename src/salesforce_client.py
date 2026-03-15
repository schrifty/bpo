"""Salesforce API client using JWT Bearer Flow (OAuth 2.0).

Authenticates with a Connected App via JWT signed by a private key.
Queries Account (Entity Contract) and Opportunity (creation count, pipeline ARR).
"""

import time
from pathlib import Path
from typing import Any

import requests

from .config import (
    SF_LOGIN_URL,
    SF_CONSUMER_KEY,
    SF_USERNAME,
    SF_PRIVATE_KEY,
    SF_PRIVATE_KEY_PATH,
    logger,
)

# Account (Entity Contract): Type = 'Customer Entity'
ACCOUNT_FIELDS = (
    "Id", "Name", "LeanDNA_Entity_Name__c", "US_Persons_Only_Customer__c",
    "Contract_Status__c", "Contract_Contract_Start_Date__c", "Contract_Contract_End_Date__c", "ARR__c",
)
# Opportunity types for creation and pipeline
OPP_TYPES = ("New Business", "New Expansion Business", "Expansion Business", "POC")
PIPELINE_STAGES = ("3-Business Validation", "4-Proposal", "5-Contracts")


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

    def _query(self, soql: str) -> list[dict]:
        """Run SOQL query and return list of records."""
        self._ensure_token()
        url = f"{self._instance_url}/services/data/v59.0/query"
        params = {"q": soql}
        headers = {"Authorization": f"Bearer {self._token}"}
        out: list[dict] = []
        req_url: str | None = url
        req_params: dict | None = params
        while req_url:
            resp = requests.get(req_url, params=req_params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            out.extend(data.get("records", []))
            next_path = data.get("nextRecordsUrl")
            if next_path:
                req_url = f"{self._instance_url}{next_path}"
                req_params = None
            else:
                req_url = None
        return out

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
        self._ensure_token()
        url = f"{self._instance_url}/services/data/v59.0/query"
        resp = requests.get(
            url, params={"q": soql}, headers={"Authorization": f"Bearer {self._token}"}, timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("totalSize", 0)

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
