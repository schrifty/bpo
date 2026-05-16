"""LangChain tools for LeanDNA Connect **Data** API (``/data/...``).

- **Read:** ``data_api_get_json`` — any validated ``GET`` path.
- **Write:** ``data_api_mutate_json`` — ``POST``, ``PUT``, or ``DELETE`` on validated paths
  (Lean Project CRUD-ish operations, metric data points, write-back transitions).

OpenAPI UI (requires login): https://app.leandna.com/application/apidocs/dist/index.html?urls.primaryName=Data#/

Auth: ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` — see
``docs/SETUP/LEANDNA_SETUP.md``. This module does **not** call the separate Auth API;
supply a valid session token or session cookie per LeanDNA docs.

Mutations change tenant data — use only with appropriate approvals and correct ``RequestedSites``.
"""

from __future__ import annotations

import functools
import json
from typing import Any

from langchain_core.tools import BaseTool
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout

from ..config import logger

# Curated catalog (resource paths relative to ``/data/``). Query params vary by tenant — see OpenAPI.
_DATA_API_GET_CATALOG: tuple[dict[str, str], ...] = (
    {"path": "identity", "group": "Session", "notes": "User + authorizedSites[]"},
    {"path": "ItemMasterData", "group": "Inventory", "notes": "Item-level DOI, risk, CTB, lead time"},
    {"path": "Inventory/Purchased", "group": "Inventory", "notes": "On-hand by location"},
    {"path": "Metric", "group": "Metrics", "notes": "Metric definitions catalog"},
    {"path": "MetricReport", "group": "Metrics", "notes": "Fiscal report — use query e.g. fiscalYear"},
    {"path": "MaterialShortages/ShortagesByItem/Weekly", "group": "Shortages", "notes": "Weekly buckets"},
    {"path": "MaterialShortages/ShortagesByItem/Daily", "group": "Shortages", "notes": "Daily buckets"},
    {"path": "MaterialShortages/ShortagesByItem/Monthly", "group": "Shortages", "notes": "Monthly buckets"},
    {"path": "MaterialShortages/ShortagesByOrder", "group": "Shortages", "notes": "By production order"},
    {
        "path": "MaterialShortages/ShortagesByItemWithScheduledDeliveries/Weekly",
        "group": "Shortages",
        "notes": "Weekly + scheduled deliveries",
    },
    {
        "path": "MaterialShortages/ShortagesByItemWithScheduledDeliveries/Daily",
        "group": "Shortages",
        "notes": "Daily + scheduled deliveries",
    },
    {
        "path": "MaterialShortages/ShortagesByItemWithScheduledDeliveries/Monthly",
        "group": "Shortages",
        "notes": "Monthly + scheduled deliveries",
    },
    {"path": "LeanProject", "group": "Lean projects", "notes": "List — add dateFrom/dateTo query params per swagger"},
    {
        "path": "LeanProject/{projectIds}/Savings",
        "group": "Lean projects",
        "notes": "Replace {projectIds} with comma-separated ids",
    },
    {"path": "LeanProject/{projectId}/Tasks", "group": "Lean projects", "notes": "Single project id"},
    {"path": "LeanProject/{projectId}/Issues", "group": "Lean projects", "notes": "Single project id"},
    {
        "path": "LeanProject/{projectIds}/Stage/History",
        "group": "Lean projects",
        "notes": "Comma-separated project ids",
    },
    {"path": "LeanProject/Areas", "group": "Lean projects", "notes": "Taxonomy"},
    {"path": "LeanProject/Types", "group": "Lean projects", "notes": "Taxonomy"},
    {"path": "LeanProject/Categories", "group": "Lean projects", "notes": "Taxonomy"},
    {"path": "SupplyOrder/PurchaseOrder", "group": "Orders", "notes": "PO lines — filter params per swagger"},
    {"path": "DataShare", "group": "Bulk", "notes": "Parquet export metadata / signed URLs"},
    {
        "path": "WriteBack/v1/PurchaseOrderActions",
        "group": "Write-back",
        "notes": "GET pending PO write-back actions (query FilteredActionTypes required per OpenAPI)",
    },
)

# Documented POST / PUT / DELETE from tenant Data API swagger (paths relative to ``/data/``).
_DATA_API_MUTATION_CATALOG: tuple[dict[str, str], ...] = (
    {
        "method": "POST",
        "path": "LeanProject",
        "group": "Lean projects",
        "notes": "Create project — RequestedSites must list exactly one site; JSON body LeanProjectCreate",
    },
    {
        "method": "PUT",
        "path": "LeanProject/{projectId}",
        "group": "Lean projects",
        "notes": "Update project — body LeanProjectUpdate",
    },
    {
        "method": "POST",
        "path": "LeanProject/{projectId}/Task",
        "group": "Lean projects",
        "notes": "Create task — body LeanProjectTaskCreate",
    },
    {
        "method": "PUT",
        "path": "LeanProject/{projectId}/Task/{taskId}",
        "group": "Lean projects",
        "notes": "Update task — body LeanProjectTaskUpdate",
    },
    {
        "method": "POST",
        "path": "LeanProject/{projectId}/Issue",
        "group": "Lean projects",
        "notes": "Create issue — body LeanProjectIssueCreate",
    },
    {
        "method": "PUT",
        "path": "LeanProject/{projectId}/Issue/{issueId}",
        "group": "Lean projects",
        "notes": "Update issue — body LeanProjectIssueUpdate",
    },
    {
        "method": "POST",
        "path": "Metric/{metricId}/MetricDataPoint",
        "group": "Metrics",
        "notes": "Add metric result — body MetricDataPoint (fields optional per swagger)",
    },
    {
        "method": "DELETE",
        "path": "Metric/{metricId}/MetricDataPoint",
        "group": "Metrics",
        "notes": "Delete metric results — optional query startDate, endDate (YYYY-MM-DD)",
    },
    {
        "method": "PUT",
        "path": "WriteBack/v1/TransitionActions",
        "group": "Write-back",
        "notes": "Transition write-back attempts — JSON array body of WriteBackTransition objects",
    },
)


def _network_safe(fn):
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except (ReqConnectionError, Timeout, OSError) as e:
            err_type = type(e).__name__
            logger.warning("Tool %s network error: %s: %s", self.name, err_type, str(e)[:120])
            return json.dumps(
                {"error": f"Network error ({err_type}): could not reach LeanDNA Data API. Retry or check VPN."}
            )

    return wrapper


class LeanDNADataApiCatalogTool(BaseTool):
    """Static index of common Data API reads and documented writes (no HTTP)."""

    name: str = "leandna_data_api_catalog"
    description: str = (
        "List curated LeanDNA Data API resources: GET paths plus documented POST/PUT/DELETE "
        "(method, example path, notes). Does not call the network. "
        "Use leandna_data_api_get for reads and leandna_data_api_mutate for writes. "
        "Input: optional ignored string (pass empty or 'list')."
    )

    def _run(self, query: str) -> str:  # noqa: ARG002
        from ..leandna_data_api_request import data_api_base_url

        try:
            base_eff = data_api_base_url()
        except ValueError as e:
            base_eff = f"(misconfigured: {e})"

        doc = {
            "openapi_ui": (
                "https://app.leandna.com/application/apidocs/dist/index.html?urls.primaryName=Data#/"
            ),
            "base_url_effective": base_eff,
            "get_resources": list(_DATA_API_GET_CATALOG),
            "mutation_operations": list(_DATA_API_MUTATION_CATALOG),
            "usage_get": (
                'leandna_data_api_get: {"path": "Metric"} or '
                '{"path": "LeanProject", "query": {"startMonth": "2026-01", "endMonth": "2026-03"}}'
            ),
            "usage_mutate": (
                'leandna_data_api_mutate: {"method": "POST", "path": "Metric/638/MetricDataPoint", '
                '"body": {"dataPointDate": "2026-05-01", "value": 95.0}, "requested_sites": "172"}'
            ),
        }
        return json.dumps(doc, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class LeanDNADataApiGetTool(BaseTool):
    """Authenticated GET for any validated path under ``/data/``."""

    name: str = "leandna_data_api_get"
    description: str = (
        "Call LeanDNA Data API with GET {base}/data/{path}. "
        "Input: a **JSON object** (string). Required key: \"path\" (e.g. \"Metric\", "
        "\"ItemMasterData\", \"MaterialShortages/ShortagesByItem/Weekly\"). "
        "Optional: \"query\" (object of query parameters), \"requested_sites\" (comma-separated site ids), "
        "\"max_response_chars\" (int, default 500000). "
        "For paths with {placeholders}, substitute literals (e.g. LeanProject/123,456/Savings). "
        "Returns JSON: on success {ok, body, url, truncated?}; on failure {ok: false, error, ...}. "
        "Read-only — for POST/PUT/DELETE use leandna_data_api_mutate."
    )

    @_network_safe
    def _run(self, query: str) -> str:
        from ..leandna_data_api_request import data_api_get_json

        raw = (query or "").strip()
        if not raw:
            return json.dumps({"error": 'Pass a JSON object, e.g. {"path": "identity"}'})
        try:
            spec: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {e}"})

        if not isinstance(spec, dict):
            return json.dumps({"error": "Top-level JSON must be an object"})

        path = spec.get("path")
        if not isinstance(path, str) or not path.strip():
            return json.dumps({"error": 'Missing non-empty string field "path"'})

        q = spec.get("query")
        if q is not None and not isinstance(q, dict):
            return json.dumps({"error": 'If present, "query" must be a JSON object'})

        sites = spec.get("requested_sites")
        if sites is not None and not isinstance(sites, str):
            return json.dumps({"error": 'If present, "requested_sites" must be a string'})

        max_chars = spec.get("max_response_chars", 500_000)
        try:
            max_chars_i = int(max_chars)
        except (TypeError, ValueError):
            return json.dumps({"error": '"max_response_chars" must be an integer'})
        max_chars_i = max(1_000, min(2_000_000, max_chars_i))

        out = data_api_get_json(
            path.strip(),
            query=q if isinstance(q, dict) else None,
            requested_sites=sites.strip() if isinstance(sites, str) else None,
            timeout_seconds=120.0,
            max_response_chars=max_chars_i,
            user_agent_suffix="leandna-data-api-tool/1.0",
        )
        return json.dumps(out, indent=2, default=str)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class LeanDNADataApiMutateTool(BaseTool):
    """Authenticated POST / PUT / DELETE for validated paths under ``/data/``."""

    name: str = "leandna_data_api_mutate"
    description: str = (
        "LeanDNA Data API **mutations** — POST, PUT, or DELETE on {base}/data/{path}. "
        "Input: JSON object with required \"method\" (POST|PUT|DELETE) and \"path\" "
        "(e.g. \"Metric/638/MetricDataPoint\", \"LeanProject\", \"WriteBack/v1/TransitionActions\"). "
        "Optional: \"query\" (object), \"body\" (JSON object or array sent as request body for POST/PUT), "
        "\"requested_sites\" (comma-separated site ids), \"max_response_chars\" (int). "
        "Substitute path literals for swagger {placeholders}. "
        "Returns same envelope as leandna_data_api_get: {ok, body, url} or {ok: false, error, ...}. "
        "Blocked when EXECUTION_ENV is Production or CI unless BPO_ALLOW_PRODUCTION_MUTATIONS=true."
    )

    @_network_safe
    def _run(self, query: str) -> str:
        from ..leandna_data_api_request import data_api_mutate_json

        raw = (query or "").strip()
        if not raw:
            return json.dumps(
                {
                    "error": (
                        'Pass JSON, e.g. {"method":"POST","path":"Metric/1/MetricDataPoint",'
                        '"body":{"dataPointDate":"2026-01-15","value":1.0}}'
                    )
                }
            )
        try:
            spec: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {e}"})

        if not isinstance(spec, dict):
            return json.dumps({"error": "Top-level JSON must be an object"})

        method = spec.get("method")
        if not isinstance(method, str) or not method.strip():
            return json.dumps({"error": 'Missing non-empty string field "method" (POST, PUT, or DELETE)'})

        path = spec.get("path")
        if not isinstance(path, str) or not path.strip():
            return json.dumps({"error": 'Missing non-empty string field "path"'})

        q = spec.get("query")
        if q is not None and not isinstance(q, dict):
            return json.dumps({"error": 'If present, "query" must be a JSON object'})

        body = spec.get("body", None)
        if body is not None and not isinstance(body, (dict, list)):
            return json.dumps({"error": 'If present, "body" must be a JSON object or array'})

        sites = spec.get("requested_sites")
        if sites is not None and not isinstance(sites, str):
            return json.dumps({"error": 'If present, "requested_sites" must be a string'})

        max_chars = spec.get("max_response_chars", 500_000)
        try:
            max_chars_i = int(max_chars)
        except (TypeError, ValueError):
            return json.dumps({"error": '"max_response_chars" must be an integer'})
        max_chars_i = max(1_000, min(2_000_000, max_chars_i))

        m = method.strip().upper()
        json_body = None if m == "DELETE" else body

        out = data_api_mutate_json(
            m,
            path.strip(),
            query=q if isinstance(q, dict) else None,
            json_body=json_body,
            requested_sites=sites.strip() if isinstance(sites, str) else None,
            timeout_seconds=120.0,
            max_response_chars=max_chars_i,
            user_agent_suffix="leandna-data-api-tool/1.0",
        )
        return json.dumps(out, indent=2, default=str)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


def get_leandna_tools() -> list[BaseTool]:
    """Return LeanDNA Data API tools for LangChain agents."""
    return [
        LeanDNADataApiCatalogTool(),
        LeanDNADataApiGetTool(),
        LeanDNADataApiMutateTool(),
    ]
