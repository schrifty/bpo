"""LeanDNA Data API — Metrics catalog and fiscal MetricReport.

Surfaces from the OpenAPI **Metrics** group (same host as Item Master / Lean Project):

- ``GET /data/Metric`` — list metric definitions (Manual, Automatic, ProcurementLog, Calculated).
- ``GET /data/MetricReport`` — fiscal-year metric report (monthly aggregates), optionally filtered.

Auth: ``Authorization: Bearer {LEANDNA_DATA_API_BEARER_TOKEN}``. Optional site scope:
``RequestedSites: <comma-separated site ids>``.

Exact query parameter names can vary by LeanDNA release — defaults use camelCase
(``fiscalYear``, ``metrics``, ``valueStreams``). If the API returns **400**, confirm names in
the tenant swagger (``scripts/fetch_leandna_swagger.py``) and use ``extra_query`` on
:func:`fetch_metric_report`.
"""

from __future__ import annotations

from typing import Any

import requests

from .config import LEANDNA_DATA_API_BASE_URL, LEANDNA_DATA_API_BEARER_TOKEN, logger


def _base_url() -> str:
    return (LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").rstrip("/")


def _bearer() -> str:
    token = (LEANDNA_DATA_API_BEARER_TOKEN or "").strip()
    if not token:
        raise ValueError("LEANDNA_DATA_API_BEARER_TOKEN not configured in .env")
    return token


def _raise_for_status(resp: requests.Response) -> None:
    """Raise on HTTP error; log 401 hints (staging vs prod base URL)."""
    if resp.ok:
        return
    snippet = (resp.text or "").strip().replace("\n", " ")[:500]
    if resp.status_code == 401:
        logger.error(
            "LeanDNA Data API 401 — token invalid/expired, or LEANDNA_DATA_API_BASE_URL does not "
            "match the environment where the token was issued (production "
            "`https://app.leandna.com/api` vs staging, often `https://app.staging.leandna.com/api`). "
            "URL=%s body_prefix=%r",
            resp.url,
            snippet,
        )
    resp.raise_for_status()


def _headers(requested_sites: str | None) -> dict[str, str]:
    h = {
        "Authorization": f"Bearer {_bearer()}",
        "Accept": "application/json",
        "User-Agent": "bpo-leandna-metrics-client/1.0",
    }
    if requested_sites:
        h["RequestedSites"] = requested_sites.strip()
    return h


def _unwrap_metric_definition_rows(data: Any) -> list[dict[str, Any]]:
    """Normalize ``GET /data/Metric`` body to a list of metric dicts."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("metrics", "data", "items", "results"):
            block = data.get(key)
            if isinstance(block, list):
                return [x for x in block if isinstance(x, dict)]
    logger.warning("LeanDNA Metric: unexpected response shape %s", type(data).__name__)
    return []


def list_metric_definitions(
    requested_sites: str | None = None,
    *,
    timeout_seconds: float = 120.0,
    extra_query: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return metric definitions from ``GET /data/Metric``.

    Typical fields (tenant-dependent): ``id``, ``name``, ``siteId``, ``metricType``,
    ``possibleValueStreams``, ``currentCategories``.
    """
    url = f"{_base_url()}/data/Metric"
    params = dict(extra_query or {})
    logger.info("LeanDNA Metric: GET %s (sites=%s)", url, requested_sites or "all")
    r = requests.get(url, headers=_headers(requested_sites), params=params, timeout=timeout_seconds)
    _raise_for_status(r)
    return _unwrap_metric_definition_rows(r.json())


def fetch_metric_report(
    fiscal_year: int | str,
    *,
    requested_sites: str | None = None,
    metric_ids: list[str] | None = None,
    value_streams: list[str] | None = None,
    timeout_seconds: float = 180.0,
    extra_query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch ``GET /data/MetricReport`` for a fiscal year.

    Parameters:
        fiscal_year: Fiscal year label accepted by the API (often integer e.g. ``2026``).
        requested_sites: Optional ``RequestedSites`` header value.
        metric_ids: Filter to these metric ids (sent as comma-separated ``metrics`` query).
        value_streams: Filter to these value streams (comma-separated ``valueStreams`` query).
        extra_query: Additional query parameters merged last (override keys above if needed).

    Returns:
        Parsed JSON object. Common keys (per internal docs): ``metrics``, ``metricValues``,
        ``fiscalYear``, ``startTimestamp``, ``endTimestamp``, ``currency``.
    """
    url = f"{_base_url()}/data/MetricReport"
    params: dict[str, Any] = {"fiscalYear": fiscal_year}
    if metric_ids:
        params["metrics"] = ",".join(str(x).strip() for x in metric_ids if str(x).strip())
    if value_streams:
        params["valueStreams"] = ",".join(str(x).strip() for x in value_streams if str(x).strip())
    params.update(extra_query or {})

    logger.info(
        "LeanDNA MetricReport: GET %s fiscalYear=%s sites=%s",
        url,
        fiscal_year,
        requested_sites or "all",
    )
    r = requests.get(url, headers=_headers(requested_sites), params=params, timeout=timeout_seconds)
    _raise_for_status(r)
    body = r.json()
    if not isinstance(body, dict):
        logger.warning("LeanDNA MetricReport: expected object, got %s", type(body).__name__)
        return {"raw": body}
    return body
