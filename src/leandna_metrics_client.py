"""LeanDNA Data API — Metrics catalog and fiscal MetricReport.

Surfaces from the OpenAPI **Metrics** group (same host as Item Master / Lean Project):

- ``GET /data/Metric`` — list metric definitions (Manual, Automatic, ProcurementLog, Calculated).
- ``GET /data/MetricReport`` — fiscal-year metric report (monthly aggregates), optionally filtered.
- :func:`format_first_kpi_line_from_metric_report` — one-line summary of the first ``metricValues`` row (for logs / smoke tests).

Auth: see :mod:`leandna_data_api_http` — **Bearer** and/or **browser session cookie**
(``LEANDNA_DATA_API_COOKIE``). Optional ``RequestedSites`` header.

Exact query parameter names can vary by LeanDNA release — defaults use camelCase
(``fiscalYear``, ``metrics``, ``valueStreams``). If the API returns **400**, confirm names in
the tenant swagger (``scripts/fetch_leandna_swagger.py``) and use ``extra_query`` on
:func:`fetch_metric_report`.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import requests

from .leandna_data_api_request import data_api_base_url
from .config import logger
from .leandna_data_api_http import build_leandna_data_api_headers


def _connect_read_timeout(connect_seconds: float, read_seconds: float) -> float | tuple[float, float]:
    """``requests`` timeout: separate connect vs read when ``connect_seconds`` > 0."""
    c = float(connect_seconds)
    r = float(read_seconds)
    if c <= 0:
        return r
    return (c, r)


def _base_url() -> str:
    return data_api_base_url()


def _raise_for_status(resp: requests.Response) -> None:
    """Raise on HTTP error; log 401 hints (staging vs prod base URL)."""
    if resp.ok:
        return
    snippet = (resp.text or "").strip().replace("\n", " ")[:500]
    if resp.status_code == 401:
        logger.error(
            "LeanDNA Data API 401 — invalid/expired Bearer, wrong LEANDNA_DATA_API_BASE_URL, or session "
            "expired. Try LEANDNA_DATA_API_COOKIE from the browser while logged in (see "
            "src/leandna_data_api_http.py). URL=%s body_prefix=%r",
            resp.url,
            snippet,
        )
    resp.raise_for_status()


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
    connect_timeout_seconds: float = 15.0,
    timeout_seconds: float = 120.0,
    extra_query: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return metric definitions from ``GET /data/Metric``.

    Typical fields (tenant-dependent): ``id``, ``name``, ``siteId``, ``metricType``,
    ``possibleValueStreams``, ``currentCategories``.

    ``connect_timeout_seconds`` bounds TCP/TLS handshake (default 15s); ``timeout_seconds``
    bounds time waiting for the response body after connect (default 120s).
    """
    url = f"{_base_url()}/data/Metric"
    params = dict(extra_query or {})
    logger.info("LeanDNA Metric: GET %s (sites=%s)", url, requested_sites or "all")
    r = requests.get(
        url,
        headers=build_leandna_data_api_headers(
            requested_sites=requested_sites,
            user_agent_suffix="leandna-metrics-client/1.0",
        ),
        params=params,
        timeout=_connect_read_timeout(connect_timeout_seconds, timeout_seconds),
    )
    _raise_for_status(r)
    return _unwrap_metric_definition_rows(r.json())


def fetch_metric_report(
    fiscal_year: int | str,
    *,
    requested_sites: str | None = None,
    metric_ids: list[str] | None = None,
    value_streams: list[str] | None = None,
    connect_timeout_seconds: float = 15.0,
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
    r = requests.get(
        url,
        headers=build_leandna_data_api_headers(
            requested_sites=requested_sites,
            user_agent_suffix="leandna-metrics-client/1.0",
        ),
        params=params,
        timeout=_connect_read_timeout(connect_timeout_seconds, timeout_seconds),
    )
    _raise_for_status(r)
    body = r.json()
    if not isinstance(body, dict):
        logger.warning("LeanDNA MetricReport: expected object, got %s", type(body).__name__)
        return {"raw": body}
    return body


def format_first_kpi_line_from_metric_report(report: dict[str, Any]) -> str:
    """Format one human-readable KPI line from a ``MetricReport`` JSON body.

    Uses the first row of ``metricValues`` and matches ``metricId`` to ``metrics`` for a label.
    Percent-style metric names (label ending with ``%``) render the value with a ``%`` suffix.
    """
    meta_rows = report.get("metrics") or []
    id_to_label: dict[str, str] = {}
    if isinstance(meta_rows, list):
        for m in meta_rows:
            if not isinstance(m, dict):
                continue
            mid = m.get("id")
            if mid is None:
                continue
            label = m.get("name") or m.get("crossSiteName") or str(mid)
            id_to_label[str(mid)] = str(label)

    values_block = report.get("metricValues")
    if not isinstance(values_block, list) or not values_block:
        return "KPI: (no metricValues)"

    row = values_block[0]
    if not isinstance(row, dict):
        return "KPI: (metricValues[0] is not an object)"

    mid = row.get("metricId", row.get("id"))
    label = id_to_label.get(str(mid), str(mid) if mid is not None else "metric")

    val: Any = row.get("value")
    if val is None:
        for k, v in row.items():
            if k in ("metricId", "id", "dataPointDate"):
                continue
            if isinstance(v, (int, float)):
                val = v
                break
        if val is None:
            val = next((v for k, v in row.items() if k not in ("metricId", "id")), None)

    fy = report.get("fiscalYear", "")
    suffix = f" (FY{fy})" if fy != "" else ""
    if isinstance(val, (int, float)) and str(label).rstrip().endswith("%"):
        return f"KPI: {label} = {val}%{suffix}"
    return f"KPI: {label} = {val!r}{suffix}"


def unwrap_metric_datapoint_rows(body: Any) -> list[dict[str, Any]]:
    """Normalize ``GET /data/Metric/{id}/MetricDataPoint`` body to a list of point dicts."""
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("data", "items", "results", "metricDataPoints"):
            block = body.get(key)
            if isinstance(block, list):
                return [x for x in block if isinstance(x, dict)]
    return []


def resolve_metric_datapoint_window(
    *,
    lookback_days: int = 90,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str, str]:
    """Return ``(startDate, endDate)`` ISO date strings for MetricDataPoint queries."""
    if start_date and end_date:
        return start_date.strip(), end_date.strip()
    end_d = date.today()
    if end_date:
        end_d = date.fromisoformat(end_date.strip())
    start_d = end_d - timedelta(days=max(1, lookback_days))
    if start_date:
        start_d = date.fromisoformat(start_date.strip())
    return start_d.isoformat(), end_d.isoformat()


def fetch_metric_datapoints(
    metric_id: Any,
    *,
    start_date: str,
    end_date: str,
    requested_sites: str | None = None,
    connect_timeout_seconds: float = 15.0,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Fetch ``GET /data/Metric/{id}/MetricDataPoint`` for a date window.

    Returns ``(rows sorted by dataPointDate, None)`` on success, or ``([], error_envelope)``
    when the Data API GET fails (same envelope shape as ``data_api_get_json``).
    """
    from .leandna_data_api_request import data_api_get_json

    path = f"Metric/{metric_id}/MetricDataPoint"
    env = data_api_get_json(
        path,
        query={"startDate": start_date, "endDate": end_date},
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        user_agent_suffix="leandna-metrics-client/1.0",
    )
    if not env.get("ok"):
        return [], env
    rows = unwrap_metric_datapoint_rows(env.get("body"))
    rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    return rows, None


def slim_metric_datapoint_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only date + value for CLI / export payloads."""
    return [
        {"dataPointDate": p.get("dataPointDate"), "value": p.get("value")}
        for p in rows
        if isinstance(p, dict)
    ]
