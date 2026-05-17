"""LeanDNA classic app metrics API — list, read entries, write, delete.

Mirrors ``kpi/update-kpi/metric_management.py`` (session + ``/api/2/factndx/{factory}/...``),
not the OpenAPI ``/data/Metric`` Data API.

Metric ids in this API are ``ndx`` (``metricNdx``). They may differ from Data API catalog ``id``.
"""

from __future__ import annotations

import datetime
import logging
import time
from datetime import date, timedelta
from typing import Any

import requests

from urllib.parse import quote

from .config import (
    LEANDNA_APP_API_SERVER,
    LEANDNA_APP_FACTORY_NDX,
    LEANDNA_APP_METRIC_OWNER,
    LEANDNA_APP_METRICS_VIEW_QUERY,
    LEANDNA_APP_USER_NDX,
    leandna_http_mutation_blocked_envelope,
    logger,
)
from .leandna_app_metrics_http import build_leandna_app_api_headers, leandna_app_session_configured
from .leandna_metrics_client import resolve_metric_datapoint_window

_log = logging.getLogger("bpo")


def _factory_base_path(factory_ndx: int | None = None) -> str:
    f = LEANDNA_APP_FACTORY_NDX if factory_ndx is None else int(factory_ndx)
    return f"/api/2/factndx/{f}"


def _api_call(
    end_point: str,
    body: Any | None = None,
    *,
    request_type: str | None = None,
    full_url: str | None = None,
    factory_ndx: int | None = None,
    timeout: float = 30.0,
    user_agent_suffix: str = "leandna-app-metrics/1.0",
) -> requests.Response:
    """HTTP to app API with retries (same pattern as kpi ``metric_management._api_call``)."""
    url = full_url or f"{LEANDNA_APP_API_SERVER.rstrip('/')}{_factory_base_path(factory_ndx)}{end_point}"
    headers = build_leandna_app_api_headers(user_agent_suffix=user_agent_suffix)
    method = (request_type or "get").lower()
    cumulative_retry_duration = 0
    next_retry_time = 1.0
    last_exc: Exception | None = None
    while cumulative_retry_duration < 60:
        try:
            if method == "post":
                resp = requests.post(url, json=body, headers=headers, timeout=timeout)
            elif method == "put":
                resp = requests.put(url, json=body, headers=headers, timeout=timeout)
            elif method == "delete":
                resp = requests.delete(url, json=body, headers=headers, timeout=timeout)
            else:
                resp = requests.get(url, json=body, headers=headers, timeout=timeout)
            if resp.status_code in (200, 201, 204, 409):
                return resp
            _log.warning("LeanDNA app API %s %s responded %s", method.upper(), end_point, resp.status_code)
        except Exception as e:
            last_exc = e
            _log.exception("LeanDNA app API %s %s error", method.upper(), end_point)
        time.sleep(next_retry_time)
        cumulative_retry_duration += next_retry_time
        next_retry_time *= 2
    if last_exc is not None:
        raise ConnectionError(f"{url} app API request failed") from last_exc
    raise ConnectionError(f"{url} app API request failed")


def switch_site(factory_ndx: int | None = None) -> None:
    """``POST /auth/1/authenticate/ldnasession/switchSite/{factory}`` (required for site-scoped calls)."""
    f = LEANDNA_APP_FACTORY_NDX if factory_ndx is None else int(factory_ndx)
    url = f"{LEANDNA_APP_API_SERVER.rstrip('/')}/auth/1/authenticate/ldnasession/switchSite/{f}"
    _log.info("LeanDNA app API: switch site to %s", f)
    _api_call("", request_type="post", full_url=url, factory_ndx=f)


_OWNER_FIELD_KEYS = (
    "assignedUserNdx",
    "ownerId",
    "ownerNdx",
    "metricOwnerNdx",
    "assignedUser",
    "metricOwner",
)


def _ndx_matches(raw: Any, want: Any) -> bool:
    if raw is None or want is None:
        return False
    try:
        return int(raw) == int(want)
    except (TypeError, ValueError):
        return str(raw).strip() == str(want).strip()


def append_metrics_view_query_param(query: str, key: str, value: Any) -> str:
    """Add or replace ``key=value`` in a Metrics/View query string."""
    q = (query or "").strip().lstrip("?")
    parts = [p for p in q.split("&") if p and not p.lower().startswith(f"{key.lower()}=")]
    parts.append(f"{key}={value}")
    return "&".join(parts)


def fetch_app_identity(*, factory_ndx: int | None = None, timeout: float = 30.0) -> dict[str, Any]:
    """``GET {host}/api/data/identity`` using the app session cookie (no Bearer)."""
    if factory_ndx is not None:
        switch_site(factory_ndx)
    url = f"{LEANDNA_APP_API_SERVER.rstrip('/')}/api/data/identity"
    resp = _api_call("", request_type="get", full_url=url, factory_ndx=factory_ndx, timeout=timeout)
    body = resp.json() if resp.text else {}
    return body if isinstance(body, dict) else {"raw": body}


def resolve_app_user_ndx(
    *,
    factory_ndx: int | None = None,
    override_ndx: str | int | None = None,
    timeout: float = 30.0,
) -> tuple[str, dict[str, Any] | None]:
    """Return ``(user_ndx, identity_body)`` for the logged-in user (numeric id when available)."""
    if override_ndx is not None and str(override_ndx).strip():
        return str(override_ndx).strip(), None
    if LEANDNA_APP_USER_NDX:
        return LEANDNA_APP_USER_NDX, None
    try:
        identity = fetch_app_identity(factory_ndx=factory_ndx, timeout=timeout)
    except ConnectionError:
        identity = {}
    for key in ("userId", "userNdx", "ndx", "id"):
        raw = identity.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip(), identity
    return "", identity if identity else None


def resolve_app_metric_owner(
    *,
    factory_ndx: int | None = None,
    metric_owner: str | None = None,
    user_ndx: str | int | None = None,
    timeout: float = 30.0,
) -> tuple[str, dict[str, Any] | None, str]:
    """Return ``(metricOwner filter value, identity_or_none, kind)`` where kind is ``name`` or ``ndx``.

    The app ``Metrics/View`` API often expects ``metricOwner`` as a **display name**
    (e.g. ``Marc Schriftman``), not the numeric ``userNdx`` used in kpi scripts.
    """
    if metric_owner is not None and str(metric_owner).strip():
        return str(metric_owner).strip(), None, "name"
    if LEANDNA_APP_METRIC_OWNER:
        return LEANDNA_APP_METRIC_OWNER, None, "name"

    ndx, identity = resolve_app_user_ndx(
        factory_ndx=factory_ndx, override_ndx=user_ndx, timeout=timeout
    )
    if ndx and not str(ndx).strip().isdigit():
        return str(ndx).strip(), identity, "name"
    if ndx:
        return ndx, identity, "ndx"
    return "", identity, "ndx"


def _metric_owner_query_value(owner: str, kind: str) -> str:
    if kind == "name":
        return quote(owner, safe="")
    return owner


def identity_display_name(identity: dict[str, Any] | None) -> str:
    if not identity:
        return ""
    for key in ("userName", "emailAddress", "name", "email"):
        v = identity.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def metric_owned_by_user(metric: dict[str, Any], owner: Any) -> bool:
    """True when any known owner field on a Metrics/View row matches ``owner`` (name or ndx)."""
    want = str(owner or "").strip().lower()
    if not want:
        return False
    for key in _OWNER_FIELD_KEYS:
        raw = metric.get(key)
        if raw is None:
            continue
        if _ndx_matches(raw, owner):
            return True
        if str(raw).strip().lower() == want:
            return True
    return False


def filter_metrics_owned_by_user(
    rows: list[dict[str, Any]], owner: Any
) -> list[dict[str, Any]]:
    return [r for r in rows if isinstance(r, dict) and metric_owned_by_user(r, owner)]


def list_metric_owner_histogram(
    *,
    view_query: str | None = None,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout: float = 60.0,
) -> list[tuple[str, int]]:
    """Count metrics per ``metricOwner`` label from ``Metrics/View`` (for whoami / debugging)."""
    rows = list_metrics_view(
        view_query=view_query,
        factory_ndx=factory_ndx,
        switch_site_first=switch_site_first,
        timeout=timeout,
    )
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = row.get("metricOwner")
        if label is None:
            continue
        key = str(label).strip()
        if key:
            counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))


def list_my_metrics_view(
    owner: str | int,
    *,
    owner_kind: str = "name",
    view_query: str | None = None,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout: float = 60.0,
) -> list[dict[str, Any]]:
    """Metrics assigned to ``owner`` (display name or numeric ndx).

    For **display names**, the API often returns 204 when ``metricOwner`` is in the query,
    so we load the view without that param and filter client-side on ``metricOwner``.
    """
    base_q = view_query if view_query is not None else LEANDNA_APP_METRICS_VIEW_QUERY
    if owner_kind == "name":
        rows = list_metrics_view(
            view_query=base_q,
            factory_ndx=factory_ndx,
            switch_site_first=switch_site_first,
            timeout=timeout,
        )
        return filter_metrics_owned_by_user(rows, owner)

    q_val = _metric_owner_query_value(str(owner), owner_kind)
    scoped_q = append_metrics_view_query_param(base_q, "metricOwner", q_val)
    rows = list_metrics_view(
        view_query=scoped_q,
        factory_ndx=factory_ndx,
        switch_site_first=switch_site_first,
        timeout=timeout,
    )
    owned = filter_metrics_owned_by_user(rows, owner)
    return owned


def list_metrics_view(
    *,
    view_query: str | None = None,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout: float = 60.0,
) -> list[dict[str, Any]]:
    """``GET …/Metrics/View?…`` — same listing kpi uses before updates."""
    if switch_site_first:
        switch_site(factory_ndx)
    q = (view_query if view_query is not None else LEANDNA_APP_METRICS_VIEW_QUERY).strip()
    if q.startswith("?"):
        q = q[1:]
    end_point = f"/Metrics/View?{q}" if q else "/Metrics/View"
    resp = _api_call(end_point, request_type="get", factory_ndx=factory_ndx, timeout=timeout)
    data = resp.json() if resp.text else []
    return normalize_metric_view_rows(data)


def normalize_metric_view_rows(data: Any) -> list[dict[str, Any]]:
    """Normalize ``Metrics/View`` JSON and add ``id`` alias for ``ndx`` (CLI parity with Data API tools)."""
    rows: list[Any]
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        block = None
        for key in ("metrics", "data", "items", "results"):
            if isinstance(data.get(key), list):
                block = data[key]
                break
        rows = block if block is not None else [data]
    else:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        m = dict(r)
        ndx = m.get("ndx", m.get("metricNdx"))
        if ndx is not None and m.get("id") is None:
            m["id"] = ndx
        name = m.get("name") or m.get("metricName") or m.get("crossSiteName")
        if name is not None:
            m.setdefault("name", name)
        out.append(m)
    return out


def _unwrap_entry_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("entries", "metricEntries", "data", "items", "results"):
            block = data.get(key)
            if isinstance(block, list):
                return [x for x in block if isinstance(x, dict)]
        return [data]
    return []


def get_metric_entries_for_date(
    entry_date: str,
    *,
    metric_ndx: int | None = None,
    factory_ndx: int | None = None,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """``GET …/MetricEntries?&date=YYYY-MM-DD`` (optional ``metricNdx`` filter)."""
    end_point = f"/MetricEntries?&date={entry_date}"
    if metric_ndx is not None:
        end_point += f"&metricNdx={int(metric_ndx)}"
    resp = _api_call(end_point, request_type="get", factory_ndx=factory_ndx, timeout=timeout)
    return _unwrap_entry_rows(resp.json() if resp.text else [])


def _entry_matches_metric(row: dict[str, Any], metric_ndx: int, value_stream_ndx: int | None) -> bool:
    raw_ndx = row.get("metricNdx", row.get("ndx"))
    try:
        if int(raw_ndx) != int(metric_ndx):
            return False
    except (TypeError, ValueError):
        if str(raw_ndx) != str(metric_ndx):
            return False
    if value_stream_ndx is None:
        return True
    try:
        return int(row.get("valueStreamNdx", 0) or 0) == int(value_stream_ndx)
    except (TypeError, ValueError):
        return str(row.get("valueStreamNdx")) == str(value_stream_ndx)


def slim_app_metric_entry_rows(rows: list[dict[str, Any]], *, entry_date: str) -> list[dict[str, Any]]:
    """Shape similar to Data API ``MetricDataPoint`` slim rows for shared display helpers."""
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date") or r.get("entryDate") or entry_date)
        out.append(
            {
                "dataPointDate": d,
                "value": r.get("value"),
                "numeratorValue": r.get("valueA"),
                "denominatorValue": r.get("valueB"),
                "valueStreamNdx": r.get("valueStreamNdx"),
                "metricNdx": r.get("metricNdx"),
                "enabled": r.get("enabled"),
            }
        )
    return out


def fetch_metric_entries_range(
    metric_ndx: int,
    *,
    start_date: str,
    end_date: str,
    value_stream_ndx: int | None = None,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout_per_day: float = 30.0,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Load daily ``MetricEntries`` for each date in ``[start_date, end_date]`` (inclusive)."""
    if switch_site_first:
        switch_site(factory_ndx)
    start_d = date.fromisoformat(start_date.strip())
    end_d = date.fromisoformat(end_date.strip())
    if end_d < start_d:
        return [], {"ok": False, "error": "end_date before start_date"}
    merged: list[dict[str, Any]] = []
    cur = start_d
    while cur <= end_d:
        iso = cur.isoformat()
        try:
            day_rows = get_metric_entries_for_date(
                iso, metric_ndx=metric_ndx, factory_ndx=factory_ndx, timeout=timeout_per_day
            )
        except Exception as e:
            return merged, {"ok": False, "error": str(e), "status": None, "failed_date": iso}
        for row in day_rows:
            if _entry_matches_metric(row, metric_ndx, value_stream_ndx):
                merged.extend(slim_app_metric_entry_rows([row], entry_date=iso))
        cur += timedelta(days=1)
    merged.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    return merged, None


def _compute_display_value(numerator: int | float, denominator: int | float) -> str:
    if denominator == 0:
        return str(numerator)
    return str(round((float(numerator) / float(denominator)) * 100, 3))


def build_metric_entry_put_body(
    *,
    metric_ndx: int,
    value_stream_ndx: int,
    entry_date: str,
    numerator: int | float,
    denominator: int | float,
    factory_ndx: int | None = None,
    enabled: bool = True,
) -> list[dict[str, Any]]:
    """Body array for ``PUT …/MetricEntries?&date=…`` (same shape as kpi ``enter_metric_data``)."""
    f = LEANDNA_APP_FACTORY_NDX if factory_ndx is None else int(factory_ndx)
    value = _compute_display_value(numerator, denominator)
    return [
        {
            "factoryNdx": f,
            "metricNdx": int(metric_ndx),
            "valueStreamNdx": int(value_stream_ndx),
            "value": value,
            "valueA": str(numerator),
            "valueB": "0" if denominator == 0 else str(denominator),
            "enabled": enabled,
            "dateModified": round(datetime.datetime.now().timestamp()) * 1000,
        }
    ]


def put_metric_entries(
    entry_date: str,
    body: list[dict[str, Any]],
    *,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """``PUT …/MetricEntries?&date=…`` — write or overwrite a daily entry."""
    blocked = leandna_http_mutation_blocked_envelope(method="PUT", path="MetricEntries")
    if blocked is not None:
        return blocked
    if switch_site_first:
        switch_site(factory_ndx)
    end_point = f"/MetricEntries?&date={entry_date}"
    try:
        resp = _api_call(
            end_point, body, request_type="put", factory_ndx=factory_ndx, timeout=timeout
        )
    except ConnectionError as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "status": resp.status_code, "body": resp.json() if resp.text else None}


def delete_metric_entries(
    entry_date: str,
    body: list[dict[str, Any]],
    *,
    factory_ndx: int | None = None,
    switch_site_first: bool = True,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Remove entries: try ``DELETE`` on ``MetricEntries``, then ``PUT`` with ``enabled: false``."""
    blocked = leandna_http_mutation_blocked_envelope(method="DELETE", path="MetricEntries")
    if blocked is not None:
        return blocked
    if switch_site_first:
        switch_site(factory_ndx)
    end_point = f"/MetricEntries?&date={entry_date}"
    try:
        resp = _api_call(
            end_point, body, request_type="delete", factory_ndx=factory_ndx, timeout=timeout
        )
        return {"ok": True, "status": resp.status_code, "method": "DELETE"}
    except ConnectionError:
        pass
    disabled = []
    for row in body:
        if not isinstance(row, dict):
            continue
        copy = dict(row)
        copy["enabled"] = False
        disabled.append(copy)
    if not disabled:
        return {"ok": False, "error": "empty delete body"}
    try:
        resp = _api_call(
            end_point, disabled, request_type="put", factory_ndx=factory_ndx, timeout=timeout
        )
    except ConnectionError as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "status": resp.status_code, "method": "PUT(enabled=false)"}


def metric_view_label(metric: dict[str, Any]) -> str:
    return str(metric.get("name") or metric.get("metricName") or metric.get("ndx") or metric.get("id") or "").strip()


def pick_metric_by_ndx(rows: list[dict[str, Any]], ndx: Any) -> dict[str, Any] | None:
    want = str(ndx).strip()
    for r in rows:
        if not isinstance(r, dict):
            continue
        for key in ("ndx", "id", "metricNdx"):
            raw = r.get(key)
            if raw is None:
                continue
            try:
                if int(raw) == int(want):
                    return r
            except (TypeError, ValueError):
                if str(raw).strip() == want:
                    return r
    return None


def grep_metrics_by_name(rows: list[dict[str, Any]], needle: str) -> list[dict[str, Any]]:
    n = (needle or "").strip().lower()
    if not n:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        blob = f"{r.get('name', '')!s} {r.get('metricName', '')!s} {r.get('crossSiteName', '')!s}".lower()
        if n in blob:
            out.append(r)
    return out


__all__ = [
    "append_metrics_view_query_param",
    "build_metric_entry_put_body",
    "delete_metric_entries",
    "fetch_app_identity",
    "fetch_metric_entries_range",
    "filter_metrics_owned_by_user",
    "get_metric_entries_for_date",
    "grep_metrics_by_name",
    "identity_display_name",
    "list_metric_owner_histogram",
    "list_my_metrics_view",
    "list_metrics_view",
    "metric_owned_by_user",
    "resolve_app_metric_owner",
    "resolve_app_user_ndx",
    "metric_view_label",
    "normalize_metric_view_rows",
    "pick_metric_by_ndx",
    "put_metric_entries",
    "resolve_metric_datapoint_window",
    "slim_app_metric_entry_rows",
    "switch_site",
]
