"""LeanDNA Data API metric catalog reads (filter, owned metrics, datapoint series)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .leandna_data_api_http import leandna_data_api_credentials_configured
from .leandna_data_api_request import data_api_get_json
from .leandna_metrics_client import (
    fetch_metric_datapoints,
    list_metric_definitions,
    metric_definition_label,
    metric_id_matches,
    metric_requested_sites,
    resolve_metric_datapoint_window,
    slim_metric_datapoint_rows,
    sort_metrics_by_id,
)


class MetricsCatalogError(RuntimeError):
    """Raised when catalog fetch or filter fails."""


@dataclass(frozen=True)
class MetricIdentity:
    user_id: str
    owner_label: str
    body: dict[str, Any]


def require_data_api_credentials() -> None:
    if not leandna_data_api_credentials_configured():
        raise MetricsCatalogError(
            "Missing LeanDNA Data API credentials — set PR_LEANDNA_DATA_API_BEARER_TOKEN and/or "
            "LEANDNA_DATA_API_COOKIE in .env."
        )


def is_catalog_id_token(token: str | None) -> bool:
    t = (token or "").strip()
    return bool(t) and t.isdigit()


def grep_metrics_by_name_substring(rows: list[Any], needle: str) -> list[dict[str, Any]]:
    n = (needle or "").strip().lower()
    if not n:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        blob = f"{r.get('name', '')!s} {r.get('crossSiteName', '')!s}".lower()
        if n in blob:
            out.append(r)
    return out


def filter_metric_catalog(
    catalog: list[dict[str, Any]],
    *,
    filter_token: str | None,
    use_all: bool,
    max_metrics: int,
) -> list[dict[str, Any]]:
    """Resolve *filter_token* (id, name substring, or all) against a catalog list."""
    metrics = [m for m in catalog if isinstance(m, dict)]
    catalog_count = len(metrics)
    if use_all:
        picked = sort_metrics_by_id(metrics)[: max(1, max_metrics)]
        if catalog_count > len(picked):
            return picked
        return picked
    if filter_token and is_catalog_id_token(filter_token):
        picked = [m for m in metrics if metric_id_matches(m.get("id"), filter_token)]
        if not picked:
            raise MetricsCatalogError(
                f"No metric with id={filter_token!r} in catalog ({catalog_count} definition(s))."
            )
        return picked
    picked = sort_metrics_by_id(grep_metrics_by_name_substring(metrics, filter_token or ""))
    if not picked:
        raise MetricsCatalogError(
            f"No metrics matched substring {filter_token!r} ({catalog_count} in catalog)."
        )
    return picked


def format_metric_brief_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for r in rows:
        mid = r.get("id", "")
        name = str(r.get("name") or r.get("crossSiteName") or "").replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        site = str(r.get("siteId") or "")
        cats = r.get("currentCategories")
        cat_s = ""
        if isinstance(cats, list):
            cat_s = ",".join(str(x) for x in cats[:8])
            if len(cats) > 8:
                cat_s += ",…"
        elif cats is not None:
            cat_s = str(cats)
        vstreams = r.get("possibleValueStreams")
        vs_s = ""
        if isinstance(vstreams, list):
            vs_s = ",".join(str(x.get("id", x) if isinstance(x, dict) else x) for x in vstreams[:5])
            if len(vstreams) > 5:
                vs_s += ",…"
        lines.append(
            f"{mid}\t{name}\t{mtype}\tsiteId={site}\tcategories={cat_s}\tvalueStreams={vs_s}"
        )
    return lines


def fetch_data_api_identity(*, timeout_seconds: float = 60.0) -> MetricIdentity:
    require_data_api_credentials()
    env = data_api_get_json("identity", timeout_seconds=timeout_seconds)
    if not env.get("ok"):
        raise MetricsCatalogError(f"GET /data/identity failed: {env.get('error') or env}")
    body = env.get("body")
    if not isinstance(body, dict):
        raise MetricsCatalogError("GET /data/identity returned unexpected body")
    user_id = str(body.get("userId") or "").strip()
    if not user_id:
        raise MetricsCatalogError("GET /data/identity did not return userId")
    label = str(body.get("userName") or body.get("emailAddress") or user_id).strip()
    return MetricIdentity(user_id=user_id, owner_label=label, body=body)


def list_metric_definitions_filtered(
    *,
    metric_id: str | None = None,
    requested_sites: str | None = None,
    connect_timeout_seconds: float = 15.0,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], int]:
    """``GET /data/Metric`` with optional client-side id filter. Returns ``(rows, catalog_count)``."""
    require_data_api_credentials()
    try:
        rows = list_metric_definitions(
            requested_sites=requested_sites,
            connect_timeout_seconds=connect_timeout_seconds,
            timeout_seconds=timeout_seconds,
            extra_query=None,
        )
    except requests.Timeout as e:
        raise MetricsCatalogError(f"LeanDNA catalog request timed out: {e}") from e
    except requests.HTTPError as e:
        raise MetricsCatalogError(f"Failed to fetch metrics: {e}") from e
    except Exception as e:
        raise MetricsCatalogError(f"Failed to fetch metrics: {e}") from e

    rows = sort_metrics_by_id(rows)
    catalog_count = len(rows)
    if metric_id is not None:
        want = metric_id
        filtered = [r for r in rows if metric_id_matches(r.get("id"), want)]
        if not filtered:
            raise MetricsCatalogError(
                f"No metric with id={want!r} in GET /data/Metric response "
                f"({catalog_count} definition(s) in catalog)."
            )
        return filtered, catalog_count
    if not rows:
        raise MetricsCatalogError("No metric definitions returned.")
    return rows, catalog_count


def fetch_my_metric_definitions(
    *,
    requested_sites: str | None = None,
    timeout_seconds: float = 60.0,
) -> tuple[list[dict[str, Any]], MetricIdentity]:
    """Metrics owned by the bearer token user (``ownerId`` = ``identity.userId``)."""
    identity = fetch_data_api_identity(timeout_seconds=timeout_seconds)
    catalog = list_metric_definitions(
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        extra_query=None,
    )
    rows = [
        m
        for m in catalog
        if isinstance(m, dict) and str(m.get("ownerId") or "").strip() == identity.user_id
    ]
    return sort_metrics_by_id(rows), identity


def fetch_metric_datapoint_series(
    metric: dict[str, Any],
    *,
    lookback_days: int = 90,
    start_date: str | None = None,
    end_date: str | None = None,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], dict[str, str] | None]:
    """Slim datapoint rows for one metric. Returns ``(points, error_env_or_none)``."""
    mid = metric.get("id")
    if mid is None:
        return [], {"error": "metric has no id"}
    start_s, end_s = resolve_metric_datapoint_window(
        lookback_days=lookback_days,
        start_date=start_date,
        end_date=end_date,
    )
    sites = metric_requested_sites(metric, requested_sites)
    points, err = fetch_metric_datapoints(
        mid,
        start_date=start_s,
        end_date=end_s,
        requested_sites=sites,
        timeout_seconds=timeout_seconds,
    )
    if err is not None:
        return [], err
    return slim_metric_datapoint_rows(points), None


def fetch_metrics_with_datapoints(
    *,
    filter_token: str | None,
    use_all: bool,
    max_metrics: int,
    start_date: str | None,
    end_date: str | None,
    lookback_days: int = 90,
    requested_sites: str | None = None,
    connect_timeout_seconds: float = 15.0,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], int, str, str]:
    """Catalog filter + per-metric ``MetricDataPoint`` GET.

    Returns ``(results, fetch_errors, start_date, end_date)`` where each result has
    ``id``, ``name``, ``siteId``, ``window``, ``values``.
    """
    require_data_api_credentials()
    start_s, end_s = resolve_metric_datapoint_window(
        lookback_days=lookback_days,
        start_date=start_date,
        end_date=end_date,
    )
    try:
        catalog = list_metric_definitions(
            requested_sites=requested_sites,
            connect_timeout_seconds=connect_timeout_seconds,
            timeout_seconds=timeout_seconds,
            extra_query=None,
        )
    except requests.Timeout as e:
        raise MetricsCatalogError(f"LeanDNA catalog request timed out: {e}") from e
    except requests.HTTPError as e:
        raise MetricsCatalogError(f"Failed to fetch metric catalog: {e}") from e
    except Exception as e:
        raise MetricsCatalogError(f"Failed to fetch metric catalog: {e}") from e

    metrics = filter_metric_catalog(
        catalog,
        filter_token=filter_token,
        use_all=use_all,
        max_metrics=max_metrics,
    )

    results: list[dict[str, Any]] = []
    fetch_errors = 0
    for m in metrics:
        mid = m.get("id")
        name = metric_definition_label(m)
        points, err = fetch_metric_datapoint_series(
            m,
            start_date=start_s,
            end_date=end_s,
            requested_sites=requested_sites,
            timeout_seconds=timeout_seconds,
        )
        if err is not None:
            fetch_errors += 1
            continue
        results.append(
            {
                "id": mid,
                "name": name,
                "siteId": m.get("siteId"),
                "window": {"startDate": start_s, "endDate": end_s},
                "values": points,
            }
        )
    return results, fetch_errors, start_s, end_s


def build_my_metrics_payload(
    rows: list[dict[str, Any]],
    identity: MetricIdentity,
) -> dict[str, Any]:
    return {
        "source": "data_api",
        "ownerLabel": identity.owner_label,
        "metrics": rows,
        "identity": identity.body,
    }
