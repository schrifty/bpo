"""Copy a LeanDNA metric definition and datapoints from production to staging."""

from __future__ import annotations

import logging
from typing import Any

from .leandna_data_api_env import (
    LeanDNAEnvConfig,
    env_get_json,
    env_mutate_json,
    load_leandna_env_config,
)
from .leandna_metrics_client import (
    _unwrap_metric_definition_rows,
    metric_definition_label,
    resolve_metric_datapoint_window,
    unwrap_metric_datapoint_rows,
)

logger = logging.getLogger("bpo")

# Fields omitted when POSTing a new metric definition (server assigns id / owner).
_METRIC_CREATE_OMIT = frozenset({"id", "ownerId", "match_score"})


def find_metric_by_id(catalog: list[dict[str, Any]], metric_id: Any) -> dict[str, Any] | None:
    want = str(metric_id).strip()
    for m in catalog:
        if not isinstance(m, dict):
            continue
        if str(m.get("id")).strip() == want:
            return m
        try:
            if int(m.get("id")) == int(metric_id):
                return m
        except (TypeError, ValueError):
            continue
    return None


def list_metrics_for_env(
    config: LeanDNAEnvConfig,
    *,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    env = env_get_json(
        config,
        "Metric",
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        user_agent_suffix="leandna-metrics-copy/1.0",
    )
    if not env.get("ok"):
        return [], env
    return _unwrap_metric_definition_rows(env.get("body")), None


def build_metric_create_body(
    source: dict[str, Any],
    *,
    staging_site_id: Any | None = None,
) -> dict[str, Any]:
    """Payload for ``POST /data/Metric`` (writable catalog fields only)."""
    body: dict[str, Any] = {}
    for key, val in source.items():
        if key in _METRIC_CREATE_OMIT:
            continue
        if val is not None:
            body[key] = val
    if staging_site_id is not None:
        body["siteId"] = staging_site_id
    if "name" not in body and source.get("crossSiteName"):
        body["name"] = source.get("crossSiteName")
    if "crossSiteName" not in body and body.get("name"):
        body["crossSiteName"] = body["name"]
    if "metricType" not in body:
        body["metricType"] = "Manual"
    return body


def _parse_created_metric_id(body: Any) -> Any | None:
    if isinstance(body, dict):
        for key in ("id", "metricId"):
            if body.get(key) is not None:
                return body[key]
        nested = body.get("metric")
        if isinstance(nested, dict) and nested.get("id") is not None:
            return nested["id"]
    return None


def create_metric_definition(
    staging: LeanDNAEnvConfig,
    body: dict[str, Any],
    *,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
) -> tuple[Any | None, dict[str, Any]]:
    """``POST /data/Metric`` — returns ``(new_id, envelope)``."""
    env = env_mutate_json(
        staging,
        "POST",
        "Metric",
        json_body=body,
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        user_agent_suffix="leandna-metrics-copy/1.0",
    )
    if not env.get("ok"):
        return None, env
    new_id = _parse_created_metric_id(env.get("body"))
    return new_id, env


def find_staging_metric_by_name(
    catalog: list[dict[str, Any]],
    source: dict[str, Any],
    *,
    staging_site_id: Any | None = None,
) -> list[dict[str, Any]]:
    """Staging rows with the same ``name`` (and ``siteId`` when provided)."""
    name = metric_definition_label(source).lower()
    if not name:
        return []
    site = staging_site_id if staging_site_id is not None else source.get("siteId")
    out: list[dict[str, Any]] = []
    for m in catalog:
        if not isinstance(m, dict):
            continue
        if metric_definition_label(m).lower() != name:
            continue
        if site is not None and m.get("siteId") is not None:
            try:
                if int(m["siteId"]) != int(site):
                    continue
            except (TypeError, ValueError):
                if str(m.get("siteId")) != str(site):
                    continue
        out.append(m)
    return out


def build_datapoint_post_body(row: dict[str, Any], staging_metric_id: Any) -> dict[str, Any]:
    """Body for ``POST /data/Metric/{id}/MetricDataPoint`` (aligned with integration tests)."""
    val = row.get("value")
    body: dict[str, Any] = {
        "dataPointDate": row.get("dataPointDate"),
        "metricId": staging_metric_id,
        "category": str(row.get("category") or ""),
        "value": val,
        "numeratorValue": row.get("numeratorValue", val),
        "denominatorValue": row.get("denominatorValue", 1),
    }
    if row.get("valueStreamId") is not None:
        body["valueStreamId"] = row.get("valueStreamId")
    return body


def fetch_datapoints_for_env(
    config: LeanDNAEnvConfig,
    metric_id: Any,
    *,
    start_date: str,
    end_date: str,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """MetricDataPoint GET using environment-specific credentials."""
    path = f"Metric/{metric_id}/MetricDataPoint"
    env = env_get_json(
        config,
        path,
        query={"startDate": start_date, "endDate": end_date},
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        user_agent_suffix="leandna-metrics-copy/1.0",
    )
    if not env.get("ok"):
        return [], env
    rows = unwrap_metric_datapoint_rows(env.get("body"))
    rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    return rows, None


def post_datapoint_for_env(
    staging: LeanDNAEnvConfig,
    staging_metric_id: Any,
    body: dict[str, Any],
    *,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    return env_mutate_json(
        staging,
        "POST",
        f"Metric/{staging_metric_id}/MetricDataPoint",
        json_body=body,
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
        user_agent_suffix="leandna-metrics-copy/1.0",
    )


def copy_metric_production_to_staging(
    production_metric_id: Any,
    *,
    lookback_days: int = 365,
    start_date: str | None = None,
    end_date: str | None = None,
    staging_site_id: Any | None = None,
    requested_sites: str | None = None,
    copy_datapoints: bool = True,
    reuse_staging_by_name: bool = True,
    dry_run: bool = False,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    """Copy metric definition (new id on staging) and optional datapoint history."""
    production = load_leandna_env_config("production")
    staging = load_leandna_env_config("staging")

    start_s, end_s = (
        resolve_metric_datapoint_window(
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
        )
        if copy_datapoints
        else (None, None)
    )

    prod_catalog, cat_err = list_metrics_for_env(
        production,
        requested_sites=requested_sites,
        timeout_seconds=timeout_seconds,
    )
    if cat_err is not None:
        return {
            "ok": False,
            "error": "production GET /data/Metric failed",
            "detail": cat_err,
            "production_metric_id": production_metric_id,
        }

    source = find_metric_by_id(prod_catalog, production_metric_id)
    if source is None:
        return {
            "ok": False,
            "error": f"Production metric id={production_metric_id!r} not found in catalog.",
            "production_catalog_size": len(prod_catalog),
        }

    prod_sites = requested_sites
    if prod_sites is None and source.get("siteId") is not None:
        prod_sites = str(source.get("siteId")).strip() or None

    stg_sites = requested_sites
    if stg_sites is None and staging_site_id is not None:
        stg_sites = str(staging_site_id).strip() or None
    elif stg_sites is None and source.get("siteId") is not None:
        stg_sites = str(source.get("siteId")).strip() or None

    create_body = build_metric_create_body(source, staging_site_id=staging_site_id)
    result: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "production": {
            "base_url": production.base_url,
            "metric_id": source.get("id"),
            "name": metric_definition_label(source),
            "siteId": source.get("siteId"),
            "definition": source,
        },
        "staging": {
            "base_url": staging.base_url,
            "create_body": create_body,
        },
        "data_window": (
            {"startDate": start_s, "endDate": end_s} if copy_datapoints and start_s else None
        ),
    }

    staging_metric_id: Any | None = None
    create_env: dict[str, Any] | None = None

    if dry_run:
        stg_catalog, _ = list_metrics_for_env(staging, requested_sites=stg_sites, timeout_seconds=timeout_seconds)
        name_matches = find_staging_metric_by_name(stg_catalog, source, staging_site_id=staging_site_id)
        result["staging"]["would_create_via"] = "POST /data/Metric"
        result["staging"]["existing_name_matches"] = [
            {"id": m.get("id"), "name": metric_definition_label(m)} for m in name_matches
        ]
        staging_metric_id = name_matches[0].get("id") if len(name_matches) == 1 else None
    else:
        create_env = {}
        staging_metric_id, create_env = create_metric_definition(
            staging,
            create_body,
            requested_sites=stg_sites,
            timeout_seconds=timeout_seconds,
        )
        result["staging"]["create_response"] = {
            "ok": create_env.get("ok"),
            "status": create_env.get("status"),
        }
        if staging_metric_id is None and reuse_staging_by_name:
            stg_catalog, stg_cat_err = list_metrics_for_env(
                staging,
                requested_sites=stg_sites,
                timeout_seconds=timeout_seconds,
            )
            if stg_cat_err is not None:
                result["ok"] = False
                result["error"] = "staging GET /data/Metric failed after create attempt"
                result["detail"] = stg_cat_err
                return result
            name_matches = find_staging_metric_by_name(
                stg_catalog, source, staging_site_id=staging_site_id,
            )
            if len(name_matches) == 1:
                staging_metric_id = name_matches[0].get("id")
                result["staging"]["reused_existing_by_name"] = True
                result["staging"]["name_match"] = {
                    "id": staging_metric_id,
                    "name": metric_definition_label(name_matches[0]),
                }
            elif len(name_matches) > 1:
                result["ok"] = False
                result["error"] = (
                    "POST /data/Metric did not return a new id; multiple staging metrics share this name. "
                    "Pass --staging-metric-id to target one."
                )
                result["staging"]["name_matches"] = [
                    {"id": m.get("id"), "name": metric_definition_label(m)} for m in name_matches
                ]
                return result
        if staging_metric_id is None:
            result["ok"] = False
            result["error"] = "Could not create or resolve staging metric id."
            result["staging"]["create_envelope"] = create_env
            return result

    result["staging"]["metric_id"] = staging_metric_id

    if not copy_datapoints:
        result["datapoints"] = {"copied": 0, "skipped": True}
        return result

    assert start_s and end_s
    prod_points, dp_err = fetch_datapoints_for_env(
        production,
        source.get("id"),
        start_date=start_s,
        end_date=end_s,
        requested_sites=prod_sites,
        timeout_seconds=timeout_seconds,
    )
    if dp_err is not None:
        result["ok"] = False
        result["error"] = "production MetricDataPoint fetch failed"
        result["detail"] = dp_err
        return result

    result["datapoints"] = {
        "source_count": len(prod_points),
        "posted": 0,
        "failed": 0,
        "skipped_dry_run": dry_run,
        "errors": [],
    }

    if dry_run:
        result["datapoints"]["sample_post_bodies"] = [
            build_datapoint_post_body(prod_points[0], staging_metric_id)
        ] if prod_points else []
        return result

    posted = 0
    failed = 0
    errors: list[dict[str, Any]] = []
    for row in prod_points:
        if not isinstance(row, dict) or not row.get("dataPointDate"):
            continue
        post_body = build_datapoint_post_body(row, staging_metric_id)
        env = post_datapoint_for_env(
            staging,
            staging_metric_id,
            post_body,
            requested_sites=stg_sites,
            timeout_seconds=timeout_seconds,
        )
        if env.get("ok"):
            posted += 1
        else:
            failed += 1
            if len(errors) < 10:
                errors.append(
                    {
                        "dataPointDate": row.get("dataPointDate"),
                        "status": env.get("status"),
                        "error": env.get("error"),
                    }
                )
    result["datapoints"]["posted"] = posted
    result["datapoints"]["failed"] = failed
    result["datapoints"]["errors"] = errors
    if posted == 0 and len(prod_points) > 0 and failed > 0:
        result["ok"] = False
        result["error"] = "No datapoints copied; all MetricDataPoint POSTs failed."
    return result
