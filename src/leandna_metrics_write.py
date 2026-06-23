"""LeanDNA Data API metric datapoint writes (insert, upsert, delete)."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from typing import Any

import requests

from src.config import CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET
from src.leandna_data_api_http import leandna_data_api_credentials_configured
from src.leandna_metrics_client import (
    build_metric_datapoint_post_body,
    default_datapoint_category,
    delete_metric_datapoint,
    fetch_identity_authorized_site_ids,
    fetch_metric_datapoints,
    metric_datapoint_exists_for_date,
    metric_requested_sites,
    post_metric_datapoint,
    resolve_metric_catalog_row,
)

READ_TIMEOUT_S = 120.0
CATALOG_LOOKUP_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class MetricWriteArgs:
    metric_id: int
    entry_date: str
    numerator: float
    denominator: float
    requested_sites: str | None
    category: str | None
    skip_catalog: bool
    timeout_seconds: float
    verbose: bool


@dataclass(frozen=True)
class MetricDeleteArgs:
    metric_id: int
    entry_date: str
    requested_sites: str | None
    skip_catalog: bool
    timeout_seconds: float
    verbose: bool


def _configure_logging(verbose: bool) -> None:
    logging.getLogger("cortex").setLevel(logging.INFO if verbose else logging.WARNING)


def _require_data_api_credentials() -> str | None:
    if not leandna_data_api_credentials_configured():
        return (
            "Missing LeanDNA Data API credentials — set PR_LEANDNA_DATA_API_BEARER_TOKEN "
            "or LEANDNA_DATA_API_COOKIE in .env."
        )
    return None


def resolve_write_sites_and_category(
    args: MetricWriteArgs | MetricDeleteArgs,
) -> tuple[str | None, str, dict[str, Any] | None]:
    metric: dict[str, Any] | None = None
    if not args.skip_catalog and args.requested_sites is None:
        print(
            f"Resolving siteId for metric {args.metric_id} from GET /data/Metric…",
            file=sys.stderr,
            flush=True,
        )
        metric = resolve_metric_catalog_row(
            args.metric_id, timeout_seconds=CATALOG_LOOKUP_TIMEOUT_S
        )

    if metric is not None:
        sites = metric_requested_sites(metric, args.requested_sites)
        cat = ""
        if isinstance(args, MetricWriteArgs):
            cat = args.category if args.category is not None else default_datapoint_category(metric)
        if sites:
            print(f"Using RequestedSites={sites!r} from catalog siteId.", file=sys.stderr)
        return sites, cat, metric

    sites = None if args.skip_catalog and args.requested_sites is None else args.requested_sites
    cat = ""
    if isinstance(args, MetricWriteArgs):
        cat = args.category if args.category is not None else ""
    if sites is None:
        print(
            "No RequestedSites header (metric not in your catalog slice — pass --requested-sites if needed).",
            file=sys.stderr,
        )
    return sites, cat, metric


def _unauthorized_site_hint(requested_sites: str | None) -> str:
    authorized = fetch_identity_authorized_site_ids(timeout_seconds=CATALOG_LOOKUP_TIMEOUT_S)
    auth_s = ", ".join(str(s) for s in authorized) if authorized else "(none from GET /data/identity)"
    req_s = requested_sites if requested_sites is not None else "(omitted)"
    return (
        f"RequestedSites={req_s} but your bearer token authorizes site id(s): {auth_s}.\n"
        "Copy PR_LEANDNA_DATA_API_BEARER_TOKEN from DevTools while logged into the site "
        "where this metric lives (often site 416 for internal KPIs), or pass --requested-sites "
        "with a site your token can access."
    )


def insert_metric_datapoint(args: MetricWriteArgs) -> dict[str, Any]:
    sites, cat, metric = resolve_write_sites_and_category(args)
    body = build_metric_datapoint_post_body(
        metric_id=args.metric_id,
        data_point_date=args.entry_date,
        numerator=args.numerator,
        denominator=args.denominator,
        category=cat,
    )
    print(
        f"POST Metric/{args.metric_id}/MetricDataPoint date={args.entry_date} "
        f"value={body['value']} sites={sites!r} (timeout={args.timeout_seconds:.0f}s)…",
        file=sys.stderr,
        flush=True,
    )
    env = post_metric_datapoint(
        args.metric_id,
        body,
        requested_sites=sites,
        timeout_seconds=args.timeout_seconds,
    )
    if env.get("ok"):
        env["auth"] = "data_api"
        if metric is not None:
            env["metricName"] = metric.get("name") or metric.get("crossSiteName")
        env["requestedSites"] = sites
        env["postBody"] = body
    elif env.get("status") == 403 and "unauthorized site" in str(env.get("error") or "").lower():
        env["hint"] = _unauthorized_site_hint(sites)
    elif env.get("status") == 409:
        env["hint"] = (
            f"Datapoint already exists for {args.entry_date!r} — use entry-upsert to replace it."
        )
    return env


def delete_metric_datapoint_for_date(args: MetricDeleteArgs, *, sites: str | None) -> dict[str, Any]:
    print(
        f"DELETE Metric/{args.metric_id}/MetricDataPoint date={args.entry_date} sites={sites!r}…",
        file=sys.stderr,
    )
    return delete_metric_datapoint(
        args.metric_id,
        data_point_date=args.entry_date,
        requested_sites=sites,
        timeout_seconds=args.timeout_seconds,
    )


def upsert_metric_datapoint(args: MetricWriteArgs) -> dict[str, Any]:
    result: dict[str, Any] = {"upsert": True, "deleted": False}
    sites, _cat, _metric = resolve_write_sites_and_category(args)
    category = args.category if args.category is not None else None
    lookup_timeout = min(args.timeout_seconds, CATALOG_LOOKUP_TIMEOUT_S)
    exists = metric_datapoint_exists_for_date(
        args.metric_id,
        args.entry_date,
        requested_sites=sites,
        category=category,
        timeout_seconds=lookup_timeout,
    )
    if exists:
        rows, err = fetch_metric_datapoints(
            args.metric_id,
            start_date=args.entry_date,
            end_date=args.entry_date,
            requested_sites=sites,
            timeout_seconds=lookup_timeout,
        )
        sibling_categories = (
            len(rows) > 1
            if err is None and category is not None and str(category).strip()
            else False
        )
        if sibling_categories:
            insert_env = insert_metric_datapoint(args)
            result["insert"] = insert_env
            result["ok"] = bool(insert_env.get("ok"))
            if not result["ok"] and insert_env.get("status") == 409:
                result["hint"] = (
                    f"Datapoint already exists for category {category!r} on {args.entry_date!r}. "
                    "LeanDNA DELETE is date-scoped and cannot replace one team row when others "
                    "share the same date — delete that category manually or use entry-delete."
                )
            return result
        del_args = MetricDeleteArgs(
            metric_id=args.metric_id,
            entry_date=args.entry_date,
            requested_sites=args.requested_sites,
            skip_catalog=True,
            timeout_seconds=args.timeout_seconds,
            verbose=args.verbose,
        )
        del_env = delete_metric_datapoint_for_date(del_args, sites=sites)
        result["delete"] = del_env
        if not del_env.get("ok"):
            result["ok"] = False
            return result
        result["deleted"] = True
    insert_env = insert_metric_datapoint(args)
    result["insert"] = insert_env
    result["ok"] = bool(insert_env.get("ok"))
    return result


def run_insert(args: MetricWriteArgs) -> tuple[int, dict[str, Any]]:
    _configure_logging(args.verbose)
    cred_err = _require_data_api_credentials()
    if cred_err:
        print(cred_err, file=sys.stderr)
        return 1, {"ok": False, "error": cred_err}

    print(
        f"Auth: LeanDNA Data API POST MetricDataPoint (EXECUTION_ENV={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )
    try:
        env = insert_metric_datapoint(args)
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1, {"ok": False, "error": str(e)}
    return (0 if env.get("ok") else 1), env


def run_upsert(args: MetricWriteArgs) -> tuple[int, dict[str, Any]]:
    _configure_logging(args.verbose)
    cred_err = _require_data_api_credentials()
    if cred_err:
        print(cred_err, file=sys.stderr)
        return 1, {"ok": False, "error": cred_err}

    print(
        f"Auth: LeanDNA Data API upsert MetricDataPoint (EXECUTION_ENV={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )
    try:
        result = upsert_metric_datapoint(args)
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1, {"ok": False, "error": str(e), "upsert": True}

    if not result.get("ok"):
        print(json.dumps(result, indent=2, default=str))
    return (0 if result.get("ok") else 1), result


def run_delete(args: MetricDeleteArgs) -> tuple[int, dict[str, Any]]:
    _configure_logging(args.verbose)
    cred_err = _require_data_api_credentials()
    if cred_err:
        print(cred_err, file=sys.stderr)
        return 1, {"ok": False, "error": cred_err}

    print(
        f"Auth: LeanDNA Data API DELETE MetricDataPoint (EXECUTION_ENV={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )
    sites, _cat, _metric = resolve_write_sites_and_category(args)
    exists = metric_datapoint_exists_for_date(
        args.metric_id,
        args.entry_date,
        requested_sites=sites,
        timeout_seconds=min(args.timeout_seconds, CATALOG_LOOKUP_TIMEOUT_S),
    )
    if not exists:
        env = {"ok": True, "skipped": True, "reason": "no datapoint for date"}
        return 0, env

    try:
        env = delete_metric_datapoint_for_date(args, sites=sites)
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1, {"ok": False, "error": str(e)}
    return (0 if env.get("ok") else 1), env
