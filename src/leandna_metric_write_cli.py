"""Shared CLI logic for entry-insert / entry-upsert."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any

import requests

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET, LEANDNA_APP_FACTORY_NDX
from src.leandna_app_metrics_client import (
    build_metric_entry_put_body,
    delete_metric_entries,
    get_metric_entries_for_date,
    list_metrics_view,
    pick_metric_by_ndx,
    put_metric_entries,
)
from src.leandna_app_metrics_http import (
    LeanDNAAppSessionError,
    leandna_app_session_configured,
    misconfigured_app_session_message,
)
from src.leandna_data_api_http import leandna_data_api_credentials_configured
from src.leandna_metrics_client import (
    build_metric_datapoint_post_body,
    default_datapoint_category,
    delete_metric_datapoint,
    fetch_identity_authorized_site_ids,
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
    value_stream_ndx: int | None
    factory_ndx: int
    verbose: bool


def add_metric_write_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--metric-ndx",
        type=int,
        required=True,
        help="Metric catalog id (Data API id / app ndx)",
    )
    ap.add_argument(
        "--value-stream-ndx",
        type=int,
        default=None,
        help="App API only — skip Metrics/View lookup when set",
    )
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--numerator", type=float, required=True)
    ap.add_argument("--denominator", type=float, default=1.0)
    ap.add_argument(
        "--category",
        default=None,
        help="MetricDataPoint category (default: from catalog when found, else empty)",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        help="RequestedSites header (default: metric siteId from catalog, else omitted)",
    )
    ap.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Do not call GET /data/Metric to resolve siteId (use --requested-sites or omit header)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=READ_TIMEOUT_S,
        metavar="SEC",
        help=f"HTTP read timeout (default: {READ_TIMEOUT_S:.0f})",
    )
    ap.add_argument("--factory-ndx", type=int, default=None, help="App API only")
    ap.add_argument("-v", "--verbose", action="store_true")


def metric_write_args_from_namespace(ns: argparse.Namespace) -> MetricWriteArgs:
    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    return MetricWriteArgs(
        metric_id=ns.metric_ndx,
        entry_date=ns.date,
        numerator=ns.numerator,
        denominator=ns.denominator,
        requested_sites=ns.requested_sites,
        category=ns.category,
        skip_catalog=ns.skip_catalog,
        timeout_seconds=ns.timeout,
        value_stream_ndx=ns.value_stream_ndx,
        factory_ndx=factory,
        verbose=ns.verbose,
    )


def _resolve_data_api_metadata(
    args: MetricWriteArgs,
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
        cat = args.category if args.category is not None else default_datapoint_category(metric)
        if sites:
            print(f"Using RequestedSites={sites!r} from catalog siteId.", file=sys.stderr)
        return sites, cat, metric

    if args.skip_catalog and args.requested_sites is None:
        sites = None
    else:
        sites = args.requested_sites
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


def _insert_via_data_api(args: MetricWriteArgs) -> dict[str, Any]:
    sites, cat, metric = _resolve_data_api_metadata(args)
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


def _delete_via_data_api(args: MetricWriteArgs, *, sites: str | None) -> dict[str, Any]:
    print(
        f"DELETE Metric/{args.metric_id}/MetricDataPoint date={args.entry_date} sites={sites!r}…",
        file=sys.stderr,
        flush=True,
    )
    return delete_metric_datapoint(
        args.metric_id,
        data_point_date=args.entry_date,
        requested_sites=sites,
        timeout_seconds=args.timeout_seconds,
    )


def _resolve_value_stream_ndx(args: MetricWriteArgs) -> int:
    vs = args.value_stream_ndx
    if vs is not None:
        return vs
    rows = list_metrics_view(factory_ndx=args.factory_ndx)
    metric = pick_metric_by_ndx(rows, args.metric_id)
    if metric is not None and metric.get("valueStreamNdx") is not None:
        vs = int(metric["valueStreamNdx"])
        print(f"Using valueStreamNdx={vs} from Metrics/View.", file=sys.stderr)
        return vs
    print(
        "Warning: --value-stream-ndx not set and not in catalog row; using 0.",
        file=sys.stderr,
    )
    return 0


def _app_entry_body(args: MetricWriteArgs, *, value_stream_ndx: int) -> list[dict[str, Any]]:
    return build_metric_entry_put_body(
        metric_ndx=args.metric_id,
        value_stream_ndx=value_stream_ndx,
        entry_date=args.entry_date,
        numerator=args.numerator,
        denominator=args.denominator,
        factory_ndx=args.factory_ndx,
    )


def _insert_via_app_api(args: MetricWriteArgs) -> dict[str, Any]:
    vs = _resolve_value_stream_ndx(args)
    body = _app_entry_body(args, value_stream_ndx=vs)
    env = put_metric_entries(args.entry_date, body, factory_ndx=args.factory_ndx)
    if env.get("ok"):
        env["auth"] = "app"
    return env


def _delete_via_app_api(args: MetricWriteArgs) -> dict[str, Any]:
    rows = get_metric_entries_for_date(
        args.entry_date, metric_ndx=args.metric_id, factory_ndx=args.factory_ndx
    )
    body = [r for r in rows if str(r.get("metricNdx", "")) == str(args.metric_id)]
    if not body:
        return {"ok": True, "skipped": True, "reason": "no existing entry"}
    print(
        f"DELETE MetricEntries date={args.entry_date} metric={args.metric_id}…",
        file=sys.stderr,
        flush=True,
    )
    return delete_metric_entries(
        args.entry_date, body, factory_ndx=args.factory_ndx, switch_site_first=False
    )


def _configure_logging(verbose: bool) -> None:
    logging.getLogger("bpo").setLevel(logging.INFO if verbose else logging.WARNING)


def _validate_credentials(*, data_api: bool, app_api: bool) -> str | None:
    if not data_api and not app_api:
        return (
            "No LeanDNA write credentials configured.\n"
            "  Data API (preferred): set PR_LEANDNA_DATA_API_BEARER_TOKEN or PR_LEANDNA_DATA_API_COOKIE\n"
            "  App API (fallback): set LEANDNA_APP_SESSION_ID (bin/test-script --show-session)"
        )
    misconfigured = misconfigured_app_session_message()
    if misconfigured and not data_api:
        return misconfigured
    return None


def run_insert(args: MetricWriteArgs) -> tuple[int, dict[str, Any]]:
    _configure_logging(args.verbose)
    data_api = leandna_data_api_credentials_configured()
    app_api = leandna_app_session_configured()
    cred_err = _validate_credentials(data_api=data_api, app_api=app_api)
    if cred_err:
        print(cred_err, file=sys.stderr)
        return 1, {"ok": False, "error": cred_err}

    try:
        if data_api:
            print(
                f"Auth: LeanDNA Data API POST MetricDataPoint (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            env = _insert_via_data_api(args)
        else:
            print(
                f"Auth: LeanDNA app API PUT MetricEntries (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            env = _insert_via_app_api(args)
    except LeanDNAAppSessionError as e:
        print(str(e), file=sys.stderr)
        return 1, {"ok": False, "error": str(e)}
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1, {"ok": False, "error": str(e)}

    return (0 if env.get("ok") else 1), env


def run_upsert(args: MetricWriteArgs) -> tuple[int, dict[str, Any]]:
    _configure_logging(args.verbose)
    data_api = leandna_data_api_credentials_configured()
    app_api = leandna_app_session_configured()
    cred_err = _validate_credentials(data_api=data_api, app_api=app_api)
    if cred_err:
        print(cred_err, file=sys.stderr)
        return 1, {"ok": False, "error": cred_err}

    result: dict[str, Any] = {"upsert": True, "deleted": False}

    try:
        if data_api:
            print(
                f"Auth: LeanDNA Data API upsert MetricDataPoint (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            sites, _cat, _metric = _resolve_data_api_metadata(args)
            exists = metric_datapoint_exists_for_date(
                args.metric_id,
                args.entry_date,
                requested_sites=sites,
                timeout_seconds=min(args.timeout_seconds, CATALOG_LOOKUP_TIMEOUT_S),
            )
            if exists:
                del_env = _delete_via_data_api(args, sites=sites)
                result["delete"] = del_env
                if not del_env.get("ok"):
                    result["ok"] = False
                    print(json.dumps(result, indent=2, default=str))
                    return 1, result
                result["deleted"] = True
            insert_env = _insert_via_data_api(args)
            result["insert"] = insert_env
            result["ok"] = bool(insert_env.get("ok"))
        else:
            print(
                f"Auth: LeanDNA app API upsert MetricEntries (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            rows = get_metric_entries_for_date(
                args.entry_date, metric_ndx=args.metric_id, factory_ndx=args.factory_ndx
            )
            exists = any(str(r.get("metricNdx", "")) == str(args.metric_id) for r in rows)
            if exists:
                del_env = _delete_via_app_api(args)
                result["delete"] = del_env
                if not del_env.get("ok"):
                    result["ok"] = False
                    print(json.dumps(result, indent=2, default=str))
                    return 1, result
                result["deleted"] = True
            insert_env = _insert_via_app_api(args)
            result["insert"] = insert_env
            result["ok"] = bool(insert_env.get("ok"))
            if insert_env.get("ok"):
                insert_env["auth"] = "app"
    except LeanDNAAppSessionError as e:
        print(str(e), file=sys.stderr)
        return 1, {"ok": False, "error": str(e), "upsert": True}
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1, {"ok": False, "error": str(e), "upsert": True}

    return (0 if result.get("ok") else 1), result


def print_result_env(env: dict[str, Any]) -> None:
    print(json.dumps(env, indent=2, default=str))
    if env.get("ok"):
        return
    hint = env.get("hint")
    if hint:
        print(hint, file=sys.stderr)
    insert = env.get("insert") if env.get("upsert") else env
    if isinstance(insert, dict):
        hint = insert.get("hint")
        if hint:
            print(hint, file=sys.stderr)
    err = str((insert or env).get("error") or "")
    if "mutations" in err.lower() and BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "production":
        print(
            "Production writes blocked — prefix command with BPO_ALLOW_PRODUCTION_MUTATIONS=true",
            file=sys.stderr,
        )
    elif (insert or env).get("status") == 401 or "session not found" in err.lower():
        print(
            "Bearer token expired or invalid — refresh PR_LEANDNA_DATA_API_BEARER_TOKEN "
            "from DevTools (Authorization header on any /api/data/… request while logged in).",
            file=sys.stderr,
        )
    elif (insert or env).get("status") == 504 or "504" in err:
        print(
            "LeanDNA returned 504 Gateway Timeout — retry in a minute; their API may be slow.",
            file=sys.stderr,
        )
