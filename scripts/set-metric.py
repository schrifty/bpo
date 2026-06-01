#!/usr/bin/env python3
"""Write one daily metric value.

Uses **Data API** ``POST /data/Metric/{id}/MetricDataPoint`` when
``PR_LEANDNA_DATA_API_BEARER_TOKEN`` / ``LEANDNA_DATA_API_COOKIE`` is configured
(preferred). Falls back to app API ``PUT …/MetricEntries`` when only
``LEANDNA_APP_SESSION_ID`` is available.

Examples::

  set-metric --metric-ndx 2076 --date 2026-05-22 --numerator 1 --denominator 100
  BPO_ALLOW_PRODUCTION_MUTATIONS=true set-metric \\
    --metric-ndx 2076 --date 2026-05-22 --numerator 85 --denominator 100 \\
    --requested-sites 416
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

import requests  # noqa: E402

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import (  # noqa: E402
    BPO_LEANDNA_DATA_API_EXECUTION_BUCKET,
    LEANDNA_APP_FACTORY_NDX,
)
from src.leandna_app_metrics_client import (  # noqa: E402
    build_metric_entry_put_body,
    list_metrics_view,
    pick_metric_by_ndx,
    put_metric_entries,
)
from src.leandna_app_metrics_http import (  # noqa: E402
    LeanDNAAppSessionError,
    leandna_app_session_configured,
    misconfigured_app_session_message,
)
from src.leandna_data_api_http import leandna_data_api_credentials_configured  # noqa: E402
from src.leandna_metrics_client import (  # noqa: E402
    build_metric_datapoint_post_body,
    default_datapoint_category,
    fetch_identity_authorized_site_ids,
    metric_requested_sites,
    post_metric_datapoint,
    resolve_metric_catalog_row,
)

_READ_TIMEOUT_S = 120.0
_CATALOG_LOOKUP_TIMEOUT_S = 30.0


def _resolve_data_api_metadata(
    *,
    metric_id: int,
    requested_sites: str | None,
    category: str | None,
    skip_catalog: bool,
    timeout_seconds: float,
) -> tuple[str | None, str, dict[str, Any] | None]:
    """Return ``(requested_sites, category, metric_row_or_none)`` for POST body."""
    metric: dict[str, Any] | None = None
    if not skip_catalog and requested_sites is None:
        print(
            f"Resolving siteId for metric {metric_id} from GET /data/Metric…",
            file=sys.stderr,
            flush=True,
        )
        metric = resolve_metric_catalog_row(metric_id, timeout_seconds=_CATALOG_LOOKUP_TIMEOUT_S)

    if metric is not None:
        sites = metric_requested_sites(metric, requested_sites)
        cat = category if category is not None else default_datapoint_category(metric)
        if sites:
            print(f"Using RequestedSites={sites!r} from catalog siteId.", file=sys.stderr)
        return sites, cat, metric

    if skip_catalog and requested_sites is None:
        sites = None
    else:
        sites = requested_sites
    cat = category if category is not None else ""
    if sites is None:
        print(
            "No RequestedSites header (metric not in your catalog slice — pass --requested-sites if needed).",
            file=sys.stderr,
        )
    return sites, cat, metric


def _unauthorized_site_hint(requested_sites: str | None) -> str:
    authorized = fetch_identity_authorized_site_ids(timeout_seconds=_CATALOG_LOOKUP_TIMEOUT_S)
    auth_s = ", ".join(str(s) for s in authorized) if authorized else "(none from GET /data/identity)"
    req_s = requested_sites if requested_sites is not None else "(omitted)"
    return (
        f"RequestedSites={req_s} but your bearer token authorizes site id(s): {auth_s}.\n"
        "Copy PR_LEANDNA_DATA_API_BEARER_TOKEN from DevTools while logged into the site "
        "where this metric lives (often site 416 for internal KPIs), or pass --requested-sites "
        "with a site your token can access."
    )


def _write_via_data_api(
    *,
    metric_id: int,
    entry_date: str,
    numerator: float,
    denominator: float,
    requested_sites: str | None,
    category: str | None,
    skip_catalog: bool,
    timeout_seconds: float,
) -> dict[str, Any]:
    sites, cat, metric = _resolve_data_api_metadata(
        metric_id=metric_id,
        requested_sites=requested_sites,
        category=category,
        skip_catalog=skip_catalog,
        timeout_seconds=timeout_seconds,
    )
    body = build_metric_datapoint_post_body(
        metric_id=metric_id,
        data_point_date=entry_date,
        numerator=numerator,
        denominator=denominator,
        category=cat,
    )
    print(
        f"POST Metric/{metric_id}/MetricDataPoint date={entry_date} "
        f"value={body['value']} sites={sites!r} (timeout={timeout_seconds:.0f}s)…",
        file=sys.stderr,
        flush=True,
    )
    env = post_metric_datapoint(
        metric_id,
        body,
        requested_sites=sites,
        timeout_seconds=timeout_seconds,
    )
    if env.get("ok"):
        env["auth"] = "data_api"
        if metric is not None:
            env["metricName"] = metric.get("name") or metric.get("crossSiteName")
        env["requestedSites"] = sites
        env["postBody"] = body
    elif env.get("status") == 403 and "unauthorized site" in str(env.get("error") or "").lower():
        env["hint"] = _unauthorized_site_hint(sites)
    return env


def _write_via_app_api(
    *,
    metric_id: int,
    entry_date: str,
    numerator: float,
    denominator: float,
    value_stream_ndx: int | None,
    factory_ndx: int,
) -> dict[str, Any]:
    vs = value_stream_ndx
    if vs is None:
        rows = list_metrics_view(factory_ndx=factory_ndx)
        metric = pick_metric_by_ndx(rows, metric_id)
        if metric is not None and metric.get("valueStreamNdx") is not None:
            vs = int(metric["valueStreamNdx"])
            print(f"Using valueStreamNdx={vs} from Metrics/View.", file=sys.stderr)
        else:
            vs = 0
            print(
                "Warning: --value-stream-ndx not set and not in catalog row; using 0.",
                file=sys.stderr,
            )
    body = build_metric_entry_put_body(
        metric_ndx=metric_id,
        value_stream_ndx=vs,
        entry_date=entry_date,
        numerator=numerator,
        denominator=denominator,
        factory_ndx=factory_ndx,
    )
    env = put_metric_entries(entry_date, body, factory_ndx=factory_ndx)
    if env.get("ok"):
        env["auth"] = "app"
    return env


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Write one metric value (Data API bearer preferred; app session fallback).",
    )
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
        default=_READ_TIMEOUT_S,
        metavar="SEC",
        help=f"HTTP read timeout (default: {_READ_TIMEOUT_S:.0f})",
    )
    ap.add_argument("--factory-ndx", type=int, default=None, help="App API only")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    data_api = leandna_data_api_credentials_configured()
    app_api = leandna_app_session_configured()

    if not data_api and not app_api:
        print(
            "No LeanDNA write credentials configured.\n"
            "  Data API (preferred): set PR_LEANDNA_DATA_API_BEARER_TOKEN or PR_LEANDNA_DATA_API_COOKIE\n"
            "  App API (fallback): set LEANDNA_APP_SESSION_ID (bin/test-script --show-session)",
            file=sys.stderr,
        )
        return 1

    misconfigured = misconfigured_app_session_message()
    if misconfigured and not data_api:
        print(misconfigured, file=sys.stderr)
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX

    try:
        if data_api:
            print(
                f"Auth: LeanDNA Data API POST MetricDataPoint (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            env = _write_via_data_api(
                metric_id=ns.metric_ndx,
                entry_date=ns.date,
                numerator=ns.numerator,
                denominator=ns.denominator,
                requested_sites=ns.requested_sites,
                category=ns.category,
                skip_catalog=ns.skip_catalog,
                timeout_seconds=ns.timeout,
            )
        else:
            print(
                f"Auth: LeanDNA app API PUT MetricEntries (EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                file=sys.stderr,
            )
            env = _write_via_app_api(
                metric_id=ns.metric_ndx,
                entry_date=ns.date,
                numerator=ns.numerator,
                denominator=ns.denominator,
                value_stream_ndx=ns.value_stream_ndx,
                factory_ndx=factory,
            )
    except LeanDNAAppSessionError as e:
        print(str(e), file=sys.stderr)
        return 1
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    except requests.exceptions.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        return 1
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(json.dumps(env, indent=2, default=str))
    if not env.get("ok"):
        hint = env.get("hint")
        if hint:
            print(hint, file=sys.stderr)
        err = str(env.get("error") or "")
        if "mutations" in err.lower() and BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "production":
            print(
                "Production writes blocked — prefix command with BPO_ALLOW_PRODUCTION_MUTATIONS=true",
                file=sys.stderr,
            )
        elif env.get("status") == 401 or "session not found" in err.lower():
            print(
                "Bearer token expired or invalid — refresh PR_LEANDNA_DATA_API_BEARER_TOKEN "
                "from DevTools (Authorization header on any /api/data/… request while logged in).",
                file=sys.stderr,
            )
        elif env.get("status") == 504 or "504" in err:
            print(
                "LeanDNA returned 504 Gateway Timeout — retry in a minute; their API may be slow.",
                file=sys.stderr,
            )
    return 0 if env.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
