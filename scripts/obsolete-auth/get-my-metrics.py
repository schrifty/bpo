#!/usr/bin/env python3
"""List LeanDNA metric definitions owned by the current API user, with optional data series.

Calls ``GET /data/identity`` for ``userId``, then ``GET /data/Metric`` and keeps rows
whose ``ownerId`` matches that user. By default, also fetches ``GET /data/Metric/{id}/MetricDataPoint``
for each owned metric over a date window (same auth / ``EXECUTION_ENV`` rules as ``get-metrics.py``).

Examples::

  python3 scripts/get-my-metrics.py
  python3 scripts/get-my-metrics.py --format json
  python3 scripts/get-my-metrics.py --format brief
  python3 scripts/get-my-metrics.py --no-data
  python3 scripts/get-my-metrics.py --requested-sites 416
  python3 scripts/get-my-metrics.py --lookback-days 30
  python3 scripts/get-my-metrics.py --user-id 75321
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_http import leandna_data_api_credentials_configured  # noqa: E402
from src.leandna_data_api_request import data_api_base_url, data_api_get_json  # noqa: E402
from src.leandna_metrics_client import (  # noqa: E402
    fetch_metric_datapoints,
    list_metric_definitions,
    resolve_metric_datapoint_window,
    slim_metric_datapoint_rows,
)
from src.leandna_metrics_display import (  # noqa: E402
    print_metrics_datapoint_table,
    print_metrics_grouped_display,
)


def _id_matches(raw_id: Any, want_s: str) -> bool:
    w = (want_s or "").strip()
    if not w:
        return False
    try:
        return int(raw_id) == int(w)
    except (TypeError, ValueError):
        return str(raw_id).strip() == w


def _sort_rows(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("id")
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _metric_name(m: dict[str, Any]) -> str:
    return str(m.get("name") or m.get("crossSiteName") or m.get("id") or "").strip()


def _requested_sites_for_metric(metric: dict[str, Any], cli_sites: str | None) -> str | None:
    if cli_sites is not None and str(cli_sites).strip():
        return str(cli_sites).strip()
    sid = metric.get("siteId")
    if sid is None:
        return None
    s = str(sid).strip()
    return s or None


def _latest_point(series: list[dict[str, Any]]) -> tuple[str, Any]:
    if not series:
        return "", None
    last = series[-1]
    return str(last.get("dataPointDate") or ""), last.get("value")


def _brief_lines(rows: list[dict]) -> list[str]:
    lines = []
    for r in rows:
        mid = r.get("id", "")
        name = _metric_name(r).replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        site = str(r.get("siteId") or "")
        owner = str(r.get("ownerId") or "")
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
        series = r.get("dataSeries") if isinstance(r.get("dataSeries"), list) else []
        latest_d, latest_v = _latest_point(series)
        err = r.get("dataSeriesError")
        err_s = ""
        if isinstance(err, dict):
            err_s = str(err.get("error") or err.get("status") or "error")
        lines.append(
            f"{mid}\t{name}\t{mtype}\tsiteId={site}\townerId={owner}\t"
            f"points={len(series)}\tlatest={latest_d}\tlatest_value={latest_v}\t"
            f"categories={cat_s}\tvalueStreams={vs_s}"
            + (f"\tdata_error={err_s}" if err_s else "")
        )
    return lines


def _identity_user_id(body: Any) -> str | None:
    if not isinstance(body, dict):
        return None
    raw = body.get("userId")
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _identity_display_name(body: dict[str, Any]) -> str:
    for key in ("userName", "emailAddress"):
        v = body.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _fetch_identity(
    *,
    requested_sites: str | None,
    read_timeout_seconds: float,
) -> dict[str, Any]:
    env = data_api_get_json(
        "identity",
        requested_sites=requested_sites,
        timeout_seconds=read_timeout_seconds,
        user_agent_suffix="get-my-metrics/1.0",
    )
    if not env.get("ok"):
        raise RuntimeError(f"GET /data/identity failed: {env!r}")
    body = env.get("body")
    if not isinstance(body, dict):
        raise RuntimeError(f"GET /data/identity returned unexpected body: {type(body).__name__}")
    return body


def _attach_data_series(
    rows: list[dict[str, Any]],
    *,
    start_s: str,
    end_s: str,
    requested_sites: str | None,
    read_timeout_seconds: float,
) -> tuple[list[dict[str, Any]], int]:
    """Return metric dicts with ``dataSeries`` / optional ``dataSeriesError``; count fetch failures."""
    out: list[dict[str, Any]] = []
    failures = 0
    window = {"startDate": start_s, "endDate": end_s}
    for m in rows:
        block = dict(m)
        mid = m.get("id")
        sites = _requested_sites_for_metric(m, requested_sites)
        points, err = fetch_metric_datapoints(
            mid,
            start_date=start_s,
            end_date=end_s,
            requested_sites=sites,
            timeout_seconds=read_timeout_seconds,
        )
        block["dataWindow"] = window
        if err is not None:
            failures += 1
            block["dataSeries"] = []
            block["dataSeriesError"] = {
                "status": err.get("status"),
                "error": err.get("error"),
            }
            print(
                f"Failed MetricDataPoint for id={mid!r} name={_metric_name(m)!r}: "
                f"HTTP {err.get('status')} {err.get('error')!r}",
                file=sys.stderr,
            )
        else:
            block["dataSeries"] = slim_metric_datapoint_rows(points)
        out.append(block)
    return out, failures


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "List LeanDNA metrics where ownerId matches the current API user; "
            "includes MetricDataPoint series by default."
        ),
    )
    ap.add_argument(
        "--format",
        choices=("display", "json", "brief", "table"),
        default="display",
        help=(
            "display: JSON metric definition + datapoint table per metric (default); "
            "json: single machine-readable payload; "
            "brief: one TSV summary line per metric; "
            "table: one TSV row per datapoint"
        ),
    )
    ap.add_argument(
        "--no-data",
        action="store_true",
        help="Catalog definitions only (skip MetricDataPoint GETs)",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        metavar="N",
        help="Data series window when dates omitted (default: 90)",
    )
    ap.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="MetricDataPoint window start (ISO date)",
    )
    ap.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="MetricDataPoint window end (ISO date; default: today)",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="Value for RequestedSites header on identity + Metric GETs (default: each metric's siteId)",
    )
    ap.add_argument(
        "--user-id",
        default=None,
        metavar="ID",
        help="Skip identity GET; filter catalog by this ownerId (debug / override)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO logging from BPO clients",
    )
    ap.add_argument(
        "--connect-timeout",
        type=float,
        default=15.0,
        metavar="SEC",
        help="TCP/TLS connect timeout (default: 15)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        dest="read_timeout",
        metavar="SEC",
        help="Read timeout after connect (default: 120)",
    )
    ns = ap.parse_args()

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    if not leandna_data_api_credentials_configured():
        print(
            "Missing LeanDNA Data API credentials — set LEANDNA_DATA_API_BEARER_TOKEN and/or "
            "LEANDNA_DATA_API_COOKIE in .env.",
            file=sys.stderr,
        )
        return 1

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    bucket = BPO_LEANDNA_DATA_API_EXECUTION_BUCKET
    print(
        f"LeanDNA target: GET {base}/data/identity + GET {base}/data/Metric"
        + ("" if ns.no_data else f" + GET {{id}}/MetricDataPoint")
        + f"  (EXECUTION_ENV bucket: {bucket})",
        file=sys.stderr,
    )

    owner_id = (str(ns.user_id).strip() if ns.user_id is not None else "") or None

    if owner_id is None:
        try:
            identity = _fetch_identity(
                requested_sites=ns.requested_sites,
                read_timeout_seconds=ns.read_timeout,
            )
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 1
        except requests.Timeout as e:
            print(f"LeanDNA identity request timed out: {e}", file=sys.stderr)
            return 1
        owner_id = _identity_user_id(identity)
        if not owner_id:
            print("GET /data/identity did not return userId.", file=sys.stderr)
            return 1
        owner_label = _identity_display_name(identity)
        print(
            f"Session user: {owner_label or '(no userName)'} (userId={owner_id})",
            file=sys.stderr,
        )
    else:
        print(f"Using --user-id {owner_id!r} (identity GET skipped).", file=sys.stderr)

    try:
        catalog = list_metric_definitions(
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
            extra_query=None,
        )
    except requests.Timeout as e:
        print(f"LeanDNA Metric catalog request timed out: {e}", file=sys.stderr)
        return 1
    except requests.HTTPError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        code = getattr(e.response, "status_code", None) if e.response is not None else None
        if code == 401:
            print(
                "LeanDNA returned 401 — align base URL and credentials with EXECUTION_ENV (ST_/PR_*).",
                file=sys.stderr,
            )
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    catalog_count = len(catalog)
    rows = _sort_rows(
        [
            r
            for r in catalog
            if isinstance(r, dict) and _id_matches(r.get("ownerId"), owner_id)
        ]
    )

    data_failures = 0
    window_meta: dict[str, str] | None = None
    if not ns.no_data and rows:
        try:
            start_s, end_s = resolve_metric_datapoint_window(
                lookback_days=ns.lookback_days,
                start_date=ns.start_date,
                end_date=ns.end_date,
            )
        except ValueError as e:
            print(f"Invalid date: {e}", file=sys.stderr)
            return 1
        window_meta = {"startDate": start_s, "endDate": end_s}
        print(f"Data window: {start_s!r} .. {end_s!r}", file=sys.stderr)
        rows, data_failures = _attach_data_series(
            rows,
            start_s=start_s,
            end_s=end_s,
            requested_sites=ns.requested_sites,
            read_timeout_seconds=ns.read_timeout,
        )

    if ns.format == "display":
        if ns.no_data:
            for block in rows:
                print(
                    json.dumps(block, indent=2, default=str, ensure_ascii=False),
                )
                print()
        else:
            print_metrics_grouped_display(rows, values_key="dataSeries")
    elif ns.format == "brief":
        header = (
            "id\tname\tmetricType\tsiteId\townerId\tpoints\tlatest\tlatest_value\t"
            "categories\tvalueStreams"
        )
        if not ns.no_data:
            header += "\tdata_error"
        print(header)
        for ln in _brief_lines(rows):
            print(ln)
    elif ns.format == "table":
        if ns.no_data:
            print("table format requires MetricDataPoint data (omit --no-data).", file=sys.stderr)
            return 1
        print_metrics_datapoint_table(rows, values_key="dataSeries")
    else:
        payload: Any = rows
        if not ns.no_data and window_meta is not None:
            payload = {
                "ownerId": owner_id,
                "dataWindow": window_meta,
                "metrics": rows,
            }
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    n = len(rows)
    total_points = sum(len(r.get("dataSeries") or []) for r in rows if isinstance(r, dict))
    msg = (
        f"Displayed {n} metric{'s' if n != 1 else ''} owned by userId={owner_id!r} "
        f"(of {catalog_count} in catalog)"
    )
    if not ns.no_data:
        msg += f"; {total_points} datapoint(s)"
        if data_failures:
            msg += f"; {data_failures} MetricDataPoint fetch(es) failed"
    print(msg + ".", file=sys.stderr)
    return 0 if n else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
