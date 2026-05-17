#!/usr/bin/env python3
"""Show LeanDNA metric names with ``MetricDataPoint`` values over a date window.

Resolves metrics via ``GET /data/Metric`` (optional id / name filter), then for each metric
``GET /data/Metric/{id}/MetricDataPoint`` with ``startDate`` / ``endDate``.

Uses the same auth and ``EXECUTION_ENV`` rules as ``get-metrics.py``.

Examples::

  python3 scripts/get-metrics-data.py 2171
  python3 scripts/get-metrics-data.py "median ttr" --format brief
  python3 scripts/get-metrics-data.py
  python3 scripts/get-metrics-data.py --max-metrics 10
  python3 scripts/get-metrics-data.py 638 --start-date 2026-01-01 --end-date 2026-03-31
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
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
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


def _pop_leading_numeric_metric_id(argv: list[str]) -> tuple[list[str], str | None]:
    if len(argv) < 2:
        return argv, None
    token = argv[1]
    if token.startswith("-"):
        return argv, None
    s = token.strip()
    if s and s.isdigit():
        return [argv[0]] + argv[2:], s
    return argv, None


def _id_matches(raw_id: Any, want_s: str) -> bool:
    w = (want_s or "").strip()
    if not w:
        return False
    try:
        return int(raw_id) == int(w)
    except (TypeError, ValueError):
        return str(raw_id).strip() == w


def _is_catalog_id_token(s: str) -> bool:
    t = s.strip()
    return bool(t) and t.isdigit()


def _grep_name_substring(rows: list[Any], needle: str) -> list[dict]:
    n = (needle or "").strip().lower()
    if not n:
        return []
    out: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        blob = f"{r.get('name', '')!s} {r.get('crossSiteName', '')!s}".lower()
        if n in blob:
            out.append(r)
    return out


def _sort_metrics(rows: list[dict]) -> list[dict]:
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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Metric names + MetricDataPoint values (GET /data/Metric + …/MetricDataPoint).",
        epilog="No filter = all catalog metrics (same as --all). Leading digits = id; other text = name substring.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "filter",
        nargs="?",
        default=None,
        metavar="ID_OR_SUBSTRING",
        help="Optional: catalog id (digits) or name substring; default with no arg = all metrics",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="All catalog metrics (default when no filter is given; respect --max-metrics)",
    )
    ap.add_argument(
        "--max-metrics",
        type=int,
        default=50,
        metavar="N",
        help="Cap how many catalog metrics to fetch when no filter (default: 50)",
    )
    ap.add_argument(
        "--format",
        choices=("json", "brief", "table"),
        default="brief",
        help="brief: section per metric (default); table: one TSV row per datapoint; json: array",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        metavar="N",
        help="Window length ending today if --start-date/--end-date omitted (default: 90)",
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
        help="RequestedSites header (default: each metric's siteId when set)",
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
        help="TCP/TLS connect timeout for catalog GET (default: 15)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        dest="read_timeout",
        metavar="SEC",
        help="Read timeout per HTTP GET (default: 120)",
    )
    argv_mod, leading_id = _pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

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

    try:
        start_s, end_s = resolve_metric_datapoint_window(
            lookback_days=ns.lookback_days,
            start_date=ns.start_date,
            end_date=ns.end_date,
        )
    except ValueError as e:
        print(f"Invalid date: {e}", file=sys.stderr)
        return 1

    filter_token = (leading_id or (str(ns.filter).strip() if ns.filter else None)) or None
    if filter_token == "":
        filter_token = None

    use_all = ns.all or filter_token is None
    if ns.all and filter_token:
        print("Use either --all or a filter argument, not both.", file=sys.stderr)
        return 1

    print(
        f"LeanDNA target: GET {base}/data/Metric + GET {{id}}/MetricDataPoint  "
        f"window {start_s!r}..{end_s!r}  (EXECUTION_ENV bucket: {BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    try:
        catalog = list_metric_definitions(
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
            extra_query=None,
        )
    except requests.Timeout as e:
        print(f"LeanDNA catalog request timed out: {e}", file=sys.stderr)
        return 1
    except requests.HTTPError as e:
        print(f"Failed to fetch metric catalog: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Failed to fetch metric catalog: {e}", file=sys.stderr)
        return 1

    catalog_count = len(catalog)
    metrics = [m for m in catalog if isinstance(m, dict)]

    if use_all:
        metrics = _sort_metrics(metrics)[: max(1, ns.max_metrics)]
        if catalog_count > len(metrics):
            print(
                f"Limiting to {len(metrics)} of {catalog_count} catalog metrics (--max-metrics).",
                file=sys.stderr,
            )
    elif filter_token and _is_catalog_id_token(filter_token):
        metrics = [m for m in metrics if _id_matches(m.get("id"), filter_token)]
        if not metrics:
            print(
                f"No metric with id={filter_token!r} in catalog ({catalog_count} definition(s)).",
                file=sys.stderr,
            )
            return 1
    else:
        metrics = _sort_metrics(_grep_name_substring(metrics, filter_token or ""))
        if not metrics:
            print(
                f"No metrics matched substring {filter_token!r} ({catalog_count} in catalog).",
                file=sys.stderr,
            )
            return 1

    results: list[dict[str, Any]] = []
    fetch_errors = 0

    for m in metrics:
        mid = m.get("id")
        name = _metric_name(m)
        sites = _requested_sites_for_metric(m, ns.requested_sites)
        points, err = fetch_metric_datapoints(
            mid,
            start_date=start_s,
            end_date=end_s,
            requested_sites=sites,
            timeout_seconds=ns.read_timeout,
        )
        if err is not None:
            fetch_errors += 1
            print(
                f"Failed MetricDataPoint for id={mid!r} name={name!r}: HTTP {err.get('status')} {err.get('error')!r}",
                file=sys.stderr,
            )
            if ns.verbose:
                print(f"  detail: {err!r}", file=sys.stderr)
            continue
        slim_points = slim_metric_datapoint_rows(points)
        results.append(
            {
                "id": mid,
                "name": name,
                "siteId": m.get("siteId"),
                "window": {"startDate": start_s, "endDate": end_s},
                "values": slim_points,
            }
        )

    if not results and fetch_errors:
        print("No metric data retrieved (all MetricDataPoint GETs failed).", file=sys.stderr)
        return 1

    if ns.format == "json":
        print(json.dumps(results, indent=2, default=str, ensure_ascii=False))
    elif ns.format == "table":
        print_metrics_datapoint_table(results, values_key="values")
    else:
        print_metrics_grouped_display(
            results,
            values_key="values",
            include_json_definition=False,
        )

    total_points = sum(len(r.get("values") or []) for r in results)
    print(
        f"Displayed {len(results)} metric(s), {total_points} datapoint row(s)"
        + (f"; {fetch_errors} MetricDataPoint fetch(es) failed" if fetch_errors else "")
        + ".",
        file=sys.stderr,
    )
    return 0 if results else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
