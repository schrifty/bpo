#!/usr/bin/env python3
"""Metric definitions + ``MetricDataPoint`` values over a date window (Data API).

Examples::

  metric-get-with-data 2076 --start-date 2026-05-23 --end-date 2026-05-23 --requested-sites 416
  metric-get-with-data "job success" --format brief
  metric-get-with-data --max-metrics 10
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_catalog import MetricsCatalogError, fetch_metrics_with_datapoints  # noqa: E402
from src.leandna_metrics_cli import configure_cortex_logging, pop_leading_numeric_metric_id  # noqa: E402
from src.leandna_metrics_display import (  # noqa: E402
    print_metrics_datapoint_table,
    print_metrics_grouped_display,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Metric names + MetricDataPoint values (GET /data/Metric + …/MetricDataPoint).",
        epilog="No filter = all catalog metrics (same as --all). Leading digits = id; other text = name substring.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("filter", nargs="?", default=None, metavar="ID_OR_SUBSTRING")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--max-metrics", type=int, default=50, metavar="N")
    ap.add_argument("--format", choices=("json", "brief", "table"), default="brief")
    ap.add_argument("--lookback-days", type=int, default=90, metavar="N")
    ap.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--requested-sites", default=None, metavar="ID")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--connect-timeout", type=float, default=15.0, metavar="SEC")
    ap.add_argument("--timeout", type=float, default=120.0, dest="read_timeout", metavar="SEC")
    argv_mod, leading_id = pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

    configure_cortex_logging(verbose=ns.verbose)

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    filter_token = (leading_id or (str(ns.filter).strip() if ns.filter else None)) or None
    if filter_token == "":
        filter_token = None
    use_all = ns.all or filter_token is None
    if ns.all and filter_token:
        print("Use either --all or a filter argument, not both.", file=sys.stderr)
        return 1

    try:
        results, fetch_errors, start_s, end_s = fetch_metrics_with_datapoints(
            filter_token=filter_token,
            use_all=use_all,
            max_metrics=ns.max_metrics,
            start_date=ns.start_date,
            end_date=ns.end_date,
            lookback_days=ns.lookback_days,
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
        )
    except MetricsCatalogError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(
        f"LeanDNA target: GET {base}/data/Metric + GET {{id}}/MetricDataPoint  "
        f"window {start_s!r}..{end_s!r}  (EXECUTION_ENV bucket: {CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    if not results and fetch_errors:
        print("No metric data retrieved (all MetricDataPoint GETs failed).", file=sys.stderr)
        return 1

    if ns.format == "json":
        print(json.dumps(results, indent=2, default=str, ensure_ascii=False))
    elif ns.format == "table":
        print_metrics_datapoint_table(results, values_key="values")
    else:
        print_metrics_grouped_display(results, values_key="values", include_json_definition=False)

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
