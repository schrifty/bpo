#!/usr/bin/env python3
"""Recent MetricDataPoint values for each ``config/my-metrics.yaml`` row with a ``metric-id``.

Examples::

  metrics-get-latest
  metrics-get-latest --requested-sites 416
  metrics-get-latest --recent-count 5
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metric_registry_resolve import METRICS_REGISTRY_DEFAULT_SITE_ID  # noqa: E402
from src.leandna_metrics_cli import configure_bpo_logging  # noqa: E402
from src.metrics_latest import (  # noqa: E402
    DEFAULT_RECENT_DATAPOINT_COUNT,
    fetch_registry_recent_datapoints,
    format_metric_recent_block,
)
from src.metrics_registry import count_automated_metrics  # noqa: E402

_DEFAULT_LOOKBACK_DAYS = 365
_READ_TIMEOUT_S = 60.0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Recent MetricDataPoint date/value rows for my-metrics.yaml entries with metric-id.",
    )
    ap.add_argument(
        "--requested-sites",
        default=str(METRICS_REGISTRY_DEFAULT_SITE_ID),
        metavar="ID",
        help=f"RequestedSites header (default: {METRICS_REGISTRY_DEFAULT_SITE_ID})",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=_DEFAULT_LOOKBACK_DAYS,
        metavar="N",
        help=f"Search window ending today (default: {_DEFAULT_LOOKBACK_DAYS})",
    )
    ap.add_argument(
        "--recent-count",
        type=int,
        default=DEFAULT_RECENT_DATAPOINT_COUNT,
        metavar="N",
        help=f"Number of newest datapoints to show per metric (default: {DEFAULT_RECENT_DATAPOINT_COUNT})",
    )
    ap.add_argument(
        "--names-only",
        action="store_true",
        help="Print only indented {date}: {value} lines (omit metric headers)",
    )
    ap.add_argument("--timeout", type=float, default=_READ_TIMEOUT_S, metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    configure_bpo_logging(verbose=ns.verbose)

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(
        f"LeanDNA target: GET /data/Metric/{{id}}/MetricDataPoint  "
        f"(lookback={ns.lookback_days}d, recent={ns.recent_count}, "
        f"requestedSites={ns.requested_sites!r}, "
        f"EXECUTION_ENV bucket={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    rows = fetch_registry_recent_datapoints(
        requested_sites=ns.requested_sites,
        lookback_days=ns.lookback_days,
        timeout_seconds=ns.timeout,
        limit=ns.recent_count,
    )
    if not rows:
        print("No metrics with metric-id in config/my-metrics.yaml.", file=sys.stderr)
        return 1

    failures = 0
    shown_automated = 0
    for index, row in enumerate(rows):
        if row.error and not row.recent:
            failures += 1
        if row.automated:
            shown_automated += 1
        block = format_metric_recent_block(row)
        if ns.names_only:
            block = [line for line in block if line.startswith("  ")]
        if index and block:
            print()
        print("\n".join(block))

    automated_total, registry_total = count_automated_metrics()
    print(
        f"Fetched up to {ns.recent_count} datapoint(s) for {len(rows)} metric(s) with a metric-id "
        f"({shown_automated} automated). "
        f"Registry automation: {automated_total} of {registry_total} KPI(s) automated.",
        file=sys.stderr,
    )
    return 1 if failures == len(rows) else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
