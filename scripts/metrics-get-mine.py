#!/usr/bin/env python3
"""List LeanDNA metrics owned by the current user (Data API only).

Uses ``GET /data/identity`` for ``userId``, then ``GET /data/Metric`` filtered by ``ownerId``.
Optional ``--values``: ``GET /data/Metric/{id}/MetricDataPoint`` per owned metric.

Examples::

  metrics-get-mine
  metrics-get-mine --values
  metrics-get-mine --requested-sites 416
  metrics-get-mine --values --requested-sites 416
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_catalog import (  # noqa: E402
    MetricsCatalogError,
    build_my_metrics_payload,
    fetch_metric_datapoint_series,
    fetch_my_metric_definitions,
)
from src.leandna_metrics_cli import configure_cortex_logging  # noqa: E402
from src.leandna_metrics_client import metric_definition_label  # noqa: E402
from src.leandna_metrics_display import print_metric_value_chart  # noqa: E402

_VALUES_LOOKBACK_DAYS = 120
_VALUES_POINT_COUNT = 10
_READ_TIMEOUT_S = 60.0


def _print_value_charts(
    rows: list[dict[str, Any]],
    *,
    requested_sites: str | None,
    timeout_seconds: float,
) -> None:
    print()
    print(f"Last {_VALUES_POINT_COUNT} datapoints per metric:")
    for metric in rows:
        points, err = fetch_metric_datapoint_series(
            metric,
            lookback_days=_VALUES_LOOKBACK_DAYS,
            requested_sites=requested_sites,
            timeout_seconds=timeout_seconds,
        )
        if err is not None and not points:
            mid = metric.get("id")
            name = metric_definition_label(metric)
            print(f"\n=== {name} (id={mid}) ===")
            print(f"(datapoints unavailable: {err.get('error') if isinstance(err, dict) else err})")
            continue
        print_metric_value_chart(metric, points, max_points=_VALUES_POINT_COUNT)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List metrics owned by you (Data API: /data/identity + /data/Metric).",
    )
    ap.add_argument(
        "--values",
        action="store_true",
        help=f"Show ASCII chart of last {_VALUES_POINT_COUNT} date/value pairs per metric",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="RequestedSites header (default: sole site from identity when unambiguous)",
    )
    ap.add_argument("--timeout", type=float, default=_READ_TIMEOUT_S, metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    configure_cortex_logging(verbose=ns.verbose)

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        rows, identity, effective_sites = fetch_my_metric_definitions(
            requested_sites=ns.requested_sites,
            timeout_seconds=ns.timeout,
        )
    except MetricsCatalogError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    print(
        f"LeanDNA target: {base}/data/identity + /data/Metric  "
        f"(owner={identity.owner_label!r}, requestedSites={effective_sites!r}, "
        f"EXECUTION_ENV bucket={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )
    print(
        json.dumps(
            build_my_metrics_payload(rows, identity, requested_sites=effective_sites),
            indent=2,
            default=str,
            ensure_ascii=False,
        )
    )
    if ns.values:
        _print_value_charts(rows, requested_sites=effective_sites, timeout_seconds=ns.timeout)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
