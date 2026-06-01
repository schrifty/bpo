#!/usr/bin/env python3
"""List LeanDNA metrics owned by the current user (Data API).

Uses ``GET /data/identity`` for ``userId``, then filters ``GET /data/Metric`` by ``ownerId``.
Optional ``--values``: ASCII chart of the last 10 datapoints per metric.

Examples::

  metrics-get-mine
  metrics-get-mine --values
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

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_metrics_catalog import (  # noqa: E402
    MetricsCatalogError,
    build_my_metrics_payload,
    fetch_metric_datapoint_series,
    fetch_my_metric_definitions,
)
from src.leandna_metrics_client import metric_definition_label  # noqa: E402
from src.leandna_metrics_display import print_metric_value_chart  # noqa: E402

_VALUES_LOOKBACK_DAYS = 120
_VALUES_POINT_COUNT = 10
_READ_TIMEOUT_S = 60.0


def _print_value_charts(rows: list[dict[str, Any]]) -> None:
    print()
    print(f"Last {_VALUES_POINT_COUNT} datapoints per metric:")
    for metric in rows:
        points, err = fetch_metric_datapoint_series(
            metric,
            lookback_days=_VALUES_LOOKBACK_DAYS,
            timeout_seconds=_READ_TIMEOUT_S,
        )
        if err is not None and not points:
            mid = metric.get("id")
            name = metric_definition_label(metric)
            print(f"\n=== {name} (id={mid}) ===")
            print(f"(datapoints unavailable: {err.get('error') if isinstance(err, dict) else err})")
            continue
        print_metric_value_chart(metric, points, max_points=_VALUES_POINT_COUNT)


def main() -> int:
    ap = argparse.ArgumentParser(description="List metrics owned by you (Data API identity).")
    ap.add_argument(
        "--values",
        action="store_true",
        help=f"Show ASCII chart of last {_VALUES_POINT_COUNT} date/value pairs per metric",
    )
    ap.add_argument("--requested-sites", default=None, metavar="ID")
    ap.add_argument("--timeout", type=float, default=_READ_TIMEOUT_S, metavar="SEC")
    ns = ap.parse_args()

    try:
        rows, identity = fetch_my_metric_definitions(
            requested_sites=ns.requested_sites,
            timeout_seconds=ns.timeout,
        )
    except MetricsCatalogError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    print(
        f"Auth: LeanDNA Data API (/data/identity + /data/Metric, owner={identity.owner_label!r}, "
        f"EXECUTION_ENV={BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )
    print(json.dumps(build_my_metrics_payload(rows, identity), indent=2, default=str, ensure_ascii=False))
    if ns.values:
        _print_value_charts(rows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
