#!/usr/bin/env python3
"""List every ``config/my-metrics.yaml`` KPI carrying a tag, with current value.

Tags live under each metric's ``tags:`` list. One KPI may carry many tags. This
tool selects all KPIs for one tag and fetches their newest MetricDataPoint(s) from
the LeanDNA Data API (KPIs without a ``metric-id`` are listed without a value).

Examples::

  metrics-by-tag --tag engineering
  metrics-by-tag --tag ai --recent-count 5
  metrics-by-tag --list
  metrics-by-tag --tag delivery --json
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
from src.leandna_metric_registry_resolve import METRICS_REGISTRY_DEFAULT_SITE_ID  # noqa: E402
from src.leandna_metrics_cli import configure_cortex_logging  # noqa: E402
from src.metrics_latest import (  # noqa: E402
    DEFAULT_RECENT_DATAPOINT_COUNT,
    fetch_recent_datapoints_by_tag,
    format_metric_recent_block,
)
from src.metrics_registry import all_registry_tags  # noqa: E402

_DEFAULT_LOOKBACK_DAYS = 365
_READ_TIMEOUT_S = 60.0


def _print_tag_catalog() -> int:
    tags = all_registry_tags()
    if not tags:
        print("No tags defined in config/my-metrics.yaml.", file=sys.stderr)
        return 1
    print("Tags in config/my-metrics.yaml (tag: KPI count):")
    for tag, count in tags:
        print(f"  {tag}: {count}")
    return 0


def _row_to_json(row: object) -> dict[str, object]:
    return {
        "metric_name": row.metric_name,  # type: ignore[attr-defined]
        "metric_id": row.metric_id or None,  # type: ignore[attr-defined]
        "automated": row.automated,  # type: ignore[attr-defined]
        "tags": list(row.tags),  # type: ignore[attr-defined]
        "description": row.description,  # type: ignore[attr-defined]
        "error": row.error,  # type: ignore[attr-defined]
        "recent": [{"date": p.date, "value": p.value} for p in row.recent],  # type: ignore[attr-defined]
        "current_value": (row.recent[0].value if row.recent else None),  # type: ignore[attr-defined]
        "current_value_date": (row.recent[0].date if row.recent else None),  # type: ignore[attr-defined]
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List KPIs for a tag with current values (config/my-metrics.yaml + LeanDNA).",
    )
    ap.add_argument("--tag", metavar="NAME", help="Tag to filter KPIs by (normalized: lowercase, hyphenated)")
    ap.add_argument("--list", action="store_true", help="List all defined tags and their KPI counts, then exit")
    ap.add_argument("--json", action="store_true", help="Emit a JSON array of matching KPIs with current values")
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
        help=f"Newest datapoints to show per KPI (default: {DEFAULT_RECENT_DATAPOINT_COUNT})",
    )
    ap.add_argument("--timeout", type=float, default=_READ_TIMEOUT_S, metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    configure_cortex_logging(verbose=ns.verbose)

    if ns.list:
        return _print_tag_catalog()

    if not ns.tag:
        print("Provide --tag NAME (or --list to see available tags).", file=sys.stderr)
        return 2

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(
        f"LeanDNA target: GET /data/Metric/{{id}}/MetricDataPoint  "
        f"(tag={ns.tag!r}, lookback={ns.lookback_days}d, recent={ns.recent_count}, "
        f"requestedSites={ns.requested_sites!r}, "
        f"EXECUTION_ENV bucket={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    try:
        rows = fetch_recent_datapoints_by_tag(
            ns.tag,
            requested_sites=ns.requested_sites,
            lookback_days=ns.lookback_days,
            timeout_seconds=ns.timeout,
            limit=ns.recent_count,
        )
    except Exception as e:  # noqa: BLE001 — surface Data API/HTTP failures cleanly
        print(f"Failed to fetch datapoints for tag {ns.tag!r}: {e}", file=sys.stderr)
        return 1
    if not rows:
        available = ", ".join(tag for tag, _ in all_registry_tags()) or "(none)"
        print(f"No KPIs tagged {ns.tag!r}. Available tags: {available}", file=sys.stderr)
        return 1

    if ns.json:
        print(json.dumps([_row_to_json(row) for row in rows], indent=2, default=str, ensure_ascii=False))
    else:
        for index, row in enumerate(rows):
            if index:
                print()
            print("\n".join(format_metric_recent_block(row)))

    with_value = sum(1 for row in rows if row.recent)
    without_id = sum(1 for row in rows if row.metric_id <= 0)
    print(
        f"Tag {ns.tag!r}: {len(rows)} KPI(s) — {with_value} with a current value, "
        f"{without_id} without a metric-id.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
