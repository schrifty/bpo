#!/usr/bin/env python3
"""List LeanDNA metric definitions from ``GET /data/Metric`` (Data API).

Examples::

  metrics-get
  metrics-get --format brief
  metrics-get --requested-sites 416
  metrics-get 638
  metrics-get 638 --format brief
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
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_catalog import (  # noqa: E402
    MetricsCatalogError,
    format_metric_brief_lines,
    list_metric_definitions_filtered,
)
from src.leandna_metrics_cli import configure_bpo_logging, pop_leading_numeric_metric_id  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List LeanDNA metrics (GET /data/Metric).",
        epilog="If the first argument is all digits (e.g. metrics-get 638), it is treated as --metric-id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--format", choices=("json", "brief"), default="json")
    ap.add_argument("--requested-sites", default=None, metavar="ID")
    ap.add_argument("--metric-id", default=None, metavar="ID")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--connect-timeout", type=float, default=15.0, metavar="SEC")
    ap.add_argument("--timeout", type=float, default=120.0, dest="read_timeout", metavar="SEC")
    argv_mod, leading_metric_id = pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

    flag_metric_id = str(ns.metric_id).strip() if ns.metric_id is not None else None
    if flag_metric_id == "":
        flag_metric_id = None
    if leading_metric_id is not None and flag_metric_id is not None and leading_metric_id != flag_metric_id:
        print(
            f"Conflicting metric id: leading argument {leading_metric_id!r} vs "
            f"--metric-id {flag_metric_id!r}.",
            file=sys.stderr,
        )
        return 1
    metric_id_filter = flag_metric_id or leading_metric_id

    configure_bpo_logging(verbose=ns.verbose)

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    print(
        f"LeanDNA target: GET {base}/data/Metric  "
        f"(EXECUTION_ENV bucket: {BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    try:
        rows, _catalog_count = list_metric_definitions_filtered(
            metric_id=metric_id_filter,
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
        )
    except MetricsCatalogError as e:
        print(str(e), file=sys.stderr)
        return 1

    if ns.format == "brief":
        print("id\tname\tmetricType\tsiteId\tcategories\tvalueStreams")
        for ln in format_metric_brief_lines(rows):
            print(ln)
    else:
        single_object = metric_id_filter is not None and len(rows) == 1
        payload: Any = rows[0] if single_object else rows
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    print(f"Displayed {len(rows)} metric{'s' if len(rows) != 1 else ''}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
