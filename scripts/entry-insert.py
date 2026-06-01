#!/usr/bin/env python3
"""Insert one daily metric value (fails if the date already exists).

Uses **Data API** ``POST /data/Metric/{id}/MetricDataPoint`` when bearer/cookie is
configured (preferred). Falls back to app API ``PUT …/MetricEntries`` when only
``LEANDNA_APP_SESSION_ID`` is available. Use ``entry-upsert`` to replace an existing date.

Examples::

  entry-insert --metric-ndx 2076 --date 2026-05-22 --numerator 1 --denominator 100
  BPO_ALLOW_PRODUCTION_MUTATIONS=true entry-insert \\
    --metric-ndx 2076 --date 2026-05-22 --numerator 85 --denominator 100 \\
    --requested-sites 416
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

from src.leandna_metric_write_cli import (  # noqa: E402
    add_metric_write_arguments,
    metric_write_args_from_namespace,
    print_result_env,
    run_insert,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Insert one metric value (409 if date already exists; use entry-upsert to replace).",
    )
    add_metric_write_arguments(ap)
    ns = ap.parse_args()
    code, env = run_insert(metric_write_args_from_namespace(ns))
    print_result_env(env)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
