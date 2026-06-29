#!/usr/bin/env python3
"""Delete one daily metric datapoint via Data API.

Examples::

  entry-delete --metric-ndx 2076 --date 2026-05-23 --requested-sites 416
  CORTEX_ALLOW_PRODUCTION_MUTATIONS=true entry-delete --metric-ndx 2076 --date 2026-05-23
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

from src.leandna_metrics_cli import (  # noqa: E402
    add_metric_delete_arguments,
    metric_delete_args_from_namespace,
    print_result_env,
)
from src.leandna_metrics_write import run_delete  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="DELETE MetricDataPoint for one date (Data API).")
    add_metric_delete_arguments(ap)
    code, env = run_delete(metric_delete_args_from_namespace(ap.parse_args()))
    print_result_env(env)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
