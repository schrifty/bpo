#!/usr/bin/env python3
"""Run ``config/metrics.yaml`` generators and upsert LeanDNA MetricDataPoint rows.

For each registry row with ``metric-generator`` set, call the generator and upsert
the value for ``--date`` (default today). Rows without a generator are skipped.

Examples::

  metrics-upsert --dry-run
  metrics-upsert --format json
  metrics-upsert --metric "KPI Automation %"
  EXECUTION_ENV=Staging metrics-upsert --requested-sites 416
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.metrics_upsert import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
