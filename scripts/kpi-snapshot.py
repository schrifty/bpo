#!/usr/bin/env python3
"""Generate ``config/my-metrics.yaml`` KPIs into the SQLite KPI store.

Examples::

  kpi-snapshot --dry-run
  kpi-snapshot --tag engineering --dry-run
  kpi-snapshot --tag support --history-months 24 --skip-s3
  kpi-snapshot --skip-s3 --db /tmp/kpi.sqlite
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

from src.kpi_snapshot import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
