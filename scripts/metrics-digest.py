#!/usr/bin/env python3
"""Live-generate ``config/my-metrics.yaml`` KPIs with generators and email a digest.

Examples::

  metrics-report                 # columnar report to stdout (no email)
  metrics-digest --dry-run       # same as metrics-report
  metrics-digest                 # generate and email via SES
  metrics-digest --days 30 --timeout 180
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

from src.metrics_digest import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
