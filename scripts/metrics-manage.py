#!/usr/bin/env python3
"""Add, edit, delete, or show KPI definitions in ``config/my-metrics.yaml``.

Examples::

  cortex kpi add "My KPI" --tags engineering --description "..."
  cortex kpi edit "My KPI" --target 10 --direction lower
  cortex kpi delete "My KPI" --yes
  cortex kpi show "My KPI"
  metrics-manage add "My KPI" --tags engineering --description "..."
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from src.kpi_manage_cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
