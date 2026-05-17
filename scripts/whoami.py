#!/usr/bin/env python3
"""Show LeanDNA app session info and metricOwner labels (to configure get-my-metrics).

The classic app API does not expose ``GET /api/data/identity`` with session-only auth (401).
Use the ``metricOwner`` display name from this output in ``LEANDNA_APP_METRIC_OWNER``.

Examples::

  python3 scripts/whoami.py
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

from src.config import (  # noqa: E402
    LEANDNA_APP_API_SERVER,
    LEANDNA_APP_FACTORY_NDX,
    LEANDNA_APP_METRIC_OWNER,
    LEANDNA_APP_USER_NDX,
)
from src.leandna_app_metrics_client import list_metric_owner_histogram  # noqa: E402
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402


def main() -> int:
    if not leandna_app_session_configured():
        print("Missing LEANDNA_APP_SESSION_ID or LDNASESSIONID cookie.", file=sys.stderr)
        return 1

    print(f"Host: {LEANDNA_APP_API_SERVER}  factoryNdx: {LEANDNA_APP_FACTORY_NDX}")
    print("GET /api/data/identity with session cookie typically returns 401 (expected).")
    print()
    if LEANDNA_APP_METRIC_OWNER:
        print(f"LEANDNA_APP_METRIC_OWNER (configured): {LEANDNA_APP_METRIC_OWNER!r}")
    if LEANDNA_APP_USER_NDX:
        print(
            f"LEANDNA_APP_USER_NDX (configured): {LEANDNA_APP_USER_NDX!r}  "
            "— if this is a kpi numeric id (e.g. 29036), it is probably NOT your app metricOwner."
        )
    print()
    print("metricOwner labels on Metrics/View (pick yours for .env):")
    try:
        hist = list_metric_owner_histogram()
    except Exception as e:
        print(f"Failed to list metrics: {e}", file=sys.stderr)
        return 1
    for label, count in hist:
        mark = "  <-- configured" if LEANDNA_APP_METRIC_OWNER and label == LEANDNA_APP_METRIC_OWNER else ""
        print(f"  {label!r}: {count} metric(s){mark}")
    print()
    print('Set in .env:  LEANDNA_APP_METRIC_OWNER="Your Name As Shown Above"')
  # remove LEANDNA_APP_USER_NDX unless you know the numeric id applies.')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
