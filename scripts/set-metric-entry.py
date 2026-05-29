#!/usr/bin/env python3
"""Write one daily metric entry via app API ``PUT …/MetricEntries`` (kpi-style).

Examples::

  python3 scripts/set-metric-entry.py --metric-ndx 638 --date 2026-05-12 --numerator 33 --denominator 1
  python3 scripts/set-metric-entry.py --metric-ndx 638 --date 2026-05-12 --numerator 10 --denominator 0
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import LEANDNA_APP_FACTORY_NDX  # noqa: E402
from src.leandna_app_metrics_client import (  # noqa: E402
    build_metric_entry_put_body,
    list_metrics_view,
    pick_metric_by_ndx,
    put_metric_entries,
)
from src.leandna_app_metrics_http import (  # noqa: E402
    LeanDNAAppSessionError,
    leandna_app_session_configured,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="PUT MetricEntries for one date (app session auth).")
    ap.add_argument("--metric-ndx", type=int, required=True)
    ap.add_argument(
        "--value-stream-ndx",
        type=int,
        default=None,
        help="Skip Metrics/View lookup when set (required if session cannot list metrics)",
    )
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--numerator", type=float, required=True)
    ap.add_argument("--denominator", type=float, default=1.0)
    ap.add_argument("--factory-ndx", type=int, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    if not leandna_app_session_configured():
        print("Missing LEANDNA_APP_SESSION_ID or LDNASESSIONID cookie.", file=sys.stderr)
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    vs = ns.value_stream_ndx
    if vs is None:
        try:
            rows = list_metrics_view(factory_ndx=factory)
            metric = pick_metric_by_ndx(rows, ns.metric_ndx)
            if metric is not None and metric.get("valueStreamNdx") is not None:
                vs = int(metric["valueStreamNdx"])
                print(f"Using valueStreamNdx={vs} from Metrics/View.", file=sys.stderr)
            else:
                vs = 0
                print(
                    "Warning: --value-stream-ndx not set and not in catalog row; using 0.",
                    file=sys.stderr,
                )
        except LeanDNAAppSessionError as e:
            print(str(e), file=sys.stderr)
            return 1
    body = build_metric_entry_put_body(
        metric_ndx=ns.metric_ndx,
        value_stream_ndx=vs,
        entry_date=ns.date,
        numerator=ns.numerator,
        denominator=ns.denominator,
        factory_ndx=factory,
    )
    env = put_metric_entries(ns.date, body, factory_ndx=factory)
    print(json.dumps(env, indent=2, default=str))
    if not env.get("ok"):
        err = str(env.get("error") or "")
        if "401" in err or "rejected" in err.lower() or "session" in err.lower():
            print(
                "App session expired or invalid — run bin/test-script --show-session "
                "and update LEANDNA_APP_SESSION_ID in .env.",
                file=sys.stderr,
            )
    return 0 if env.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
