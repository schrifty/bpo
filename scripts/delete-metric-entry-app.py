#!/usr/bin/env python3
"""Remove/disable a daily metric entry via app API (DELETE or PUT enabled=false).

Builds the same entry body shape as :func:`build_metric_entry_put_body` from current
``GET …/MetricEntries`` when ``--from-existing`` is set, otherwise requires explicit
numerator/denominator/value-stream (to match the row you want removed).

Examples::

  python3 scripts/delete-metric-entry-app.py --metric-ndx 638 --date 2026-05-12 --from-existing
  python3 scripts/delete-metric-entry-app.py --metric-ndx 638 --date 2026-05-12 \\
      --value-stream-ndx 0 --numerator 33 --denominator 1
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
    delete_metric_entries,
    get_metric_entries_for_date,
    switch_site,
)
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="DELETE / disable MetricEntries for one date.")
    ap.add_argument("--metric-ndx", type=int, required=True)
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--value-stream-ndx", type=int, default=None)
    ap.add_argument("--numerator", type=float, default=None)
    ap.add_argument("--denominator", type=float, default=None)
    ap.add_argument(
        "--from-existing",
        action="store_true",
        help="Load existing row for that date/metric from GET MetricEntries",
    )
    ap.add_argument("--factory-ndx", type=int, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    if not leandna_app_session_configured():
        print("Missing LEANDNA_APP_SESSION_ID or LDNASESSIONID cookie.", file=sys.stderr)
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    switch_site(factory)

    if ns.from_existing:
        rows = get_metric_entries_for_date(ns.date, metric_ndx=ns.metric_ndx, factory_ndx=factory)
        body = []
        for r in rows:
            try:
                if int(r.get("metricNdx")) != ns.metric_ndx:
                    continue
            except (TypeError, ValueError):
                continue
            if ns.value_stream_ndx is not None:
                try:
                    if int(r.get("valueStreamNdx", 0) or 0) != ns.value_stream_ndx:
                        continue
                except (TypeError, ValueError):
                    continue
            body.append(r)
        if not body:
            print(f"No existing entry for ndx={ns.metric_ndx} date={ns.date!r}.", file=sys.stderr)
            return 1
    else:
        if ns.value_stream_ndx is None or ns.numerator is None or ns.denominator is None:
            print(
                "Without --from-existing, pass --value-stream-ndx, --numerator, and --denominator.",
                file=sys.stderr,
            )
            return 1
        body = build_metric_entry_put_body(
            metric_ndx=ns.metric_ndx,
            value_stream_ndx=ns.value_stream_ndx,
            entry_date=ns.date,
            numerator=ns.numerator,
            denominator=ns.denominator,
            factory_ndx=factory,
        )

    env = delete_metric_entries(ns.date, body, factory_ndx=factory, switch_site_first=False)
    print(json.dumps(env, indent=2, default=str))
    return 0 if env.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
