#!/usr/bin/env python3
"""List LeanDNA metrics via the **classic app API** (``GET …/Metrics/View``).

Auth: browser session — ``LEANDNA_APP_SESSION_ID`` or cookie containing ``LDNASESSIONID=``
(same as ``kpi/update-kpi``). **No Data API Bearer token.**

Examples::

  python3 scripts/get-metrics.py
  python3 scripts/get-metrics.py --format brief
  python3 scripts/get-metrics.py 638
  python3 scripts/get-metrics.py --factory-ndx 416
"""
from __future__ import annotations

import argparse
import json
import logging
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

import requests  # noqa: E402

from src.config import LEANDNA_APP_API_SERVER, LEANDNA_APP_FACTORY_NDX  # noqa: E402
from src.leandna_app_metrics_client import (  # noqa: E402
    list_metrics_view,
    metric_view_label,
    pick_metric_by_ndx,
)
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402


def _pop_leading_numeric_metric_id(argv: list[str]) -> tuple[list[str], str | None]:
    if len(argv) < 2:
        return argv, None
    token = argv[1]
    if token.startswith("-"):
        return argv, None
    s = token.strip()
    if s and s.isdigit():
        return [argv[0]] + argv[2:], s
    return argv, None


def _sort_rows(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("ndx", r.get("id"))
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _brief_lines(rows: list[dict]) -> list[str]:
    lines = []
    for r in rows:
        mid = r.get("ndx", r.get("id", ""))
        name = metric_view_label(r).replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        vs = r.get("valueStreamNdx", "")
        lines.append(f"{mid}\t{name}\t{mtype}\tvalueStreamNdx={vs}")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List LeanDNA metrics (app API GET …/Metrics/View).",
        epilog="Leading digits = metric ndx filter (app id, not necessarily Data API id).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--format", choices=("json", "brief"), default="json")
    ap.add_argument(
        "--view-query",
        default=None,
        help="Override Metrics/View query string (default: LEANDNA_APP_METRICS_VIEW_QUERY)",
    )
    ap.add_argument("--factory-ndx", type=int, default=None, help=f"Site factory ndx (default {LEANDNA_APP_FACTORY_NDX})")
    ap.add_argument("--metric-id", default=None, metavar="NDX", help="Filter to this metric ndx")
    ap.add_argument("--no-switch-site", action="store_true", help="Skip switchSite POST before list")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--timeout", type=float, default=60.0, metavar="SEC")
    argv_mod, leading_id = _pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

    metric_filter = (ns.metric_id or leading_id or "").strip() or None

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    if not leandna_app_session_configured():
        print(
            "Missing LeanDNA app session — set LEANDNA_APP_SESSION_ID or a cookie with LDNASESSIONID= "
            "(from DevTools while logged into the web app).",
            file=sys.stderr,
        )
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    print(
        f"LeanDNA app API: GET {LEANDNA_APP_API_SERVER}/api/2/factndx/{factory}/Metrics/View  "
        f"(session auth)",
        file=sys.stderr,
    )

    try:
        rows = list_metrics_view(
            view_query=ns.view_query,
            factory_ndx=factory,
            switch_site_first=not ns.no_switch_site,
            timeout=ns.timeout,
        )
    except requests.HTTPError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    rows = _sort_rows(rows)
    catalog_count = len(rows)

    if metric_filter is not None:
        picked = pick_metric_by_ndx(rows, metric_filter)
        if picked is None:
            print(
                f"No metric with ndx={metric_filter!r} in Metrics/View ({catalog_count} row(s)).",
                file=sys.stderr,
            )
            return 1
        rows = [picked]

    if ns.format == "brief":
        print("ndx\tname\tmetricType\tvalueStreamNdx")
        for ln in _brief_lines(rows):
            print(ln)
    else:
        payload: Any = rows[0] if metric_filter and len(rows) == 1 else rows
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    print(f"Displayed {len(rows)} metric(s).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
