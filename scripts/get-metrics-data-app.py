#!/usr/bin/env python3
"""Metric names + daily **MetricEntries** via the classic LeanDNA app API.

Lists metrics with ``GET …/Metrics/View``, then loads ``GET …/MetricEntries?&date=…`` per day
in the window (same write path as kpi ``enter_metric_data``).

Auth: ``LEANDNA_APP_SESSION_ID`` or cookie with ``LDNASESSIONID=`` — no Data API Bearer.

Examples::

  python3 scripts/get-metrics-data-app.py 638
  python3 scripts/get-metrics-data-app.py "job success"
  python3 scripts/get-metrics-data-app.py --max-metrics 5
  python3 scripts/get-metrics-data-app.py 638 --start-date 2026-01-01 --end-date 2026-03-31
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

from src.config import LEANDNA_APP_API_SERVER, LEANDNA_APP_FACTORY_NDX  # noqa: E402
from src.leandna_app_metrics_client import (  # noqa: E402
    fetch_metric_entries_range,
    grep_metrics_by_name,
    list_metrics_view,
    metric_view_label,
    pick_metric_by_ndx,
    resolve_metric_datapoint_window,
)
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402
from src.leandna_metrics_display import (  # noqa: E402
    print_metrics_datapoint_table,
    print_metrics_grouped_display,
)


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


def _is_ndx_token(s: str) -> bool:
    t = s.strip()
    return bool(t) and t.isdigit()


def _sort_metrics(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("ndx", r.get("id"))
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _value_stream_ndx(metric: dict[str, Any]) -> int | None:
    raw = metric.get("valueStreamNdx")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="App API: metric list + MetricEntries per day in a date window.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("filter", nargs="?", default=None, metavar="NDX_OR_SUBSTRING")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--max-metrics", type=int, default=50, metavar="N")
    ap.add_argument("--format", choices=("json", "brief", "table"), default="brief")
    ap.add_argument("--lookback-days", type=int, default=90, metavar="N")
    ap.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--factory-ndx", type=int, default=None)
    ap.add_argument("--view-query", default=None)
    ap.add_argument("--no-switch-site", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--timeout", type=float, default=30.0, metavar="SEC")
    argv_mod, leading_id = _pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    if not leandna_app_session_configured():
        print(
            "Missing LeanDNA app session — set LEANDNA_APP_SESSION_ID or LDNASESSIONID cookie.",
            file=sys.stderr,
        )
        return 1

    try:
        start_s, end_s = resolve_metric_datapoint_window(
            lookback_days=ns.lookback_days,
            start_date=ns.start_date,
            end_date=ns.end_date,
        )
    except ValueError as e:
        print(f"Invalid date: {e}", file=sys.stderr)
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    filter_token = (leading_id or (str(ns.filter).strip() if ns.filter else None)) or None
    use_all = ns.all or filter_token is None

    print(
        f"LeanDNA app API: Metrics/View + MetricEntries  window {start_s!r}..{end_s!r}  "
        f"factory={factory}  host={LEANDNA_APP_API_SERVER}",
        file=sys.stderr,
    )

    try:
        catalog = list_metrics_view(
            view_query=ns.view_query,
            factory_ndx=factory,
            switch_site_first=not ns.no_switch_site,
            timeout=max(ns.timeout, 60.0),
        )
    except Exception as e:
        print(f"Failed to list metrics: {e}", file=sys.stderr)
        return 1

    metrics = [m for m in catalog if isinstance(m, dict)]
    if use_all:
        metrics = _sort_metrics(metrics)[: max(1, ns.max_metrics)]
    elif filter_token and _is_ndx_token(filter_token):
        picked = pick_metric_by_ndx(metrics, filter_token)
        if picked is None:
            print(f"No metric ndx={filter_token!r} in view.", file=sys.stderr)
            return 1
        metrics = [picked]
    else:
        metrics = _sort_metrics(grep_metrics_by_name(metrics, filter_token or ""))
        if not metrics:
            print(f"No metrics matched {filter_token!r}.", file=sys.stderr)
            return 1

    results: list[dict[str, Any]] = []
    errors = 0
    for m in metrics:
        ndx = m.get("ndx", m.get("id"))
        name = metric_view_label(m)
        vs = _value_stream_ndx(m)
        try:
            ndx_i = int(ndx)
        except (TypeError, ValueError):
            print(f"Skipping metric without numeric ndx: {m!r}", file=sys.stderr)
            errors += 1
            continue
        points, err = fetch_metric_entries_range(
            ndx_i,
            start_date=start_s,
            end_date=end_s,
            value_stream_ndx=vs,
            factory_ndx=factory,
            switch_site_first=False,
            timeout_per_day=ns.timeout,
        )
        if err is not None:
            errors += 1
            print(f"MetricEntries failed for ndx={ndx_i} name={name!r}: {err!r}", file=sys.stderr)
            continue
        results.append(
            {
                "id": ndx_i,
                "ndx": ndx_i,
                "name": name,
                "valueStreamNdx": vs,
                "window": {"startDate": start_s, "endDate": end_s},
                "values": points,
            }
        )

    if not results and errors:
        return 1

    if ns.format == "json":
        print(json.dumps(results, indent=2, default=str, ensure_ascii=False))
    elif ns.format == "table":
        print_metrics_datapoint_table(results, values_key="values")
    else:
        print_metrics_grouped_display(results, values_key="values", include_json_definition=False)

    total = sum(len(r.get("values") or []) for r in results)
    print(
        f"Displayed {len(results)} metric(s), {total} entry row(s)"
        + (f"; {errors} failed" if errors else "")
        + ".",
        file=sys.stderr,
    )
    return 0 if results else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
