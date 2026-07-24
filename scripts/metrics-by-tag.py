#!/usr/bin/env python3
"""List every ``config/my-metrics.yaml`` KPI carrying a tag, with current value.

Tags live under each metric's ``tags:`` list. One KPI may carry many tags. Values
come from live generators by default (``--mode live``). Use ``--mode stored`` only
to inspect LeanDNA Data API datapoints.

Text output streams each KPI as soon as it resolves. JSON still buffers the full
list so the document is valid.

Examples::

  metrics-by-tag                     # list tags
  metrics-by-tag engineering         # KPIs + live values (streamed)
  metrics-by-tag engineering --mode stored
  metrics-by-tag ai --json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.config import CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.kpi_service import (  # noqa: E402
    DEFAULT_RESOLVE_MODE,
    RESOLVE_MODES,
    column_widths_for_tag,
    default_resolve_context,
    format_kpi_resolved_line,
    iter_resolve_kpis_by_tag,
    kpi_resolved_to_json,
)
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metric_registry_resolve import METRICS_REGISTRY_DEFAULT_SITE_ID  # noqa: E402
from src.leandna_metrics_cli import configure_cortex_logging  # noqa: E402
from src.metrics_latest import DEFAULT_RECENT_DATAPOINT_COUNT  # noqa: E402
from src.metrics_registry import all_registry_tags  # noqa: E402

_DEFAULT_LOOKBACK_DAYS = 365
_READ_TIMEOUT_S = 60.0
_DEFAULT_LIVE_DAYS = 30


def _print_tag_catalog() -> int:
    tags = all_registry_tags()
    if not tags:
        print("No tags defined in config/my-metrics.yaml.", file=sys.stderr)
        return 1
    print("Tags in config/my-metrics.yaml (tag: KPI count):")
    for tag, count in tags:
        print(f"  {tag}: {count}")
    print("\nUsage: metrics-by-tag <tag>   e.g. metrics-by-tag engineering")
    return 0


def _data_api_configured() -> bool:
    try:
        data_api_base_url()
        return True
    except ValueError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "List KPIs for a tag with current values (config/my-metrics.yaml). "
            "Reads are live by default: values come from generators, not stored datapoints. "
            "Use --mode stored only to inspect what the LeanDNA Data API holds. "
            "Text mode prints each KPI as soon as it resolves."
        ),
    )
    ap.add_argument(
        "tag",
        nargs="?",
        metavar="TAG",
        help="Tag to filter KPIs by (normalized: lowercase, hyphenated). Omit to list tags.",
    )
    ap.add_argument(
        "--tag",
        dest="tag_flag",
        metavar="NAME",
        help="Same as positional TAG (kept for compatibility)",
    )
    ap.add_argument("--list", action="store_true", help="List all defined tags and their KPI counts, then exit")
    ap.add_argument("--json", action="store_true", help="Emit a JSON array of matching KPIs with current values")
    ap.add_argument(
        "--mode",
        choices=RESOLVE_MODES,
        default=DEFAULT_RESOLVE_MODE,
        help="live=compute from generators (default); stored=inspect LeanDNA Data API datapoints",
    )
    ap.add_argument(
        "--requested-sites",
        default=str(METRICS_REGISTRY_DEFAULT_SITE_ID),
        metavar="ID",
        help=f"RequestedSites header (default: {METRICS_REGISTRY_DEFAULT_SITE_ID})",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=_DEFAULT_LOOKBACK_DAYS,
        metavar="N",
        help=f"Stored datapoint search window ending today (default: {_DEFAULT_LOOKBACK_DAYS})",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=_DEFAULT_LIVE_DAYS,
        metavar="N",
        help=f"Trailing window for live generators (default: {_DEFAULT_LIVE_DAYS})",
    )
    ap.add_argument(
        "--recent-count",
        type=int,
        default=DEFAULT_RECENT_DATAPOINT_COUNT,
        metavar="N",
        help=f"(stored mode) newest datapoints to show per KPI (default: {DEFAULT_RECENT_DATAPOINT_COUNT})",
    )
    ap.add_argument("--timeout", type=float, default=_READ_TIMEOUT_S, metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    configure_cortex_logging(verbose=ns.verbose)

    tag = (ns.tag_flag or ns.tag or "").strip() or None

    if ns.list or not tag:
        return _print_tag_catalog()

    # Stored mode inspects the Data API, so it requires the Data API to be configured.
    if ns.mode == "stored" and not _data_api_configured():
        try:
            data_api_base_url()
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1

    if ns.mode == "stored":
        print(
            f"KPI inspect (stored): tag={tag!r} lookback={ns.lookback_days}d recent={ns.recent_count} "
            f"requestedSites={ns.requested_sites!r} "
            f"EXECUTION_ENV bucket={CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET}",
            file=sys.stderr,
        )
    else:
        print(
            f"KPI resolve (live): tag={tag!r} days={ns.days} — computed from generators, no stored reads.",
            file=sys.stderr,
        )

    ctx = default_resolve_context(
        days=ns.days,
        timeout_seconds=ns.timeout,
        requested_sites=ns.requested_sites,
        verbose=ns.verbose,
    )

    rows = []
    widths = None if ns.json else column_widths_for_tag(tag)
    try:
        if widths is not None:
            print(widths.header, flush=True)
            print(f"{'─' * widths.name}  {'─' * widths.tags}  {'─' * 5}", flush=True)
        for row in iter_resolve_kpis_by_tag(
            tag,
            mode=ns.mode,
            ctx=ctx,
            requested_sites=ns.requested_sites,
            lookback_days=ns.lookback_days,
            timeout_seconds=ns.timeout,
            recent_count=ns.recent_count,
        ):
            rows.append(row)
            if widths is not None:
                print("\n".join(format_kpi_resolved_line(row, widths=widths)), flush=True)
    except Exception as e:  # noqa: BLE001 — surface resolve/Data API failures cleanly
        print(f"Failed to resolve KPIs for tag {tag!r}: {e}", file=sys.stderr)
        return 1

    if not rows:
        available = ", ".join(t for t, _ in all_registry_tags()) or "(none)"
        print(f"No KPIs tagged {tag!r}. Available tags: {available}", file=sys.stderr)
        return 1

    if ns.json:
        print(json.dumps([kpi_resolved_to_json(row) for row in rows], indent=2, default=str, ensure_ascii=False))

    with_value = sum(1 for row in rows if row.observation.display_value is not None and not row.observation.error)
    live_n = sum(1 for row in rows if row.observation.origin == "live" and row.observation.ok)
    stored_n = sum(1 for row in rows if row.observation.origin == "stored" and row.observation.ok)
    err_n = sum(1 for row in rows if row.observation.error)
    print(
        f"Tag {tag!r}: {len(rows)} KPI(s) — {with_value} with a value "
        f"({live_n} live, {stored_n} stored), {err_n} error(s).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
