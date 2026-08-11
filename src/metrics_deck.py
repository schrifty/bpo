"""Generate a Google Slides deck from KPI metrics (config/my-metrics.yaml).

Separate from the email digest — this command creates a presentation in the
Output folder showing metric values vs targets.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from typing import Any, Sequence

from .config import logger
from .metrics_digest import (
    DigestRow,
    build_digest_rows,
    generate_metrics_digest_deck,
)
from .metrics_registry import load_metrics_registry, normalize_tag
from .metrics_upsert import MetricUpsertContext


def run_metrics_deck(
    *,
    days: int = 30,
    timeout_seconds: float = 120.0,
    as_of: str | None = None,
    registry: dict[str, Any] | None = None,
    tag: str | None = None,
    use_claude: bool | None = None,
) -> dict[str, Any]:
    """Generate metrics deck and return result dict with deck_id, deck_url, or error."""
    as_of_s = as_of or date.today().isoformat()
    ctx = MetricUpsertContext(
        entry_date=as_of_s,
        requested_sites=None,
        skip_catalog=True,
        timeout_seconds=timeout_seconds,
        verbose=False,
        dry_run=True,
        days=days,
        max_issues_per_board=500,
        workers=6,
        metric_name_filter=None,
    )
    rows = build_digest_rows(registry=registry, ctx=ctx, tag=tag)
    return generate_metrics_digest_deck(
        rows, tag=tag, as_of=as_of_s, use_claude=use_claude
    )


def add_metrics_deck_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--days", type=int, default=30, help="Trailing window for generators (default: 30)")
    ap.add_argument("--timeout", type=float, default=120.0, metavar="SEC")
    ap.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Report as-of date (default: today)",
    )
    ap.add_argument(
        "--tag",
        default=None,
        metavar="TAG",
        help="Only include KPIs with this registry tag (e.g. akkr, mfr, engineering)",
    )
    ap.add_argument(
        "--claude",
        dest="use_claude",
        action="store_true",
        default=None,
        help="Have Claude design the scorecard slides (default when ANTHROPIC_API_KEY is set)",
    )
    ap.add_argument(
        "--no-claude",
        dest="use_claude",
        action="store_false",
        help="Use the fixed KPI-card grid instead of Claude-designed slides",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def run_metrics_deck_cli(argv: Sequence[str] | None = None, *, prog: str = "metrics-deck") -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Generate a Google Slides deck from my-metrics.yaml KPIs.",
    )
    add_metrics_deck_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    if ns.verbose:
        logging.getLogger("cortex").setLevel(logging.INFO)

    tag = (str(ns.tag).strip() or None) if ns.tag else None
    tag_label = tag.upper() if tag else "KPI"

    from .metrics_claude_slides import metrics_claude_slides_enabled

    use_claude = ns.use_claude
    designer = (
        "Claude-designed slides"
        if (metrics_claude_slides_enabled() if use_claude is None else use_claude)
        else "fixed KPI-card layout"
    )
    print(f"Generating {tag_label} metrics deck ({designer})...")

    result = run_metrics_deck(
        days=int(ns.days),
        timeout_seconds=float(ns.timeout),
        as_of=str(ns.date),
        tag=tag,
        use_claude=use_claude,
    )

    if result.get("error"):
        print(f"Error: {result['error']}", file=sys.stderr)
        return 1

    print()
    print("=" * 60)
    print(f"  OK   {result['deck_url']}")
    print("=" * 60)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run_metrics_deck_cli(argv, prog="metrics-deck")


if __name__ == "__main__":
    raise SystemExit(main())
