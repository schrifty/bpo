#!/usr/bin/env python3
"""Compare Salesforce active customers to Pendo prefixes (LLM export data bundle).

Loads the same portfolio + Salesforce universe as ``cortex export-all`` / ``build_llm_export_snapshot_report``,
then reports:

1. Active Salesforce portfolio labels with no Pendo usage data
2. Pendo prefixes with usage but no active Salesforce label match
3. Fuzzy cross-gap hints (subsidiaries, acronyms, entity names)

Examples::

  compare-sf-pendo-customers
  compare-sf-pendo-customers --days 90 --out output/sf-pendo-reconcile.md
  compare-sf-pendo-customers --format json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.data_sources.llm_export_report import build_llm_export_snapshot_report  # noqa: E402
from src.pendo_client import PendoClient  # noqa: E402
from src.sf_pendo_reconcile import build_reconcile_report, render_reconcile_markdown  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Reconcile Salesforce active customers with Pendo (export data bundle).",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=90,
        help="Pendo portfolio window (default 90, same as typical export)",
    )
    ap.add_argument("--format", choices=("markdown", "json"), default="markdown")
    ap.add_argument(
        "--out",
        default=None,
        help="Write markdown/json to this path (default: stdout only)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("cortex").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    print("Loading portfolio + Salesforce (LLM export bundle)…", file=sys.stderr)
    portfolio = build_llm_export_snapshot_report(PendoClient(), days=ns.days)
    if portfolio.get("error"):
        print(f"Export bundle failed: {portfolio['error']}", file=sys.stderr)
        return 1

    rep = build_reconcile_report(portfolio, days=ns.days)

    if ns.format == "json":
        payload = {
            "days": rep.days,
            "salesforce_configured": rep.salesforce_configured,
            "active_sf_labels": rep.active_sf_labels,
            "pendo_prefix_count": len(rep.pendo_prefixes),
            "sf_active_no_pendo": rep.sf_active_no_pendo,
            "pendo_no_sf": rep.pendo_no_sf,
            "suggested_pairs": [asdict(h) for h in rep.suggested_pairs],
            "provenance": rep.provenance,
        }
        text = json.dumps(payload, indent=2, default=str)
    else:
        text = render_reconcile_markdown(rep)

    if ns.out:
        out_path = Path(ns.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        print(f"Wrote {out_path}", file=sys.stderr)

    print(text)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
