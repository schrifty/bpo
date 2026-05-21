#!/usr/bin/env python3
"""List Salesforce portfolio customers by contract status with Pendo, CSR, and JSM name matches.

Uses Salesforce Customer Entity rollups as the master list. For each label, resolves:

- Pendo sitename prefix (``config/sf_portfolio_pendo_aliases.yaml`` + heuristics)
- CS Report ``customer`` column (``config/cs_report_customer_aliases.yaml`` + cohort keys)
- JSM organization directory (``config/jsm_organization_aliases.yaml`` + fuzzy match)

Requires ``SF_*`` in ``.env``. Pendo, CS Report (Drive), and Jira are optional; sections show
``(no match)`` or warnings when a source is skipped or unavailable.

Examples::

  match-companies
  match-companies --format json --out output/match-companies.json
  match-companies --no-jira --days 90
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

from src.match_companies import build_company_match_report, render_match_report_text  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Salesforce customers by status with Pendo / CSR / JSM name matches.",
    )
    ap.add_argument("--days", type=int, default=30, help="Pendo sitename window (default 30)")
    ap.add_argument("--format", choices=("text", "json"), default="text")
    ap.add_argument("--out", default=None, help="Write output to this path")
    ap.add_argument("--no-pendo", action="store_true", help="Skip Pendo prefix resolution")
    ap.add_argument("--no-csr", action="store_true", help="Skip CS Report matching")
    ap.add_argument("--no-jira", action="store_true", help="Skip JSM organization matching")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    print("Loading Salesforce portfolio and cross-system names…", file=sys.stderr)
    report = build_company_match_report(
        pendo_days=ns.days,
        include_pendo=not ns.no_pendo,
        include_csr=not ns.no_csr,
        include_jsm=not ns.no_jira,
    )

    if not report.get("salesforce_configured"):
        print("Salesforce is not configured (set SF_* in .env).", file=sys.stderr)
        text = render_match_report_text(report) if ns.format == "text" else json.dumps(report, indent=2)
        if ns.out:
            Path(ns.out).write_text(text, encoding="utf-8")
        else:
            print(text)
        return 1

    if ns.format == "json":
        text = json.dumps(report, indent=2, default=str)
    else:
        text = render_match_report_text(report)

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
