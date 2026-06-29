#!/usr/bin/env python3
"""HELP Time-to-Resolution SLA adherence % from Jira (trailing window).

Among HELP tickets **resolved** in the last N days (default 30), reports the percent
with **Time to resolution** SLA completed and **not breached** (``customfield_10665``).

Requires ``JIRA_*`` credentials in ``.env`` only — no LeanDNA / ``EXECUTION_ENV``.

Examples::

  get-help-ttr
  get-help-ttr --days 30 --format json
  get-help-ttr --customer Carrier --include-tickets
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

from src.jira_client import get_shared_jira_client  # noqa: E402


def _print_brief(payload: dict[str, Any]) -> None:
    days = payload.get("window_days", 30)
    print(f"HELP TTR SLA adherence % — resolved in trailing {days}d")
    print(
        "(% of resolved tickets with Time to resolution SLA completed and not breached)"
    )
    print()

    if payload.get("error"):
        print(f"Error: {payload['error']}")
        return

    customer = payload.get("customer")
    scope = f"customer {customer!r}" if customer else "portfolio"
    adh = payload.get("ttr_sla_adherence") or {}
    print(f"Scope: {scope}")
    print(f"Resolved in window: {payload.get('resolved_in_window')}")
    if adh.get("pct") is not None:
        print(
            f"TTR SLA adherence: {adh.get('pct')}%  "
            f"({adh.get('met', 0)} met / {adh.get('measured', 0)} with completed TTR SLA, "
            f"{adh.get('breached', 0)} breached)"
        )
    else:
        print("TTR SLA adherence: — (no tickets with completed TTR SLA in fetch)")
    if adh.get("waiting"):
        print(f"TTR SLA still in progress (excluded from %): {adh.get('waiting')}")

    tickets = payload.get("tickets")
    if tickets:
        print()
        print("key\tresolved\tTTR_SLA_measured\tTTR_SLA_met\torganizations\tsummary")
        for t in tickets:
            key = t.get("key", "")
            rd = t.get("resolutiondate", "")
            meas = "yes" if t.get("ttr_sla_measured") else "no"
            met = "yes" if t.get("ttr_sla_met") else ("no" if t.get("ttr_sla_measured") else "")
            orgs_s = ",".join(t.get("organizations") or [])[:60]
            summary = str(t.get("summary") or "").replace("\t", " ").replace("\n", " ")[:80]
            print(f"{key}\t{rd}\t{meas}\t{met}\t{orgs_s}\t{summary}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "HELP TTR SLA adherence %: resolved tickets in a trailing window "
            "with Time to resolution SLA met (Jira only)."
        ),
    )
    ap.add_argument("--days", type=int, default=30, metavar="N", help="Trailing window (default: 30)")
    ap.add_argument("--customer", default=None, metavar="NAME", help="HELP customer / JSM org scope")
    ap.add_argument("--match-term", action="append", default=[], metavar="TEXT")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument("--include-tickets", action="store_true", help="Include per-issue HELP rows")
    ap.add_argument("--max-results", type=int, default=None, metavar="N", help="Cap HELP issues fetched")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    cortex_log = logging.getLogger("cortex")
    cortex_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    cortex_log.propagate = False

    try:
        jira = get_shared_jira_client()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    match_terms = [t.strip() for t in ns.match_term if t and str(t).strip()]
    customer = (str(ns.customer).strip() if ns.customer is not None else "") or None

    try:
        payload = jira.get_help_time_to_resolution(
            days=ns.days,
            customer_name=customer,
            match_terms=match_terms or None,
            max_results=ns.max_results,
            include_tickets=ns.include_tickets,
        )
    except Exception as e:
        print(f"Jira fetch failed: {e}", file=sys.stderr)
        return 1

    if ns.format == "json":
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    else:
        _print_brief(payload)

    return 1 if payload.get("error") else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
