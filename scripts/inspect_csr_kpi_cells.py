#!/usr/bin/env python3
"""Pull the latest CS Report from Drive and print raw + parsed KPI cell JSON.

Shows whatever the spreadsheet contains for a given column (default: clearToBuildPercent),
including keys like startValue, endValue, deltaPercent when present.

Usage (from repo root; requires GOOGLE_APPLICATION_CREDENTIALS and Drive access to Data Exports):

  python scripts/inspect_csr_kpi_cells.py Bombardier
  python scripts/inspect_csr_kpi_cells.py Bombardier --column clearToCommitPercent
  python scripts/inspect_csr_kpi_cells.py Bombardier --delta week --max-rows 20

  # First row in file that has this column set (any customer) — for quick discovery
  python scripts/inspect_csr_kpi_cells.py --any-customer --column clearToBuildPercent
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _format_cell(raw: Any) -> tuple[str, dict[str, Any] | None]:
    """Return (raw display, parsed KPI dict or None)."""
    if raw is None:
        return "(empty / None)", None
    s = str(raw).strip()
    if not s:
        return "(empty string)", None
    try:
        d = json.loads(s)
        if isinstance(d, dict):
            return s, d
    except json.JSONDecodeError:
        pass
    return s, None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Inspect raw CS Report KPI cells (JSON) from the latest Drive export."
    )
    ap.add_argument(
        "customer",
        nargs="?",
        default="",
        help="Customer name (matches CS Report `customer` column, case-insensitive). "
        "Omit with --any-customer.",
    )
    ap.add_argument(
        "--column",
        "-c",
        default="clearToBuildPercent",
        help="Spreadsheet column name (default: clearToBuildPercent).",
    )
    ap.add_argument(
        "--delta",
        default="week",
        help="Row filter for `delta` column (default: week).",
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=100,
        help="Max matching rows to print (default: 100).",
    )
    ap.add_argument(
        "--any-customer",
        action="store_true",
        help="Ignore customer; print at most --max-rows rows that have a non-empty --column.",
    )
    args = ap.parse_args()

    from src import cs_report_client as csr

    rows = csr._fetch_latest_report()
    cache = csr._cache or {}
    file_name = cache.get("file", "?")
    modified = cache.get("modified", "?")
    print(f"CS Report file: {file_name}")
    print(f"Modified: {modified}")
    print(f"Total rows loaded: {len(rows)}")
    print(f"Column: {args.column!r}  |  delta filter: {args.delta!r}")
    print("-" * 72)

    col = args.column
    matching: list[dict[str, Any]] = []

    if args.any_customer:
        for r in rows:
            if r.get("delta") != args.delta:
                continue
            raw = r.get(col)
            if raw is None or (isinstance(raw, str) and not str(raw).strip()):
                continue
            matching.append(r)
            if len(matching) >= args.max_rows:
                break
        if not matching:
            print(
                f"No rows with non-empty {col!r} for delta={args.delta!r}.",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        customer = (args.customer or "").strip()
        if not customer:
            print("Provide a customer name or use --any-customer.", file=sys.stderr)
            sys.exit(2)
        for r in rows:
            if r.get("customer", "").lower() != customer.lower():
                continue
            if r.get("delta") != args.delta:
                continue
            matching.append(r)
        if not matching:
            print(
                f"No rows for customer={customer!r} delta={args.delta!r}.",
                file=sys.stderr,
            )
            sys.exit(1)
        matching = matching[: args.max_rows]

    for i, r in enumerate(matching):
        factory = r.get("factoryName", "?")
        cust = r.get("customer", "?")
        raw = r.get(col)
        raw_disp, parsed = _format_cell(raw)
        print(f"\n[{i + 1}] customer={cust!r}  factory={factory!r}")
        print(f"    raw: {raw_disp[:500]}{'…' if len(raw_disp) > 500 else ''}")
        if parsed is not None:
            print(f"    parsed keys: {list(parsed.keys())}")
            print(f"    pretty:\n{json.dumps(parsed, indent=6)}")
        else:
            print("    (not valid KPI JSON — shown as raw string above)")

    print("\n" + "-" * 72)
    print(
        "Tip: `_kpi_end` uses endValue, else startValue. "
        "`deltaPercent` is not surfaced by BPO unless you add it to the pipeline."
    )


if __name__ == "__main__":
    main()
