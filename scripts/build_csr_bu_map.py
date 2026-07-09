#!/usr/bin/env python3
"""Build a CS-Report-derived business-unit map fragment for a customer's Pendo sites.

The CS Report is authoritative for division when a customer's report is delivered
split by division (its ``customer`` column carries the division — e.g. Safran). This
tool joins each Pendo site to the CS Report factory/entity list and emits:

  * a coverage report (how many sites are CSR-confirmed / self-labeled / unmatched), and
  * a ``pendo_site_bu_map.yaml`` rules fragment to review and paste in.

Pendo site names come from one of:
  --pendo-md PATH    parse "| <Prefix> ... |" table rows from a Pendo export markdown
  --pendo-json PATH  a JSON list of site names, or {"sites": [{"sitename": ...}]}
  --live             pull the customer's active sites via PendoClient (heavier)

Examples:
  python scripts/build_csr_bu_map.py --customer Safran \\
      --pendo-md "~/Downloads/Pendo Detailed Export  (Safran, 30d)-persistent.md"
  python scripts/build_csr_bu_map.py --customer Safran --live
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.csr_business_unit_map import assign_sites, emit_bu_rules_yaml  # noqa: E402


def _load_csr_rows() -> list[dict]:
    from src.cs_report_client import _fetch_latest_report

    rows = _fetch_latest_report()
    return [r for r in rows if r.get("delta") == "week"]


def _sitenames_from_md(path: Path, customer_prefix: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    pat = re.compile(rf"^\|\s*({re.escape(customer_prefix)}[^|]+?)\s*\|", re.IGNORECASE)
    out: list[str] = []
    for line in text.splitlines():
        m = pat.match(line)
        if m:
            out.append(m.group(1).strip())
    return out


def _sitenames_from_json(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        sites = data.get("sites")
        if isinstance(sites, dict):
            sites = sites.get("sites")
        data = sites or []
    out: list[str] = []
    for item in data or []:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and item.get("sitename"):
            out.append(str(item["sitename"]))
    return out


def _sitenames_live(customer: str) -> list[str]:
    from src.export_customer_pendo_snapshot import (
        build_customer_pendo_export_report,
        merge_active_site_rows,
        resolve_pendo_customer_prefix,
    )

    prefix = resolve_pendo_customer_prefix(customer) or customer
    report = build_customer_pendo_export_report(customer, days=30)
    raw = ((report.get("sites") or {}).get("sites")) or []
    active, _a, _p = merge_active_site_rows(raw)
    return [str(s.get("sitename") or "") for s in active if s.get("sitename")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer", required=True, help="Customer name / Pendo prefix (e.g. Safran)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--pendo-md", help="Path to a Pendo export markdown to parse site names from")
    src.add_argument("--pendo-json", help="Path to a JSON file of site names")
    src.add_argument("--live", action="store_true", help="Pull active sites via PendoClient")
    ap.add_argument("--default-bu", default="Unmapped — needs review")
    args = ap.parse_args()

    prefix = args.customer.strip()

    if args.pendo_md:
        sitenames = _sitenames_from_md(Path(os.path.expanduser(args.pendo_md)), prefix)
    elif args.pendo_json:
        sitenames = _sitenames_from_json(Path(os.path.expanduser(args.pendo_json)))
    else:
        sitenames = _sitenames_live(args.customer)

    sitenames = sorted({s for s in sitenames if s})
    if not sitenames:
        print("No Pendo site names found.", file=sys.stderr)
        return 2

    rows = _load_csr_rows()
    csr_for_cust = [r for r in rows if (r.get("customer") or "").lower().startswith(prefix.lower())]

    assignments = assign_sites(sitenames, prefix, rows)

    by_source = Counter(a["source"] for a in assignments)
    by_conf = Counter(a["confidence"] for a in assignments)
    conflicts = [a for a in assignments if a["source"] in ("name_vs_csr_conflict", "csr_ambiguous")]
    unmatched = [a for a in assignments if a["business_unit"] is None]

    print(f"# CSR→BU map for {prefix}")
    print(f"#   Pendo sites: {len(sitenames)} | CS Report rows for customer: {len(csr_for_cust)}")
    print(f"#   By source:     {dict(by_source)}")
    print(f"#   By confidence: {dict(by_conf)}")
    if conflicts:
        print(f"#   Conflicts/ambiguous ({len(conflicts)}):")
        for a in conflicts:
            print(f"#     {a['sitename']!r}: {a['candidates']} (via {a['matched_key']!r}, {a['source']})")
    if unmatched:
        print(f"#   Unmatched ({len(unmatched)}): {[a['sitename'] for a in unmatched][:15]}")
    print()
    print(emit_bu_rules_yaml(assignments, prefix, default_business_unit=args.default_bu))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
