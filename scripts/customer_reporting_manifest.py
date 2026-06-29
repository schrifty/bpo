"""Print Salesforce corporate reporting groups (and optional CS Report alias mapping).

Usage:
  python scripts/customer_reporting_manifest.py
  python scripts/customer_reporting_manifest.py --with-arr
  python scripts/customer_reporting_manifest.py --cs-aliases
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Salesforce-first customer reporting manifest (optional CS Report alias view).",
    )
    ap.add_argument(
        "--with-arr",
        action="store_true",
        help="Include contract ARR from Salesforce Customer Entity rollups.",
    )
    ap.add_argument(
        "--cs-aliases",
        action="store_true",
        help="Also list CS Report export names grouped under each corporate label.",
    )
    args = ap.parse_args()

    from src.data_source_health import _salesforce_configured

    if not _salesforce_configured():
        print("Salesforce not configured — set SF_* credentials in .env")
        sys.exit(1)

    from src.salesforce_client import SalesforceClient
    from src.salesforce_reporting import aggregate_accounts_by_corporate_group

    sf = SalesforceClient()
    accounts = sf.get_entity_accounts()
    by_group = aggregate_accounts_by_corporate_group(accounts)

    arr_by_group: dict[str, float] = {}
    for group, rows in by_group.items():
        arr_by_group[group] = sum(float(a.get("ARR__c") or 0) for a in rows)

    print("# Customer reporting groups (Salesforce Customer Entity hierarchy)\n")
    for group, arr in sorted(arr_by_group.items(), key=lambda kv: (-kv[1], kv[0].lower())):
        if arr <= 0:
            continue
        n_ent = len(by_group.get(group) or [])
        line = f"- **{group}** — ${arr:,.0f} contract ARR ({n_ent} entities)"
        print(line)

    if args.cs_aliases:
        from src.customer_reporting import build_reporting_group_index
        from src.cs_report_client import _fetch_latest_report

        print("\n## CS Report export aliases\n")
        idx = build_reporting_group_index()
        week_names = sorted({
            (r.get("customer") or "").strip()
            for r in _fetch_latest_report()
            if r.get("delta") == "week" and (r.get("customer") or "").strip()
        })
        by_corp: dict[str, list[str]] = defaultdict(list)
        for name in week_names:
            for corp, aliases in idx.items():
                if name.lower() in {a.lower() for a in aliases} or name == corp:
                    by_corp[corp].append(name)
                    break
            else:
                by_corp[name].append(name)

        for corp in sorted(by_corp, key=lambda c: (-arr_by_group.get(c, 0), c.lower())):
            members = sorted(set(by_corp[corp]))
            if len(members) == 1 and members[0] == corp:
                continue
            subs = ", ".join(m for m in members if m != corp)
            if subs:
                print(f"- **{corp}** ← {subs}")


if __name__ == "__main__":
    main()
