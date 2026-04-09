"""Fetch health report or CS Report (CSR) data for a customer.

Usage:
  # Full health report (Pendo + Jira + Salesforce + CS Report) — same data used by hydrate/decks
  python scripts/fetch_report.py "Customer Name"
  python scripts/fetch_report.py "Bombardier" --out report.json

  # CS Report only (platform health, supply chain, platform value from Data Exports Drive)
  python scripts/fetch_report.py "Customer Name" --csr-only
  python scripts/fetch_report.py "Bombardier" --csr-only --out csr.json

Requires:
  - Full report: .env with PENDO_INTEGRATION_KEY, GOOGLE_APPLICATION_CREDENTIALS (Drive for CSR).
    JIRA_* and SF_* optional.
  - CS Report only: GOOGLE_APPLICATION_CREDENTIALS with access to the Data Exports shared drive.
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch health or CS Report data for a customer.")
    ap.add_argument("customer", help="Customer name (e.g. Bombardier, Safran)")
    ap.add_argument("--csr-only", action="store_true", help="Fetch only CS Report (platform health, supply chain, value)")
    ap.add_argument("--out", "-o", metavar="FILE", help="Write JSON to file")
    ap.add_argument("--days", type=int, default=30, help="Lookback days for full report (default 30)")
    args = ap.parse_args()

    if args.csr_only:
        from src.cs_report_client import (
            get_customer_platform_health,
            get_customer_platform_value,
            get_customer_supply_chain,
        )
        data = {
            "customer": args.customer,
            "csr": {
                "platform_health": get_customer_platform_health(args.customer),
                "supply_chain": get_customer_supply_chain(args.customer),
                "platform_value": get_customer_platform_value(args.customer),
            },
        }
    else:
        from src.pendo_client import PendoClient
        pc = PendoClient()
        data = pc.get_customer_health_report(args.customer, days=args.days)
        data.setdefault("customer", args.customer)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Wrote {args.out}")
    else:
        print(json.dumps(data, indent=2))

    ph = (data.get("csr") or {}).get("platform_health") or {}
    err = data.get("error") or (ph.get("error") if isinstance(ph, dict) else None)
    if err:
        sys.exit(1)


if __name__ == "__main__":
    main()
