#!/usr/bin/env python3
"""Populate teams.yaml with CSM names from Pendo for all active customers."""

import sys
sys.path.insert(0, ".")

from collections import defaultdict
from pathlib import Path
import yaml
from src.pendo_client import PendoClient

TEAMS_PATH = Path("teams.yaml")
JUNK = {
    "(unknown)", "Automated", "Automatic", "By", "Customer", "LOB",
    "LeanDNA", "Manual", "Override", "Prefixed", "Professional", "Test",
}


def main():
    existing = {}
    if TEAMS_PATH.exists():
        existing = yaml.safe_load(TEAMS_PATH.read_text()) or {}

    client = PendoClient()
    print("Fetching data from Pendo...")
    client.preload(days=30)

    by_customer = client.get_sites_by_customer(30)
    customers = [c for c in by_customer["customer_list"] if c not in JUNK]
    print(f"Found {len(customers)} customers")

    partition = client._get_visitor_partition(30)
    csm_by_customer: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for v in partition["all_visitors"]:
        agent = (v.get("metadata") or {}).get("agent") or {}
        sitenames = agent.get("sitenames") or []
        raw_owner = agent.get("ownername")
        if not raw_owner:
            continue
        names = [n.strip().title() for n in str(raw_owner).split(",") if n.strip()]
        for sn in sitenames:
            sn_str = str(sn)
            for cust in customers:
                if sn_str.lower().startswith(cust.lower()):
                    for name in names:
                        csm_by_customer[cust][name] += 1
                    break

    updated = 0
    for name in customers:
        if name in existing:
            continue

        csm_votes = csm_by_customer.get(name, {})
        csm = max(csm_votes, key=csm_votes.get) if csm_votes else "Unknown"

        existing[name] = {
            "customer_team": [{"name": "TBD", "title": ""}],
            "leandna_team": [{"name": csm, "title": "CSM"}],
        }
        updated += 1

    sorted_teams = dict(sorted(existing.items()))

    TEAMS_PATH.write_text(yaml.dump(
        sorted_teams, default_flow_style=False, allow_unicode=True, sort_keys=False
    ))
    print(f"Done. {updated} new customers added, {len(sorted_teams)} total in teams.yaml")


if __name__ == "__main__":
    main()
