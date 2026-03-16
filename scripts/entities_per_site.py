#!/usr/bin/env python3
"""Report customers ranked by average entities per site (Pendo metadata.agent.entity)."""

import sys
sys.path.insert(0, ".")

from src.pendo_client import PendoClient

JUNK = {
    "(unknown)", "Automated", "Automatic", "By", "Customer", "LOB",
    "LeanDNA", "Manual", "Override", "Prefixed", "Professional", "Test",
}


def main():
    days = 30
    if len(sys.argv) > 1:
        days = int(sys.argv[1])
    top_n = 25
    if len(sys.argv) > 2:
        top_n = int(sys.argv[2])

    client = PendoClient()
    print(f"Loading customers and site/entity data ({days}d)...")
    client.preload(days=days)
    by_customer = client.get_sites_by_customer(days=days)
    customers = [c for c in by_customer["customer_list"] if c not in JUNK]

    # For each customer: get site rows (each row = site or site+entity), compute avg entities per site
    results = []
    for i, customer in enumerate(customers):
        try:
            data = client.get_customer_sites(customer, days=days)
        except Exception as e:
            print(f"  Skip {customer}: {e}", file=sys.stderr)
            continue
        if "error" in data:
            continue
        sites = data.get("sites", [])
        if not sites:
            continue
        distinct_sites = len({s["sitename"] for s in sites})
        total_rows = len(sites)
        avg = total_rows / distinct_sites if distinct_sites else 0
        results.append({
            "customer": customer,
            "sites": distinct_sites,
            "site_entity_rows": total_rows,
            "avg_entities_per_site": round(avg, 2),
        })
        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{len(customers)} customers...")

    results.sort(key=lambda x: (-x["avg_entities_per_site"], -x["sites"]))

    print(f"\nTop {top_n} customers by average entities per site ({days}d):\n")
    print(f"{'Customer':<28} {'Sites':>6} {'Rows':>6} {'Avg ent/site':>12}")
    print("-" * 56)
    for r in results[:top_n]:
        print(f"{r['customer']:<28} {r['sites']:>6} {r['site_entity_rows']:>6} {r['avg_entities_per_site']:>12}")


if __name__ == "__main__":
    main()
