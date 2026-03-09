#!/usr/bin/env python3
"""Generate decks for all active customers.

Usage:
    python decks.py cs_health_review
    python decks.py cs_health_review --quarter Q4 2025
    python decks.py product_adoption --days 60 --max 10 --workers 2
    python decks.py --list
"""

import argparse
import sys
import time


def main():
    parser = argparse.ArgumentParser(description="Generate decks from a deck definition")
    parser.add_argument("deck", nargs="?", help="Deck ID (e.g. cs_health_review)")
    parser.add_argument("--list", action="store_true", help="List available deck types and exit")
    parser.add_argument("--quarter", nargs="*", default=None,
                        help="Quarter override: 'Q1 2026', 'prev', 'current', or omit for auto-detect")
    parser.add_argument("--days", type=int, default=None,
                        help="Explicit lookback window in days (overrides --quarter)")
    parser.add_argument("--max", type=int, default=None, help="Cap number of customers")
    parser.add_argument("--workers", type=int, default=4, help="Parallel threads (default: 4)")
    parser.add_argument("--customers", nargs="*", help="Specific customer names (default: all active)")
    parser.add_argument("--thumbnails", action="store_true", help="Export slide thumbnails (slow, skipped by default)")
    parser.add_argument("--sync-config", action="store_true", help="Push local decks/slides to Drive and exit")
    parser.add_argument("--sync-overwrite", action="store_true", help="Overwrite existing Drive configs during sync")
    args = parser.parse_args()

    from src.quarters import resolve_quarter

    if args.days:
        qr = None
        days = args.days
        period_label = f"{days} days"
    else:
        q_override = " ".join(args.quarter) if args.quarter else None
        qr = resolve_quarter(q_override)
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

    from src.deck_loader import list_decks
    from src.pendo_client import PendoClient
    from src.slides_client import create_health_decks_for_customers, create_portfolio_deck

    if args.sync_config:
        from src.drive_config import sync_config_to_drive
        stats = sync_config_to_drive(overwrite=args.sync_overwrite)
        print(f"Decks uploaded:     {stats['decks_uploaded']}")
        print(f"Slides uploaded:    {stats['slides_uploaded']}")
        print(f"Skipped (exist):    {stats['skipped']}")
        return

    if args.list:
        for m in list_decks():
            print(f"  {m['id']:25s}  {m['name']}")
        return

    if not args.deck:
        parser.error("deck is required (use --list to see options)")

    # Portfolio deck generates a single cross-customer deck
    if args.deck == "portfolio_review":
        print(f"Deck:       {args.deck}")
        print(f"Period:     {period_label}")
        if args.max:
            print(f"Max cust:   {args.max}")
        print()
        t0 = time.time()
        result = create_portfolio_deck(days=days, max_customers=args.max, quarter=qr)
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:80]}")
            sys.exit(1)
        else:
            print(f"  OK   {result.get('url', '')}")
        return

    # Generic words that appear as "customers" in Pendo due to site naming artifacts
    _JUNK_CUSTOMERS = {
        "(unknown)", "Automated", "Automatic", "By", "Customer", "LOB",
        "LeanDNA", "Manual", "Override", "Prefixed", "Professional", "Test",
    }

    if args.customers:
        customers = args.customers
    else:
        print("Fetching customer list from Pendo...")
        customers = PendoClient().get_sites_by_customer(days)["customer_list"]
        customers = [c for c in customers if c not in _JUNK_CUSTOMERS]

    if args.max:
        customers = customers[: args.max]

    print(f"Deck:       {args.deck}")
    print(f"Customers:  {len(customers)}")
    print(f"Period:     {period_label}")
    print(f"Workers:    {args.workers}")
    print()

    t0 = time.time()
    results = create_health_decks_for_customers(
        customers, days=days, max_customers=args.max,
        deck_id=args.deck, workers=args.workers,
        thumbnails=args.thumbnails,
        quarter=qr,
    )
    elapsed = time.time() - t0

    ok = [r for r in results if "error" not in r]
    fail = [r for r in results if "error" in r]

    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s  |  {len(ok)} succeeded  |  {len(fail)} failed")
    print(f"{'=' * 60}")
    for r in ok:
        print(f"  OK   {r.get('customer', '?'):30s} {r.get('url', '')}")
    for r in fail:
        err = r.get("error", "")
        customer = r.get("customer", "?")
        print(f"  FAIL {customer:30s} {err[:120]}")

    if fail:
        failed_names = [r.get("customer", "?") for r in fail]
        q_flag = f"--quarter {qr.label}" if qr else f"--days {days}"
        retry_cmd = f"python decks.py {args.deck} {q_flag} --customers {' '.join(failed_names)}"
        print(f"\nTo retry failed customers:\n  {retry_cmd}")

    # Generate a portfolio (book of business) deck after per-customer decks
    print(f"\nGenerating Book of Business deck...")
    t1 = time.time()
    try:
        from src.slides_client import create_health_deck
        from src.pendo_client import PendoClient
        client = PendoClient()
        portfolio_report = client.get_portfolio_report(days=days, max_customers=args.max)
        if qr:
            portfolio_report["quarter"] = qr.label
        portfolio_result = create_health_deck(portfolio_report, deck_id="portfolio_review", thumbnails=args.thumbnails)
        p_elapsed = time.time() - t1
        if "error" in portfolio_result:
            print(f"  FAIL  {portfolio_result['error'][:120]}  ({p_elapsed:.0f}s)")
        else:
            print(f"  OK    {portfolio_result.get('url', '')}  ({p_elapsed:.0f}s)")
    except Exception as e:
        print(f"  FAIL  {str(e)[:120]}  ({time.time() - t1:.0f}s)")

    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
