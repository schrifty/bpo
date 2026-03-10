#!/usr/bin/env python3
"""Generate decks from a natural-language prompt.

Usage:
    decks health review for all customers
    decks product adoption for Bombardier and JCI, Q4 2025
    decks portfolio review, max 5 customers
    decks health review for Bombardier, 60 day lookback, with thumbnails
    decks --list
    decks --sync-config
"""

import json
import sys
import time

from openai import OpenAI


def _parse_prompt(prompt: str) -> dict:
    """Use a lightweight LLM call to extract structured parameters from a prompt."""
    from src.deck_loader import list_decks

    deck_ids = [d["id"] for d in list_decks()]
    client = OpenAI()
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": (
                "Extract deck-generation parameters from the user's request. "
                "Return a JSON object with exactly these keys:\n"
                f"  deck_id   – one of {deck_ids}. Default \"cs_health_review\". "
                "Use \"portfolio_review\" for portfolio / book of business requests.\n"
                "  quarter   – e.g. \"Q1 2026\", \"prev\", \"current\", or null to auto-detect.\n"
                "  days      – integer lookback override, or null.\n"
                "  customers – list of customer name strings, or null for all.\n"
                "  max       – integer cap on customers, or null.\n"
                "  workers   – integer threads, default 4.\n"
                "  thumbnails – boolean, default false.\n\n"
                "Interpret naturally: 'last quarter' → \"prev\", "
                "'this quarter' → \"current\", 'all customers' → customers null, "
                "'top 10' → max 10, 'with thumbnails' → thumbnails true."
            )},
            {"role": "user", "content": prompt},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def main():
    # Quick utility flags that don't need LLM parsing
    if "--list" in sys.argv:
        from src.deck_loader import list_decks
        for m in list_decks():
            print(f"  {m['id']:25s}  {m['name']}")
        return

    if "--sync-config" in sys.argv:
        from src.drive_config import sync_config_to_drive
        overwrite = "--sync-overwrite" in sys.argv
        stats = sync_config_to_drive(overwrite=overwrite)
        print(f"Decks uploaded:     {stats['decks_uploaded']}")
        print(f"Slides uploaded:    {stats['slides_uploaded']}")
        print(f"Skipped (exist):    {stats['skipped']}")
        return

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__.strip())
        return

    prompt = " ".join(a for a in sys.argv[1:] if not a.startswith("-"))
    if not prompt.strip():
        print(__doc__.strip())
        sys.exit(1)

    params = _parse_prompt(prompt)
    deck_id = params.get("deck_id", "cs_health_review")
    days_override = params.get("days")
    customers_list = params.get("customers")
    max_cust = params.get("max")
    workers = params.get("workers", 4) or 4
    thumbnails = params.get("thumbnails", False)

    from src.quarters import resolve_quarter

    if days_override:
        qr = None
        days = int(days_override)
        period_label = f"{days} days"
    else:
        qr = resolve_quarter(params.get("quarter"))
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

    from src.deck_loader import list_decks
    from src.pendo_client import PendoClient
    from src.slides_client import create_health_decks_for_customers, create_portfolio_deck

    # Portfolio deck generates a single cross-customer deck
    if deck_id == "portfolio_review":
        print(f"Deck:       {deck_id}")
        print(f"Period:     {period_label}")
        if max_cust:
            print(f"Max cust:   {max_cust}")
        print()
        t0 = time.time()
        result = create_portfolio_deck(days=days, max_customers=max_cust, quarter=qr)
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

    _JUNK_CUSTOMERS = {
        "(unknown)", "Automated", "Automatic", "By", "Customer", "LOB",
        "LeanDNA", "Manual", "Override", "Prefixed", "Professional", "Test",
    }

    if customers_list:
        customers = customers_list
    else:
        print("Fetching customer list from Pendo...")
        customers = PendoClient().get_sites_by_customer(days)["customer_list"]
        customers = [c for c in customers if c not in _JUNK_CUSTOMERS]

    if max_cust:
        customers = customers[: int(max_cust)]

    print(f"Deck:       {deck_id}")
    print(f"Customers:  {len(customers)}")
    print(f"Period:     {period_label}")
    print(f"Workers:    {workers}")
    print()

    t0 = time.time()
    results = create_health_decks_for_customers(
        customers, days=days, max_customers=max_cust,
        deck_id=deck_id, workers=workers,
        thumbnails=thumbnails,
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
        retry_names = " and ".join(failed_names)
        retry_cmd = f'decks {deck_id} for {retry_names}, {q_flag.lstrip("--")}'
        print(f"\nTo retry failed customers:\n  {retry_cmd}")

    # Generate a portfolio (book of business) deck after per-customer decks
    print(f"\nGenerating Book of Business deck...")
    t1 = time.time()
    try:
        from src.slides_client import create_health_deck
        client = PendoClient()
        portfolio_report = client.get_portfolio_report(days=days, max_customers=max_cust)
        if qr:
            portfolio_report["quarter"] = qr.label
            portfolio_report["quarter_start"] = qr.start.isoformat()
            portfolio_report["quarter_end"] = qr.end.isoformat()
        portfolio_result = create_health_deck(portfolio_report, deck_id="portfolio_review", thumbnails=thumbnails)
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
