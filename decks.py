#!/usr/bin/env python3
"""Generate decks from a natural-language prompt.

Usage:
    decks health review for all customers
    decks product adoption for Bombardier and JCI, Q4 2025
    decks portfolio review, max 5 customers
    decks health review for Bombardier, 60 day lookback, with thumbnails
    decks engineering portfolio   (no LLM prompt-parse — matches phrase before _parse_prompt)
    decks --list
    decks --sync-config
    decks --evaluate
    decks hydrate                (decks shared with GOOGLE_HYDRATE_INTAKE_GROUP)
    decks hydrate Bombardier     (override customer)
    decks --hydrate / --hydrate Bombardier   (same)
    decks --qa <presentation-url>
"""

import json
import sys
import time


def _parse_prompt(prompt: str) -> dict:
    """Use a lightweight LLM call to extract structured parameters from a prompt.

    Uses the same provider as the rest of the app (Gemini or OpenAI from .env).
    """
    from src.config import LLM_MODEL_FAST, llm_client
    from src.deck_loader import list_decks

    deck_ids = [d["id"] for d in list_decks()]
    client = llm_client()
    resp = client.chat.completions.create(
        model=LLM_MODEL_FAST,
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


def _run_jira_backed_deck(deck_id: str, label: str) -> None:
    """Generate a Jira-backed single deck using engineering portfolio data."""
    from src.data_source_health import check_all_required
    from src.jira_client import JiraClient
    from src.slides_client import create_health_deck

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)
    print(f"Fetching {label.lower()} data from Jira...")
    t0 = time.time()
    eng_data = JiraClient().get_engineering_portfolio(days=30)
    report = {
        "type": "engineering_portfolio",
        "customer": "Engineering",
        "days": 30,
        "eng_portfolio": eng_data,
    }
    result = create_health_deck(report, deck_id=deck_id, thumbnails=False)
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:120]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def _run_engineering_portfolio_deck() -> None:
    """Single product-level deck from Jira — no LLM prompt parsing required."""
    _run_jira_backed_deck("engineering-portfolio", "Engineering portfolio")


def _run_support_deck() -> None:
    """Single support-focused deck from Jira — no LLM prompt parsing required."""
    from src.data_source_health import check_all_required
    from src.jira_client import JiraClient
    from src.slides_client import create_health_deck

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    print("Fetching support review data from Jira...")
    t0 = time.time()
    client = JiraClient()
    eng_data = client.get_engineering_portfolio(days=30)
    safran_name = "Safran Electronics & Defense (SED)"
    safran_metrics = client.get_customer_ticket_metrics(
        safran_name,
        match_terms=["Safran Electronics and Defense", "SED", "Defense"],
    )
    report = {
        "type": "support_review",
        "customer": safran_name,
        "days": 365,
        "eng_portfolio": eng_data,
        "jira": {
            "customer_ticket_metrics": safran_metrics,
        },
    }
    result = create_health_deck(report, deck_id="support", thumbnails=False)
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:120]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def main():
    # Quick utility flags that don't need LLM parsing
    if "--list" in sys.argv:
        from src.deck_loader import list_decks
        for m in list_decks():
            print(f"  {m['id']:25s}  {m['name']}")
        return

    if "--evaluate" in sys.argv:
        from src.evaluate import evaluate_new_slides
        verbose = "--verbose" in sys.argv or "-v" in sys.argv
        results = evaluate_new_slides(verbose=verbose)
        if results:
            reproducible = sum(1 for r in results if "fully" in r.get("feasibility", ""))
            mostly = sum(1 for r in results if "mostly" in r.get("feasibility", ""))
            partial = sum(1 for r in results if "partially" in r.get("feasibility", ""))
            blocked = sum(1 for r in results if "not" in r.get("feasibility", ""))
            print(f"{'=' * 60}")
            print(f"Summary: {len(results)} slides evaluated")
            print(f"  ✅ Fully reproducible:     {reproducible}")
            print(f"  🟡 Mostly reproducible:    {mostly}")
            print(f"  🟠 Partially reproducible: {partial}")
            print(f"  ❌ Not reproducible:        {blocked}")
            print(f"{'=' * 60}")
        return

    if "--qa" in sys.argv:
        from src.evaluate import visual_qa
        import re as _re
        rest = " ".join(a for a in sys.argv[1:] if a != "--qa").strip()
        m = _re.search(r"presentation/d/([a-zA-Z0-9_-]+)", rest)
        pres_id = m.group(1) if m else rest
        if not pres_id:
            print("Usage: decks --qa <presentation-url-or-id>")
            sys.exit(1)
        results = visual_qa(pres_id)
        issues = [r for r in results if not r.get("pass", True)]
        if not issues:
            print("\nAll slides passed visual QA.")
        sys.exit(1 if issues else 0)

    if "--hydrate" in sys.argv:
        from src.evaluate import hydrate_new_slides
        rest = [a for a in sys.argv[1:] if a not in ("--hydrate",)]
        override = " ".join(rest).strip() if rest else None
        if not override:
            full = " ".join(sys.argv[1:])
            import re
            m = re.search(r"(?:for|hydrate)\s+(.+)", full, re.I)
            override = m.group(1).strip() if m else None
        hydrate_new_slides(customer_override=override)
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

    prompt = " ".join(a for a in sys.argv[1:] if not a.startswith("-")).strip()
    if not prompt:
        print(__doc__.strip())
        sys.exit(1)

    # "decks hydrate" or "decks hydrate Bombardier" / "decks hydrate for Safran" → group intake
    # Hydrate only needs Drive access — no Pendo/SF/CSR preflight required
    if prompt.lower() == "hydrate" or prompt.lower().startswith("hydrate "):
        from src.evaluate import hydrate_new_slides
        rest = prompt[7:].strip()  # after "hydrate"
        if rest.lower().startswith("for "):
            rest = rest[4:].strip()
        override = rest if rest else None
        hydrate_new_slides(customer_override=override)
        return

    # Engineering portfolio — run before _parse_prompt so LLM is not required for this phrase
    _ep_triggers = ("engineering portfolio", "eng portfolio", "engineering review",
                    "generate the engineering", "generate engineering")
    _support_triggers = ("support review", "support deck", "generate support")
    pl = prompt.lower()
    if any(t in pl for t in _ep_triggers):
        _run_engineering_portfolio_deck()
        return
    if pl.strip() == "support" or any(t in pl for t in _support_triggers):
        _run_support_deck()
        return

    params = _parse_prompt(prompt)
    deck_id = params.get("deck_id", "cs_health_review")

    if deck_id == "engineering-portfolio":
        _run_engineering_portfolio_deck()
        return
    if deck_id == "support":
        _run_support_deck()
        return

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
    from src.data_source_health import check_all_required

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

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

    # Only generate portfolio deck when running for multiple customers (not a targeted run)
    if customers_list and len(customers_list) <= 3:
        sys.exit(1 if fail else 0)

    # Generate a portfolio (book of business) deck after per-customer decks
    # Brief cooldown to avoid Drive rate limits after chart/deck creation
    print(f"\nGenerating Book of Business deck (pausing 10s for rate limit cooldown)...")
    time.sleep(10)
    t1 = time.time()
    max_retries = 3
    for attempt in range(1, max_retries + 1):
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
            if "error" in portfolio_result and "rate" in portfolio_result["error"].lower():
                if attempt < max_retries:
                    wait = 15 * attempt
                    print(f"  Rate limited, retrying in {wait}s (attempt {attempt}/{max_retries})...")
                    time.sleep(wait)
                    continue
            if "error" in portfolio_result:
                print(f"  FAIL  {portfolio_result['error'][:120]}  ({p_elapsed:.0f}s)")
            else:
                print(f"  OK    {portfolio_result.get('url', '')}  ({p_elapsed:.0f}s)")
            break
        except Exception as e:
            p_elapsed = time.time() - t1
            err = str(e)
            if "rate" in err.lower() and attempt < max_retries:
                wait = 15 * attempt
                print(f"  Rate limited, retrying in {wait}s (attempt {attempt}/{max_retries})...")
                time.sleep(wait)
            else:
                print(f"  FAIL  {err[:120]}  ({p_elapsed:.0f}s)")
                break

    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
