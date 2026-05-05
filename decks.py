#!/usr/bin/env python3
"""decks — build Google Slides decks (CS health, portfolio, Jira) and run tooling.

Two ways to run:
  • A flag command (below) — no natural-language parsing.
  • A sentence — an LLM turns it into deck type, quarter, customers, etc. (needs API keys in .env).

────────────────────────────────────────────────────────────────
Flag commands (utilities)
────────────────────────────────────────────────────────────────
  decks --help, -h
      Show this text.

  decks --list
      Print configured deck ids and display names (from local YAML), grouped into
      customer-scoped vs portfolio / cross-customer decks.

  decks --hydrate [customer]
  decks hydrate [customer]
      Hydrate slide content for presentations shared with the intake group (see .env:
      GOOGLE_HYDRATE_INTAKE_GROUP). Optional customer name overrides detection.

  decks --evaluate [--verbose|-v]
      Run reproducibility checks on slides. Summary prints at the end.

  decks --qa <url-or-presentation-id>
      Visual QA for one presentation (URL may contain /presentation/d/<id>/).

  decks --sync-config [--sync-overwrite]
      Upload deck/slide YAML config to Google Drive.

  decks --upload-portfolio-snapshot [--days N] [--max-customers M]
      Run full Pendo portfolio crawl and upload JSON to the portfolio snapshot
      folder: BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID if set, else "Cache" under QBR generator
      under GOOGLE_QBR_GENERATOR_FOLDER_ID. If you omit --days, uses the same
      calendar length as resolve_quarter() (matches default QBR cohort window).
      QBR may auto-refresh this snapshot on weekends when Drive needs an update (see
      ``pendo_portfolio_snapshot_drive.ensure_daily_portfolio_snapshot_for_qbr``).

  decks --all-customer-decks "Customer Name" [--days N] [--quarter Q1 2026] [--thumbnails] [--workers N]
      Run every **customer-scoped** deck id (see ``decks --list``) for one account, in sequence.
      Uses the same health-report path as ``decks <natural language>``; pauses briefly between
      decks to reduce Drive rate limits.

  decks --portfolio [--days N] [--max-customers M] [--quarter …] [--thumbnails] [--csm "Name"]
      Run every **portfolio** deck: portfolio_review, cohort_review, engineering-portfolio,
      implementations_review, support_review_portfolio. Optional ``--csm`` also runs ``csm_book_of_business``
      for that Pendo CSM substring. No customer name — these decks are org- or all-customer scoped.

  decks --data
      Print canonical data element paths from ``config/comprehensive_data_element_list.json``.

────────────────────────────────────────────────────────────────
Generate decks (natural language — LLM parses the prompt)
────────────────────────────────────────────────────────────────
  decks health review for all customers
  decks product adoption for Bombardier and JCI, Q4 2025
  decks portfolio review, max 5 customers
  decks health review for Bombardier, 60 day lookback, with thumbnails

  These flows need Pendo (and related) data sources; a preflight runs first.

  Fixed phrases (no LLM — matched before parsing):
      engineering portfolio | eng portfolio | engineering review | ...
      support | support review | support deck | ...
      support portfolio | support review portfolio  →  Support Review Portfolio (all customers)
      csm book … --csm "<name>"  →  CSM Book of Business (Pendo ownername filter)

  Same Jira-backed decks can also be selected if the LLM returns deck_id
  engineering-portfolio, support, or support_review_portfolio.
"""

import json
import sys
import time
from pathlib import Path

# Same split as ``decks --list`` and batch commands (customer-scoped vs portfolio / cross-customer).
_PORTFOLIO_SCOPE_DECK_IDS: frozenset[str] = frozenset(
    {
        "portfolio_review",
        "cohort_review",
        "engineering-portfolio",
        "implementations_review",
        "support_review_portfolio",
        "csm_book_of_business",
    }
)

# Order for ``--all-customer-decks`` (heavier / slower decks later is arbitrary; adjust if needed).
_CUSTOMER_SCOPED_DECK_BATCH_ORDER: tuple[str, ...] = (
    "cs_health_review",
    "engineering",
    "executive_summary",
    "platform_value_summary",
    "product_adoption",
    "salesforce_comprehensive",
    "supply_chain_review",
    "support",
)

# Order for ``--portfolio`` (Pendo-heavy first, then Jira-only).
_PORTFOLIO_DECK_BATCH_ORDER: tuple[str, ...] = (
    "portfolio_review",
    "cohort_review",
    "engineering-portfolio",
    "implementations_review",
    "support_review_portfolio",
)


_CANONICAL_DATA_CATALOG_PATH = (
    Path(__file__).resolve().parent / "config" / "comprehensive_data_element_list.json"
)


def _run_data_catalog_cli() -> None:
    """Print canonical data element paths from comprehensive_data_element_list.json."""
    catalog_path = _CANONICAL_DATA_CATALOG_PATH
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Missing canonical catalog: {catalog_path}")
        sys.exit(1)
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in canonical catalog ({catalog_path}): {exc}")
        sys.exit(1)

    entries = payload.get("entries")
    if not isinstance(entries, list):
        print(f"Invalid catalog format in {catalog_path}: expected top-level 'entries' list")
        sys.exit(1)

    rows: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        if isinstance(path, str) and path.strip():
            rows.append(path.strip())

    if not rows:
        print(f"0 fields  {catalog_path}")
        return

    width = max(len(path) for path in rows)
    for path in rows:
        print(f"{path:{width}s}")
    print(f"{len(rows)} row(s)  {catalog_path}")


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
                "Use \"portfolio_review\" for cross-customer portfolio health requests. "
                "Use \"cohort_review\" for manufacturing cohort comparison (cohorts.yaml). "
                "Use \"implementations_review\" for Jira CUSTOMER / implementation escalations only (org-wide). "
                "Use \"csm_book_of_business\" for a CSM-scoped portfolio (requires csm_owner).\n"
                "  quarter   – e.g. \"Q1 2026\", \"prev\", \"current\", or null to auto-detect.\n"
                "  days      – integer lookback override, or null.\n"
                "  customers – list of customer name strings, or null for all.\n"
                "  csm_owner – string substring to match Pendo CSM/ownername when deck_id is "
                "csm_book_of_business; null otherwise.\n"
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
    from src.jira_client import get_shared_jira_client
    from src.slides_client import create_health_deck

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)
    print(f"Fetching {label.lower()} data from Jira...")
    t0 = time.time()
    eng_data = get_shared_jira_client().get_engineering_portfolio(days=30)
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


def _run_implementations_review_deck() -> None:
    """Jira CUSTOMER project snapshot deck — same data slice as former eng-portfolio slide."""
    _run_jira_backed_deck("implementations_review", "Implementations review")


def _run_support_deck() -> None:
    """Single support-focused deck from Jira — no LLM prompt parsing required."""
    import argparse
    from src.data_source_health import check_all_required
    from src.slides_client import create_health_deck

    parser = argparse.ArgumentParser(description="Generate support review deck")
    parser.add_argument(
        "--customer",
        type=str,
        default=None,
        help="Customer name to filter tickets (default: all customers across HELP/CUSTOMER/LEAN)",
    )
    
    # Parse only the args after 'support'
    import sys
    support_idx = sys.argv.index("support") if "support" in sys.argv else -1
    args_to_parse = sys.argv[support_idx + 1:] if support_idx >= 0 else []
    args = parser.parse_args(args_to_parse)

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    customer = args.customer
    if customer:
        print(f"Generating support review deck for: {customer}")
    else:
        print("Generating support review deck for: All Customers")
    t0 = time.time()
    
    # Minimal report - let create_health_deck() fetch all support-specific data
    report = {
        "type": "support_review",
        "customer": customer,
        "days": 365,
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


def _run_csm_book_deck() -> None:
    """CSM book of business from ``decks csm book --csm \"Name\"`` (flags after ``book``)."""
    import argparse

    from src.data_source_health import check_all_required
    from src.deck_variants import csm_book_cli_argv_anchor
    from src.quarters import resolve_quarter
    from src.slides_client import create_csm_book_of_business_deck

    anchor = csm_book_cli_argv_anchor(sys.argv[1:])
    rest = sys.argv[anchor + 1:] if anchor >= 0 else []

    parser = argparse.ArgumentParser(description="CSM Book of Business (Pendo CSM / ownername filter)")
    parser.add_argument("--csm", type=str, default=None, help="Substring to match Pendo visitor ownername (CSM)")
    parser.add_argument("--days", type=int, default=None, help="Lookback days (default: quarter window)")
    parser.add_argument("--max-customers", type=int, default=None, dest="max_customers")
    parser.add_argument("--quarter", type=str, default=None, help='e.g. "Q1 2026", prev, current')
    args = parser.parse_args(rest)

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    csm = (args.csm or "").strip()
    if not csm:
        print("Usage: decks csm book --csm \"<Pendo CSM name substring>\" [--days N] [--max-customers M] [--quarter Q1 2026]")
        sys.exit(1)

    if args.days is not None:
        qr = None
        days = int(args.days)
        period_label = f"{days} days"
    else:
        qr = resolve_quarter(args.quarter)
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

    print("Deck:       csm_book_of_business")
    print(f"CSM filter: {csm}")
    print(f"Period:     {period_label}")
    if args.max_customers:
        print(f"Max cust:   {args.max_customers}")
    print()
    t0 = time.time()
    result = create_csm_book_of_business_deck(
        csm_owner=csm,
        days=days,
        max_customers=args.max_customers,
        quarter=qr,
        thumbnails=False,
    )
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:200]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def _run_support_review_portfolio_deck() -> None:
    """All-customers support deck under id ``support_review_portfolio`` (explicit portfolio title)."""
    import argparse
    import sys
    from src.data_source_health import check_all_required
    from src.slides_client import create_health_deck

    parser = argparse.ArgumentParser(description="Generate Support Review Portfolio (all customers)")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Jira lookback days (default: 365, same as support CLI)",
    )
    anchor_idx = -1
    for token in ("support_review_portfolio", "support-review-portfolio", "support-portfolio"):
        if token in sys.argv:
            anchor_idx = max(anchor_idx, sys.argv.index(token))
    if anchor_idx < 0 and "portfolio" in sys.argv and "support" in sys.argv:
        anchor_idx = max(sys.argv.index("support"), sys.argv.index("portfolio"))
    args_to_parse = sys.argv[anchor_idx + 1:] if anchor_idx >= 0 else []
    args = parser.parse_args(args_to_parse)

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    days = int(args.days) if args.days is not None else 365
    print("Generating Support Review Portfolio (all customers)")
    t0 = time.time()
    report = {
        "type": "support_review",
        "customer": None,
        "days": days,
    }
    result = create_health_deck(report, deck_id="support_review_portfolio", thumbnails=False)
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:120]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def _run_all_customer_decks() -> None:
    """CLI: ``--all-customer-decks CUSTOMER`` — every customer-scoped deck type, one account."""
    import argparse

    from src.data_source_health import check_all_required
    from src.quarters import resolve_quarter
    from src.slides_client import create_health_deck, create_health_decks_for_customers

    ap = argparse.ArgumentParser(
        prog="decks",
        description="Run every customer-scoped deck for a single named account (see decks --list).",
    )
    ap.add_argument(
        "--all-customer-decks",
        nargs=1,
        metavar="CUSTOMER",
        required=True,
        help="Pendo customer name (quoted if it contains spaces).",
    )
    ap.add_argument("--days", type=int, default=None, help="Lookback days (default: current quarter window)")
    ap.add_argument("--quarter", type=str, default=None, help='Quarter label, e.g. "Q1 2026", prev, current')
    ap.add_argument("--thumbnails", action="store_true", help="Request slide thumbnails for each deck")
    ap.add_argument("--workers", type=int, default=2, help="Parallel workers per deck (default 2)")
    args = ap.parse_args()

    customer = str(args.all_customer_decks[0]).strip()
    if not customer:
        ap.error("CUSTOMER must be non-empty")

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    if args.days is not None:
        qr = None
        days = int(args.days)
        period_label = f"{days} days"
    else:
        qr = resolve_quarter(args.quarter)
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

    print("Batch:      all customer-scoped decks")
    print(f"Customer:   {customer}")
    print(f"Period:     {period_label}")
    print(f"Deck ids:   {', '.join(_CUSTOMER_SCOPED_DECK_BATCH_ORDER)}")
    print()

    failures = 0
    pause_s = 8
    for deck_id in _CUSTOMER_SCOPED_DECK_BATCH_ORDER:
        print(f"\n{'=' * 60}\nDeck: {deck_id}\n{'=' * 60}")
        t0 = time.time()
        if deck_id == "support":
            report = {"type": "support_review", "customer": customer, "days": 365}
            result = create_health_deck(report, deck_id="support", thumbnails=args.thumbnails)
        else:
            results = create_health_decks_for_customers(
                [customer],
                days=days,
                deck_id=deck_id,
                workers=max(1, int(args.workers)),
                thumbnails=args.thumbnails,
                quarter=qr,
            )
            result = results[0] if results else {"error": "no result", "customer": customer}
        elapsed = time.time() - t0
        if "error" in result:
            failures += 1
            err = str(result.get("error", ""))[:200]
            print(f"  FAIL ({elapsed:.0f}s)  {err}")
        else:
            print(f"  OK   ({elapsed:.0f}s)  {result.get('url', '')}")
        time.sleep(pause_s)

    sys.exit(1 if failures else 0)


def _run_all_portfolio_decks() -> None:
    """CLI: ``--portfolio`` — every portfolio deck; optional ``--csm`` for CSM book."""
    import argparse

    from src.data_source_health import check_all_required
    from src.jira_client import get_shared_jira_client
    from src.quarters import resolve_quarter
    from src.slides_client import (
        create_cohort_deck,
        create_csm_book_of_business_deck,
        create_health_deck,
        create_portfolio_deck,
    )

    ap = argparse.ArgumentParser(
        prog="decks",
        description="Run every portfolio / cross-customer deck (see decks --list).",
    )
    ap.add_argument(
        "--portfolio",
        "--all-portfolio-decks",
        dest="portfolio",
        action="store_true",
        required=True,
        help="Run the full portfolio deck batch (`--all-portfolio-decks` kept as alias).",
    )
    ap.add_argument("--days", type=int, default=None, help="Lookback days (default: current quarter window)")
    ap.add_argument("--max-customers", type=int, default=None, dest="max_customers")
    ap.add_argument("--quarter", type=str, default=None, help='Quarter label, e.g. "Q1 2026", prev, current')
    ap.add_argument("--thumbnails", action="store_true")
    ap.add_argument(
        "--csm",
        type=str,
        default=None,
        metavar="NAME",
        help="If set, also run csm_book_of_business with this Pendo CSM / ownername substring",
    )
    args = ap.parse_args()

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    if args.days is not None:
        qr = None
        days = int(args.days)
        period_label = f"{days} days"
    else:
        qr = resolve_quarter(args.quarter)
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

    print("Batch:      all portfolio decks")
    print(f"Period:     {period_label}")
    if args.max_customers is not None:
        print(f"Max cust:   {args.max_customers}")
    if args.csm:
        print(f"Also CSM:   csm_book_of_business — filter {args.csm!r}")
    print(f"Deck ids:   {', '.join(_PORTFOLIO_DECK_BATCH_ORDER)}" + (" + csm_book_of_business" if args.csm else ""))
    print()

    failures = 0
    pause_s = 8
    eng_portfolio_cache: dict | None = None

    for deck_id in _PORTFOLIO_DECK_BATCH_ORDER:
        print(f"\n{'=' * 60}\nDeck: {deck_id}\n{'=' * 60}")
        t0 = time.time()
        result: dict
        if deck_id == "portfolio_review":
            result = create_portfolio_deck(
                days=days, max_customers=args.max_customers, quarter=qr
            )
        elif deck_id == "cohort_review":
            result = create_cohort_deck(
                days=days,
                max_customers=args.max_customers,
                quarter=qr,
                thumbnails=args.thumbnails,
            )
        elif deck_id == "engineering-portfolio":
            print("Fetching engineering portfolio data from Jira...")
            eng_portfolio_cache = get_shared_jira_client().get_engineering_portfolio(days=30)
            report = {
                "type": "engineering_portfolio",
                "customer": "Engineering",
                "days": 30,
                "eng_portfolio": eng_portfolio_cache,
            }
            result = create_health_deck(
                report, deck_id="engineering-portfolio", thumbnails=args.thumbnails
            )
        elif deck_id == "implementations_review":
            if eng_portfolio_cache is None:
                print("Fetching engineering portfolio data from Jira (implementations review)...")
                eng_portfolio_cache = get_shared_jira_client().get_engineering_portfolio(days=30)
            report = {
                "type": "engineering_portfolio",
                "customer": "Engineering",
                "days": 30,
                "eng_portfolio": eng_portfolio_cache,
            }
            result = create_health_deck(
                report, deck_id="implementations_review", thumbnails=args.thumbnails
            )
        elif deck_id == "support_review_portfolio":
            report = {"type": "support_review", "customer": None, "days": 365}
            result = create_health_deck(
                report, deck_id="support_review_portfolio", thumbnails=args.thumbnails
            )
        else:
            result = {"error": f"unknown portfolio batch id {deck_id!r}"}
        elapsed = time.time() - t0
        if "error" in result:
            failures += 1
            err = str(result.get("error", ""))[:200]
            print(f"  FAIL ({elapsed:.0f}s)  {err}")
        else:
            print(f"  OK   ({elapsed:.0f}s)  {result.get('url', '')}")
        time.sleep(pause_s)

    if args.csm:
        csm = str(args.csm).strip()
        if csm:
            print(f"\n{'=' * 60}\nDeck: csm_book_of_business\n{'=' * 60}")
            t0 = time.time()
            result = create_csm_book_of_business_deck(
                csm_owner=csm,
                days=days,
                max_customers=args.max_customers,
                quarter=qr,
                thumbnails=args.thumbnails,
            )
            elapsed = time.time() - t0
            if "error" in result:
                failures += 1
                err = str(result.get("error", ""))[:200]
                print(f"  FAIL ({elapsed:.0f}s)  {err}")
            else:
                print(f"  OK   ({elapsed:.0f}s)  {result.get('url', '')}")

    sys.exit(1 if failures else 0)


def main():
    # Quick utility flags that don't need LLM parsing
    if "--data" in sys.argv:
        _run_data_catalog_cli()
        return

    if "--list" in sys.argv:
        from src.deck_loader import list_decks

        rows = list_decks()
        port = sorted(
            (m for m in rows if m["id"] in _PORTFOLIO_SCOPE_DECK_IDS),
            key=lambda m: (m.get("name") or m["id"]).lower(),
        )
        cust = sorted(
            (m for m in rows if m["id"] not in _PORTFOLIO_SCOPE_DECK_IDS),
            key=lambda m: (m.get("name") or m["id"]).lower(),
        )
        print("Customer-scoped (one or more named accounts)")
        for m in cust:
            print(f"  {m['name']}")
        print()
        print("Portfolio & cross-customer")
        for m in port:
            print(f"  {m['name']}")
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

    if "--upload-portfolio-snapshot" in sys.argv:
        from src.pendo_portfolio_snapshot_drive import run_upload_portfolio_snapshot_cli
        from src.quarters import resolve_quarter

        days: int | None = None
        max_cust: int | None = None
        argv = sys.argv[1:]
        i = 0
        while i < len(argv):
            if argv[i] == "--days" and i + 1 < len(argv):
                days = int(argv[i + 1])
                i += 2
                continue
            if argv[i] == "--max-customers" and i + 1 < len(argv):
                max_cust = int(argv[i + 1])
                i += 2
                continue
            i += 1
        if days is None:
            days = resolve_quarter(None).days
            print(
                f"Using --days {days} from resolve_quarter() (same window as default QBR); "
                "pass --days explicitly to override."
            )
        print(f"Uploading portfolio snapshot (days={days}, max_customers={max_cust})...")
        result = run_upload_portfolio_snapshot_cli(days, max_cust)
        if result.get("error"):
            print(f"  FAIL: {result['error']}")
            sys.exit(1)
        print(f"  OK   file_id={result.get('file_id')}  {result.get('filename')}")
        print(f"       customers in snapshot: {result.get('customer_count')}")
        return

    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__.strip())
        return

    if "--all-customer-decks" in sys.argv:
        _run_all_customer_decks()
        return
    if "--portfolio" in sys.argv or "--all-portfolio-decks" in sys.argv:
        _run_all_portfolio_decks()
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
    _impl_triggers = (
        "implementations review",
        "implementation review",
        "implementations deck",
        "implementation deck",
        "customer implementation escalations",
    )
    _support_portfolio_triggers = (
        "support review portfolio",
        "support portfolio",
        "support-review-portfolio",
        "support_review_portfolio",
    )
    _support_triggers = ("support review", "support deck", "generate support")
    pl = prompt.lower()
    if any(t in pl for t in _ep_triggers):
        _run_engineering_portfolio_deck()
        return
    if any(t in pl for t in _impl_triggers):
        _run_implementations_review_deck()
        return
    if any(t in pl for t in _support_portfolio_triggers):
        _run_support_review_portfolio_deck()
        return
    # "support" alone, or "support <customer>..." (non-flag args only), or phrases like "support review"
    if pl.strip() == "support" or pl.startswith("support ") or any(t in pl for t in _support_triggers):
        _run_support_deck()
        return

    from src.deck_variants import csm_book_cli_argv_anchor

    if csm_book_cli_argv_anchor(sys.argv[1:]) >= 0:
        _run_csm_book_deck()
        return

    _cohort_exact = (
        "cohort review", "cohorts review", "cohort deck", "cohorts deck",
        "manufacturing cohort", "manufacturing cohorts",
    )
    if pl.strip() in _cohort_exact:
        from src.data_source_health import check_all_required
        from src.quarters import resolve_quarter
        from src.slides_client import create_cohort_deck

        preflight_errors = check_all_required()
        if preflight_errors:
            print("Data source check failed — not running:")
            for msg in preflight_errors:
                print(f"  • {msg}")
            sys.exit(1)
        qr = resolve_quarter(None)
        print("Deck:       cohort_review")
        print(f"Period:     {qr.label} ({qr.start.strftime('%-d %b')} – {qr.end.strftime('%-d %b %Y')}, {qr.days}d)")
        print()
        t0 = time.time()
        result = create_cohort_deck(days=qr.days, quarter=qr, thumbnails=False)
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:80]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
        return

    params = _parse_prompt(prompt)
    deck_id = params.get("deck_id", "cs_health_review")

    if deck_id == "engineering-portfolio":
        _run_engineering_portfolio_deck()
        return
    if deck_id == "implementations_review":
        _run_implementations_review_deck()
        return
    if deck_id == "support":
        _run_support_deck()
        return
    if deck_id == "support_review_portfolio":
        _run_support_review_portfolio_deck()
        return
    if deck_id == "csm_book_of_business":
        _cust_ll = params.get("customers")
        csm_owner = (params.get("csm_owner") or "").strip()
        if not csm_owner and isinstance(_cust_ll, list) and len(_cust_ll) == 1:
            csm_owner = str(_cust_ll[0]).strip()
        if not csm_owner:
            print("csm_book_of_business requires csm_owner (Pendo CSM substring), e.g.:")
            print('  decks csm book --csm "Josh"')
            print('  decks "book of business for Josh Fox"  # LLM must set csm_owner')
            sys.exit(1)
        from src.data_source_health import check_all_required
        from src.quarters import resolve_quarter
        from src.slides_client import create_csm_book_of_business_deck

        preflight_errors = check_all_required()
        if preflight_errors:
            print("Data source check failed — not running:")
            for msg in preflight_errors:
                print(f"  • {msg}")
            sys.exit(1)
        days_override = params.get("days")
        max_cust = params.get("max")
        thumbnails = params.get("thumbnails", False)
        if days_override:
            qr = None
            days = int(days_override)
            period_label = f"{days} days"
        else:
            qr = resolve_quarter(params.get("quarter"))
            days = qr.days
            period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"
        print(f"Deck:       {deck_id}")
        print(f"CSM filter: {csm_owner}")
        print(f"Period:     {period_label}")
        print()
        t0 = time.time()
        result = create_csm_book_of_business_deck(
            csm_owner=csm_owner,
            days=days,
            max_customers=max_cust,
            quarter=qr,
            thumbnails=thumbnails,
        )
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:200]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
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
    from src.slides_client import (
        create_cohort_deck,
        create_health_decks_for_customers,
        create_portfolio_deck,
    )
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

    if deck_id == "cohort_review":
        print(f"Deck:       {deck_id}")
        print(f"Period:     {period_label}")
        if max_cust:
            print(f"Max cust:   {max_cust}")
        print()
        t0 = time.time()
        result = create_cohort_deck(
            days=days,
            max_customers=max_cust,
            quarter=qr,
            thumbnails=thumbnails,
        )
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:80]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
        return

    from src.portfolio_exclude_prefixes import is_skipped_customer_prefix

    if customers_list:
        customers = customers_list
    else:
        print("Fetching customer list from Pendo...")
        customers = PendoClient().get_sites_by_customer(days)["customer_list"]
        customers = [c for c in customers if not is_skipped_customer_prefix(c)]

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

    # Generate a portfolio health deck after per-customer decks
    # Brief cooldown to avoid Drive rate limits after chart/deck creation
    print(f"\nGenerating Portfolio Health deck (pausing 10s for rate limit cooldown)...")
    time.sleep(10)
    t1 = time.time()
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            from src.deck_variants import enrich_portfolio_report_with_revenue_book
            from src.slides_client import create_health_deck

            client = PendoClient()
            portfolio_report = client.get_portfolio_report(days=days, max_customers=max_cust)
            if qr:
                portfolio_report["quarter"] = qr.label
                portfolio_report["quarter_start"] = qr.start.isoformat()
                portfolio_report["quarter_end"] = qr.end.isoformat()
            enrich_portfolio_report_with_revenue_book(portfolio_report)
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
