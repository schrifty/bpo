#!/usr/bin/env python3
"""cortex — build Google Slides decks (CS health, portfolio, Jira) and run tooling.

All deck generation uses explicit flags or subcommands (no natural-language / LLM parsing).

────────────────────────────────────────────────────────────────
Flag commands (utilities)
────────────────────────────────────────────────────────────────
  cortex --help, -h
      Show this text.

  cortex --list
      Print configured deck ids and display names (from local YAML), grouped into
      customer-scoped vs portfolio / cross-customer decks.

  cortex --qa <url-or-presentation-id>
      Visual QA for one presentation (URL may contain /presentation/d/<id>/).

  cortex --sync-config [--sync-overwrite]
      Upload deck/slide YAML config to Google Drive.

  cortex --upload-portfolio-snapshot [--days N] [--max-customers M]
      Run full Pendo portfolio crawl and upload JSON to the portfolio snapshot
      folder: CORTEX_PORTFOLIO_SNAPSHOT_FOLDER_ID if set, else "Cache" under QBR generator
      under GOOGLE_QBR_GENERATOR_FOLDER_ID. If you omit --days, uses the same
      calendar length as resolve_quarter(). Portfolio runs may auto-refresh this snapshot on weekends when Drive needs an update (see
      ``pendo_portfolio_snapshot_drive.ensure_weekend_portfolio_snapshot``).

  cortex --customer "Customer Name" [--days N] [--quarter Q1 2026] [--thumbnails] [--workers N]
      Run every **customer-scoped** deck id (see ``cortex --list``) for one account, in sequence.
      Pauses briefly between decks to reduce Drive rate limits.

  cortex --portfolio [--days N] [--max-customers M] [--quarter …] [--thumbnails] [--csm "Name"]
      Run every **portfolio** deck: portfolio_review, cohort_review, engineering-portfolio,
      implementations_review, support_review_portfolio. Optional ``--csm`` also runs ``csm_book_of_business``
      for that Pendo CSM substring. No customer name — these decks are org- or all-customer scoped.

  cortex --data
      Print canonical data element paths from ``config/comprehensive_data_element_list.json``.

  cortex --export [--days N] [--max-bytes N] [--signals-cap N]
      [--customers-sf-allowlist] [--customers-exclude-sf-churned]
      [--exclude-customer LABEL ...]
      Build the all-customers LLM context markdown snapshot and upload it to Drive **twice**: under
      ``<QBR Generator>/Output/LLM-Context-All_Customers.md`` (stable) and under the dated folder
      ``Output/{ISO-date} - Output/LLM-Context-All_Customers.md`` (same calendar day).
      Section 7 LLM churn/account-risk insights are always appended to the export markdown.

  cortex --schedule [--prefix NAME] [--region REGION]
      Show EventBridge cron schedules for ECS batch jobs (live AWS when credentials are available,
      plus catalog defaults from ``infra/terraform/variables.tf``). Cron times are UTC.
      Default prefix: ``CORTEX_SCHEDULE_NAME_PREFIX`` or ``bpo`` (matches deployed AWS ``name_prefix``).

  cortex --running [--cluster NAME] [--family FAMILY] [--region REGION]
      List ECS Fargate tasks currently running Cortex batch jobs (``desiredStatus=RUNNING``).
      Defaults: ``CORTEX_ECS_*``, ``terraform.tfvars`` ``name_prefix``, or ``bpo`` / ``bpo-decks``.

  cortex run-job --job <name> [--dry-run] [--no-json-summary]
      Run a declarative batch job from ``config/jobs/<name>.yaml`` (or ``CORTEX_JOB=<name>``).
      Steps invoke ``cortex.py`` subcommands sequentially; emits ``CORTEX_RUN_SUMMARY=…`` on stdout.

────────────────────────────────────────────────────────────────
Generate one deck (explicit)
────────────────────────────────────────────────────────────────
  cortex run --deck <id> [options]
      ``--deck`` must be an id from ``cortex --list``. Typical options:
      ``--customer NAME`` (repeatable), ``--all-customers``, ``--quarter``, ``--days``,
      ``--max-customers``, ``--workers``, ``--thumbnails``. For ``csm_book_of_business`` use ``--csm``.
      Portfolio follow-on deck runs only when using ``--all-customers`` or more than three
      explicit ``--customer`` values (same rule as the old batch behavior).

  cortex cohort [--days N] [--quarter …] [--max-customers M] [--thumbnails]
      Manufacturing cohort review only.

  cortex engineering-portfolio
  cortex implementations-review
      Jira-backed org decks (same payloads as ``--portfolio`` batch).

  cortex regenerate-slides --deck engineering-portfolio --cursor [--presentation-id ID|URL]
      Rebuild Cursor slides in the latest (or specified) Engineering Review presentation in Drive.

  cortex support [--customer NAME]
      Support review deck (single customer or all).

  cortex support-portfolio [--days N]
      All-customers support portfolio deck.

  cortex support-kpis [--customer NAME] [--days N]
      HELP operational KPI deck (intake, flow, backlog, SLA, etc.).

  cortex csm book --csm "<name>" [--days N] [--max-customers M] [--quarter …]
      CSM book of business (Pendo ownername filter).

  cortex kpi [--values] [--requested-sites ID]
      List LeanDNA metrics owned by you (``metrics-get-mine``; Data API only).
      Pass ``--values`` for per-metric datapoint charts.

  cortex metrics-upsert [--date YYYY-MM-DD] [--dry-run] [--metric NAME] [--requested-sites ID]
      For each row in ``config/my-metrics.yaml`` with ``metric-generator`` set, call the generator
      and upsert ``MetricDataPoint`` for that date via the Data API. Rows without a generator
      are skipped.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from src.cli_warning_filters import apply_cli_warning_filters

apply_cli_warning_filters()

# Same split as ``cortex --list`` and batch commands (customer-scoped vs portfolio / cross-customer).
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

# Order for ``--customer`` batch (heavier / slower decks later is arbitrary; adjust if needed).
_CUSTOMER_SCOPED_DECK_BATCH_ORDER: tuple[str, ...] = (
    "cs_health_review",
    "engineering",
    "executive_summary",
    "platform_value_summary",
    "product_adoption",
    "salesforce_comprehensive",
    "supply_chain_review",
    "support",
    "support-kpis",
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


def _run_cohort_review_cli(rest: list[str]) -> None:
    """``cortex cohort …`` — cohort review only."""
    import argparse

    from src.data_source_health import check_all_required
    from src.quarters import resolve_quarter
    from src.slides_client import create_cohort_deck

    ap = argparse.ArgumentParser(prog="cortex cohort", description="Manufacturing cohort review deck.")
    ap.add_argument("--days", type=int, default=None, help="Lookback days (default: quarter window)")
    ap.add_argument("--max-customers", type=int, default=None, dest="max_customers")
    ap.add_argument("--quarter", type=str, default=None, help='e.g. "Q1 2026", prev, current')
    ap.add_argument("--thumbnails", action="store_true")
    args = ap.parse_args(rest)

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
        period_label = f"{qr.label} ({qr.start.strftime('%-d %b')} – {qr.end.strftime('%-d %b %Y')}, {days}d)"

    print("Deck:       cohort_review")
    print(f"Period:     {period_label}")
    print()
    t0 = time.time()
    result = create_cohort_deck(
        days=days,
        max_customers=args.max_customers,
        quarter=qr,
        thumbnails=args.thumbnails,
    )
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:80]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def _run_deck_run_cli(rest: list[str]) -> None:
    """``cortex run --deck ID …`` — one deck with explicit parameters."""
    import argparse

    ap = argparse.ArgumentParser(
        prog="cortex run",
        description="Generate a single deck by id (see cortex --list). No natural-language parsing.",
    )
    ap.add_argument("--deck", required=True, metavar="ID", help="Deck id from cortex --list")
    ap.add_argument("--customer", action="append", dest="customers", metavar="NAME", help="Repeat for multiple accounts")
    ap.add_argument("--all-customers", action="store_true", dest="all_customers", help="Use full Pendo customer list")
    ap.add_argument("--quarter", type=str, default=None)
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--max-customers", type=int, default=None, dest="max_customers")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--thumbnails", action="store_true")
    ap.add_argument(
        "--csm",
        type=str,
        default=None,
        metavar="NAME",
        help="Pendo CSM / ownername substring (required when --deck is csm_book_of_business)",
    )
    args = ap.parse_args(rest)

    from src.data_source_health import check_all_required
    from src.deck_loader import load_deck
    from src.pendo_client import PendoClient
    from src.portfolio_exclude_prefixes import is_skipped_customer_prefix
    from src.quarters import resolve_quarter
    from src.slides_client import (
        create_cohort_deck,
        create_csm_book_of_business_deck,
        create_health_deck,
        create_health_decks_for_customers,
        create_portfolio_deck,
    )

    deck_id = args.deck
    if not load_deck(deck_id):
        ap.error(f"unknown deck id {deck_id!r} (run cortex --list)")

    if args.all_customers and args.customers:
        ap.error("use either --all-customers or one or more --customer, not both")

    no_customer_flags = (
        "portfolio_review",
        "cohort_review",
        "engineering-portfolio",
        "implementations_review",
        "support_review_portfolio",
    )
    if deck_id in no_customer_flags:
        if args.all_customers or args.customers:
            ap.error(f"--deck {deck_id} does not take --customer or --all-customers")

    if deck_id == "csm_book_of_business":
        csm_owner = (args.csm or "").strip()
        if not csm_owner:
            ap.error("--csm is required when --deck is csm_book_of_business")

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    days_override = args.days
    max_cust = args.max_customers
    workers = args.workers or 4
    thumbnails = args.thumbnails

    if days_override is not None:
        qr = None
        days = int(days_override)
        period_label = f"{days} days"
    else:
        qr = resolve_quarter(args.quarter)
        days = qr.days
        period_label = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')}, {days}d)"

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

    if deck_id in ("engineering-portfolio", "implementations_review"):
        _run_jira_backed_deck(deck_id, deck_id.replace("-", " ").title())
        return

    if deck_id == "support_review_portfolio":
        days_sp = int(args.days) if args.days is not None else 365
        print("Deck:       support_review_portfolio")
        print(f"Period:     Jira lookback {days_sp}d")
        t0 = time.time()
        report = {"type": "support_review", "customer": None, "days": days_sp}
        result = create_health_deck(report, deck_id="support_review_portfolio", thumbnails=thumbnails)
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:120]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
        return

    if deck_id == "csm_book_of_business":
        csm_owner = (args.csm or "").strip()
        print(f"Deck:       {deck_id}")
        print(f"CSM filter: {csm_owner}")
        print(f"Period:     {period_label}")
        if max_cust:
            print(f"Max cust:   {max_cust}")
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

    if deck_id == "support":
        cust = None
        if args.customers:
            if len(args.customers) != 1:
                ap.error("--deck support accepts at most one --customer")
            cust = args.customers[0].strip() or None
        if args.all_customers:
            ap.error("--deck support does not support --all-customers (omit --customer for all)")
        print(f"Deck:       support")
        if cust:
            print(f"Customer:   {cust}")
        else:
            print("Customer:   (all)")
        t0 = time.time()
        report = {"type": "support_review", "customer": cust, "days": 365}
        result = create_health_deck(report, deck_id="support", thumbnails=thumbnails)
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:120]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
        return

    if deck_id == "support-kpis":
        cust = None
        if args.customers:
            if len(args.customers) != 1:
                ap.error("--deck support-kpis accepts at most one --customer")
            cust = args.customers[0].strip() or None
        if args.all_customers:
            ap.error("--deck support-kpis does not support --all-customers (omit --customer for all)")
        days_kpi = int(args.days) if args.days is not None else 180
        print("Deck:       support-kpis")
        if cust:
            print(f"Customer:   {cust}")
        else:
            print("Customer:   (all)")
        print(f"Period:     Jira lookback {days_kpi}d")
        t0 = time.time()
        report = {"type": "support_kpis", "customer": cust, "days": days_kpi}
        result = create_health_deck(report, deck_id="support-kpis", thumbnails=thumbnails)
        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"Done in {elapsed:.0f}s")
        print(f"{'=' * 60}")
        if "error" in result:
            print(f"  FAIL: {result['error'][:120]}")
            sys.exit(1)
        print(f"  OK   {result.get('url', '')}")
        return

    explicit_customers: list[str] | None = None
    if args.customers:
        explicit_customers = [str(c).strip() for c in args.customers if str(c).strip()]
        if not explicit_customers:
            explicit_customers = None

    if args.all_customers:
        explicit_customers = None

    skip_portfolio_follow_on = explicit_customers is not None and len(explicit_customers) <= 3

    if explicit_customers is None:
        print("Fetching customer list from Pendo...")
        customers = PendoClient().get_sites_by_customer(days)["customer_list"]
        customers = [c for c in customers if not is_skipped_customer_prefix(c)]
    else:
        customers = list(explicit_customers)

    if max_cust:
        customers = customers[: int(max_cust)]

    print(f"Deck:       {deck_id}")
    print(f"Customers:  {len(customers)}")
    print(f"Period:     {period_label}")
    print(f"Workers:    {workers}")
    print()

    t0 = time.time()
    results = create_health_decks_for_customers(
        customers,
        days=days,
        max_customers=max_cust,
        deck_id=deck_id,
        workers=int(workers),
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
        parts = ["cortex", "run", "--deck", deck_id]
        for n in failed_names:
            parts.extend(["--customer", str(n)])
        if qr:
            parts.extend(["--quarter", qr.label])
        else:
            parts.extend(["--days", str(days)])
        retry_cmd = " ".join(parts)
        print(f"\nTo retry failed customers:\n  {retry_cmd}")

    if skip_portfolio_follow_on:
        sys.exit(1 if fail else 0)

    print(f"\nGenerating Portfolio Health deck (pausing 10s for rate limit cooldown)...")
    time.sleep(10)
    t1 = time.time()
    max_retries = 3
    portfolio_failed = False
    for attempt in range(1, max_retries + 1):
        try:
            from src.deck_variants import enrich_portfolio_report_with_revenue_book

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
                portfolio_failed = True
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
                portfolio_failed = True
                print(f"  FAIL  {err[:120]}  ({p_elapsed:.0f}s)")
                break

    sys.exit(1 if (fail or portfolio_failed) else 0)


def _run_jira_backed_deck(deck_id: str, label: str) -> None:
    """Generate a Jira-backed single deck using engineering portfolio data."""
    from src.config import CORTEX_CURSOR_SLIDES_ONLY
    from src.data_source_health import check_all_required
    from src.jira_client import get_shared_jira_client
    from src.slides_client import create_health_deck

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)
    t0 = time.time()
    if CORTEX_CURSOR_SLIDES_ONLY and deck_id == "engineering-portfolio":
        print("CORTEX_CURSOR_SLIDES_ONLY set — skipping Jira portfolio fetch")
        report = {
            "type": "engineering_portfolio",
            "customer": "Engineering",
            "days": 30,
        }
    else:
        print(f"Fetching {label.lower()} data from Jira...")
        from src.engineering_portfolio_cache import load_or_fetch_engineering_portfolio

        eng_data = load_or_fetch_engineering_portfolio(days=30)
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


def _run_regenerate_slides_cli(rest: list[str]) -> None:
    """``cortex regenerate-slides …`` — rebuild selected slides in an existing presentation."""
    import argparse
    import sys
    import time

    from src.deck_governance import _CURSOR_SLIDE_TYPES
    from src.deck_slide_regenerate import find_latest_presentation_for_deck, regenerate_deck_slides

    ap = argparse.ArgumentParser(
        prog="cortex regenerate-slides",
        description="Rebuild specific slides in an existing Google Slides deck (in place).",
    )
    ap.add_argument("--deck", default="engineering-portfolio", help="Deck id (default: engineering-portfolio)")
    ap.add_argument(
        "--presentation-id",
        metavar="ID_OR_URL",
        default=None,
        help="Presentation to patch (default: newest matching deck in Drive Output)",
    )
    ap.add_argument("--days", type=int, default=30, help="Cursor lookback window (default: 30)")
    ap.add_argument(
        "--cursor",
        action="store_true",
        help="Rebuild all Cursor slide types (cost, usage, efficiency, users, model mix)",
    )
    ap.add_argument(
        "--slide-type",
        action="append",
        dest="slide_types",
        metavar="TYPE",
        help="Repeat to rebuild specific slide_type values (e.g. cursor_efficiency)",
    )
    args = ap.parse_args(rest)

    slide_types: set[str] = set(args.slide_types or [])
    if args.cursor:
        slide_types |= set(_CURSOR_SLIDE_TYPES)
    if not slide_types:
        ap.error("Pass --cursor and/or one or more --slide-type values")

    pres_id = (args.presentation_id or "").strip()
    if not pres_id:
        from src.slides_api import _get_service

        _, drive_service, _ = _get_service()
        found = find_latest_presentation_for_deck(drive_service, deck_id=args.deck)
        if not found:
            print("No matching presentation found in Drive Output — pass --presentation-id")
            sys.exit(1)
        pres_id = found["id"]
        print(f"Target: {found.get('name')} ({pres_id})")

    print(f"Regenerating {len(slide_types)} slide type(s) in {args.deck}…")
    t0 = time.time()
    result = regenerate_deck_slides(
        pres_id,
        deck_id=args.deck,
        slide_types=slide_types,
        days=int(args.days),
    )
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if result.get("error"):
        print(f"  FAIL: {result['error']}")
        if result.get("rebuilt"):
            print(f"  Partial: rebuilt {', '.join(result['rebuilt'])}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")
    print(f"  Rebuilt: {', '.join(result.get('rebuilt') or [])}")
    skipped = result.get("skipped") or []
    if skipped:
        print(f"  Skipped (not found on deck): {', '.join(skipped)}")


def _run_engineering_portfolio_deck() -> None:
    """Single product-level deck from Jira — no LLM prompt parsing required."""
    _run_jira_backed_deck("engineering-portfolio", "Engineering portfolio")


def _run_implementations_review_deck() -> None:
    """Jira CUSTOMER project snapshot deck — same data slice as former eng-portfolio slide."""
    _run_jira_backed_deck("implementations_review", "Implementations review")


def _run_support_deck(rest: list[str]) -> None:
    """``cortex support …`` — single support-focused deck from Jira."""
    import argparse

    from src.data_source_health import check_all_required
    from src.slides_client import create_health_deck

    parser = argparse.ArgumentParser(prog="cortex support", description="Generate support review deck")
    parser.add_argument(
        "--customer",
        type=str,
        default=None,
        help="Customer name to filter tickets (default: all customers across HELP/CUSTOMER/LEAN)",
    )
    args = parser.parse_args(rest)

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


def _run_support_kpis_deck(rest: list[str]) -> None:
    """``cortex support-kpis …`` — HELP operational KPI deck."""
    import argparse

    from src.data_source_health import check_all_required
    from src.slides_client import create_health_deck

    parser = argparse.ArgumentParser(
        prog="cortex support-kpis",
        description="Generate HELP operational KPI deck",
    )
    parser.add_argument("--customer", type=str, default=None, help="Scope to one JSM customer/org")
    parser.add_argument("--days", type=int, default=180, help="Trailing window for KPIs (default: 180)")
    args = parser.parse_args(rest)

    preflight_errors = check_all_required()
    if preflight_errors:
        print("Data source check failed — not running:")
        for msg in preflight_errors:
            print(f"  • {msg}")
        sys.exit(1)

    customer = args.customer
    print(f"Generating support-kpis deck for: {customer or 'All Customers'}")
    print(f"Window: {args.days} days")
    t0 = time.time()
    report = {"type": "support_kpis", "customer": customer, "days": int(args.days)}
    result = create_health_deck(report, deck_id="support-kpis", thumbnails=False)
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s")
    print(f"{'=' * 60}")
    if "error" in result:
        print(f"  FAIL: {result['error'][:120]}")
        sys.exit(1)
    print(f"  OK   {result.get('url', '')}")


def _run_kpi_cli(rest: list[str]) -> None:
    """Run ``scripts/metrics-get-mine.py`` (Data API owned-metrics list)."""
    root = Path(__file__).resolve().parent
    script = root / "scripts" / "metrics-get-mine.py"
    if not script.is_file():
        print(f"error: missing {script}", file=sys.stderr)
        sys.exit(1)
    rc = subprocess.run([sys.executable, str(script), *rest], cwd=str(root)).returncode
    raise SystemExit(rc)


def _run_metrics_upsert_cli(rest: list[str]) -> None:
    """``cortex metrics-upsert`` — generate registry metrics and upsert via Data API."""
    from dotenv import load_dotenv

    from src.metrics_upsert import run_metrics_upsert_cli

    load_dotenv(Path(__file__).resolve().parent / ".env")
    raise SystemExit(run_metrics_upsert_cli(rest, prog="cortex metrics-upsert"))


def _run_csm_book_deck() -> None:
    """CSM book of business from ``cortex csm book --csm \"Name\"`` (flags after ``book``)."""
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
        print("Usage: cortex csm book --csm \"<Pendo CSM name substring>\" [--days N] [--max-customers M] [--quarter Q1 2026]")
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


def _run_support_review_portfolio_deck(rest: list[str]) -> None:
    """``cortex support-portfolio …`` — all-customers support deck."""
    import argparse

    from src.data_source_health import check_all_required
    from src.slides_client import create_health_deck

    parser = argparse.ArgumentParser(
        prog="cortex support-portfolio",
        description="Generate Support Review Portfolio (all customers)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Jira lookback days (default: 365, same as support CLI)",
    )
    args = parser.parse_args(rest)

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
    """CLI: ``--customer CUSTOMER`` — every customer-scoped deck type, one account."""
    import argparse

    from src.data_source_health import check_all_required
    from src.quarters import resolve_quarter
    from src.slides_client import create_health_deck, create_health_decks_for_customers

    ap = argparse.ArgumentParser(
        prog="cortex",
        description="Run every customer-scoped deck for a single named account (see cortex --list).",
    )
    ap.add_argument(
        "--customer",
        "--all-customer-decks",
        dest="customer_batch",
        nargs=1,
        metavar="CUSTOMER",
        required=True,
        help="Pendo customer name (quoted if it contains spaces). ``--all-customer-decks`` is a legacy alias.",
    )
    ap.add_argument("--days", type=int, default=None, help="Lookback days (default: current quarter window)")
    ap.add_argument("--quarter", type=str, default=None, help='Quarter label, e.g. "Q1 2026", prev, current')
    ap.add_argument("--thumbnails", action="store_true", help="Request slide thumbnails for each deck")
    ap.add_argument("--workers", type=int, default=2, help="Parallel workers per deck (default 2)")
    args = ap.parse_args()

    customer = str(args.customer_batch[0]).strip()
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
        elif deck_id == "support-kpis":
            days_kpi = int(args.days) if args.days is not None else 180
            report = {"type": "support_kpis", "customer": customer, "days": days_kpi}
            result = create_health_deck(report, deck_id="support-kpis", thumbnails=args.thumbnails)
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
        prog="cortex",
        description="Run every portfolio / cross-customer deck (see cortex --list).",
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
            from src.config import CORTEX_CURSOR_SLIDES_ONLY

            if CORTEX_CURSOR_SLIDES_ONLY:
                print("CORTEX_CURSOR_SLIDES_ONLY set — skipping Jira portfolio fetch")
                report = {
                    "type": "engineering_portfolio",
                    "customer": "Engineering",
                    "days": 30,
                }
            else:
                print("Fetching engineering portfolio data from Jira...")
                from src.engineering_portfolio_cache import load_or_fetch_engineering_portfolio

                eng_portfolio_cache = load_or_fetch_engineering_portfolio(days=30)
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
                from src.engineering_portfolio_cache import load_or_fetch_engineering_portfolio

                eng_portfolio_cache = load_or_fetch_engineering_portfolio(days=30)
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


def _run_run_job_cli(rest: list[str]) -> None:
    import argparse

    from src.job_runner import run_job

    ap = argparse.ArgumentParser(prog="cortex run-job", description="Run a declarative YAML batch job.")
    ap.add_argument("--job", default=os.environ.get("CORTEX_JOB", "").strip() or None, help="Job name or path")
    ap.add_argument("--dry-run", action="store_true", help="Print steps without executing")
    ap.add_argument("--no-json-summary", action="store_true", help="Omit EMF metrics line from summary")
    args = ap.parse_args(rest)
    if not args.job:
        ap.error("--job is required (or set CORTEX_JOB)")
    sys.exit(
        run_job(
            args.job,
            dry_run=bool(args.dry_run),
            json_summary=not args.no_json_summary,
        )
    )


def main():
    # Utility flags and explicit subcommands (``run``, ``cohort``, …)
    if "--data" in sys.argv:
        _run_data_catalog_cli()
        return

    if "--export" in sys.argv:
        from src.export_llm_context_snapshot import export_main

        rest = [a for a in sys.argv[1:] if a != "--export"]
        export_main(rest, prog="cortex --export")
        return

    if "--schedule" in sys.argv:
        from src.ecs_schedule_report import schedule_main

        rest = [a for a in sys.argv[1:] if a != "--schedule"]
        sys.exit(schedule_main(rest, prog="cortex --schedule"))

    if "--running" in sys.argv:
        from src.ecs_running_report import running_main

        rest = [a for a in sys.argv[1:] if a != "--running"]
        sys.exit(running_main(rest, prog="cortex --running"))

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

    if "--qa" in sys.argv:
        from src.visual_qa import visual_qa
        import re as _re
        rest = " ".join(a for a in sys.argv[1:] if a != "--qa").strip()
        m = _re.search(r"presentation/d/([a-zA-Z0-9_-]+)", rest)
        pres_id = m.group(1) if m else rest
        if not pres_id:
            print("Usage: cortex --qa <presentation-url-or-id>")
            sys.exit(1)
        results = visual_qa(pres_id)
        issues = [r for r in results if not r.get("pass", True)]
        if not issues:
            print("\nAll slides passed visual QA.")
        sys.exit(1 if issues else 0)

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
                f"Using --days {days} from resolve_quarter(); "
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

    # Top-level help only when the first argument is -h/--help (not ``cortex run --help``).
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        print(__doc__.strip())
        return

    # Batch shortcuts only when the *first* argument is the flag (not ``support --customer X`` or
    # ``run --deck … --customer X``, which also contain ``--customer``).
    if len(sys.argv) >= 2 and sys.argv[1] in ("--customer", "--all-customer-decks"):
        _run_all_customer_decks()
        return
    if len(sys.argv) >= 2 and sys.argv[1] in ("--portfolio", "--all-portfolio-decks"):
        _run_all_portfolio_decks()
        return

    if len(sys.argv) <= 1:
        print(__doc__.strip())
        sys.exit(1)

    sub = sys.argv[1]
    if sub == "run":
        _run_deck_run_cli(sys.argv[2:])
        return
    if sub == "run-job":
        _run_run_job_cli(sys.argv[2:])
        return
    if sub == "cohort":
        _run_cohort_review_cli(sys.argv[2:])
        return
    if sub == "engineering-portfolio":
        _run_engineering_portfolio_deck()
        return
    if sub == "implementations-review":
        _run_implementations_review_deck()
        return
    if sub == "regenerate-slides":
        _run_regenerate_slides_cli(sys.argv[2:])
        return
    if sub == "support":
        _run_support_deck(sys.argv[2:])
        return
    if sub in ("support-portfolio", "support_review_portfolio", "support-review-portfolio"):
        _run_support_review_portfolio_deck(sys.argv[2:])
        return
    if sub in ("support-kpis", "support_kpis"):
        _run_support_kpis_deck(sys.argv[2:])
        return
    if sub == "kpi":
        _run_kpi_cli(sys.argv[2:])
        return
    if sub == "metrics-upsert":
        _run_metrics_upsert_cli(sys.argv[2:])
        return

    from src.deck_variants import csm_book_cli_argv_anchor

    if csm_book_cli_argv_anchor(sys.argv[1:]) >= 0:
        _run_csm_book_deck()
        return

    print(f"error: unknown command {sub!r}. Use flags or subcommands — try: cortex --help", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
