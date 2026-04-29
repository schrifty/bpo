"""Convenience deck generation entry points built on the core deck creator."""

from __future__ import annotations

from typing import Any

from .config import logger


def enrich_portfolio_report_with_revenue_book(report: dict[str, Any]) -> None:
    """Attach ``portfolio_revenue_book`` from Salesforce (mutates *report* in place)."""
    from .data_source_health import _salesforce_configured

    if not _salesforce_configured():
        report["portfolio_revenue_book"] = {"configured": False}
        return
    customers = report.get("customers") or []
    names = [str(s.get("customer") or "").strip() for s in customers if isinstance(s, dict) and s.get("customer")]
    if not names:
        report["portfolio_revenue_book"] = {
            "configured": True,
            "empty": True,
            "pendo_customers": 0,
            "salesforce_matched_customers": 0,
            "salesforce_unmatched_customers": 0,
            "total_arr": 0.0,
            "active_installed_base_arr": 0.0,
            "churned_contract_arr": 0.0,
            "pipeline_arr": 0.0,
            "opportunity_count_this_year": 0,
            "active_customer_count": 0,
            "churned_customer_count": 0,
            "top_customers_by_arr": [],
            "churned_customer_names_sample": [],
        }
        return
    try:
        from .salesforce_client import SalesforceClient

        sf = SalesforceClient()
        report["portfolio_revenue_book"] = sf.get_portfolio_revenue_book_metrics(names)
    except Exception as e:
        logger.warning("portfolio: Salesforce revenue book enrichment failed: %s", e)
        report["portfolio_revenue_book"] = {"configured": True, "error": str(e)}


def create_portfolio_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
) -> dict[str, Any]:
    """Generate a single portfolio-level deck across all customers."""
    from .deck_orchestrator import create_health_deck
    from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

    report = try_load_portfolio_snapshot_for_request(days, max_customers)
    if report is None:
        from .pendo_client import PendoClient

        client = PendoClient()
        report = client.get_portfolio_report(days=days, max_customers=max_customers)
    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    enrich_portfolio_report_with_revenue_book(report)
    return create_health_deck(report, deck_id="portfolio_review")


def create_cohort_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
    thumbnails: bool = False,
    output_folder_id: str | None = None,
    portfolio_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Single deck: cohort buckets from cohorts.yaml + portfolio metrics."""
    from .deck_orchestrator import create_health_deck

    if portfolio_report is not None:
        report = portfolio_report
    else:
        from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

        report = try_load_portfolio_snapshot_for_request(days, max_customers)
        if report is None:
            from .pendo_client import PendoClient

            client = PendoClient()
            report = client.get_portfolio_report(days=days, max_customers=max_customers)

    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    logger.info(
        "cohort_review: portfolio report ready (%d customers) - sending to Google Slides",
        report.get("customer_count", 0),
    )

    try:
        from .data_source_health import _salesforce_configured

        if _salesforce_configured():
            from .salesforce_client import SalesforceClient

            sf = SalesforceClient()
            digest = report.get("cohort_digest") or {}
            all_names: list[str] = []
            for block in digest.values():
                if isinstance(block, dict):
                    all_names.extend(block.get("customers") or [])
            if all_names:
                arr_map = sf.get_arr_by_customer_names(all_names)
                report["_arr_by_customer"] = arr_map
                logger.info(
                    "cohort_review: loaded ARR for %d/%d customers from Salesforce",
                    len(arr_map),
                    len(all_names),
                )

                active_names = sf.get_active_customer_names(all_names)
                churned = set(all_names) - active_names
                if churned:
                    logger.info("cohort_review: filtering %d churned customer(s) from cohort slides", len(churned))
                    from .pendo_client import compute_cohort_portfolio_rollup

                    customers = report.get("customers") or []
                    active_summaries = [s for s in customers if s.get("customer") not in churned]
                    new_digest, new_findings = compute_cohort_portfolio_rollup(active_summaries)
                    report["cohort_digest"] = new_digest
                    report["cohort_findings_bullets"] = new_findings
                    report["customer_count"] = len(active_summaries)
                    report["_churned_customers"] = sorted(churned)
    except Exception as e:
        logger.warning("cohort_review: Salesforce ARR lookup failed (continuing without): %s", e)

    return create_health_deck(
        report,
        deck_id="cohort_review",
        thumbnails=thumbnails,
        output_folder_id=output_folder_id,
    )


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    deck_id: str = "cs_health_review",
    workers: int = 4,
    thumbnails: bool = False,
    quarter: "QuarterRange | None" = None,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a deck definition in parallel."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from .deck_orchestrator import create_health_deck
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names
    quarter_label = quarter.label if quarter else None
    quarter_start = quarter.start.isoformat() if quarter else None
    quarter_end = quarter.end.isoformat() if quarter else None

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.debug("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, deck_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            if quarter_label:
                report["quarter"] = quarter_label
                report["quarter_start"] = quarter_start
                report["quarter_end"] = quarter_end
            return create_health_deck(report, deck_id=deck_id, thumbnails=thumbnails)
        except Exception as e:
            return {"error": str(e), "customer": name}

    results: list[dict[str, Any]] = [{}] * len(customers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_build_one, (i, n)): i for i, n in enumerate(customers)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"error": str(e), "customer": customers[idx]}
            r = results[idx]
            if "error" in r and "403" in str(r.get("error", "")):
                logger.error("Got 403 for %s - cancelling remaining.", customers[idx])
                for f in futures:
                    f.cancel()
                break

    return results
