"""Deck-specific data enrichment before slide rendering."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import logger
from .slide_leandna_shortage import SLIDES_NEEDING_LEANDNA_SHORTAGE as _SLIDES_NEEDING_LEANDNA_SHORTAGE
from .slide_salesforce import (
    filter_salesforce_comprehensive_slide_plan as _filter_salesforce_comprehensive_slide_plan,
)

# Decks that share support Jira enrichment, Notable second pass, and slide-plan rules.
SUPPORT_DECK_IDS: frozenset[str] = frozenset({"support", "support_review_portfolio"})


def enrich_deck_report_data(
    deck_id: str,
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Apply deck-specific enrichment and slide-plan filtering before rendering."""
    if deck_id == "supply_chain_review":
        _stamp_support_deck_generated_at(report)

    if deck_id in SUPPORT_DECK_IDS:
        slide_plan = prepare_support_slide_plan(report, slide_plan, customer)

    if deck_id == "salesforce_comprehensive":
        slide_plan = enrich_salesforce_comprehensive(report, slide_plan, customer)

    if deck_id in SUPPORT_DECK_IDS:
        enrich_support_jira_data(report, customer, slide_plan)

    if deck_id == "engineering-portfolio":
        enrich_engineering_portfolio_if_needed(report)

    report = enrich_leandna_shortage_if_needed(report, slide_plan, customer)
    return report, slide_plan


def enrich_engineering_portfolio_if_needed(report: dict[str, Any]) -> None:
    """Populate ``eng_portfolio`` when absent (e.g. QBR bundle reuses a health report).

    ``decks.py`` pre-fills this for the standalone CLI path; companion generation passes
    a deep-copied customer report that does not include portfolio-wide LEAN/ER data.
    """
    if report.get("eng_portfolio"):
        return
    days = int(report.get("days") or 30)
    try:
        from .jira_client import get_shared_jira_client

        logger.info(
            "engineering-portfolio deck: fetching Jira portfolio snapshot (%d-day window)",
            days,
        )
        report["eng_portfolio"] = get_shared_jira_client().get_engineering_portfolio(days=days)
    except Exception as e:
        logger.warning("engineering-portfolio: could not load eng_portfolio: %s", e)


def prepare_support_slide_plan(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> list[dict[str, Any]]:
    """Apply support-deck title scoping and all-customer slide filters."""
    if not customer:
        # Avoid "All Customers CUSTOMER ..." (Jira project + audience phrasing clash).
        for entry in slide_plan:
            t = entry.get("title")
            if not isinstance(t, str):
                continue
            t2 = t.replace("All Customers CUSTOMER", "All customers — Jira CUSTOMER")
            t2 = t2.replace("All Customers LEAN", "All customers — Jira LEAN")
            t2 = t2.replace("All Customers HELP", "All customers — Jira HELP")
            entry["title"] = t2
    if customer:
        report["support_deck_scoped_titles"] = True
        # All-customers-only: organization ranking table is not meaningful for a single account.
        slide_plan = [
            e for e in slide_plan
            if e.get("slide_type") != "support_help_orgs_by_opened"
        ]
    else:
        report.pop("support_deck_scoped_titles", None)

    _stamp_support_deck_generated_at(report)
    return slide_plan


def enrich_salesforce_comprehensive(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> list[dict[str, Any]]:
    """Fetch Salesforce comprehensive data and filter Salesforce slides."""
    from .data_source_health import _salesforce_configured

    empty_sf = {
        "customer": customer,
        "accounts": [],
        "account_ids": [],
        "matched": False,
        "opportunity_count_this_year": 0,
        "pipeline_arr": 0.0,
        "row_limit": 75,
        "categories": {},
        "category_errors": {},
    }
    if _salesforce_configured():
        try:
            from .customer_identity import lookup_salesforce_identity
            from .salesforce_client import SalesforceClient

            sf_ids, sf_prim = lookup_salesforce_identity(str(customer or "").strip())
            sf_kwargs: dict[str, Any] = {}
            if sf_ids:
                sf_kwargs["preferred_account_ids"] = sf_ids
                sf_kwargs["primary_account_id"] = sf_prim
            report["salesforce_comprehensive"] = SalesforceClient().get_customer_salesforce_comprehensive(
                customer,
                **sf_kwargs,
            )
        except Exception as e:
            logger.warning("Salesforce comprehensive fetch failed: %s", e)
            report["salesforce_comprehensive"] = {
                **empty_sf,
                "error": str(e)[:500],
            }
    else:
        report["salesforce_comprehensive"] = {**empty_sf, "error": "Salesforce not configured"}

    sfc = report.get("salesforce_comprehensive") or {}
    report["salesforce_primary_account_id"] = sfc.get("primary_account_id")
    resolution = sfc.get("resolution")
    if resolution == "salesforce_account_id":
        report["customer_key_type"] = "salesforce_account_id"
    elif resolution == "name":
        report["customer_key_type"] = "name"
    elif resolution == "none" or sfc.get("matched") is False:
        report["customer_key_type"] = "none"

    return _filter_salesforce_comprehensive_slide_plan(
        slide_plan, report.get("salesforce_comprehensive") or {}
    )


def enrich_support_jira_data(
    report: dict[str, Any],
    customer: str | None,
    slide_plan: list[dict[str, Any]] | None = None,
) -> None:
    """Fetch Jira-backed support deck data into ``report``.

    When ``slide_plan`` is provided (normal ``create_health_deck`` path), only
    Jira **products** required by those slides — plus Notable digest extras when
    ``cs_notable`` is present — are fetched. Unknown ``slide_type`` values fall
    back to the full legacy bundle. When ``slide_plan`` is omitted, the full
    bundle is always fetched (tests and ad-hoc callers).
    """
    from .support_jira_data_products import (
        JIRA_SUPPORT_PRODUCT_IDS,
        collect_support_jira_product_ids,
    )

    customer_display = "All Customers" if not customer else customer

    if slide_plan is None:
        need: frozenset[str] = JIRA_SUPPORT_PRODUCT_IDS
    else:
        products, use_fallback = collect_support_jira_product_ids(slide_plan, customer=customer)
        need = JIRA_SUPPORT_PRODUCT_IDS if use_fallback else products
        if use_fallback:
            logger.info("Support deck: Jira enrichment mode=full (fallback) for %s", customer_display)
        elif need:
            logger.info(
                "Support deck: Jira enrichment mode=selective (%d products) for %s",
                len(need),
                customer_display,
            )
        else:
            logger.info("Support deck: Jira enrichment skipped (no products) for %s", customer_display)

    if not need:
        report.setdefault("jira", {})
        jira = report["jira"]
        if "base_url" not in jira:
            try:
                from .jira_client import get_shared_jira_client

                jira["base_url"] = (get_shared_jira_client().base_url or "").rstrip("/")
            except Exception:
                jira.setdefault("base_url", "")
        return

    try:
        from .jira_client import get_shared_jira_client

        jira_client = get_shared_jira_client()

        if "jira" not in report:
            report["jira"] = {}

        if "base_url" not in report["jira"]:
            report["jira"]["base_url"] = (jira_client.base_url or "").rstrip("/")

        if "customer_ticket_metrics" in need and "customer_ticket_metrics" not in report["jira"]:
            logger.info("Support deck: fetching customer ticket metrics for %s", customer_display)
            report["jira"]["customer_ticket_metrics"] = jira_client.get_customer_ticket_metrics(customer)

        if "help_ticket_volume_trends" in need and "help_ticket_volume_trends" not in report["jira"]:
            logger.info("Support deck: fetching HELP volume trends for %s", customer_display)
            report["jira"]["help_ticket_volume_trends"] = jira_client.get_help_ticket_volume_trends(customer)

        if (
            "help_orgs_by_opened" in need
            and not customer
            and "help_orgs_by_opened" not in report["jira"]
        ):
            logger.info("Support deck: fetching HELP org ranking (all customers) for %s", customer_display)
            report["jira"]["help_orgs_by_opened"] = jira_client.get_help_organizations_by_opened(
                days=90, max_results=5000
            )

        if "help_customer_escalations" in need and "help_customer_escalations" not in report["jira"]:
            logger.info("Support deck: fetching HELP customer escalations for %s", customer_display)
            report["jira"]["help_customer_escalations"] = jira_client.get_help_customer_escalations(customer)

        if "help_escalation_metrics" in need and "help_escalation_metrics" not in report["jira"]:
            logger.info("Support deck: fetching HELP escalation metrics for %s", customer_display)
            report["jira"]["help_escalation_metrics"] = jira_client.get_help_escalation_metrics(customer)

        if "customer_help_recent" in need and "customer_help_recent" not in report["jira"]:
            logger.info("Support deck: fetching recent HELP tickets for %s", customer_display)
            report["jira"]["customer_help_recent"] = jira_client.get_customer_help_recent_tickets(
                customer,
                opened_within_days=None,
                closed_within_days=None,
                max_each=200,
            )

        if "help_resolved_by_assignee" in need and "help_resolved_by_assignee" not in report["jira"]:
            logger.info("Support deck: fetching HELP resolved tickets by assignee for %s", customer_display)
            report["jira"]["help_resolved_by_assignee"] = jira_client.get_resolved_tickets_by_assignee(
                "HELP", customer, days=90
            )

        if "customer_project_recent" in need and "customer_project_recent" not in report["jira"]:
            logger.info("Support deck: fetching recent CUSTOMER project tickets for %s", customer_display)
            report["jira"]["customer_project_recent"] = jira_client.get_customer_project_recent_tickets(
                "CUSTOMER",
                customer,
                opened_within_days=None,
                closed_within_days=None,
                max_each=200,
            )

        if "customer_project_open_breakdown" in need and "customer_project_open_breakdown" not in report["jira"]:
            logger.info("Support deck: fetching CUSTOMER open breakdown for %s", customer_display)
            report["jira"]["customer_project_open_breakdown"] = jira_client.get_customer_project_open_breakdown(
                "CUSTOMER", customer
            )

        if "customer_project_volume_trends" in need and "customer_project_volume_trends" not in report["jira"]:
            logger.info("Support deck: fetching CUSTOMER volume trends for %s", customer_display)
            report["jira"]["customer_project_volume_trends"] = jira_client.get_project_ticket_volume_trends(
                "CUSTOMER", customer
            )

        if "customer_project_ticket_metrics" in need and "customer_project_ticket_metrics" not in report["jira"]:
            logger.info("Support deck: fetching CUSTOMER ticket KPI metrics for %s", customer_display)
            report["jira"]["customer_project_ticket_metrics"] = jira_client.get_project_ticket_metrics(
                "CUSTOMER", customer
            )

        if "lean_project_recent" in need and "lean_project_recent" not in report["jira"]:
            logger.info("Support deck: fetching recent LEAN project tickets for %s", customer_display)
            report["jira"]["lean_project_recent"] = jira_client.get_customer_project_recent_tickets(
                "LEAN",
                customer,
                opened_within_days=None,
                closed_within_days=None,
                max_each=200,
            )

        if "lean_project_open_breakdown" in need and "lean_project_open_breakdown" not in report["jira"]:
            logger.info("Support deck: fetching LEAN open breakdown for %s", customer_display)
            report["jira"]["lean_project_open_breakdown"] = jira_client.get_customer_project_open_breakdown(
                "LEAN", customer
            )

        if "lean_project_volume_trends" in need and "lean_project_volume_trends" not in report["jira"]:
            logger.info("Support deck: fetching LEAN volume trends for %s", customer_display)
            report["jira"]["lean_project_volume_trends"] = jira_client.get_project_ticket_volume_trends(
                "LEAN", customer
            )

        if "lean_project_ticket_metrics" in need and "lean_project_ticket_metrics" not in report["jira"]:
            logger.info("Support deck: fetching LEAN ticket KPI metrics for %s", customer_display)
            report["jira"]["lean_project_ticket_metrics"] = jira_client.get_project_ticket_metrics(
                "LEAN", customer
            )

        if "customer_resolved_by_assignee" in need and "customer_resolved_by_assignee" not in report["jira"]:
            logger.info("Support deck: fetching CUSTOMER resolved tickets by assignee for %s", customer_display)
            report["jira"]["customer_resolved_by_assignee"] = jira_client.get_resolved_tickets_by_assignee(
                "CUSTOMER", customer, days=90
            )

        if "lean_resolved_by_assignee" in need and "lean_resolved_by_assignee" not in report["jira"]:
            logger.info("Support deck: fetching LEAN resolved tickets by assignee for %s", customer_display)
            report["jira"]["lean_resolved_by_assignee"] = jira_client.get_resolved_tickets_by_assignee(
                "LEAN", customer, days=90
            )

        j = report.get("jira") or {}
        chr_ = j.get("customer_help_recent") or {}
        cpr = j.get("customer_project_recent") or {}
        lpr = j.get("lean_project_recent") or {}
        hra = j.get("help_resolved_by_assignee") or {}
        cra = j.get("customer_resolved_by_assignee") or {}
        lra = j.get("lean_resolved_by_assignee") or {}
        logger.info(
            "Support deck: fetched Jira slice for %s (HELP recent %d/%d, CUSTOMER %d/%d, LEAN %d/%d, resolved H/C/L %s/%s/%s)",
            customer_display,
            len(chr_.get("recently_opened", [])),
            len(chr_.get("recently_closed", [])),
            len(cpr.get("recently_opened", [])),
            len(cpr.get("recently_closed", [])),
            len(lpr.get("recently_opened", [])),
            len(lpr.get("recently_closed", [])),
            hra.get("total_resolved", "—"),
            cra.get("total_resolved", "—"),
            lra.get("total_resolved", "—"),
        )
    except Exception as e:
        _apply_support_jira_error_fallback(report, customer, e)

    if "help_escalation_metrics" in need:
        hem_post = (report.get("jira") or {}).get("help_escalation_metrics")
        if isinstance(hem_post, dict) and not hem_post.get("error"):
            try:
                from .support_notable_llm import generate_help_escalation_nature_quote_llm

                enq = generate_help_escalation_nature_quote_llm(report)
                if enq:
                    hem_post["llm_nature_summary"] = enq
            except Exception as e:
                logger.warning("Support deck: escalation nature quote LLM failed: %s", e)


def enrich_leandna_shortage_if_needed(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> dict[str, Any]:
    """Fetch LeanDNA shortage data when the resolved deck plan requires it."""
    if (
        customer
        and slide_plan
        and "leandna_shortage_trends" not in report
        and _SLIDES_NEEDING_LEANDNA_SHORTAGE
        & {str((e or {}).get("slide_type") or (e or {}).get("id") or "") for e in slide_plan}
    ):
        try:
            from .leandna_shortage_enrich import enrich_qbr_with_shortage_trends

            return enrich_qbr_with_shortage_trends(
                report, str(customer).strip(), weeks_forward=12
            )
        except Exception as e:
            logger.warning("create_health_deck: LeanDNA shortage enrichment failed: %s", e)
            report.setdefault(
                "leandna_shortage_trends",
                {"enabled": False, "reason": str(e)[:200]},
            )
    return report


def _stamp_support_deck_generated_at(report: dict[str, Any]) -> None:
    report["support_deck_generated_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def _apply_support_jira_error_fallback(
    report: dict[str, Any],
    customer: str | None,
    error: Exception,
) -> None:
    logger.warning("Support deck: Jira data fetch failed for %s: %s", customer, error)
    if "jira" not in report:
        report["jira"] = {}
    error_text = str(error)[:500]
    if "customer_ticket_metrics" not in report["jira"]:
        report["jira"]["customer_ticket_metrics"] = {
            "error": error_text,
            "customer": customer,
        }
    report["jira"]["customer_help_recent"] = {
        "error": error_text,
        "customer": customer,
        "recently_opened": [],
        "recently_closed": [],
    }
    report["jira"]["customer_project_recent"] = {
        "error": error_text,
        "project": "CUSTOMER",
        "customer": customer,
        "recently_opened": [],
        "recently_closed": [],
    }
    report["jira"]["lean_project_recent"] = {
        "error": error_text,
        "project": "LEAN",
        "customer": customer,
        "recently_opened": [],
        "recently_closed": [],
    }
    report["jira"]["customer_project_open_breakdown"] = {
        "error": error_text,
        "project": "CUSTOMER",
        "customer": customer,
        "unresolved_count": 0,
        "by_type_open": {},
        "by_status_open": {},
    }
    report["jira"]["lean_project_open_breakdown"] = {
        "error": error_text,
        "project": "LEAN",
        "customer": customer,
        "unresolved_count": 0,
        "by_type_open": {},
        "by_status_open": {},
    }
    report["jira"]["help_resolved_by_assignee"] = {
        "error": error_text,
        "project": "HELP",
        "customer": customer,
        "by_assignee": [],
        "total_resolved": 0,
    }
    report["jira"]["customer_resolved_by_assignee"] = {
        "error": error_text,
        "project": "CUSTOMER",
        "customer": customer,
        "by_assignee": [],
        "total_resolved": 0,
    }
    report["jira"]["lean_resolved_by_assignee"] = {
        "error": error_text,
        "project": "LEAN",
        "customer": customer,
        "by_assignee": [],
        "total_resolved": 0,
    }
    report["jira"]["help_ticket_volume_trends"] = {
        "error": error_text,
        "customer": customer,
        "all": [],
        "escalated": [],
        "non_escalated": [],
    }
    report["jira"]["customer_project_volume_trends"] = {
        "error": error_text,
        "all": [],
        "escalated": [],
        "non_escalated": [],
    }
    report["jira"]["lean_project_volume_trends"] = {
        "error": error_text,
        "all": [],
        "escalated": [],
        "non_escalated": [],
    }
    report["jira"]["customer_project_ticket_metrics"] = {
        "error": error_text,
        "project": "CUSTOMER",
        "customer": customer,
    }
    report["jira"]["lean_project_ticket_metrics"] = {
        "error": error_text,
        "project": "LEAN",
        "customer": customer,
    }
    report["jira"]["help_orgs_by_opened"] = {
        "error": error_text,
        "by_organization": [],
        "total_issues": 0,
        "days": 90,
    }
    report["jira"]["help_customer_escalations"] = {
        "error": error_text,
        "customer": customer,
        "tickets": [],
    }
    report["jira"]["help_escalation_metrics"] = {
        "error": error_text,
        "customer": customer,
        "not_done_escalation_count": 0,
        "escalations_opened_90d": 0,
        "escalations_closed_90d": 0,
    }
