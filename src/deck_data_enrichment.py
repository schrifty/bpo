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

# HELP KPI deck — lighter Jira fetch (``support_kpis`` blob only).
SUPPORT_KPI_DECK_IDS: frozenset[str] = frozenset({"support-kpis"})

# Decks that need ``eng_portfolio`` from ``get_engineering_portfolio`` when absent.
_ENG_PORTFOLIO_DECK_IDS: frozenset[str] = frozenset({"engineering-portfolio", "implementations_review"})


def enrich_deck_report_data(
    deck_id: str,
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Apply deck-specific enrichment and slide-plan filtering before rendering."""
    report["_slide_plan"] = slide_plan
    if deck_id == "supply_chain_review":
        _stamp_support_deck_generated_at(report)

    if deck_id in SUPPORT_DECK_IDS or deck_id in SUPPORT_KPI_DECK_IDS:
        slide_plan = prepare_support_slide_plan(report, slide_plan, customer)

    if deck_id == "salesforce_comprehensive":
        slide_plan = enrich_salesforce_comprehensive(report, slide_plan, customer)

    if deck_id in SUPPORT_DECK_IDS:
        enrich_support_jira_data(report, customer)

    if deck_id in SUPPORT_KPI_DECK_IDS:
        enrich_support_kpis_jira_data(report, customer)

    if deck_id in _ENG_PORTFOLIO_DECK_IDS:
        enrich_engineering_portfolio_if_needed(report, deck_id=deck_id)
        slide_plan = enrich_cursor_usage_if_needed(report, slide_plan, deck_id=deck_id)
        enrich_github_productivity_if_needed(report, deck_id=deck_id)

    report = enrich_leandna_shortage_if_needed(report, slide_plan, customer)

    from .deck_governance import attach_deck_governance

    attach_deck_governance(report, slide_plan, deck_id=deck_id)
    return report, slide_plan


def enrich_cursor_usage_if_needed(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    *,
    deck_id: str = "engineering-portfolio",
) -> list[dict[str, Any]]:
    """Populate ``cursor_usage`` for eng decks; drop Cursor slides when unconfigured.

    The Cursor slides (cost / usage / user behavior) are optional infrastructure — teams
    without a ``CURSOR_ADMIN_API_KEY`` should not get empty/missing-data slides, so they
    are filtered out of the plan instead. All three share the one ``cursor_usage`` blob.
    """
    cursor_slide_types = {
        "cursor_cost", "cursor_usage", "cursor_usage_non_engineers",
        "cursor_efficiency", "cursor_users", "cursor_users_non_engineers",
    }

    def _drop_cursor(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [e for e in plan if e.get("slide_type") not in cursor_slide_types]

    has_cursor_slide = any(e.get("slide_type") in cursor_slide_types for e in slide_plan)
    if not has_cursor_slide:
        return slide_plan

    from .cursor_client import cursor_configured

    if not cursor_configured():
        logger.info("%s deck: CURSOR_ADMIN_API_KEY not set — omitting Cursor slides", deck_id)
        return _drop_cursor(slide_plan)

    if not report.get("cursor_usage"):
        days = int(report.get("days") or 30)
        try:
            from .cursor_usage_report import (
                build_cursor_usage_report,
                generate_cursor_usage_takeaways,
            )

            logger.info("%s deck: building Cursor usage report", deck_id)
            cursor_usage = build_cursor_usage_report(window_days=days)
            if cursor_usage.get("configured"):
                cursor_usage["takeaways"] = generate_cursor_usage_takeaways(cursor_usage)
            report["cursor_usage"] = cursor_usage
        except Exception as e:
            logger.warning("%s: could not load cursor_usage: %s", deck_id, e)
            report["cursor_usage"] = {"configured": True, "errors": [str(e)]}

    # If the report came back unconfigured for any reason, drop the slides.
    if not (report.get("cursor_usage") or {}).get("configured", False):
        return _drop_cursor(slide_plan)
    return slide_plan


def enrich_github_productivity_if_needed(
    report: dict[str, Any],
    *,
    deck_id: str = "engineering-portfolio",
) -> None:
    """Populate ``github_productivity``, ``ai_productivity``, and QA ``github`` for eng decks."""
    if deck_id not in _ENG_PORTFOLIO_DECK_IDS:
        return

    from .github_client import GitHubClient, _github_org, github_configured

    if not github_configured():
        return

    days = int(report.get("days") or 30)
    try:
        from .ai_productivity_correlation import build_ai_productivity_correlation
        from .engineer_identity_map import build_engineer_identity_map
        from .github_productivity_report import build_github_productivity_report, github_qa_blob
        from .jira_client import JiraClient

        gh = GitHubClient()
        org = _github_org()
        identity = build_engineer_identity_map(
            jira_client=JiraClient(),
            github_client=gh,
            github_org=org,
        )
        gh_prod = build_github_productivity_report(
            window_days=days,
            client=gh,
            identity=identity,
        )
        if not gh_prod:
            return
        report["engineer_identity"] = identity
        report["github_productivity"] = gh_prod
        report["github"] = github_qa_blob(gh_prod)
        cu = report.get("cursor_usage")
        if cu and cu.get("configured"):
            report["ai_productivity"] = build_ai_productivity_correlation(cu, gh_prod, identity)
    except Exception as e:
        logger.warning("%s: github/ai productivity enrichment failed: %s", deck_id, e)
        report["github_productivity"] = {"configured": False, "error": str(e)[:200]}


def enrich_engineering_portfolio_if_needed(report: dict[str, Any], *, deck_id: str = "engineering-portfolio") -> None:
    """Populate ``eng_portfolio`` when absent (e.g. health report lacked Jira portfolio data).

    ``decks.py`` usually pre-fills this for programmatic deck builds; callers with a trimmed
    report may rely on this fetch instead.
    """
    if report.get("eng_portfolio"):
        return
    days = int(report.get("days") or 30)
    try:
        from .jira_client import get_shared_jira_client

        logger.info(
            "%s deck: fetching Jira portfolio snapshot (%d-day window)",
            deck_id,
            days,
        )
        report["eng_portfolio"] = get_shared_jira_client().get_engineering_portfolio(days=days)
    except Exception as e:
        logger.warning("%s: could not load eng_portfolio: %s", deck_id, e)


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


def _support_help_escalation_llm_post(report: dict[str, Any]) -> None:
    hem_post = (report.get("jira") or {}).get("help_escalation_metrics")
    if isinstance(hem_post, dict) and not hem_post.get("error"):
        try:
            from .support_notable_llm import generate_help_escalation_nature_quote_llm

            enq = generate_help_escalation_nature_quote_llm(report)
            if enq:
                hem_post["llm_nature_summary"] = enq
        except Exception as e:
            logger.warning("Support deck: escalation nature quote LLM failed: %s", e)


def enrich_salesforce_comprehensive(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    customer: str | None,
) -> list[dict[str, Any]]:
    """Fetch Salesforce comprehensive data and filter Salesforce slides."""
    from .customer_identity import lookup_salesforce_identity
    from .data_source_health import _salesforce_configured
    from .salesforce_comprehensive_cache import load_or_fetch_salesforce_comprehensive

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
        sf_ids, sf_prim = lookup_salesforce_identity(str(customer or "").strip())
        sf_kwargs: dict[str, Any] = {}
        if sf_ids:
            sf_kwargs["preferred_account_ids"] = sf_ids
            sf_kwargs["primary_account_id"] = sf_prim
        report["salesforce_comprehensive"], _src = load_or_fetch_salesforce_comprehensive(
            str(customer or "").strip(),
            row_limit=75,
            **sf_kwargs,
        )
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


def enrich_support_kpis_jira_data(report: dict[str, Any], customer: str | None) -> None:
    """Fetch HELP KPI bundle for ``support-kpis`` deck."""
    customer_display = customer or "All Customers"
    window_days = int(report.get("days") or 180)
    try:
        from .jira_client import get_shared_jira_client

        jira_client = get_shared_jira_client()
        if "jira" not in report:
            report["jira"] = {}
        if "base_url" not in report["jira"]:
            report["jira"]["base_url"] = (jira_client.base_url or "").rstrip("/")
        if "support_kpis" not in report["jira"]:
            logger.info(
                "Support KPIs deck: fetching HELP KPIs for %s (%dd window)",
                customer_display,
                window_days,
            )
            report["jira"]["support_kpis"] = jira_client.get_support_kpis(
                customer,
                window_days=window_days,
            )
    except Exception as e:
        logger.warning("Support KPIs deck: Jira fetch failed for %s: %s", customer_display, e)
        report.setdefault("jira", {})
        report["jira"]["support_kpis"] = {
            "error": str(e)[:500],
            "customer": customer,
            "window_days": window_days,
        }


def enrich_support_jira_data(report: dict[str, Any], customer: str | None) -> None:
    """Fetch Jira-backed support deck data into ``report``."""
    customer_display = "All Customers" if not customer else customer

    from .integration_drive_cache import (
        KIND_JIRA_SUPPORT,
        integration_drive_cache_reads_enabled,
        save_integration_payload,
        try_load_integration_payload,
    )

    if integration_drive_cache_reads_enabled():
        cached = try_load_integration_payload(KIND_JIRA_SUPPORT, customer)
        if cached is not None:
            report["jira"] = cached
            try:
                from .jira_client import get_shared_jira_client

                jc = get_shared_jira_client()
                report["jira"]["base_url"] = (jc.base_url or "").rstrip("/")
            except Exception as e:
                logger.warning(
                    "Support deck: could not refresh Jira base_url after Drive cache hit: %s",
                    e,
                )
            if isinstance(report.get("jira"), dict) and "help_factory_start_day_buckets" not in report["jira"]:
                try:
                    from .jira_client import get_shared_jira_client as _get_jira

                    jc = _get_jira()
                    report["jira"]["help_factory_start_day_buckets"] = jc.get_help_factory_start_day_buckets(
                        customer,
                    )
                    save_integration_payload(KIND_JIRA_SUPPORT, customer, report["jira"])
                except Exception as e2:
                    logger.warning(
                        "Support deck: factory start HELP buckets fetch failed after cache: %s",
                        e2,
                    )
                    report["jira"]["help_factory_start_day_buckets"] = {
                        "error": str(e2)[:500],
                        "customer": customer,
                    }
            if isinstance(report.get("jira"), dict) and "help_monthly_operational_metrics" not in report["jira"]:
                try:
                    from .jira_client import get_shared_jira_client as _get_jira2

                    jc2 = _get_jira2()
                    report["jira"]["help_monthly_operational_metrics"] = jc2.get_help_monthly_operational_table(
                        customer,
                    )
                    save_integration_payload(KIND_JIRA_SUPPORT, customer, report["jira"])
                except Exception as e3:
                    logger.warning(
                        "Support deck: HELP monthly operational table fetch failed after cache: %s",
                        e3,
                    )
                    report["jira"]["help_monthly_operational_metrics"] = {
                        "error": str(e3)[:500],
                        "customer": customer,
                    }
            _support_help_escalation_llm_post(report)
            logger.info(
                "Support deck: using Drive cache for Jira data (%s)",
                customer_display,
            )
            return

    try:
        from .jira_client import get_shared_jira_client

        jira_client = get_shared_jira_client()

        if "jira" not in report:
            report["jira"] = {}

        if "base_url" not in report["jira"]:
            report["jira"]["base_url"] = (jira_client.base_url or "").rstrip("/")

        if "customer_ticket_metrics" not in report["jira"]:
            logger.info("Support deck: fetching customer ticket metrics for %s", customer_display)
            customer_ticket_metrics = jira_client.get_customer_ticket_metrics(customer)
            report["jira"]["customer_ticket_metrics"] = customer_ticket_metrics

        if "help_factory_start_day_buckets" not in report["jira"]:
            logger.info(
                "Support deck: fetching HELP factory start day buckets for %s",
                customer_display,
            )
            report["jira"]["help_factory_start_day_buckets"] = jira_client.get_help_factory_start_day_buckets(
                customer,
            )

        if "help_monthly_operational_metrics" not in report["jira"]:
            logger.info(
                "Support deck: fetching HELP monthly operational table for %s",
                customer_display,
            )
            report["jira"]["help_monthly_operational_metrics"] = jira_client.get_help_monthly_operational_table(
                customer,
            )

        if "help_ticket_volume_trends" not in report["jira"]:
            logger.info("Support deck: fetching HELP volume trends for %s", customer_display)
            report["jira"]["help_ticket_volume_trends"] = jira_client.get_help_ticket_volume_trends(
                customer,
            )

        if not customer and "help_orgs_by_opened" not in report["jira"]:
            logger.info("Support deck: fetching HELP org ranking (all customers) for %s", customer_display)
            report["jira"]["help_orgs_by_opened"] = jira_client.get_help_organizations_by_opened(
                days=90, max_results=5000
            )

        if "help_customer_escalations" not in report["jira"]:
            logger.info("Support deck: fetching HELP customer escalations for %s", customer_display)
            report["jira"]["help_customer_escalations"] = jira_client.get_help_customer_escalations(
                customer,
            )

        if "help_escalation_metrics" not in report["jira"]:
            logger.info("Support deck: fetching HELP escalation metrics for %s", customer_display)
            report["jira"]["help_escalation_metrics"] = jira_client.get_help_escalation_metrics(
                customer,
            )

        logger.info("Support deck: fetching recent HELP tickets for %s", customer_display)
        customer_help_recent = jira_client.get_customer_help_recent_tickets(
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
        )
        report["jira"]["customer_help_recent"] = customer_help_recent

        logger.info("Support deck: fetching HELP resolved tickets by assignee for %s", customer_display)
        help_resolved_by_assignee = jira_client.get_resolved_tickets_by_assignee(
            "HELP",
            customer,
            days=90,
        )
        report["jira"]["help_resolved_by_assignee"] = help_resolved_by_assignee

        logger.info("Support deck: fetching recent CUSTOMER project tickets for %s", customer_display)
        customer_project_recent = jira_client.get_customer_project_recent_tickets(
            "CUSTOMER",
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
        )
        report["jira"]["customer_project_recent"] = customer_project_recent
        customer_project_open_breakdown = jira_client.get_customer_project_open_breakdown(
            "CUSTOMER",
            customer,
        )
        report["jira"]["customer_project_open_breakdown"] = customer_project_open_breakdown
        logger.info("Support deck: fetching CUSTOMER volume trends for %s", customer_display)
        report["jira"]["customer_project_volume_trends"] = jira_client.get_project_ticket_volume_trends(
            "CUSTOMER", customer
        )
        logger.info("Support deck: fetching CUSTOMER ticket KPI metrics for %s", customer_display)
        report["jira"]["customer_project_ticket_metrics"] = jira_client.get_project_ticket_metrics(
            "CUSTOMER", customer
        )

        logger.info("Support deck: fetching recent LEAN project tickets for %s", customer_display)
        lean_project_recent = jira_client.get_customer_project_recent_tickets(
            "LEAN",
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
        )
        report["jira"]["lean_project_recent"] = lean_project_recent
        lean_project_open_breakdown = jira_client.get_customer_project_open_breakdown(
            "LEAN",
            customer,
        )
        report["jira"]["lean_project_open_breakdown"] = lean_project_open_breakdown
        logger.info("Support deck: fetching LEAN volume trends for %s", customer_display)
        report["jira"]["lean_project_volume_trends"] = jira_client.get_project_ticket_volume_trends(
            "LEAN", customer
        )
        logger.info("Support deck: fetching LEAN ticket KPI metrics for %s", customer_display)
        report["jira"]["lean_project_ticket_metrics"] = jira_client.get_project_ticket_metrics(
            "LEAN", customer
        )

        logger.info("Support deck: fetching CUSTOMER resolved tickets by assignee for %s", customer_display)
        customer_resolved_by_assignee = jira_client.get_resolved_tickets_by_assignee(
            "CUSTOMER",
            customer,
            days=90,
        )
        report["jira"]["customer_resolved_by_assignee"] = customer_resolved_by_assignee

        logger.info("Support deck: fetching LEAN resolved tickets by assignee for %s", customer_display)
        lean_resolved_by_assignee = jira_client.get_resolved_tickets_by_assignee(
            "LEAN",
            customer,
            days=90,
        )
        report["jira"]["lean_resolved_by_assignee"] = lean_resolved_by_assignee

        logger.info(
            "Support deck: fetched data for %s (HELP: %d/%d, CUSTOMER: %d/%d, LEAN: %d/%d, HELP/CUSTOMER/LEAN resolved: %d/%d/%d)",
            customer_display,
            len(customer_help_recent.get("recently_opened", [])),
            len(customer_help_recent.get("recently_closed", [])),
            len(customer_project_recent.get("recently_opened", [])),
            len(customer_project_recent.get("recently_closed", [])),
            len(lean_project_recent.get("recently_opened", [])),
            len(lean_project_recent.get("recently_closed", [])),
            help_resolved_by_assignee.get("total_resolved", 0),
            customer_resolved_by_assignee.get("total_resolved", 0),
            lean_resolved_by_assignee.get("total_resolved", 0),
        )
    except Exception as e:
        _apply_support_jira_error_fallback(report, customer, e)
    else:
        if isinstance(report.get("jira"), dict):
            save_integration_payload(KIND_JIRA_SUPPORT, customer, report["jira"])

    _support_help_escalation_llm_post(report)


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
    report["jira"]["help_factory_start_day_buckets"] = {
        "error": error_text,
        "customer": customer,
        "jql_queries": [],
    }
