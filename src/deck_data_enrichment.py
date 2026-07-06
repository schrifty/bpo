"""Deck-specific data enrichment before slide rendering."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .config import CORTEX_CURSOR_SLIDES_ONLY, CORTEX_SUPPORT_JIRA_ALLOW_FALLBACK, logger
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

_GITHUB_PRODUCTIVITY_SLIDE_TYPES = frozenset({
    "github_engineering_output",
    "productivity_summary",
    "productivity_trend",
    "productivity_coaching",
    "ai_output_correlation",
    "ai_productivity_matrix",
})


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
        enrich_support_jira_data(report, customer, slide_plan)

    if deck_id in SUPPORT_KPI_DECK_IDS:
        enrich_support_kpis_jira_data(report, customer)

    if deck_id in _ENG_PORTFOLIO_DECK_IDS:
        slide_plan = filter_cursor_only_slide_plan(slide_plan, deck_id=deck_id)
        if not CORTEX_CURSOR_SLIDES_ONLY:
            enrich_engineering_portfolio_if_needed(report, deck_id=deck_id)
        slide_plan = enrich_cursor_usage_if_needed(report, slide_plan, deck_id=deck_id)
        if not CORTEX_CURSOR_SLIDES_ONLY:
            enrich_github_productivity_if_needed(report, deck_id=deck_id)
            slide_plan = filter_github_productivity_slides(report, slide_plan, deck_id=deck_id)

    report = enrich_leandna_shortage_if_needed(report, slide_plan, customer)

    from .deck_governance import attach_deck_governance

    attach_deck_governance(report, slide_plan, deck_id=deck_id)
    return report, slide_plan


def filter_cursor_only_slide_plan(
    slide_plan: list[dict[str, Any]],
    *,
    deck_id: str = "engineering-portfolio",
) -> list[dict[str, Any]]:
    """When ``CORTEX_CURSOR_SLIDES_ONLY`` is set, keep only Cursor slide types in the plan."""
    if not CORTEX_CURSOR_SLIDES_ONLY or deck_id not in _ENG_PORTFOLIO_DECK_IDS:
        return slide_plan
    from .deck_governance import _CURSOR_SLIDE_TYPES

    filtered = [e for e in slide_plan if (e.get("slide_type") or "") in _CURSOR_SLIDE_TYPES]
    logger.debug(
        "%s deck: CORTEX_CURSOR_SLIDES_ONLY — building %d cursor slide(s) only",
        deck_id,
        len(filtered),
    )
    return filtered


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
    from .deck_governance import _CURSOR_SLIDE_TYPES

    cursor_slide_types = set(_CURSOR_SLIDE_TYPES)

    def _drop_cursor(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [e for e in plan if e.get("slide_type") not in cursor_slide_types]

    has_cursor_slide = any(e.get("slide_type") in cursor_slide_types for e in slide_plan)
    if not has_cursor_slide:
        return slide_plan

    from .cursor_client import cursor_configured

    if not cursor_configured():
        logger.debug("%s deck: CURSOR_ADMIN_API_KEY not set — omitting Cursor slides", deck_id)
        return _drop_cursor(slide_plan)

    if not report.get("cursor_usage"):
        days = int(report.get("days") or 30)
        try:
            from .cursor_usage_report import (
                build_cursor_usage_report,
                generate_cursor_usage_takeaways,
            )

            logger.debug("%s deck: building Cursor usage report", deck_id)
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


def filter_github_productivity_slides(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    *,
    deck_id: str = "engineering-portfolio",
) -> list[dict[str, Any]]:
    """Drop GitHub/AI productivity slides when report blobs are missing."""
    if deck_id not in _ENG_PORTFOLIO_DECK_IDS:
        return slide_plan
    gp_ok = bool((report.get("github_productivity") or {}).get("configured"))
    ai_ok = bool((report.get("ai_productivity") or {}).get("configured"))

    def _keep(entry: dict[str, Any]) -> bool:
        st = (entry.get("slide_type") or "").strip()
        if st in (
            "github_engineering_output",
            "github_engineer_contribution",
            "github_delivery_flow",
            "github_change_profile",
        ) and not gp_ok:
            return False
        if st in (
            "productivity_summary",
            "productivity_trend",
            "productivity_coaching",
            "ai_output_correlation",
            "ai_productivity_matrix",
        ) and not ai_ok:
            return False
        return True

    return [e for e in slide_plan if _keep(e)]


def _attach_productivity_takeaways(
    github_productivity: dict[str, Any] | None,
    ai_productivity: dict[str, Any] | None,
) -> None:
    if github_productivity and github_productivity.get("configured"):
        from .github_productivity_report import (
            compute_github_change_insights,
            compute_github_contribution_insights,
            compute_github_delivery_insights,
            compute_github_output_insights,
        )

        delivery = compute_github_delivery_insights(github_productivity)
        github_productivity["delivery_insights"] = delivery
        github_productivity["takeaways"] = {
            "github_output": compute_github_output_insights(github_productivity),
            "github_contribution": compute_github_contribution_insights(github_productivity),
            "github_change": compute_github_change_insights(github_productivity),
            "github_delivery": delivery["takeaway"],
        }
    if ai_productivity and ai_productivity.get("configured"):
        from .ai_productivity_correlation import (
            compute_ai_correlation_insights,
            compute_ai_matrix_insights,
            compute_productivity_coaching_insights,
            compute_productivity_summary_insights,
            compute_productivity_trend_insights,
        )

        ai_productivity["takeaways"] = {
            "correlation": compute_ai_correlation_insights(ai_productivity),
            "matrix": compute_ai_matrix_insights(ai_productivity),
            "productivity_summary": compute_productivity_summary_insights(ai_productivity),
            "productivity_trend": compute_productivity_trend_insights(ai_productivity),
            "productivity_coaching": compute_productivity_coaching_insights(ai_productivity),
        }


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
        from .jira_client import get_shared_jira_client

        gh = GitHubClient()
        org = _github_org()
        cu = report.get("cursor_usage") or {}
        scope = cu.get("engineer_scope") or {}
        preset_emails: set[str] | None = None
        if scope.get("configured") and scope.get("emails"):
            preset_emails = {str(e).strip().casefold() for e in scope["emails"] if e}
        identity = build_engineer_identity_map(
            engineer_emails=preset_emails,
            jira_client=get_shared_jira_client(),
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
        ai_prod = None
        if cu and cu.get("configured"):
            ai_prod = build_ai_productivity_correlation(cu, gh_prod, identity)
            report["ai_productivity"] = ai_prod
        _attach_productivity_takeaways(gh_prod, ai_prod)
        if identity.get("stats", {}).get("engineer_count") and identity.get("stats", {}).get("with_github_login") == 0:
            logger.warning(
                "%s: GitHub productivity loaded but no engineer GitHub logins mapped — check aliases",
                deck_id,
            )
    except Exception as e:
        logger.warning("%s: github/ai productivity enrichment failed: %s", deck_id, e)
        if not (report.get("github_productivity") or {}).get("configured"):
            report["github_productivity"] = {"configured": False, "error": str(e)[:200]}


def enrich_engineering_portfolio_if_needed(report: dict[str, Any], *, deck_id: str = "engineering-portfolio") -> None:
    """Populate ``eng_portfolio`` when absent (e.g. health report lacked Jira portfolio data).

    ``cortex.py`` usually pre-fills this for programmatic deck builds; callers with a trimmed
    report may rely on this fetch instead.
    """
    if report.get("eng_portfolio"):
        return
    days = int(report.get("days") or 30)
    try:
        from .engineering_portfolio_cache import load_or_fetch_engineering_portfolio

        logger.debug(
            "%s deck: loading Jira portfolio snapshot (%d-day window)",
            deck_id,
            days,
        )
        report["eng_portfolio"] = load_or_fetch_engineering_portfolio(days=days)
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
            logger.debug(
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


def _support_jira_product_missing_or_errored(jira: dict[str, Any], product: str) -> bool:
    if product not in jira:
        return True
    val = jira.get(product)
    return isinstance(val, dict) and bool(str(val.get("error") or "").strip())


def _resolve_support_jira_need(
    slide_plan: list[dict[str, Any]] | None,
    customer: str | None,
) -> tuple[frozenset[str], str]:
    from .support_jira_data_products import (
        JIRA_SUPPORT_PRODUCT_IDS,
        collect_support_jira_product_ids,
    )

    if slide_plan is None:
        return JIRA_SUPPORT_PRODUCT_IDS, "full"
    products, use_fallback = collect_support_jira_product_ids(slide_plan, customer=customer)
    if use_fallback:
        return JIRA_SUPPORT_PRODUCT_IDS, "full_fallback"
    return products, "selective"


def _handle_support_jira_fetch_failure(
    report: dict[str, Any],
    customer: str | None,
    error: Exception,
) -> None:
    logger.warning("Support deck: Jira data fetch failed for %s: %s", customer, error)
    report.setdefault("jira", {})
    report["jira"]["error"] = str(error)[:500]
    if CORTEX_SUPPORT_JIRA_ALLOW_FALLBACK:
        _apply_support_jira_error_fallback(report, customer, error)
        return
    report["error"] = f"Support deck: Jira fetch failed: {error}"


# HELP Jira products that share one JSM org-resolution pass per support deck run.
_HELP_CLAUSE_PRODUCTS: frozenset[str] = frozenset(
    {
        "customer_ticket_metrics",
        "help_ticket_volume_trends",
        "help_customer_escalations",
        "help_escalation_metrics",
        "help_monthly_operational_metrics",
        "customer_help_recent",
        "help_resolved_by_assignee",
    }
)


def _collect_support_jira_fetch_jobs(
    jira_client: Any,
    jira: dict[str, Any],
    customer: str | None,
    need: frozenset[str],
    *,
    help_clause: tuple[str, list[str]] | None,
) -> list[tuple[str, Callable[[], Any]]]:
    jobs: list[tuple[str, Callable[[], Any]]] = []

    def add(product: str, fn: Callable[[], Any]) -> None:
        if product in need and _support_jira_product_missing_or_errored(jira, product):
            jobs.append((product, fn))

    hc = help_clause

    add(
        "customer_ticket_metrics",
        lambda: jira_client.get_customer_ticket_metrics(customer, _prebuilt_clause=hc),
    )
    add(
        "help_factory_start_day_buckets",
        lambda: jira_client.get_help_factory_start_day_buckets(customer),
    )
    add(
        "help_monthly_operational_metrics",
        lambda: jira_client.get_help_monthly_operational_table(customer, _prebuilt_clause=hc),
    )
    add(
        "help_ticket_volume_trends",
        lambda: jira_client.get_help_ticket_volume_trends(customer, _prebuilt_clause=hc),
    )
    if not customer:
        add(
            "help_orgs_by_opened",
            lambda: jira_client.get_help_organizations_by_opened(days=90, max_results=5000),
        )
    add(
        "help_customer_escalations",
        lambda: jira_client.get_help_customer_escalations(customer, _prebuilt_clause=hc),
    )
    add(
        "help_escalation_metrics",
        lambda: jira_client.get_help_escalation_metrics(customer, _prebuilt_clause=hc),
    )
    add(
        "customer_help_recent",
        lambda: jira_client.get_customer_help_recent_tickets(
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
            _prebuilt_clause=hc,
        ),
    )
    add(
        "help_resolved_by_assignee",
        lambda: jira_client.get_resolved_tickets_by_assignee(
            "HELP", customer, days=90, _prebuilt_clause=hc
        ),
    )
    add(
        "customer_project_recent",
        lambda: jira_client.get_customer_project_recent_tickets(
            "CUSTOMER",
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
        ),
    )
    add(
        "customer_project_open_breakdown",
        lambda: jira_client.get_customer_project_open_breakdown("CUSTOMER", customer),
    )
    add(
        "customer_project_volume_trends",
        lambda: jira_client.get_project_ticket_volume_trends("CUSTOMER", customer),
    )
    add(
        "customer_project_ticket_metrics",
        lambda: jira_client.get_project_ticket_metrics("CUSTOMER", customer),
    )
    add(
        "lean_project_recent",
        lambda: jira_client.get_customer_project_recent_tickets(
            "LEAN",
            customer,
            opened_within_days=None,
            closed_within_days=None,
            max_each=200,
        ),
    )
    add(
        "lean_project_open_breakdown",
        lambda: jira_client.get_customer_project_open_breakdown("LEAN", customer),
    )
    add(
        "lean_project_volume_trends",
        lambda: jira_client.get_project_ticket_volume_trends("LEAN", customer),
    )
    add(
        "lean_project_ticket_metrics",
        lambda: jira_client.get_project_ticket_metrics("LEAN", customer),
    )
    add(
        "customer_resolved_by_assignee",
        lambda: jira_client.get_resolved_tickets_by_assignee("CUSTOMER", customer, days=90),
    )
    add(
        "lean_resolved_by_assignee",
        lambda: jira_client.get_resolved_tickets_by_assignee("LEAN", customer, days=90),
    )
    return jobs


def _fetch_support_jira_products(
    jira_client: Any,
    report: dict[str, Any],
    customer: str | None,
    need: frozenset[str],
    customer_display: str,
) -> None:
    if "jira" not in report:
        report["jira"] = {}
    jira = report["jira"]
    if "base_url" not in jira:
        jira["base_url"] = (jira_client.base_url or "").rstrip("/")

    help_clause = None
    if need & _HELP_CLAUSE_PRODUCTS:
        resolver = getattr(jira_client, "_help_project_customer_filter", None)
        if callable(resolver):
            help_clause = resolver(customer, None)

    jobs = _collect_support_jira_fetch_jobs(
        jira_client, jira, customer, need, help_clause=help_clause
    )
    if not jobs:
        return

    from .jira_client import _JIRA_PARALLEL_WORKERS

    if len(jobs) == 1:
        product, fn = jobs[0]
        logger.debug("Support deck: fetching Jira product %s for %s", product, customer_display)
        jira[product] = fn()
        return

    workers = max(1, min(_JIRA_PARALLEL_WORKERS, len(jobs)))
    logger.info(
        "Support deck: fetching %d Jira products in parallel (workers=%d) for %s",
        len(jobs),
        workers,
        customer_display,
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(fn): product for product, fn in jobs}
        for fut in as_completed(future_map):
            product = future_map[fut]
            jira[product] = fut.result()


def _validate_support_jira_products(
    report: dict[str, Any],
    customer: str | None,
    need: frozenset[str],
) -> None:
    jira = report.get("jira")
    if not isinstance(jira, dict):
        _handle_support_jira_fetch_failure(report, customer, RuntimeError("Jira payload missing after fetch"))
        return
    missing = [p for p in need if _support_jira_product_missing_or_errored(jira, p)]
    if not missing:
        return
    _handle_support_jira_fetch_failure(
        report,
        customer,
        RuntimeError(f"Required Jira products unavailable: {', '.join(missing)}"),
    )


def enrich_support_jira_data(
    report: dict[str, Any],
    customer: str | None,
    slide_plan: list[dict[str, Any]] | None = None,
) -> None:
    """Fetch Jira-backed support deck data into ``report``.

    When ``slide_plan`` is provided (normal ``create_health_deck`` path), only
    Jira products required by those slides — plus Notable digest extras when
    ``cs_notable`` is present — are fetched. Unknown ``slide_type`` values fall
    back to the full bundle. When ``slide_plan`` is omitted, the full bundle is
    always fetched (tests and ad-hoc callers).

    On fetch failure, raises ``report["error"]`` unless
    ``CORTEX_SUPPORT_JIRA_ALLOW_FALLBACK=true`` (legacy empty placeholders).
    """
    from .integration_drive_cache import (
        KIND_JIRA_SUPPORT,
        integration_drive_cache_reads_enabled,
        save_integration_payload,
        try_load_integration_payload,
    )

    customer_display = "All Customers" if not customer else customer
    need, mode = _resolve_support_jira_need(slide_plan, customer)

    if not need:
        logger.info("Support deck: Jira enrichment skipped (no products) for %s", customer_display)
    elif mode == "selective":
        logger.info(
            "Support deck: Jira enrichment mode=selective (%d products) for %s",
            len(need),
            customer_display,
        )
    elif mode == "full_fallback":
        logger.info("Support deck: Jira enrichment mode=full (fallback) for %s", customer_display)
    else:
        logger.info("Support deck: Jira enrichment mode=full for %s", customer_display)

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

    if integration_drive_cache_reads_enabled():
        cached = try_load_integration_payload(KIND_JIRA_SUPPORT, customer)
        if cached is not None:
            report["jira"] = cached
            try:
                from .jira_client import get_shared_jira_client

                report["jira"]["base_url"] = (get_shared_jira_client().base_url or "").rstrip("/")
            except Exception as e:
                logger.warning(
                    "Support deck: could not refresh Jira base_url after Drive cache hit: %s",
                    e,
                )
            missing = [
                p for p in need
                if _support_jira_product_missing_or_errored(report["jira"], p)
            ]
            if missing:
                try:
                    from .jira_client import get_shared_jira_client

                    _fetch_support_jira_products(
                        get_shared_jira_client(),
                        report,
                        customer,
                        frozenset(missing),
                        customer_display,
                    )
                    if isinstance(report.get("jira"), dict):
                        save_integration_payload(KIND_JIRA_SUPPORT, customer, report["jira"])
                except Exception as e:
                    _handle_support_jira_fetch_failure(report, customer, e)
                    if report.get("error"):
                        return
            _validate_support_jira_products(report, customer, need)
            if report.get("error"):
                return
            _support_help_escalation_llm_post(report)
            logger.debug("Support deck: Jira data from Drive cache (%s)", customer_display)
            return

    try:
        from .jira_client import get_shared_jira_client

        logger.info("Support deck: fetching Jira data for %s", customer_display)
        _fetch_support_jira_products(
            get_shared_jira_client(),
            report,
            customer,
            need,
            customer_display,
        )
        if isinstance(report.get("jira"), dict):
            save_integration_payload(KIND_JIRA_SUPPORT, customer, report["jira"])
    except Exception as e:
        _handle_support_jira_fetch_failure(report, customer, e)
        if report.get("error"):
            return

    _validate_support_jira_products(report, customer, need)
    if report.get("error"):
        return

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
    report["jira"]["help_monthly_operational_metrics"] = {
        "error": error_text,
        "customer": customer,
    }
