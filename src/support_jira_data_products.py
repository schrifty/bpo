"""Granular Jira data products for support decks (slide_type → fetch union).

Used by :func:`~deck_data_enrichment.enrich_support_jira_data` to prefetch only
``report["jira"]`` keys required by the resolved slide plan. Speaker notes still
use top-level ``SLIDE_DATA_REQUIREMENTS`` keys (e.g. ``jira``) where applicable.
"""

from __future__ import annotations

from typing import Any

from .config import logger

# Keys under report["jira"] populated by enrich_support_jira_data (fetch units).
JIRA_SUPPORT_PRODUCT_IDS: frozenset[str] = frozenset(
    {
        "customer_ticket_metrics",
        "help_ticket_volume_trends",
        "help_factory_start_day_buckets",
        "help_monthly_operational_metrics",
        "help_orgs_by_opened",
        "help_customer_escalations",
        "help_escalation_metrics",
        "customer_help_recent",
        "help_resolved_by_assignee",
        "customer_project_recent",
        "customer_project_open_breakdown",
        "customer_project_volume_trends",
        "customer_project_ticket_metrics",
        "lean_project_recent",
        "lean_project_open_breakdown",
        "lean_project_volume_trends",
        "lean_project_ticket_metrics",
        "customer_resolved_by_assignee",
        "lean_resolved_by_assignee",
    }
)

# Digest for Notable LLM (:func:`support_notable_llm.build_support_review_digest`).
NOTABLE_DIGEST_JIRA_PRODUCTS: frozenset[str] = frozenset(
    {
        "customer_ticket_metrics",
        "help_escalation_metrics",
        "customer_help_recent",
        "customer_project_recent",
        "lean_project_recent",
        "help_resolved_by_assignee",
        "customer_resolved_by_assignee",
        "lean_resolved_by_assignee",
        "customer_project_open_breakdown",
        "lean_project_open_breakdown",
        "customer_project_volume_trends",
        "lean_project_volume_trends",
        "customer_project_ticket_metrics",
        "lean_project_ticket_metrics",
        "help_ticket_volume_trends",
    }
)

# Slides that never need Jira prefetch for support (deck may still include them).
SUPPORT_SLIDE_TYPES_NO_JIRA: frozenset[str] = frozenset(
    {
        "support_deck_cover",
        "support_intro",
        "data_quality",
    }
)

# slide_type → subset of JIRA_SUPPORT_PRODUCT_IDS
SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE: dict[str, frozenset[str]] = {
    "customer_ticket_metrics": frozenset({"customer_ticket_metrics"}),
    "customer_ticket_metrics_charts": frozenset({"customer_ticket_metrics"}),
    "support_help_orgs_by_opened": frozenset({"help_orgs_by_opened"}),
    "support_help_factory_start_buckets": frozenset({"help_factory_start_day_buckets"}),
    "support_help_monthly_operational": frozenset({"help_monthly_operational_metrics"}),
    "support_help_escalation_metrics": frozenset({"help_escalation_metrics"}),
    "support_help_customer_escalations": frozenset({"help_customer_escalations"}),
    "support_recent_opened": frozenset({"customer_help_recent"}),
    "support_recent_closed": frozenset({"customer_help_recent"}),
    "help_resolved_by_assignee": frozenset({"help_resolved_by_assignee"}),
    "eng_help_volume_trends": frozenset({"help_ticket_volume_trends"}),
    "customer_project_volume_trends": frozenset({"customer_project_volume_trends"}),
    "customer_project_ticket_metrics": frozenset({"customer_project_ticket_metrics"}),
    "customer_project_ticket_metrics_breakdown": frozenset({"customer_project_open_breakdown"}),
    "customer_project_recent_opened": frozenset({"customer_project_recent"}),
    "customer_project_recent_closed": frozenset({"customer_project_recent"}),
    "customer_resolved_by_assignee": frozenset({"customer_resolved_by_assignee"}),
    "lean_project_volume_trends": frozenset({"lean_project_volume_trends"}),
    "lean_project_ticket_metrics": frozenset({"lean_project_ticket_metrics"}),
    "lean_project_ticket_metrics_breakdown": frozenset({"lean_project_open_breakdown"}),
    "lean_project_recent_opened": frozenset({"lean_project_recent"}),
    "lean_project_recent_closed": frozenset({"lean_project_recent"}),
    "lean_resolved_by_assignee": frozenset({"lean_resolved_by_assignee"}),
}


def collect_support_jira_product_ids(
    slide_plan: list[dict[str, Any]],
    *,
    customer: str | None,
) -> tuple[frozenset[str], bool]:
    """Return ``(product_ids, use_full_fallback_bundle)``.

    * ``use_full_fallback_bundle`` is True when any slide_type is unknown — then
      callers should run the legacy full Jira enrichment (safe default).
    * ``help_orgs_by_opened`` is dropped when ``customer`` is set (slide is never
      in plan for single-customer runs).
    """
    products: set[str] = set()
    fallback = False
    notable = False

    for entry in slide_plan or ():
        if not isinstance(entry, dict):
            continue
        st = (entry.get("slide_type") or entry.get("id") or "").strip()
        if not st:
            continue
        if st in SUPPORT_SLIDE_TYPES_NO_JIRA:
            continue
        if st == "cs_notable":
            notable = True
            continue
        mapped = SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE.get(st)
        if mapped is None:
            logger.warning(
                "support Jira products: unknown slide_type %r — using full Jira enrichment fallback",
                st,
            )
            fallback = True
            break
        products |= mapped

    if notable:
        products |= NOTABLE_DIGEST_JIRA_PRODUCTS

    if customer and "help_orgs_by_opened" in products:
        products.discard("help_orgs_by_opened")

    if fallback:
        return JIRA_SUPPORT_PRODUCT_IDS, True
    return frozenset(products), False
