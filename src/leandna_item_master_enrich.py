"""QBR report enrichment with LeanDNA Item Master Data.

Augments the report dict with item-level supply chain intelligence before slide generation.
Called from qbr_template.py after CS Report load.
"""
from __future__ import annotations

from typing import Any

from .config import logger, LEANDNA_DATA_API_BEARER_TOKEN
from .leandna_item_master_client import (
    get_item_master_data,
    get_high_risk_items,
    get_doi_backwards_summary,
    get_abc_distribution,
    get_lead_time_variance,
    get_excess_items,
)


def _resolve_customer_sites(customer: str) -> str | None:
    """Resolve customer name to LeanDNA site IDs (comma-separated).
    
    Args:
        customer: BPO customer name.
    
    Returns:
        Comma-separated site IDs or None for all authorized sites.
    
    TODO: Implement site mapping logic. Options:
      1. Add `leandna_site_ids` to teams.yaml per customer
      2. Call /data/identity API and fuzzy-match siteName
      3. Use customer-specific env var LEANDNA_SITES_{customer}
    
    For now: return None (all sites) and rely on RequestedSites header behavior.
    """
    # Placeholder: no mapping yet
    # Future: load from teams.yaml or identity API
    return None


def enrich_qbr_with_item_master(
    report: dict[str, Any],
    customer: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Enrich QBR report with LeanDNA Item Master Data.
    
    Args:
        report: QBR report dict (from PendoClient.get_customer_health_report or similar).
        customer: Customer name.
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        Modified report dict with `report["leandna_item_master"]` added.
    
    Raises:
        No exceptions raised; logs warnings and returns report unchanged on error.
    """
    # Check if LeanDNA is configured
    if not LEANDNA_DATA_API_BEARER_TOKEN:
        logger.debug("LeanDNA enrichment skipped: LEANDNA_DATA_API_BEARER_TOKEN not set")
        report.setdefault("leandna_item_master", {"enabled": False, "reason": "bearer_token_not_configured"})
        return report
    
    try:
        # Resolve sites
        sites = _resolve_customer_sites(customer)
        logger.info("LeanDNA enrichment: fetching Item Master Data for customer=%s sites=%s", customer, sites or "all")
        
        # Fetch items
        items = get_item_master_data(sites=sites, force_refresh=force_refresh)
        if not items:
            logger.warning("LeanDNA enrichment: no items returned from API")
            report.setdefault("leandna_item_master", {
                "enabled": True,
                "item_count": 0,
                "error": "no_items_returned",
            })
            return report
        
        logger.info("LeanDNA enrichment: processing %d items", len(items))
        
        # Extract enrichments
        doi_bwd = get_doi_backwards_summary(items)
        high_risk = get_high_risk_items(items, threshold=80, max_items=10)
        abc_dist = get_abc_distribution(items)
        lead_time_var = get_lead_time_variance(items, min_variance_pct=20.0)
        excess_list, total_excess = get_excess_items(items, max_items=5)
        
        # Build enrichment payload
        enrichment = {
            "enabled": True,
            "item_count": len(items),
            "sites_requested": sites,
            "doi_backwards": doi_bwd,
            "high_risk_items": [
                {
                    "itemCode": i.get("itemCode"),
                    "itemDescription": i.get("itemDescription"),
                    "site": i.get("site"),
                    "aggregateRiskScore": i.get("aggregateRiskScore"),
                    "ctbShortageImpactedValue": i.get("ctbShortageImpactedValue"),
                }
                for i in high_risk
            ],
            "abc_distribution": abc_dist,
            "lead_time_variance": {
                "high_variance_count": len(lead_time_var),
                "worst_performers": lead_time_var[:5],
            },
            "excess_breakdown": {
                "total_excess_items": len(excess_list),
                "excess_on_hand_value": round(total_excess),
                "top_excess_items": excess_list,
            },
        }
        
        report["leandna_item_master"] = enrichment
        logger.info(
            "LeanDNA enrichment complete: %d items, %d high-risk, DOI bwd mean=%.1f, excess=$%s",
            len(items),
            len(high_risk),
            doi_bwd.get("mean") or 0,
            f"{total_excess:,.0f}",
        )
        
        return report
        
    except Exception as e:
        logger.error("LeanDNA enrichment failed: %s", e, exc_info=True)
        report.setdefault("leandna_item_master", {
            "enabled": True,
            "error": str(e),
        })
        return report


def get_doi_backwards_for_site(report: dict[str, Any], site_name: str) -> float | None:
    """Extract DOI backwards for a specific site from enrichment data.
    
    Args:
        report: QBR report dict with leandna_item_master enrichment.
        site_name: Factory/site name to filter (case-insensitive match).
    
    Returns:
        Mean DOI backwards for items at that site, or None if not available.
    """
    enrichment = report.get("leandna_item_master") or {}
    if not enrichment.get("enabled"):
        return None
    
    # This would require raw items or site-aggregated data in enrichment
    # For now: return global mean (future: refactor to aggregate per-site in enrichment)
    doi_bwd = enrichment.get("doi_backwards") or {}
    return doi_bwd.get("mean")


def format_leandna_speaker_notes_supplement(report: dict[str, Any]) -> str:
    """Generate speaker notes text from LeanDNA enrichment for supply chain/platform health slides.
    
    Args:
        report: QBR report dict with leandna_item_master enrichment.
    
    Returns:
        Formatted markdown/text snippet to append to speaker notes.
    """
    enrichment = report.get("leandna_item_master") or {}
    if not enrichment.get("enabled") or enrichment.get("error"):
        return ""
    
    parts: list[str] = []
    
    # DOI backwards
    doi_bwd = enrichment.get("doi_backwards") or {}
    if doi_bwd.get("mean") is not None:
        parts.append(
            f"**DOI Backwards (historical consumption):** "
            f"Mean {doi_bwd['mean']:.1f} days, "
            f"median {doi_bwd.get('median', 0):.1f} days. "
            f"{doi_bwd.get('items_over_60_days', 0)} items exceed 60 days (slow-moving/excess risk)."
        )
    
    # High-risk items
    high_risk = enrichment.get("high_risk_items") or []
    if high_risk:
        top_3 = high_risk[:3]
        risk_lines = ", ".join([
            f"{i.get('itemCode')} (score {i.get('aggregateRiskScore')})"
            for i in top_3
        ])
        parts.append(
            f"**High-risk items:** {len(high_risk)} items with risk score >80. "
            f"Top concerns: {risk_lines}."
        )
    
    # ABC classification
    abc = enrichment.get("abc_distribution") or {}
    if abc:
        total = sum(abc.values())
        if total > 0:
            pct_a = (abc.get("A", 0) / total) * 100
            parts.append(
                f"**ABC classification:** {abc.get('A', 0)} A-items ({pct_a:.0f}% critical high-value), "
                f"{abc.get('B', 0)} B-items, {abc.get('C', 0)} C-items."
            )
    
    # Excess inventory
    excess = enrichment.get("excess_breakdown") or {}
    top_excess = excess.get("top_excess_items") or []
    if top_excess:
        top_3_excess = top_excess[:3]
        excess_lines = ", ".join([
            f"{i.get('itemCode')} (${i.get('excessOnHandValue', 0):,.0f})"
            for i in top_3_excess
        ])
        parts.append(
            f"**Excess inventory:** ${excess.get('excess_on_hand_value', 0):,.0f} total. "
            f"Top items: {excess_lines}."
        )
    
    # Lead time variance
    lt_var = enrichment.get("lead_time_variance") or {}
    worst = lt_var.get("worst_performers") or []
    if worst:
        top_2 = worst[:2]
        lt_lines = ", ".join([
            f"{i.get('itemCode')} ({i.get('variance_pct'):+.0f}% vs plan)"
            for i in top_2
        ])
        parts.append(
            f"**Lead time variance:** {lt_var.get('high_variance_count', 0)} items with >20% variance. "
            f"Worst: {lt_lines}."
        )
    
    if not parts:
        return ""
    
    return "\n\n**LeanDNA Item-Level Insights:**\n\n" + "\n\n".join(parts)
