"""QBR report enrichment with LeanDNA Material Shortage Trends.

Augments the report dict with time-series shortage intelligence before slide generation.
Called from qbr_template.py after Item Master enrichment.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .config import logger, LEANDNA_DATA_API_BEARER_TOKEN
from .leandna_shortage_client import (
    get_shortages_by_item_weekly,
    aggregate_shortage_forecast,
    get_critical_shortages_timeline,
    get_shortages_with_scheduled_deliveries_weekly,
    get_scheduled_deliveries_summary,
)


def _resolve_customer_sites(customer: str) -> str | None:
    """Resolve customer name to LeanDNA site IDs (comma-separated).
    
    Args:
        customer: BPO customer name.
    
    Returns:
        Comma-separated site IDs or None for all authorized sites.
    
    TODO: Implement site mapping logic (same as Item Master enrichment).
    For now: return None (all sites).
    """
    return None


def enrich_qbr_with_shortage_trends(
    report: dict[str, Any],
    customer: str,
    weeks_forward: int = 12,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Enrich QBR report with LeanDNA Material Shortage Trends.
    
    Args:
        report: QBR report dict (from PendoClient or after Item Master enrichment).
        customer: Customer name.
        weeks_forward: Number of weeks to include in forecast (default 12).
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        Modified report dict with `report["leandna_shortage_trends"]` added.
    
    Raises:
        No exceptions raised; logs warnings and returns report unchanged on error.
    """
    logger.info("LeanDNA Shortage Trends enrichment: starting for customer=%s", customer)
    
    # Check if LeanDNA is configured
    if not LEANDNA_DATA_API_BEARER_TOKEN:
        logger.debug("LeanDNA shortage trends skipped: LEANDNA_DATA_API_BEARER_TOKEN not set")
        report.setdefault("leandna_shortage_trends", {"enabled": False, "reason": "bearer_token_not_configured"})
        return report
    
    try:
        # Resolve sites
        sites = _resolve_customer_sites(customer)
        logger.info("LeanDNA shortage trends: fetching weekly data for customer=%s sites=%s", customer, sites or "all")
        
        # Fetch weekly shortage data
        weekly_data = get_shortages_by_item_weekly(sites=sites, force_refresh=force_refresh)
        if not weekly_data:
            logger.warning("LeanDNA shortage trends: no shortage items returned from API")
            report.setdefault("leandna_shortage_trends", {
                "enabled": True,
                "total_items_in_shortage": 0,
                "error": "no_shortage_items_returned",
            })
            return report
        
        logger.info("LeanDNA shortage trends: processing %d items", len(weekly_data))
        
        # Aggregate forecast
        forecast = aggregate_shortage_forecast(weekly_data, weeks_forward=weeks_forward)
        
        # Extract critical timeline
        critical_timeline = get_critical_shortages_timeline(weekly_data, threshold=3, max_items=20)
        
        # Try to get delivery tracking (optional; non-blocking)
        delivery_summary = {}
        try:
            logger.debug("LeanDNA shortage trends: fetching scheduled deliveries data")
            delivery_data = get_shortages_with_scheduled_deliveries_weekly(sites=sites)
            delivery_summary = get_scheduled_deliveries_summary(delivery_data, next_n_days=7)
        except Exception as e:
            logger.warning("Failed to fetch shortage delivery data (non-fatal): %s", e)
        
        # Build enrichment payload
        enrichment = {
            "enabled": True,
            "data_fetched_at": datetime.now(timezone.utc).isoformat(),
            "weeks_forward": weeks_forward,
            "total_items_in_shortage": forecast["total_items"],
            "critical_items": forecast["critical_items"],
            "forecast": {
                "buckets": forecast["buckets"][:weeks_forward],  # ensure limit
                "peak_week": forecast["peak_week"],
                "total_shortage_value": forecast["total_shortage_value"],
            },
            "critical_timeline": critical_timeline,
            "scheduled_deliveries": delivery_summary,
        }
        
        report["leandna_shortage_trends"] = enrichment
        logger.info(
            "LeanDNA shortage trends complete: %d items, %d critical, peak week=%s, CTB impact=$%s",
            forecast["total_items"],
            forecast["critical_items"],
            forecast["peak_week"] or "N/A",
            f"{forecast['total_shortage_value']:,.0f}",
        )
        
        return report
        
    except Exception as e:
        logger.error("LeanDNA shortage trends enrichment failed: %s", e, exc_info=True)
        report.setdefault("leandna_shortage_trends", {
            "enabled": True,
            "error": str(e),
        })
        return report


def format_shortage_speaker_notes_supplement(report: dict[str, Any]) -> str:
    """Generate speaker notes text from shortage trends enrichment.
    
    Args:
        report: QBR report dict with leandna_shortage_trends enrichment.
    
    Returns:
        Formatted markdown/text snippet to append to speaker notes.
    """
    enrichment = report.get("leandna_shortage_trends") or {}
    if not enrichment.get("enabled") or enrichment.get("error"):
        return ""
    
    parts: list[str] = []
    
    # Shortage summary
    total_items = enrichment.get("total_items_in_shortage", 0)
    critical_items = enrichment.get("critical_items", 0)
    if total_items > 0:
        parts.append(
            f"**Material shortages:** {total_items} items in shortage, "
            f"{critical_items} critical (criticality level ≥3)."
        )
    
    # Forecast peak
    forecast = enrichment.get("forecast") or {}
    peak_week = forecast.get("peak_week")
    total_value = forecast.get("total_shortage_value", 0)
    if peak_week:
        parts.append(
            f"**Shortage forecast:** Peak shortage week is {peak_week}. "
            f"Total CTB impact: ${total_value:,.0f}."
        )
    
    # Critical timeline (top 3)
    critical_timeline = enrichment.get("critical_timeline") or []
    if critical_timeline:
        top_3 = critical_timeline[:3]
        crit_lines = ", ".join([
            f"{i.get('itemCode')} (${i.get('ctbImpact', 0):,.0f}, critical {i.get('firstCriticalWeek')})"
            for i in top_3
        ])
        parts.append(
            f"**Top critical shortages:** {crit_lines}."
        )
    
    # Scheduled deliveries
    deliveries = enrichment.get("scheduled_deliveries") or {}
    items_with_sched = deliveries.get("items_with_schedules", 0)
    next_7_qty = deliveries.get("next_n_days_scheduled_qty", 0)
    if items_with_sched > 0:
        parts.append(
            f"**Scheduled deliveries:** {items_with_sched} items have confirmed PO schedules. "
            f"Next 7 days: {next_7_qty:,.0f} qty arriving."
        )
    
    if not parts:
        return ""
    
    return "\n\n**LeanDNA Shortage Trends:**\n\n" + "\n\n".join(parts)
