"""QBR report enrichment with LeanDNA Lean Projects data.

Augments the report dict with project portfolio health and savings tracking.
Called from qbr_template.py after CS Report load.
"""
from __future__ import annotations

from typing import Any
from datetime import datetime, timezone

from .config import logger, LEANDNA_DATA_API_BEARER_TOKEN
from .leandna_lean_projects_client import (
    get_lean_projects,
    get_project_savings,
    aggregate_portfolio_stats,
    aggregate_monthly_savings,
    get_top_projects_by_savings,
)


def _resolve_customer_sites(customer: str) -> str | None:
    """Resolve customer name to LeanDNA site IDs (comma-separated).
    
    Args:
        customer: BPO customer name.
    
    Returns:
        Comma-separated site IDs or None for all authorized sites.
    
    TODO: Implement site mapping logic (same as Item Master/Shortage enrichment).
    For now: return None (all sites).
    """
    return None


def _get_quarter_date_range(report: dict[str, Any]) -> tuple[str, str]:
    """Extract quarter date range from report dict.
    
    Returns:
        Tuple of (date_from, date_to) as ISO date strings.
    """
    start = report.get("quarter_start")
    end = report.get("quarter_end")
    
    if not start or not end:
        # Fallback: current quarter
        now = datetime.now(timezone.utc)
        quarter = (now.month - 1) // 3 + 1
        year = now.year
        
        if quarter == 1:
            start_date = f"{year}-01-01"
            end_date = f"{year}-03-31"
        elif quarter == 2:
            start_date = f"{year}-04-01"
            end_date = f"{year}-06-30"
        elif quarter == 3:
            start_date = f"{year}-07-01"
            end_date = f"{year}-09-30"
        else:
            start_date = f"{year}-10-01"
            end_date = f"{year}-12-31"
        
        return start_date, end_date
    
    # Parse ISO dates
    try:
        start_date = start[:10] if "T" in start else start
        end_date = end[:10] if "T" in end else end
        return start_date, end_date
    except Exception:
        logger.warning("Failed to parse quarter dates from report; using fallback")
        return _get_quarter_date_range({})


def enrich_qbr_with_lean_projects(
    report: dict[str, Any],
    customer: str,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Enrich QBR report with LeanDNA Lean Projects data.
    
    Args:
        report: QBR report dict (from PendoClient or after other enrichments).
        customer: Customer name.
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        Modified report dict with `report["leandna_lean_projects"]` added.
    
    Raises:
        No exceptions raised; logs warnings and returns report unchanged on error.
    """
    logger.info("LeanDNA Lean Projects enrichment: starting for customer=%s", customer)
    
    # Check if LeanDNA is configured
    if not LEANDNA_DATA_API_BEARER_TOKEN:
        logger.debug("LeanDNA Lean Projects skipped: LEANDNA_DATA_API_BEARER_TOKEN not set")
        report.setdefault("leandna_lean_projects", {
            "enabled": False,
            "reason": "bearer_token_not_configured",
        })
        return report
    
    try:
        # Resolve sites
        sites = _resolve_customer_sites(customer)
        date_from, date_to = _get_quarter_date_range(report)
        
        logger.info(
            "LeanDNA Lean Projects: fetching for customer=%s sites=%s dateFrom=%s dateTo=%s",
            customer, sites or "all", date_from, date_to
        )
        
        # Fetch projects
        projects = get_lean_projects(
            sites=sites,
            date_from=date_from,
            date_to=date_to,
            force_refresh=force_refresh,
        )
        
        if not projects:
            logger.info("LeanDNA Lean Projects: no projects found for period")
            report.setdefault("leandna_lean_projects", {
                "enabled": True,
                "total_projects": 0,
                "active_projects": 0,
                "error": "no_projects_for_period",
            })
            return report
        
        logger.info("LeanDNA Lean Projects: processing %d projects", len(projects))
        
        # Compute portfolio stats
        portfolio = aggregate_portfolio_stats(projects)
        
        # Get top projects by savings
        top_projects = get_top_projects_by_savings(projects, max_projects=10)
        
        # Fetch savings for top projects
        top_ids = [p["id"] for p in top_projects if p.get("id")]
        savings_data = []
        if top_ids:
            try:
                savings_data = get_project_savings(top_ids, sites=sites, force_refresh=force_refresh)
            except Exception as e:
                logger.warning("Failed to fetch project savings (non-fatal): %s", e)
        
        # Aggregate monthly savings
        monthly = aggregate_monthly_savings(savings_data, months=3) if savings_data else []
        
        # Build enrichment payload
        enrichment = {
            "enabled": True,
            "data_fetched_at": datetime.now(timezone.utc).isoformat(),
            "quarter_start": date_from,
            "quarter_end": date_to,
            "total_projects": portfolio["total_projects"],
            "active_projects": portfolio["active_projects"],
            "stage_distribution": portfolio["stage_distribution"],
            "state_distribution": portfolio["state_distribution"],
            "total_savings_actual": portfolio["total_savings_actual"],
            "total_savings_target": portfolio["total_savings_target"],
            "savings_achievement_pct": portfolio["savings_achievement_pct"],
            "best_practice_count": portfolio["best_practice_count"],
            "validated_results_count": portfolio["validated_results_count"],
            "top_projects": [
                {
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "stage": p.get("stage"),
                    "state": p.get("state"),
                    "savings_actual": p.get("totalActualSavingsForPeriod", 0.0),
                    "savings_target": p.get("totalTargetSavingsForPeriod", 0.0),
                    "is_best_practice": p.get("isBestPractice", False),
                    "is_validated": p.get("isProjectResultsValidated", False),
                    "project_manager": p.get("projectManager", {}).get("name"),
                }
                for p in top_projects
            ],
            "monthly_savings": monthly,
        }
        
        report["leandna_lean_projects"] = enrichment
        
        logger.info(
            "LeanDNA Lean Projects complete: %d projects, %d active, $%.0fK actual vs $%.0fK target (%.1f%%)",
            portfolio["total_projects"],
            portfolio["active_projects"],
            portfolio["total_savings_actual"] / 1000,
            portfolio["total_savings_target"] / 1000,
            portfolio["savings_achievement_pct"],
        )
        
        return report
        
    except Exception as e:
        logger.exception("LeanDNA Lean Projects enrichment failed")
        report.setdefault("leandna_lean_projects", {
            "enabled": False,
            "error": str(e),
        })
        return report


def format_lean_projects_speaker_notes_supplement(enrichment: dict[str, Any]) -> str:
    """Generate speaker notes supplement for Lean Projects enrichment.
    
    Args:
        enrichment: The `leandna_lean_projects` dict from enriched report.
    
    Returns:
        Markdown-formatted text for appending to speaker notes.
    """
    if not enrichment.get("enabled"):
        return ""
    
    total = enrichment.get("total_projects", 0)
    active = enrichment.get("active_projects", 0)
    actual = enrichment.get("total_savings_actual", 0.0)
    target = enrichment.get("total_savings_target", 0.0)
    achievement = enrichment.get("savings_achievement_pct", 0.0)
    best = enrichment.get("best_practice_count", 0)
    validated = enrichment.get("validated_results_count", 0)
    
    lines = [
        "",
        "---",
        "",
        "## LeanDNA Lean Projects Insights",
        "",
        f"- **Total Projects:** {total} ({active} active)",
        f"- **Savings Achievement:** ${actual:,.0f} actual vs ${target:,.0f} target ({achievement:.1f}%)",
        f"- **Best Practices:** {best} projects",
        f"- **Validated Results:** {validated} projects",
    ]
    
    # Stage distribution
    stage_dist = enrichment.get("stage_distribution", {})
    if stage_dist:
        lines.append("")
        lines.append("**Stage Distribution:**")
        for stage, count in sorted(stage_dist.items(), key=lambda x: -x[1]):
            lines.append(f"- {stage}: {count}")
    
    # Top projects
    top = enrichment.get("top_projects", [])[:5]
    if top:
        lines.append("")
        lines.append("**Top 5 Projects by Savings:**")
        for i, p in enumerate(top, 1):
            name = p.get("name", "Unknown")
            savings = p.get("savings_actual", 0.0)
            stage = p.get("stage", "Unknown")
            lines.append(f"{i}. {name} — ${savings:,.0f} ({stage})")
    
    return "\n".join(lines)
