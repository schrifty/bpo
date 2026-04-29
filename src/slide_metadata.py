"""Slide registry metadata used by builders, speaker notes, and data quality."""

from __future__ import annotations

from typing import Any

SLIDE_DATA_REQUIREMENTS: dict[str, list[str]] = {
    "title": ["customer", "days", "generated", "account"],
    "health": ["engagement", "benchmarks", "account"],
    "engagement": ["engagement", "account", "visitor_languages"],
    "sites": ["sites"],
    "features": ["top_pages", "top_features", "frustration", "feature_adoption_insights"],
    "champions": ["champions", "at_risk_users"],
    "benchmarks": ["benchmarks", "account"],
    "exports": ["exports"],
    "depth": ["depth", "frustration"],
    "kei": ["kei", "track_events_breakdown"],
    "guides": ["guides"],
    "jira": ["jira"],
    "customer_ticket_metrics": ["jira"],
    "customer_ticket_metrics_charts": ["jira"],
    "support_help_orgs_by_opened": ["jira"],
    "support_help_customer_escalations": ["jira"],
    "support_help_escalation_metrics": ["jira"],
    "support_recent_opened": ["jira"],
    "support_recent_closed": ["jira"],
    "customer_project_volume_trends": ["customer_project_volume_jql_trace"],
    "customer_project_ticket_metrics": ["jira"],
    "customer_project_ticket_metrics_breakdown": ["jira"],
    "customer_project_recent_opened": ["jira"],
    "customer_project_recent_closed": ["jira"],
    "lean_project_volume_trends": ["lean_project_volume_jql_trace"],
    "lean_project_ticket_metrics": ["jira"],
    "lean_project_ticket_metrics_breakdown": ["jira"],
    "lean_project_recent_opened": ["jira"],
    "lean_project_recent_closed": ["jira"],
    "help_resolved_by_assignee": ["jira"],
    "customer_resolved_by_assignee": ["jira"],
    "lean_resolved_by_assignee": ["jira"],
    "custom": ["title", "sections"],
    "signals": ["signals", "poll_events", "frustration"],
    "platform_health": ["csr", "leandna_item_master"],
    "supply_chain": ["csr", "leandna_item_master"],
    "platform_value": ["csr"],
    "platform_value_summary_cover": ["customer", "account"],
    "platform_value_summary_toc": [],
    "sla_health": ["jira"],
    "cross_validation": ["csr", "sites", "engagement"],
    "engineering": ["jira"],
    "enhancements": ["jira"],
    "support_breakdown": ["jira"],
    "data_quality": [],
    "portfolio_title": ["customer_count", "days", "generated"],
    "csm_book_title": ["customer_count", "days", "generated", "csm_owner"],
    "portfolio_revenue_book": ["portfolio_revenue_book"],
    "portfolio_signals": ["portfolio_signals"],
    "portfolio_trends": ["portfolio_trends"],
    "portfolio_leaders": ["portfolio_leaders"],
    "cohort_deck_title": ["customer_count", "days", "generated"],
    "cohort_summary": ["cohort_digest"],
    "cohort_profiles": ["cohort_digest"],
    "cohort_findings": ["cohort_findings_bullets"],
    "team": ["customer"],
    "qbr_cover": ["customer", "days"],
    "qbr_agenda": [],
    "qbr_divider": [],
    "qbr_deployment": ["sites"],
    "eng_portfolio_title": ["eng_portfolio"],
    "eng_sprint_snapshot": ["eng_portfolio"],
    "eng_bug_health": ["eng_portfolio"],
    "eng_velocity": ["eng_portfolio"],
    "eng_enhancements": ["eng_portfolio"],
    "eng_enhancements_shipped": ["eng_portfolio"],
    "eng_support_pressure": ["eng_portfolio"],
    "eng_jira_project": ["eng_portfolio"],
    "eng_help_volume_trends": ["eng_help_volume_jql_trace"],
    "support_deck_cover": [],
    "support_intro": [],
    "cs_notable": ["jira"],
    "salesforce_comprehensive_cover": ["salesforce_comprehensive"],
    "salesforce_comprehensive_toc": ["salesforce_comprehensive"],
    "salesforce_category": ["salesforce_comprehensive"],
    "shortage_forecast": ["leandna_shortage_trends"],
    "critical_shortages_detail": ["leandna_shortage_trends"],
    "shortage_deliveries": ["leandna_shortage_trends"],
    "lean_projects_portfolio": ["leandna_lean_projects"],
    "lean_projects_savings": ["leandna_lean_projects"],
    "pendo_sentiment": ["poll_events"],
    "pendo_friction": ["frustration"],
    "pendo_localization": ["visitor_languages"],
    "pendo_track_analytics": ["track_events_breakdown"],
    "pendo_definitions_appendix": ["pendo_catalog_appendix"],
}


DQ_SOURCE_LABEL_ORDER: tuple[str, ...] = (
    "Pendo",
    "CS Report",
    "JIRA",
    "Salesforce",
    "GitHub",
    "LeanDNA",
)


REPORT_KEY_TO_DQ_SOURCE: dict[str, str | None] = {
    "engagement": "Pendo",
    "account": "Pendo",
    "sites": "Pendo",
    "top_pages": "Pendo",
    "top_features": "Pendo",
    "feature_adoption_insights": "Pendo",
    "frustration": "Pendo",
    "poll_events": "Pendo",
    "track_events_breakdown": "Pendo",
    "visitor_languages": "Pendo",
    "pendo_catalog_appendix": "Pendo",
    "champions": "Pendo",
    "at_risk_users": "Pendo",
    "benchmarks": "Pendo",
    "exports": "Pendo",
    "depth": "Pendo",
    "kei": "Pendo",
    "guides": "Pendo",
    "signals": "Pendo",
    "customer": "Pendo",
    "customer_count": "Pendo",
    "csm_owner": "Pendo",
    "portfolio_signals": "Pendo",
    "portfolio_revenue_book": "Salesforce",
    "portfolio_trends": "Pendo",
    "portfolio_leaders": "Pendo",
    "cohort_digest": "Pendo",
    "cohort_findings_bullets": "Pendo",
    "jira": "JIRA",
    "csr": "CS Report",
    "customer_project_volume_jql_trace": "JIRA",
    "lean_project_volume_jql_trace": "JIRA",
    "eng_help_volume_jql_trace": "JIRA",
    "eng_portfolio": "JIRA",
    "salesforce_comprehensive": "Salesforce",
    "github": "GitHub",
    "leandna_shortage_trends": "LeanDNA",
    "leandna_item_master": "LeanDNA",
    "leandna_lean_projects": "LeanDNA",
    "title": None,
    "sections": None,
    "days": None,
    "generated": None,
}


def ordered_dq_data_sources_for_slide_plan(slide_plan: list[dict[str, Any]] | None) -> list[str] | None:
    """Data sources to show as pills for this deck, deduped, in a stable order."""
    req_keys: set[str] = set()
    for entry in slide_plan or ():
        slide_type = (entry.get("slide_type") or "").strip()
        if slide_type in ("", "data_quality", "qbr_divider"):
            continue
        for req in SLIDE_DATA_REQUIREMENTS.get(slide_type) or ():
            req_keys.add(req)
    label_set: set[str] = set()
    for key in req_keys:
        label = REPORT_KEY_TO_DQ_SOURCE.get(key)
        if label:
            label_set.add(label)
    if not label_set:
        return None
    return [label for label in DQ_SOURCE_LABEL_ORDER if label in label_set]
