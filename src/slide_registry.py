"""Slide builder registry and slide data requirement accessors."""

from __future__ import annotations

from .slide_benchmarks import benchmarks_slide as _benchmarks_slide
from .slide_cohort import (
    cohort_deck_title_slide as _cohort_deck_title_slide,
    cohort_findings_slide as _cohort_findings_slide,
    cohort_profiles_slide as _cohort_profiles_slide,
    cohort_summary_slide as _cohort_summary_slide,
)
from .slide_cs_notable import cs_notable_slide as _cs_notable_slide
from .slide_custom import custom_slide as _custom_slide
from .slide_data_quality import data_quality_slide as _data_quality_slide
from .slide_depth import depth_slide as _depth_slide
from .slide_engagement import engagement_slide as _engagement_slide
from .slide_engineering_portfolio import (
    customer_project_volume_trends_slide as _customer_project_volume_trends_slide,
    eng_bug_health_slide as _eng_bug_health_slide,
    eng_enhancements_open_slide as _eng_enhancements_open_slide,
    eng_enhancements_shipped_slide as _eng_enhancements_shipped_slide,
    eng_help_volume_trends_slide as _eng_help_volume_trends_slide,
    eng_jira_project_slide as _eng_jira_project_slide,
    eng_portfolio_title_slide as _eng_portfolio_title_slide,
    eng_sprint_snapshot_slide as _eng_sprint_snapshot_slide,
    eng_support_pressure_slide as _eng_support_pressure_slide,
    eng_velocity_slide as _eng_velocity_slide,
    lean_project_volume_trends_slide as _lean_project_volume_trends_slide,
)
from .slide_exports import exports_slide as _exports_slide
from .slide_guides import guides_slide as _guides_slide
from .slide_health import health_slide as _health_slide
from .slide_jira_support import (
    cross_validation_slide as _cross_validation_slide,
    customer_help_recent_slide as _customer_help_recent_slide,
    customer_project_recent_closed_slide as _customer_project_recent_closed_slide,
    customer_project_recent_opened_slide as _customer_project_recent_opened_slide,
    customer_project_ticket_metrics_breakdown_slide as _customer_project_ticket_metrics_breakdown_slide,
    customer_project_ticket_metrics_slide as _customer_project_ticket_metrics_slide,
    customer_resolved_by_assignee_slide as _customer_resolved_by_assignee_slide,
    customer_ticket_metrics_charts_slide as _customer_ticket_metrics_charts_slide,
    customer_ticket_metrics_slide as _customer_ticket_metrics_slide,
    engineering_slide as _engineering_slide,
    enhancement_requests_slide as _enhancement_requests_slide,
    help_resolved_by_assignee_slide as _help_resolved_by_assignee_slide,
    jira_slide as _jira_slide,
    lean_project_recent_closed_slide as _lean_project_recent_closed_slide,
    lean_project_recent_opened_slide as _lean_project_recent_opened_slide,
    lean_project_ticket_metrics_breakdown_slide as _lean_project_ticket_metrics_breakdown_slide,
    lean_project_ticket_metrics_slide as _lean_project_ticket_metrics_slide,
    lean_resolved_by_assignee_slide as _lean_resolved_by_assignee_slide,
    sla_health_slide as _sla_health_slide,
    support_breakdown_slide as _support_breakdown_slide,
    support_help_customer_escalations_slide as _support_help_customer_escalations_slide,
    support_help_escalation_metrics_slide as _support_help_escalation_metrics_slide,
    support_help_orgs_by_opened_slide as _support_help_orgs_by_opened_slide,
    support_recent_closed_slide as _support_recent_closed_slide,
    support_recent_opened_slide as _support_recent_opened_slide,
)
from .slide_kei import kei_slide as _kei_slide
from .slide_leandna_projects import (
    lean_projects_portfolio_slide as _lean_projects_portfolio_slide,
    lean_projects_savings_slide as _lean_projects_savings_slide,
)
from .slide_leandna_shortage import (
    critical_shortages_detail_slide as _critical_shortages_detail_slide,
    shortage_deliveries_slide as _shortage_deliveries_slide,
    shortage_forecast_slide as _shortage_forecast_slide,
)
from .slide_metadata import SLIDE_DATA_REQUIREMENTS
from .slide_platform_health import platform_health_slide as _platform_health_slide
from .slide_platform_value import (
    platform_value_slide as _platform_value_slide,
    platform_value_summary_cover_slide as _platform_value_summary_cover_slide,
    platform_value_summary_toc_slide as _platform_value_summary_toc_slide,
)
from .slide_pendo import (
    pendo_definitions_appendix_slide as _pendo_definitions_appendix_slide,
    pendo_friction_slide as _pendo_friction_slide,
    pendo_localization_slide as _pendo_localization_slide,
    pendo_sentiment_slide as _pendo_sentiment_slide,
    pendo_track_analytics_slide as _pendo_track_analytics_slide,
)
from .slide_portfolio import (
    portfolio_leaders_slide as _portfolio_leaders_slide,
    portfolio_revenue_book_slide as _portfolio_revenue_book_slide,
    portfolio_signals_slide as _portfolio_signals_slide,
    portfolio_title_slide as _portfolio_title_slide,
    portfolio_trends_slide as _portfolio_trends_slide,
)
from .slide_qbr_deployment import qbr_deployment_slide as _qbr_deployment_slide
from .slide_qbr_framing import (
    qbr_agenda_slide as _qbr_agenda_slide,
    qbr_cover_slide as _qbr_cover_slide,
    qbr_divider_slide as _qbr_divider_slide,
)
from .slide_salesforce import (
    salesforce_category_slide as _salesforce_category_slide,
    salesforce_comprehensive_cover_slide as _salesforce_comprehensive_cover_slide,
    salesforce_comprehensive_toc_slide as _salesforce_comprehensive_toc_slide,
)
from .slide_signals import signals_slide as _signals_slide
from .slide_sites import sites_slide as _sites_slide
from .slide_supply_chain import supply_chain_slide as _supply_chain_slide
from .slide_support_intro import (
    support_deck_cover_slide as _support_deck_cover_slide,
    support_intro_slide as _support_intro_slide,
)
from .slide_team import team_slide as _team_slide
from .slide_title_page import title_slide as _title_slide
from .slide_usage import champions_slide as _champions_slide, features_slide as _features_slide


_SLIDE_BUILDERS = {
    "title": _title_slide,
    "health": _health_slide,
    "engagement": _engagement_slide,
    "sites": _sites_slide,
    "features": _features_slide,
    "champions": _champions_slide,
    "benchmarks": _benchmarks_slide,
    "exports": _exports_slide,
    "depth": _depth_slide,
    "kei": _kei_slide,
    "guides": _guides_slide,
    "jira": _jira_slide,
    "customer_ticket_metrics": _customer_ticket_metrics_slide,
    "customer_ticket_metrics_charts": _customer_ticket_metrics_charts_slide,
    "support_help_orgs_by_opened": _support_help_orgs_by_opened_slide,
    "support_help_customer_escalations": _support_help_customer_escalations_slide,
    "support_help_escalation_metrics": _support_help_escalation_metrics_slide,
    "support_recent_opened": _support_recent_opened_slide,
    "support_recent_closed": _support_recent_closed_slide,
    "customer_project_volume_trends": _customer_project_volume_trends_slide,
    "customer_project_ticket_metrics": _customer_project_ticket_metrics_slide,
    "customer_project_ticket_metrics_breakdown": _customer_project_ticket_metrics_breakdown_slide,
    "customer_project_recent_opened": _customer_project_recent_opened_slide,
    "customer_project_recent_closed": _customer_project_recent_closed_slide,
    "lean_project_volume_trends": _lean_project_volume_trends_slide,
    "lean_project_ticket_metrics": _lean_project_ticket_metrics_slide,
    "lean_project_ticket_metrics_breakdown": _lean_project_ticket_metrics_breakdown_slide,
    "lean_project_recent_opened": _lean_project_recent_opened_slide,
    "lean_project_recent_closed": _lean_project_recent_closed_slide,
    "help_resolved_by_assignee": _help_resolved_by_assignee_slide,
    "customer_resolved_by_assignee": _customer_resolved_by_assignee_slide,
    "lean_resolved_by_assignee": _lean_resolved_by_assignee_slide,
    "custom": _custom_slide,
    "signals": _signals_slide,
    "platform_health": _platform_health_slide,
    "supply_chain": _supply_chain_slide,
    "platform_value": _platform_value_slide,
    "platform_value_summary_cover": _platform_value_summary_cover_slide,
    "platform_value_summary_toc": _platform_value_summary_toc_slide,
    "data_quality": _data_quality_slide,
    "portfolio_title": _portfolio_title_slide,
    "portfolio_revenue_book": _portfolio_revenue_book_slide,
    "portfolio_signals": _portfolio_signals_slide,
    "portfolio_trends": _portfolio_trends_slide,
    "portfolio_leaders": _portfolio_leaders_slide,
    "team": _team_slide,
    "sla_health": _sla_health_slide,
    "cross_validation": _cross_validation_slide,
    "engineering": _engineering_slide,
    "enhancements": _enhancement_requests_slide,
    "support_breakdown": _support_breakdown_slide,
    "qbr_cover": _qbr_cover_slide,
    "qbr_agenda": _qbr_agenda_slide,
    "qbr_divider": _qbr_divider_slide,
    "qbr_deployment": _qbr_deployment_slide,
    "eng_portfolio_title": _eng_portfolio_title_slide,
    "eng_sprint_snapshot": _eng_sprint_snapshot_slide,
    "eng_bug_health": _eng_bug_health_slide,
    "eng_velocity": _eng_velocity_slide,
    "eng_enhancements": _eng_enhancements_open_slide,
    "eng_enhancements_shipped": _eng_enhancements_shipped_slide,
    "eng_support_pressure": _eng_support_pressure_slide,
    "eng_jira_project": _eng_jira_project_slide,
    "eng_help_volume_trends": _eng_help_volume_trends_slide,
    "support_deck_cover": _support_deck_cover_slide,
    "support_intro": _support_intro_slide,
    "cs_notable": _cs_notable_slide,
    "salesforce_comprehensive_cover": _salesforce_comprehensive_cover_slide,
    "salesforce_comprehensive_toc": _salesforce_comprehensive_toc_slide,
    "salesforce_category": _salesforce_category_slide,
    "cohort_deck_title": _cohort_deck_title_slide,
    "cohort_summary": _cohort_summary_slide,
    "cohort_profiles": _cohort_profiles_slide,
    "cohort_findings": _cohort_findings_slide,
    "shortage_forecast": _shortage_forecast_slide,
    "critical_shortages_detail": _critical_shortages_detail_slide,
    "shortage_deliveries": _shortage_deliveries_slide,
    "lean_projects_portfolio": _lean_projects_portfolio_slide,
    "lean_projects_savings": _lean_projects_savings_slide,
    "pendo_sentiment": _pendo_sentiment_slide,
    "pendo_friction": _pendo_friction_slide,
    "pendo_localization": _pendo_localization_slide,
    "pendo_track_analytics": _pendo_track_analytics_slide,
    "pendo_definitions_appendix": _pendo_definitions_appendix_slide,
}


def get_slide_builder(slide_type: str):
    """Return the registered builder for a slide type, or None if unknown."""
    return _SLIDE_BUILDERS.get(slide_type)


def slide_builder_names() -> list[str]:
    """Return registered slide type names in registry order."""
    return list(_SLIDE_BUILDERS)


def get_slide_data_requirements(slide_type: str | None = None) -> list[str] | dict[str, list[str]]:
    """Return data requirements for one slide type, or a shallow copy of all requirements."""
    if slide_type is not None:
        return list(SLIDE_DATA_REQUIREMENTS.get(slide_type, []))
    return {key: list(value) for key, value in SLIDE_DATA_REQUIREMENTS.items()}
