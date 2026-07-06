"""Slide builder registry and slide data requirement accessors."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from importlib import import_module
from typing import Any

from .slide_metadata import SLIDE_DATA_REQUIREMENTS

_SLIDE_BUILDER_SPECS: dict[str, tuple[str, str]] = {}
_SLIDE_BUILDER_CACHE: dict[str, Callable[..., Any]] = {}


def _register(module: str, **slides: str) -> None:
    for slide_type, attr in slides.items():
        if slide_type in _SLIDE_BUILDER_SPECS:
            raise ValueError(f"duplicate slide_type {slide_type!r}")
        _SLIDE_BUILDER_SPECS[slide_type] = (module, attr)


def _load_builder(slide_type: str) -> Callable[..., Any] | None:
    cached = _SLIDE_BUILDER_CACHE.get(slide_type)
    if cached is not None:
        return cached
    spec = _SLIDE_BUILDER_SPECS.get(slide_type)
    if spec is None:
        return None
    module_name, attr = spec
    mod = import_module(f".{module_name}", package=__package__)
    fn = getattr(mod, attr)
    _SLIDE_BUILDER_CACHE[slide_type] = fn
    return fn


class _LazySlideBuilders(Mapping[str, Callable[..., Any]]):
    """Registry keys are known at import time; builder modules load on first use."""

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key in _SLIDE_BUILDER_SPECS

    def __getitem__(self, key: str) -> Callable[..., Any]:
        fn = _load_builder(key)
        if fn is None:
            raise KeyError(key)
        return fn

    def get(self, key: str, default: Any = None) -> Any:
        fn = _load_builder(key)
        return fn if fn is not None else default

    def __iter__(self) -> Iterator[str]:
        return iter(_SLIDE_BUILDER_SPECS)

    def __len__(self) -> int:
        return len(_SLIDE_BUILDER_SPECS)

    def keys(self):
        return _SLIDE_BUILDER_SPECS.keys()


_SLIDE_BUILDERS: Mapping[str, Callable[..., Any]] = _LazySlideBuilders()

_register(
    "slide_title_page",
    title="title_slide",
)
_register(
    "slide_health",
    health="health_slide",
)
_register(
    "slide_engagement",
    engagement="engagement_slide",
)
_register(
    "slide_sites",
    sites="sites_slide",
)
_register(
    "slide_usage",
    features="features_slide",
    champions="champions_slide",
)
_register(
    "slide_benchmarks",
    benchmarks="benchmarks_slide",
)
_register(
    "slide_exports",
    exports="exports_slide",
)
_register(
    "slide_depth",
    depth="depth_slide",
)
_register(
    "slide_kei",
    kei="kei_slide",
)
_register(
    "slide_guides",
    guides="guides_slide",
)
_register(
    "slide_jira_support",
    jira="jira_slide",
    customer_ticket_metrics="customer_ticket_metrics_slide",
    customer_ticket_metrics_charts="customer_ticket_metrics_charts_slide",
    support_help_factory_start_buckets="help_factory_start_day_buckets_slide",
    support_help_monthly_operational="help_monthly_operational_slide",
    support_help_orgs_by_opened="support_help_orgs_by_opened_slide",
    support_help_customer_escalations="support_help_customer_escalations_slide",
    support_help_escalation_metrics="support_help_escalation_metrics_slide",
    support_recent_opened="support_recent_opened_slide",
    support_recent_closed="support_recent_closed_slide",
    customer_project_ticket_metrics="customer_project_ticket_metrics_slide",
    customer_project_ticket_metrics_breakdown="customer_project_ticket_metrics_breakdown_slide",
    customer_project_recent_opened="customer_project_recent_opened_slide",
    customer_project_recent_closed="customer_project_recent_closed_slide",
    lean_project_ticket_metrics="lean_project_ticket_metrics_slide",
    lean_project_ticket_metrics_breakdown="lean_project_ticket_metrics_breakdown_slide",
    lean_project_recent_opened="lean_project_recent_opened_slide",
    lean_project_recent_closed="lean_project_recent_closed_slide",
    help_resolved_by_assignee="help_resolved_by_assignee_slide",
    customer_resolved_by_assignee="customer_resolved_by_assignee_slide",
    lean_resolved_by_assignee="lean_resolved_by_assignee_slide",
    sla_health="sla_health_slide",
    cross_validation="cross_validation_slide",
    engineering="engineering_slide",
    enhancements="enhancement_requests_slide",
    support_breakdown="support_breakdown_slide",
)
_register(
    "slide_engineering_portfolio",
    customer_project_volume_trends="customer_project_volume_trends_slide",
    eng_backlog_health="eng_backlog_health_slide",
    eng_bug_flow="eng_bug_flow_slide",
    eng_bug_health="eng_bug_health_slide",
    eng_capacity="eng_capacity_slide",
    eng_epic_progress="eng_epic_progress_slide",
    eng_current_sprint="eng_current_sprint_slide",
    eng_exec_summary="eng_exec_summary_slide",
    eng_flow_bottlenecks="eng_flow_bottlenecks_slide",
    eng_work_split="eng_work_split_slide",
    eng_help_volume_trends="eng_help_volume_trends_slide",
    eng_jira_project="eng_jira_project_slide",
    eng_portfolio_title="eng_portfolio_title_slide",
    eng_team_scorecard="eng_team_scorecard_slide",
    eng_team_roster="eng_team_roster_slide",
    eng_divider="eng_divider_slide",
    eng_toc="eng_toc_slide",
    eng_sprint_snapshot="eng_sprint_snapshot_slide",
    eng_support_pressure="eng_support_pressure_slide",
    eng_velocity="eng_velocity_slide",
    lean_project_volume_trends="lean_project_volume_trends_slide",
    cursor_cost="cursor_cost_slide",
    cursor_cost_models="cursor_cost_models_slide",
    cursor_efficiency="cursor_efficiency_slide",
    cursor_efficiency_engineers="cursor_efficiency_engineers_slide",
    cursor_usage="cursor_usage_slide",
    cursor_usage_non_engineers="cursor_usage_non_engineers_slide",
    cursor_model_usage="cursor_model_usage_slide",
    cursor_users_volume="cursor_users_volume_slide",
    cursor_users="cursor_users_slide",
    cursor_users_light="cursor_users_light_slide",
    cursor_users_non_engineers_volume="cursor_users_non_engineers_volume_slide",
    cursor_users_non_engineers="cursor_users_non_engineers_slide",
    cursor_users_non_engineers_light="cursor_users_non_engineers_light_slide",
    github_engineering_output="github_engineering_output_slide",
    github_engineer_contribution="github_engineer_contribution_slide",
    github_delivery_flow="github_delivery_flow_slide",
    github_change_profile="github_change_profile_slide",
    productivity_summary="productivity_summary_slide",
    productivity_trend="productivity_trend_slide",
    productivity_coaching="productivity_coaching_slide",
    ai_output_correlation="ai_output_correlation_slide",
    ai_productivity_matrix="ai_productivity_matrix_slide",
)
_register(
    "slide_custom",
    custom="custom_slide",
)
_register(
    "slide_signals",
    signals="signals_slide",
)
_register(
    "slide_platform_health",
    platform_health="platform_health_slide",
)
_register(
    "slide_supply_chain",
    supply_chain="supply_chain_slide",
)
_register(
    "slide_platform_value",
    platform_value="platform_value_slide",
    platform_value_summary_cover="platform_value_summary_cover_slide",
    platform_value_summary_toc="platform_value_summary_toc_slide",
)
_register(
    "slide_data_quality",
    data_quality="data_quality_slide",
)
_register(
    "slide_portfolio",
    portfolio_title="portfolio_title_slide",
    csm_book_title="csm_book_title_slide",
    portfolio_revenue_book="portfolio_revenue_book_slide",
    portfolio_expansion_book="portfolio_expansion_book_slide",
    portfolio_signals="portfolio_signals_slide",
    portfolio_trends="portfolio_trends_slide",
    portfolio_leaders="portfolio_leaders_slide",
)
_register(
    "slide_team",
    team="team_slide",
)
_register(
    "slide_qbr_framing",
    qbr_cover="qbr_cover_slide",
    qbr_agenda="qbr_agenda_slide",
    qbr_divider="qbr_divider_slide",
)
_register(
    "slide_qbr_deployment",
    qbr_deployment="qbr_deployment_slide",
)
_register(
    "slide_support_intro",
    support_deck_cover="support_deck_cover_slide",
    support_intro="support_intro_slide",
)
_register(
    "slide_support_kpis",
    support_kpis_intake="support_kpis_intake_slide",
    support_kpis_flow="support_kpis_flow_slide",
    support_kpis_backlog="support_kpis_backlog_slide",
    support_kpis_tail_risk="support_kpis_tail_risk_slide",
    support_kpis_sla="support_kpis_sla_slide",
    support_kpis_ttfr="support_kpis_ttfr_slide",
    support_kpis_resolution="support_kpis_resolution_slide",
    support_kpis_engineering_dependency="support_kpis_engineering_dependency_slide",
    support_kpis_escalation_backlog_engineering="support_kpis_escalation_backlog_engineering_slide",
    support_kpis_escalation_backlog_data_integration="support_kpis_escalation_backlog_data_integration_slide",
    support_kpis_customer_health="support_kpis_customer_health_slide",
    support_kpis_csat="support_kpis_csat_slide",
    support_kpis_aging_thresholds="support_kpis_aging_thresholds_slide",
)
_register(
    "slide_support_kpis_notable",
    support_kpis_notable="support_kpis_notable_slide",
)
_register(
    "slide_cs_notable",
    cs_notable="cs_notable_slide",
)
_register(
    "slide_salesforce",
    salesforce_comprehensive_cover="salesforce_comprehensive_cover_slide",
    salesforce_comprehensive_toc="salesforce_comprehensive_toc_slide",
    salesforce_category="salesforce_category_slide",
)
_register(
    "slide_cohort",
    cohort_deck_title="cohort_deck_title_slide",
    cohort_summary="cohort_summary_slide",
    cohort_profiles="cohort_profiles_slide",
    cohort_findings="cohort_findings_slide",
)
_register(
    "slide_leandna_shortage",
    shortage_forecast="shortage_forecast_slide",
    critical_shortages_detail="critical_shortages_detail_slide",
    shortage_deliveries="shortage_deliveries_slide",
)
_register(
    "slide_leandna_projects",
    lean_projects_portfolio="lean_projects_portfolio_slide",
    lean_projects_savings="lean_projects_savings_slide",
)
_register(
    "slide_pendo",
    pendo_sentiment="pendo_sentiment_slide",
    pendo_friction="pendo_friction_slide",
    pendo_localization="pendo_localization_slide",
    pendo_track_analytics="pendo_track_analytics_slide",
    pendo_definitions_appendix="pendo_definitions_appendix_slide",
)


def get_slide_builder(slide_type: str):
    """Return the registered builder for a slide type, or None if unknown."""
    return _load_builder(slide_type)


def slide_builder_names() -> list[str]:
    """Return registered slide type names in registry order."""
    return list(_SLIDE_BUILDER_SPECS)


def get_slide_data_requirements(slide_type: str | None = None) -> list[str] | dict[str, list[str]]:
    """Return data requirements for one slide type, or a shallow copy of all requirements."""
    if slide_type is not None:
        return list(SLIDE_DATA_REQUIREMENTS.get(slide_type, []))
    return {key: list(value) for key, value in SLIDE_DATA_REQUIREMENTS.items()}
