"""Google Slides client for creating CS-oriented usage report decks.

Auth, batchUpdate throttling, and chunked updates live in ``slides_api``.
Dimensions, brand palette, and shared layout helpers live in ``slides_theme``.
"""

from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path
from typing import Any

from googleapiclient.errors import HttpError

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .cs_report_client import get_csr_section
from .slide_cohort import (
    COHORT_FINDING_ROW_GAP_PT as _COHORT_FINDING_ROW_GAP_PT,
    COHORT_FINDING_ROW_H_PT as _COHORT_FINDING_ROW_H_PT,
    cohort_deck_title_slide as _cohort_deck_title_slide,
    cohort_findings_rows_per_page as _cohort_findings_rows_per_page,
    cohort_findings_slide as _cohort_findings_slide,
    cohort_profiles_slide as _cohort_profiles_slide,
    cohort_summary_slide as _cohort_summary_slide,
)
from .slide_cohort_links import (
    COHORT_BUNDLE_SIGNAL_LINK_PHRASES as _COHORT_BUNDLE_SIGNAL_LINK_PHRASES,
    apply_cohort_bundle_links_to_notable_signals,
)
from .slide_data_quality import data_quality_slide as _data_quality_slide
from .slide_custom import custom_slide as _custom_slide
from .slide_cs_notable import cs_notable_slide as _cs_notable_slide
from .slide_depth import depth_slide as _depth_slide
from .slide_engagement import engagement_slide as _engagement_slide
from .slide_exports import exports_slide as _exports_slide
from .slide_engineering_portfolio import (
    customer_project_volume_trends_slide as _customer_project_volume_trends_slide,
    eng_bug_health_slide as _eng_bug_health_slide,
    eng_enhancements_open_slide as _eng_enhancements_open_slide,
    eng_enhancements_shipped_slide as _eng_enhancements_shipped_slide,
    eng_help_volume_trends_slide as _eng_help_volume_trends_slide,
    eng_insight_bullets as _eng_insight_bullets,
    eng_jira_project_slide as _eng_jira_project_slide,
    eng_portfolio_title_slide as _eng_portfolio_title_slide,
    eng_sprint_snapshot_slide as _eng_sprint_snapshot_slide,
    eng_support_pressure_slide as _eng_support_pressure_slide,
    eng_velocity_slide as _eng_velocity_slide,
    lean_project_volume_trends_slide as _lean_project_volume_trends_slide,
)
from .slide_guides import (
    guides_no_usage_slide as _guides_no_usage_slide,
    guides_slide as _guides_slide,
)
from .slide_health import (
    composite_health as _composite_health,
    health_slide as _health_slide,
    score_engagement as _score_engagement,
    score_platform as _score_platform,
    score_support as _score_support,
)
from .slide_kei import kei_slide as _kei_slide
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
    non_help_project_ticket_kpi_slide as _non_help_project_ticket_kpi_slide,
    project_slide_bg as _project_slide_bg,
    project_recent_tickets_table_slide as _project_recent_tickets_table_slide,
    project_ticket_metrics_breakdown_slide as _project_ticket_metrics_breakdown_slide,
    resolved_by_assignee_table_slide as _resolved_by_assignee_table_slide,
    sla_health_slide as _sla_health_slide,
    support_breakdown_slide as _support_breakdown_slide,
    support_help_customer_escalations_slide as _support_help_customer_escalations_slide,
    support_help_escalation_metrics_slide as _support_help_escalation_metrics_slide,
    support_help_orgs_by_opened_slide as _support_help_orgs_by_opened_slide,
    support_recent_closed_slide as _support_recent_closed_slide,
    support_recent_opened_slide as _support_recent_opened_slide,
)
from .slide_leandna_shortage import (
    SLIDES_NEEDING_LEANDNA_SHORTAGE as _SLIDES_NEEDING_LEANDNA_SHORTAGE,
    critical_shortages_detail_slide as _critical_shortages_detail_slide,
    shortage_deliveries_slide as _shortage_deliveries_slide,
    shortage_forecast_slide as _shortage_forecast_slide,
)
from .slide_leandna_projects import (
    lean_projects_portfolio_slide as _lean_projects_portfolio_slide,
    lean_projects_savings_slide as _lean_projects_savings_slide,
)
from .slide_metadata import (
    DQ_SOURCE_LABEL_ORDER as _DQ_SOURCE_LABEL_ORDER,
    REPORT_KEY_TO_DQ_SOURCE as _REPORT_KEY_TO_DQ_SOURCE,
    SLIDE_DATA_REQUIREMENTS,
    ordered_dq_data_sources_for_slide_plan as _ordered_dq_data_sources_for_slide_plan,
)
from .slide_platform_health import (
    HEALTH_BADGE as _HEALTH_BADGE,
    platform_health_slide as _platform_health_slide,
)
from .slide_platform_value import platform_value_slide as _platform_value_slide
from .slides_api import (
    GOOGLE_API_TIMEOUT_S,
    SCOPES,
    _build_slides_service_for_thread,
    _get_service,
    _google_api_unreachable_hint,
    presentations_batch_update_chunked,
    slides_presentations_batch_update,
)
from .slide_requests import (
    append_slide as _slide,
    append_text_box as _box,
    append_wrapped_text_box as _wrap_box,
)
from .slide_benchmarks import benchmarks_slide as _benchmarks_slide
from .slide_salesforce import (
    filter_salesforce_comprehensive_slide_plan as _filter_salesforce_comprehensive_slide_plan,
    salesforce_category_slide as _salesforce_category_slide,
    salesforce_comprehensive_cover_slide as _salesforce_comprehensive_cover_slide,
    salesforce_comprehensive_toc_slide as _salesforce_comprehensive_toc_slide,
    sf_category_records as _sf_category_records,
    sf_format_cell as _sf_format_cell,
    sf_records_to_table as _sf_records_to_table,
)
from .slide_signals import signals_slide as _signals_slide
from .slide_sites import sites_slide as _sites_slide
from .slide_supply_chain import supply_chain_slide as _supply_chain_slide
from .slide_team import load_teams as _load_teams, team_slide as _team_slide
from .slide_title_page import title_slide as _title_slide
from .slide_usage import (
    champions_slide as _champions_slide,
    features_slide as _features_slide,
)
from .speaker_notes import (
    get_speaker_notes_object_id,
    set_speaker_notes,
    set_speaker_notes_batch,
)
from .slide_pipeline_traces import (
    CANONICAL_PIPELINE_TRACES as _SLIDE_CANONICAL_PIPELINE_TRACES,
    build_slide_jql_speaker_notes_for_entry as _build_slide_jql_speaker_notes_for_entry_impl,
    cohort_findings_pipeline_traces as _cohort_findings_pipeline_traces,
    cohort_profile_pipeline_rows_for_block as _cohort_profile_pipeline_rows_for_block,
    cohort_profiles_pipeline_traces as _cohort_profiles_pipeline_traces,
    cohort_summary_pipeline_traces as _cohort_summary_pipeline_traces,
    cs_notable_pipeline_traces as _cs_notable_pipeline_traces,
    health_snapshot_pipeline_traces as _health_snapshot_pipeline_traces,
    peer_benchmarks_pipeline_traces as _peer_benchmarks_pipeline_traces,
    platform_risk_pipeline_traces as _platform_risk_pipeline_traces,
    platform_value_pipeline_traces as _platform_value_pipeline_traces,
    salesforce_pipeline_traces as _salesforce_pipeline_traces,
    support_health_exec_pipeline_traces as _support_health_exec_pipeline_traces,
)
from .slide_portfolio import (
    portfolio_leaders_slide as _portfolio_leaders_slide,
    portfolio_signals_slide as _portfolio_signals_slide,
    portfolio_title_slide as _portfolio_title_slide,
    portfolio_trends_slide as _portfolio_trends_slide,
)
from .slide_primitives import (
    CHART_LEGEND_PT,
    align as _align,
    background as _bg,
    bar_rect as _bar_rect,
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    omission_note as _omission_note,
    pill as _pill,
    rect as _rect,
    red_banner as _red_banner,
    set_support_deck_corner_customer as _set_support_deck_corner_customer,
    simple_table as _simple_table,
    slide_chart_legend_vertical as _slide_chart_legend_vertical,
    slide_title as _slide_title,
    style as _style,
    support_subtitle_matched_lead as _support_subtitle_matched_lead,
    support_title_includes_project as _support_title_includes_project,
    table_cell_bg as _table_cell_bg,
)
from .slide_qbr_framing import (
    qbr_agenda_slide as _qbr_agenda_slide,
    qbr_cover_slide as _qbr_cover_slide,
    qbr_divider_slide as _qbr_divider_slide,
)
from .slide_qbr_deployment import qbr_deployment_slide as _qbr_deployment_slide
from .slide_support_intro import (
    support_deck_cover_slide as _support_deck_cover_slide,
    support_intro_slide as _support_intro_slide,
)
from .slide_utils import (
    blob_recent_tickets_window_days as _blob_recent_tickets_window_days,
    dedupe_keep_order as _dedupe_keep_order,
    max_chars_one_line_for_table_col as _max_chars_one_line_for_table_col,
    slide_object_id_base as _slide_object_id_base,
    slide_size as _sz,
    slide_transform as _tf,
    truncate_table_cell as _truncate_table_cell,
)
from .slide_text import (
    iter_flat_page_elements as _iter_flat_page_elements,
    slides_shape_text_plain as _slides_shape_text_plain,
    utf16_code_unit_len as _utf16_code_unit_len,
    utf16_ranges_for_phrases as _utf16_ranges_for_phrases,
)
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    FONT_SERIF,
    GRAY,
    LIGHT,
    LTBLUE,
    MARGIN,
    MINT,
    MONO,
    NAVY,
    SLIDE_H,
    SLIDE_PAGINATING_SLIDE_TYPES,
    SLIDE_W,
    TEAL,
    TITLE_Y,
    WHITE,
    _cap_page_count,
    _date_range,
    _estimated_body_line_height_pt,
    slide_type_may_paginate,
)

from .deck_builder_utils import (
    _build_slide_jql_speaker_notes,
    _normalize_builder_return,
    build_slide_jql_speaker_notes_for_entry,
    normalize_builder_return,
)
from .deck_composable import (
    _get_deck_output_folder,
    _slide_counter,
    add_slide,
    create_empty_deck,
)
from .deck_legacy import (
    create_deck_for_customer,
    create_decks_for_all_customers,
)
from .deck_orchestrator import (
    create_health_deck,
)
from .deck_variants import (
    create_cohort_deck,
    create_csm_book_of_business_deck,
    create_health_decks_for_customers,
    create_portfolio_deck,
)
from .slide_registry import (
    _SLIDE_BUILDERS,
    get_slide_builder,
    get_slide_data_requirements,
    slide_builder_names,
)
from .slide_thumbnail_export import export_slide_thumbnails
