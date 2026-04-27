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
    customer_help_recent_slide as _customer_help_recent_slide,
    customer_project_recent_closed_slide as _customer_project_recent_closed_slide,
    customer_project_recent_opened_slide as _customer_project_recent_opened_slide,
    customer_project_ticket_metrics_breakdown_slide as _customer_project_ticket_metrics_breakdown_slide,
    customer_project_ticket_metrics_slide as _customer_project_ticket_metrics_slide,
    customer_ticket_metrics_charts_slide as _customer_ticket_metrics_charts_slide,
    customer_ticket_metrics_slide as _customer_ticket_metrics_slide,
    lean_project_recent_opened_slide as _lean_project_recent_opened_slide,
    lean_project_ticket_metrics_slide as _lean_project_ticket_metrics_slide,
    non_help_project_ticket_kpi_slide as _non_help_project_ticket_kpi_slide,
    project_recent_tickets_table_slide as _project_recent_tickets_table_slide,
    project_ticket_metrics_breakdown_slide as _project_ticket_metrics_breakdown_slide,
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
    MAX_PAGINATED_SLIDE_PAGES,
    MINT,
    MONO,
    NAVY,
    SLIDE_H,
    SLIDE_PAGINATING_SLIDE_TYPES,
    SLIDE_W,
    TEAL,
    TITLE_Y,
    WHITE,
    _cap_chunk_list,
    _cap_page_count,
    _date_range,
    _estimated_body_line_height_pt,
    _table_rows_fit_span,
    slide_type_may_paginate,
)

# ── Builder utilities ──


def normalize_builder_return(ret: Any, default_slide_id: str) -> tuple[int, list[str]]:
    """Slide builders return ``next_idx`` (int) or ``(next_idx, [page_object_id, ...])`` for multi-page slides."""
    if isinstance(ret, tuple) and len(ret) == 2 and isinstance(ret[1], list):
        ids = [str(x) for x in ret[1] if x]
        return int(ret[0]), (ids if ids else [default_slide_id])
    return int(ret), [default_slide_id]


_normalize_builder_return = normalize_builder_return


def build_slide_jql_speaker_notes_for_entry(report: dict[str, Any], entry: dict[str, Any]) -> str:
    """Build speaker notes for one slide-plan entry using this module's slide registries."""
    return _build_slide_jql_speaker_notes_for_entry_impl(
        report,
        entry,
        data_requirements=SLIDE_DATA_REQUIREMENTS,
    )


_build_slide_jql_speaker_notes = build_slide_jql_speaker_notes_for_entry


def _jira_slide(reqs, sid, report, idx):
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")
    jira_base = jira.get("base_url", "")

    total = jira["total_issues"]
    open_n = jira["open_issues"]
    resolved = jira["resolved_issues"]
    esc = jira["escalated"]
    bugs = jira["open_bugs"]
    days = jira.get("days", 90)

    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    header = (
        f"{total} HELP tickets  ·  {date_range}  ·  {open_n} open  ·  {resolved} resolved  ·  "
        f"{esc} escalated  ·  {bugs} open bugs"
    )

    sla_lines = []
    ttfr = jira.get("ttfr", {})
    if ttfr.get("measured", 0) > 0:
        parts = [f"First Response:  median {ttfr['median']}  ·  avg {ttfr['avg']}"]
        if ttfr.get("breached"):
            parts.append(f"  ·  {ttfr['breached']} breach{'es' if ttfr['breached'] != 1 else ''}")
        if ttfr.get("waiting"):
            parts.append(f"  ·  {ttfr['waiting']} awaiting")
        sla_lines.append("".join(parts))
    ttr = jira.get("ttr", {})
    if ttr.get("measured", 0) > 0:
        parts = [f"Resolution:  median {ttr['median']}  ·  avg {ttr['avg']}"]
        if ttr.get("breached"):
            parts.append(f"  ·  {ttr['breached']} breach{'es' if ttr['breached'] != 1 else ''}")
        if ttr.get("waiting"):
            parts.append(f"  ·  {ttr['waiting']} unresolved")
        sla_lines.append("".join(parts))
    if sla_lines:
        sla_text = "\n".join(sla_lines)
        body_offset = 22 + 12 * len(sla_lines)
    else:
        sla_text = ""
        body_offset = 28

    status_items = list(jira.get("by_status", {}).items())
    sum_status = sum(c for _, c in status_items)
    sum_priority = sum(c for _, c in jira.get("by_priority", {}).items())
    status_lines = [f"{c:>4}  {s}" for s, c in status_items]
    prio_short = {"Blocker: The platform is completely down": "Blocker",
                  "Critical: Significant operational impact": "Critical",
                  "Major: Workaround available, not essential": "Major",
                  "Minor: Impairs non-essential functionality": "Minor"}
    prio_items = list(jira.get("by_priority", {}).items())
    prio_lines = [f"{c:>4}  {prio_short.get(p, p[:20])}" for p, c in prio_items]

    recent_all = list(jira.get("recent_issues", []))
    esc_issues = list(jira.get("escalated_issues", []))
    eng = jira.get("engineering", {})
    eng_open = list(eng.get("open", []))
    eng_closed = list(eng.get("recent_closed", []))
    eng_hdr = (
        f"Engineering Pipeline  ({eng.get('open_count', len(eng_open))} open · "
        f"{eng.get('closed_count', len(eng_closed))} closed)"
    )

    col_gap = 20
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    max_y = BODY_BOTTOM
    line_h = 12
    sec_title_h = 16

    def _pack_lines(lines: list[str], budget: float) -> tuple[list[str], list[str], float]:
        if budget < sec_title_h + line_h:
            return [], lines, 0.0
        used = float(sec_title_h)
        take: list[str] = []
        rest = list(lines)
        while rest and used + line_h <= budget:
            take.append(rest.pop(0))
            used += line_h
        return take, rest, used

    def _continuation_pages(
        sections: list[tuple[str, list[str]]],
        start_idx: int,
        total_pages: int,
        body_top_y: float,
        *,
        max_slide_index_exclusive: int | None = None,
        reconcile_header_total: int | None = None,
    ) -> tuple[int, list[str]]:
        oids_extra: list[str] = []
        cont_y0 = body_top_y
        per_page_lines = max(1, int((max_y - cont_y0 - 8) // line_h))
        flat: list[tuple[str, str]] = []
        if reconcile_header_total is not None and start_idx >= 1:
            flat.append((
                "n",
                f"Together with slide 1, the breakdowns below account for all "
                f"{reconcile_header_total} tickets in the header.",
            ))
        for sec_title, slines in sections:
            if not slines:
                continue
            flat.append(("h", sec_title))
            for ln in slines:
                flat.append(("l", ln))
        pos = 0
        page_num = start_idx
        while pos < len(flat) and (max_slide_index_exclusive is None or page_num < max_slide_index_exclusive):
            page_sid = f"{sid}_p{page_num}"
            oids_extra.append(page_sid)
            _slide(reqs, page_sid, idx + page_num)
            _bg(reqs, page_sid, WHITE)
            ttl = "Support Summary" if total_pages == 1 else f"Support Summary ({page_num + 1} of {total_pages})"
            _slide_title(reqs, page_sid, ttl)
            _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
            _style(reqs, f"{page_sid}_hdr", 0, len(header), size=11, color=NAVY, font=FONT, bold=True)
            y = cont_y0
            used_lines = 0
            while pos < len(flat) and used_lines < per_page_lines:
                kind, text = flat[pos]
                if kind == "h":
                    _box(reqs, f"{page_sid}_h{pos}", page_sid, MARGIN, y, CONTENT_W, sec_title_h, text)
                    _style(reqs, f"{page_sid}_h{pos}", 0, len(text), bold=True, size=10, color=BLUE, font=FONT)
                    y += sec_title_h
                    used_lines += 1
                elif kind == "n":
                    _box(reqs, f"{page_sid}_n{pos}", page_sid, MARGIN, y, CONTENT_W, line_h * 2 + 4, text)
                    _style(reqs, f"{page_sid}_n{pos}", 0, len(text), size=8, color=GRAY, font=FONT)
                    y += line_h * 2 + 6
                    used_lines += 2
                else:
                    _box(reqs, f"{page_sid}_l{pos}", page_sid, MARGIN, y, CONTENT_W, line_h, text)
                    _style(reqs, f"{page_sid}_l{pos}", 0, len(text), size=8, color=NAVY, font=MONO)
                    if jira_base and text and text.strip():
                        key = text.split()[0] if text.split() else ""
                        if key and "-" in key:
                            lk = len(key)
                            _style(reqs, f"{page_sid}_l{pos}", 0, lk, bold=True, size=8, color=BLUE, font=MONO,
                                   link=f"{jira_base}/browse/{key}")
                    y += line_h
                    used_lines += 1
                pos += 1
            page_num += 1
        return page_num, oids_extra

    # ── Page 1: pack two columns; push overflow into continuation sections ──
    status_rest: list[str] = []
    prio_rest: list[str] = []
    recent_rest: list = []
    esc_rest: list = []
    eng_open_rest: list = []
    eng_closed_rest: list = []
    oids: list[str] = []

    body_top = BODY_Y + body_offset
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap
    reserve_bottom = 36
    summary_budget = max(max_y - body_top - reserve_bottom - 140, sec_title_h + line_h)

    st_take, status_rest, st_used = _pack_lines(status_lines, summary_budget)
    pr_budget = max(summary_budget - st_used - 4, sec_title_h + line_h)
    pr_take, prio_rest, pr_used = _pack_lines(prio_lines, pr_budget)

    st_footer_h = 0
    if st_take and status_rest:
        st_footer_h = line_h * 3
    elif st_take and sum_status != total:
        st_footer_h = line_h * 2
    pr_footer_h = 0
    if pr_take and prio_rest:
        pr_footer_h = line_h * 3
    elif pr_take and sum_priority != total:
        pr_footer_h = line_h * 2

    left_y = body_top + st_used + 4 + pr_used + 4 + st_footer_h + pr_footer_h
    recent_take: list = []
    if recent_all:
        avail_recent = max(int((max_y - left_y) // line_h) - 1, 0)
        recent_take = recent_all[:avail_recent]
        recent_rest = recent_all[avail_recent:]

    right_y = body_top
    esc_take = []
    if esc_issues or esc > 0:
        esc_line_budget = max(int((max_y - right_y - reserve_bottom) // line_h) - 1, 0)
        if esc_line_budget <= 0 and esc_issues:
            esc_rest = list(esc_issues)
        else:
            esc_take = esc_issues[:esc_line_budget]
            esc_rest = esc_issues[esc_line_budget:]

    show_esc_block_p1 = bool(esc_take) or (esc > 0 and not esc_rest)
    right_y_eng = body_top
    if show_esc_block_p1:
        right_y_eng += line_h * (len(esc_take) + 1) + 10

    eng_open_take: list = []
    eng_closed_take: list = []
    eng_open_rest: list = []
    eng_closed_rest: list = []
    if eng_open or eng_closed:
        avail_eng = max(int((max_y - right_y_eng) // line_h) - 2, 1)
        eng_open_take = eng_open[:avail_eng]
        eng_open_rest = eng_open[len(eng_open_take):]
        rem = avail_eng - len(eng_open_take)
        if eng_closed and rem > 1:
            eng_closed_take = eng_closed[: rem - 1]
            eng_closed_rest = eng_closed[len(eng_closed_take):]
        elif eng_closed and rem == 1:
            eng_closed_rest = list(eng_closed)

    cont_sections: list[tuple[str, list[str]]] = []
    if status_rest:
        cont_sections.append(("By Status (continued)", status_rest))
    if prio_rest:
        cont_sections.append(("By Priority (continued)", prio_rest))
    if recent_rest:
        cont_sections.append(
            ("Recent Issues (continued)",
             [f"{r['key']}  {r['status'][:8]:8s}  {r['summary'][:52]}" for r in recent_rest])
        )
    if esc_rest:
        cont_sections.append(
            ("Escalated (continued)",
             [f"{e['key']}  {e['summary'][:48]}  ({e['status']})" for e in esc_rest])
        )
    eng_cont: list[str] = []
    if eng_open_rest:
        for t in eng_open_rest:
            assignee = t.get("assignee") or "unassigned"
            eng_cont.append(f"{t['key']}  {t['summary'][:40]}  [{assignee}]")
    if eng_closed_rest:
        eng_cont.append("— Recently Closed —")
        for t in eng_closed_rest:
            eng_cont.append(f"{t['key']}  {t['summary'][:48]}")
    if eng_cont:
        cont_sections.append(("Engineering Pipeline (continued)", eng_cont))

    if cont_sections:
        flat_lines = sum(len(s[1]) + 1 for s in cont_sections)
        per_pg = max(1, int((max_y - body_top - 8) // line_h))
        total_pages = 1 + (flat_lines + per_pg - 1) // per_pg
        total_pages = min(total_pages, MAX_PAGINATED_SLIDE_PAGES)
    else:
        total_pages = 1

    page_sid0 = f"{sid}_p0" if total_pages > 1 else sid
    oids.append(page_sid0)
    _slide(reqs, page_sid0, idx)
    _bg(reqs, page_sid0, WHITE)
    ttl0 = "Support Summary" if total_pages == 1 else f"Support Summary (1 of {total_pages})"
    _slide_title(reqs, page_sid0, ttl0)
    _box(reqs, f"{page_sid0}_hdr", page_sid0, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{page_sid0}_hdr", 0, len(header), size=11, color=NAVY, font=FONT, bold=True)
    if sla_lines:
        _box(reqs, f"{page_sid0}_sla", page_sid0, MARGIN, BODY_Y + 18, CONTENT_W, 12 * len(sla_lines) + 4, sla_text)
        _style(reqs, f"{page_sid0}_sla", 0, len(sla_text), size=9, color=GRAY, font=FONT)
        fr_label = "First Response:"
        fr_end = sla_text.find(fr_label)
        if fr_end >= 0:
            _style(reqs, f"{page_sid0}_sla", fr_end, fr_end + len(fr_label), bold=True, color=NAVY)
        res_label = "Resolution:"
        res_pos = sla_text.find(res_label)
        if res_pos >= 0:
            _style(reqs, f"{page_sid0}_sla", res_pos, res_pos + len(res_label), bold=True, color=NAVY)

    body_top = BODY_Y + body_offset
    left_y = body_top
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    if st_take:
        st_footer = ""
        if status_rest:
            k_st = len(st_take)
            sub_st = sum(c for _, c in status_items[:k_st])
            rest_st = sum(c for _, c in status_items[k_st:])
            n_st = len(status_items) - k_st
            st_footer = (
                f"\nSubtotal {sub_st} tickets in the statuses above. "
                f"Next slide(s): {rest_st} tickets across {n_st} more status row(s). "
                f"(All statuses total {sum_status} tickets.)"
            )
        elif sum_status != total:
            st_footer = f"\nNote: status rows sum to {sum_status}; header shows {total} issues."
        status_text = "By Status\n" + "\n".join(st_take) + st_footer
        st_h = sec_title_h + line_h * len(st_take) + 4 + st_footer_h
        _box(reqs, f"{page_sid0}_st", page_sid0, left_x, left_y, left_w, st_h, status_text)
        _style(reqs, f"{page_sid0}_st", 0, len(status_text), size=8, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid0}_st", 0, len("By Status"), bold=True, size=9, color=BLUE)
        if st_footer:
            _st_f0 = len("By Status\n" + "\n".join(st_take))
            _style(reqs, f"{page_sid0}_st", _st_f0, len(status_text), size=7, color=GRAY, font=FONT)
        left_y += st_h + 4
    if pr_take:
        pr_footer = ""
        if prio_rest:
            k_pr = len(pr_take)
            sub_pr = sum(c for _, c in prio_items[:k_pr])
            rest_pr = sum(c for _, c in prio_items[k_pr:])
            n_pr = len(prio_items) - k_pr
            pr_footer = (
                f"\nSubtotal {sub_pr} tickets in the priorities above. "
                f"Next slide(s): {rest_pr} tickets across {n_pr} more priority row(s). "
                f"(All priorities total {sum_priority} tickets.)"
            )
        elif sum_priority != total:
            pr_footer = f"\nNote: priority rows sum to {sum_priority}; header shows {total} issues."
        prio_text = "By Priority\n" + "\n".join(pr_take) + pr_footer
        pr_h = sec_title_h + line_h * len(pr_take) + 4 + pr_footer_h
        _box(reqs, f"{page_sid0}_pr", page_sid0, left_x, left_y, left_w, pr_h, prio_text)
        _style(reqs, f"{page_sid0}_pr", 0, len(prio_text), size=8, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid0}_pr", 0, len("By Priority"), bold=True, size=9, color=BLUE)
        if pr_footer:
            _pr_f0 = len("By Priority\n" + "\n".join(pr_take))
            _style(reqs, f"{page_sid0}_pr", _pr_f0, len(prio_text), size=7, color=GRAY, font=FONT)
        left_y += pr_h + 4

    if recent_take:
        recent_lines = [f"{r['key']}  {r['status'][:8]:8s}  {r['summary'][:30]}" for r in recent_take]
        recent_text = "Recent Issues\n" + "\n".join(recent_lines)
        _box(reqs, f"{page_sid0}_rc", page_sid0, left_x, left_y, left_w, max_y - left_y, recent_text)
        _style(reqs, f"{page_sid0}_rc", 0, len(recent_text), size=8, color=NAVY, font=MONO)
        _style(reqs, f"{page_sid0}_rc", 0, len("Recent Issues"), bold=True, size=9, color=BLUE, font=FONT)
        if jira_base:
            offset = len("Recent Issues\n")
            for r in recent_take:
                key = r["key"]
                _style(reqs, f"{page_sid0}_rc", offset, offset + len(key), bold=True, size=8,
                       color=BLUE, font=MONO, link=f"{jira_base}/browse/{key}")
                offset += len(f"{key}  {r['status'][:8]:8s}  {r['summary'][:30]}") + 1

    right_y = body_top
    if show_esc_block_p1:
        esc_lines = [f"{e['key']}  {e['summary'][:36]}  ({e['status']})" for e in esc_take]
        esc_text = f"Escalated ({esc})\n" + "\n".join(esc_lines)
        esc_h = line_h * (len(esc_take) + 1) + 6
        _box(reqs, f"{page_sid0}_esc", page_sid0, right_x, right_y, right_w, esc_h, esc_text)
        _style(reqs, f"{page_sid0}_esc", 0, len(esc_text), size=8, color=NAVY, font=FONT)
        esc_hdr = f"Escalated ({esc})"
        _style(reqs, f"{page_sid0}_esc", 0, len(esc_hdr), bold=True, size=9,
               color={"red": 0.85, "green": 0.15, "blue": 0.15})
        if jira_base and esc_take:
            offset = len(esc_hdr) + 1
            for e in esc_take:
                key = e["key"]
                _style(reqs, f"{page_sid0}_esc", offset, offset + len(key), bold=True, size=8,
                       color={"red": 0.85, "green": 0.15, "blue": 0.15},
                       link=f"{jira_base}/browse/{key}")
                offset += len(f"{key}  {e['summary'][:36]}  ({e['status']})") + 1
        right_y += esc_h + 4

    if eng_open_take or eng_closed_take:
        eng_lines_body = [eng_hdr]
        for t in eng_open_take:
            assignee = t.get("assignee") or "unassigned"
            eng_lines_body.append(f"  {t['key']}  {t['summary'][:26]}  [{assignee}]")
        if eng_closed_take:
            eng_lines_body.append("Recently Closed")
            for t in eng_closed_take:
                eng_lines_body.append(f"  {t['key']}  {t['summary'][:36]}")
        eng_text = "\n".join(eng_lines_body)
        _box(reqs, f"{page_sid0}_eng", page_sid0, right_x, right_y, right_w, max_y - right_y, eng_text)
        _style(reqs, f"{page_sid0}_eng", 0, len(eng_text), size=8, color=NAVY, font=MONO)
        _style(reqs, f"{page_sid0}_eng", 0, len(eng_hdr), bold=True, size=9, color=BLUE, font=FONT)
        rc_start = eng_text.find("Recently Closed")
        if rc_start >= 0:
            _style(reqs, f"{page_sid0}_eng", rc_start, rc_start + len("Recently Closed"),
                   bold=True, size=8, color=GRAY, font=FONT)
        if jira_base:
            off = 0
            for t in eng_open_take:
                key = t["key"]
                p = eng_text.find(key, off)
                if p >= 0:
                    _style(reqs, f"{page_sid0}_eng", p, p + len(key), bold=True, size=8, color=BLUE, font=MONO,
                           link=f"{jira_base}/browse/{key}")
                    off = p + len(key)
            off = rc_start + len("Recently Closed") if rc_start >= 0 else 0
            for t in eng_closed_take:
                key = t["key"]
                p = eng_text.find(key, off)
                if p >= 0:
                    _style(reqs, f"{page_sid0}_eng", p, p + len(key), bold=True, size=8, color=BLUE, font=MONO,
                           link=f"{jira_base}/browse/{key}")
                    off = p + len(key)

    if cont_sections:
        _, extra_oids = _continuation_pages(
            cont_sections, 1, total_pages, body_top,
            max_slide_index_exclusive=MAX_PAGINATED_SLIDE_PAGES,
            reconcile_header_total=total,
        )
        oids.extend(extra_oids)

    if len(oids) == 1:
        return idx + 1
    return idx + len(oids), oids


def _project_slide_bg(project: str) -> dict[str, float]:
    """Subtle project tint backgrounds for project-specific slides."""
    proj = (project or "").strip().upper()
    if proj == "CUSTOMER":
        return {"red": 0.95, "green": 0.98, "blue": 1.0}
    if proj == "LEAN":
        return {"red": 0.95, "green": 1.0, "blue": 0.97}
    if proj == "HELP":
        return {"red": 1.0, "green": 0.96, "blue": 0.96}
    return WHITE


def _support_help_customer_escalations_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """HELP open issues with label customer_escalation, ordered by last update (same table style as recent HELP)."""
    jira = report.get("jira") or {}
    blob = jira.get("help_customer_escalations")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP customer escalations (not in report — support deck Jira fetch)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"HELP customer escalations: {blob['error']}",
        )

    jira_base = (jira.get("base_url") or "").rstrip("/")
    items: list[dict[str, Any]] = list(blob.get("tickets") or [])
    customer = report.get("customer") or blob.get("customer") or "All Customers"
    is_all_customers = report.get("customer") is None

    entry = report.get("_current_slide") or {}
    base_title = entry.get("title") or "Customer Escalations (HELP)"
    total_n = len(items)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, _project_slide_bg("HELP"))
    _slide_title(reqs, sid, base_title)

    table_top = BODY_Y + 24
    row_h = 19.0
    max_data_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=BODY_BOTTOM,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=30,
    )
    display_items = items[:max_data_rows]
    n_show = len(display_items)
    if total_n > n_show:
        count_text = f"showing {n_show} of {total_n} tickets (by last update)"
    else:
        count_text = f"{total_n} ticket{'s' if total_n != 1 else ''}"

    port_note = " ·  no org column (portfolio scope)" if is_all_customers else ""
    _lead = _support_subtitle_matched_lead(report, customer)
    sub = (
        f"{_lead}label customer_escalation · not Done · order by updated  ·  {count_text}{port_note}"
    )
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)

    if not items:
        empty_msg = "No open HELP tickets with label customer_escalation."
        _box(reqs, f"{sid}_empty", sid, MARGIN, BODY_Y + 30, CONTENT_W, 40, empty_msg)
        _style(reqs, f"{sid}_empty", 0, len(empty_msg), size=10, color=NAVY, font=FONT)
        return idx + 1

    if is_all_customers:
        headers = ["ID", "Title", "Status", "Priority", "Created", "Updated"]
        col_widths = [60, 220, 100, 100, 64, 64]
    else:
        headers = ["ID", "Title", "Status", "Priority", "Created", "Updated"]
        col_widths = [60, 196, 100, 100, 64, 64]
    t_title = _max_chars_one_line_for_table_col(float(col_widths[1]))
    t_st = _max_chars_one_line_for_table_col(float(col_widths[2]))
    t_pr = _max_chars_one_line_for_table_col(float(col_widths[3]))
    ROW_H = row_h
    num_rows = 1 + len(display_items)
    table_id = f"{sid}_tbl"

    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * ROW_H),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })

    def _ct(row, col, text):
        if not text:
            return
        reqs.append({
            "insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text),
                "insertionIndex": 0,
            }
        })

    def _cs_e(row, col, text_len, bold=False, color=None, size=8, link=None):
        if text_len > 0:
            s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
            f = ["fontSize", "fontFamily"]
            if bold:
                s["bold"] = True
                f.append("bold")
            if color:
                s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
                f.append("foregroundColor")
            if link:
                s["link"] = {"url": link}
                f.append("link")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s, "fields": ",".join(f),
                }
            })

    _clean_table(reqs, table_id, num_rows, len(headers))

    for ci, h in enumerate(headers):
        _ct(0, ci, h)
        _cs_e(0, ci, len(h), bold=True, size=9, color=NAVY)

    for ri, it in enumerate(display_items):
        row_idx = ri + 1
        key = it.get("key") or "—"
        title = _truncate_table_cell(it.get("summary"), t_title)
        status = _truncate_table_cell(it.get("status"), t_st)
        priority = _truncate_table_cell(it.get("priority"), t_pr)
        created = it.get("created_short") or "—"
        updated = it.get("updated_short") or "—"
        vals = [key, title, status, priority, created, updated]
        for ci, v in enumerate(vals):
            _ct(row_idx, ci, v)
            if ci == 0 and jira_base and key and key != "—":
                _cs_e(row_idx, ci, len(v), bold=True, color=BLUE, size=8, link=f"{jira_base}/browse/{key}")
            else:
                _cs_e(row_idx, ci, len(v), size=8)

    return idx + 1


def _support_help_escalation_metrics_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """HELP-only KPIs: backlog TTR with/without customer_escalation label; 90d open/close counts."""
    jira = report.get("jira") or {}
    blob = jira.get("help_escalation_metrics")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP escalation metrics (not in report — support deck Jira fetch)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"HELP escalation metrics: {blob['error']}",
        )

    entry = report.get("_current_slide") or {}
    t0 = (entry.get("title") or "").strip()
    base_title = t0 or "HELP — Escalation metrics"
    tp = _support_title_includes_project(base_title, "HELP")
    t_esc = blob.get("ttr_open_backlog_customer_escalation") or {}
    t_not = blob.get("ttr_open_backlog_not_customer_escalation") or {}
    n_open = int(blob.get("not_done_escalation_count") or 0)
    n_90o = int(blob.get("escalations_opened_90d") or 0)
    n_90c = int(blob.get("escalations_closed_90d") or 0)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, _project_slide_bg("HELP"))
    _slide_title(reqs, sid, base_title)
    llm_q = (blob.get("llm_nature_summary") or "").strip()
    y = float(BODY_Y)
    if llm_q:
        # Tall box + larger body type for 2–4 short paragraphs; KPI tiles tighten slightly to fit.
        q_h = 128.0
        _wrap_box(reqs, f"{sid}_quote", sid, MARGIN, y, CONTENT_W, q_h, llm_q)
        _style(reqs, f"{sid}_quote", 0, len(llm_q), size=11, color=NAVY, font=FONT_SERIF)
        y = y + q_h + 6.0
        row_gap = 10.0
        card_h = 50.0
    else:
        row_gap = 14.0
        card_h = 54.0

    col_gap = 18
    top_card_w = (CONTENT_W - 2 * col_gap) / 3
    bot_card_w = (CONTENT_W - col_gap) / 2
    row1_y = y + 4.0
    row2_y = row1_y + card_h + row_gap

    l1 = "Open w/ label (not done)" if tp else "Open w/ label customer_escalation (not done)"
    l2 = "Created in 90d" if tp else "Created in 90d (label customer_escalation)"
    l3 = "Resolved in 90d" if tp else "Resolved in 90d (label customer_escalation)"
    l4 = "TTR (median) — w/ label" if tp else "TTR (median) — w/ label customer_escalation"
    l5 = "TTR (median) — w/o label" if tp else "TTR (median) — w/o label customer_escalation"

    _kpi_metric_card(
        reqs, f"{sid}_k1", sid, MARGIN, row1_y, top_card_w, card_h,
        l1, f"{n_open}", accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k2", sid, MARGIN + top_card_w + col_gap, row1_y, top_card_w, card_h,
        l2, f"{n_90o}", accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k3", sid, MARGIN + 2 * (top_card_w + col_gap), row1_y, top_card_w, card_h,
        l3, f"{n_90c}", accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k4", sid, MARGIN, row2_y, bot_card_w, card_h,
        l4, t_esc.get("median", "—"), accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k5", sid, MARGIN + bot_card_w + col_gap, row2_y, bot_card_w, card_h,
        l5, t_not.get("median", "—"), accent=BLUE,
    )

    return idx + 1


def _support_help_orgs_by_opened_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """All-customers: rank JSM organizations by HELP tickets opened in the lookback window.

    Omitted from the slide plan for single-customer support runs (see create_health_deck).
    """
    blob = (report.get("jira") or {}).get("help_orgs_by_opened")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP organizations by opened (not in report — all-customers support deck only)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"HELP organizations by opened: {blob['error']}",
        )

    days = int(blob.get("days") or 90)
    all_rows: list[dict[str, Any]] = list(blob.get("by_organization") or [])
    total_issues = int(blob.get("total_issues") or 0)
    n_orgs = len(all_rows)

    entry = report.get("_current_slide") or {}
    base_title = entry.get("title") or f"HELP Tickets Opened by Organization (Last {days} Days)"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, _project_slide_bg("HELP"))
    _slide_title(reqs, sid, base_title)

    table_top = BODY_Y + 24
    row_h = 22.0
    max_data_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=BODY_BOTTOM,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=20,
    )
    display_rows = all_rows[:max_data_rows]
    n_shown = len(display_rows)
    if n_orgs > n_shown:
        orgs_text = f"showing top {n_shown} of {n_orgs} organizations by volume"
    else:
        orgs_text = f"{n_orgs} organization{'s' if n_orgs != 1 else ''}"

    sub = (
        f"HELP: tickets created in the last {days} days (≈3 months), by JSM organization  ·  "
        f"{total_issues} issues  ·  {orgs_text}"
    )
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)

    if not all_rows:
        em = f"No HELP tickets created in the last {days} days."
        _box(reqs, f"{sid}_em", sid, MARGIN, BODY_Y + 30, CONTENT_W, 40, em)
        _style(reqs, f"{sid}_em", 0, len(em), size=10, color=NAVY, font=FONT)
        return idx + 1

    headers = ["Organization", "Tickets opened"]
    col_widths = [400, 100]
    num_rows = 1 + len(display_rows)
    table_id = f"{sid}_tbl"

    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * row_h),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })

    def _ct_o(row, col, text):
        if not text:
            return
        reqs.append({
            "insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text),
                "insertionIndex": 0,
            }
        })

    def _cs_o(row, col, text_len, bold=False, size=9, align=None):
        if text_len > 0:
            s: dict[str, Any] = {
                "fontSize": {"magnitude": size, "unit": "PT"},
                "fontFamily": FONT,
            }
            f = ["fontSize", "fontFamily"]
            if bold:
                s["bold"] = True
                f.append("bold")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s, "fields": ",".join(f),
                }
            })
        if align:
            reqs.append({
                "updateParagraphStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align},
                    "fields": "alignment",
                }
            })

    _clean_table(reqs, table_id, num_rows, len(headers))

    for ci, h in enumerate(headers):
        _ct_o(0, ci, h)
        _cs_o(0, ci, len(h), bold=True, size=9, align="END" if ci == 1 else None)

    for ri, rowd in enumerate(display_rows):
        rj = ri + 1
        oname = (rowd.get("organization") or "—")[:64]
        cnt = int(rowd.get("count") or 0)
        cs = str(cnt)
        _ct_o(rj, 0, oname)
        _ct_o(rj, 1, cs)
        _cs_o(rj, 0, len(oname), size=9)
        _cs_o(rj, 1, len(cs), size=9, align="END")

    return idx + 1


def _lean_project_recent_closed_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Recently closed LEAN project tickets table."""
    jira = report.get("jira") or {}
    blob = jira.get("lean_project_recent")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "LEAN project recent tickets (not in report)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"LEAN project recent tickets: {blob['error']}",
        )

    return _project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "LEAN", closed=True)


def _lean_project_ticket_metrics_breakdown_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """LEAN ticket breakdown (pie charts)."""
    return _project_ticket_metrics_breakdown_slide(
        reqs,
        sid,
        report,
        idx,
        snap_key="lean_project_open_breakdown",
        project="LEAN",
        default_title="LEAN Ticket Metrics Breakdown",
    )


def _help_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """HELP tickets resolved by assignee - last 90 days."""
    jira = report.get("jira") or {}
    blob = jira.get("help_resolved_by_assignee")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP resolved tickets by assignee (not in report)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"HELP resolved tickets by assignee: {blob['error']}",
        )

    return _resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "HELP")


def _customer_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """CUSTOMER tickets resolved by assignee - last 90 days."""
    jira = report.get("jira") or {}
    blob = jira.get("customer_resolved_by_assignee")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "CUSTOMER resolved tickets by assignee (not in report)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"CUSTOMER resolved tickets by assignee: {blob['error']}",
        )

    return _resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "CUSTOMER")


def _lean_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """LEAN tickets resolved by assignee - last 90 days."""
    jira = report.get("jira") or {}
    blob = jira.get("lean_resolved_by_assignee")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "LEAN resolved tickets by assignee (not in report)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"LEAN resolved tickets by assignee: {blob['error']}",
        )

    return _resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "LEAN")


def _resolved_by_assignee_table_slide(
    reqs: list,
    sid: str,
    report: dict,
    idx: int,
    blob: dict,
    project: str,
) -> int:
    """Generic table slide for resolved tickets grouped by assignee."""
    jira = report.get("jira") or {}
    jira_base = (jira.get("base_url") or "").rstrip("/")
    assignees = blob.get("by_assignee") or []
    total_resolved = blob.get("total_resolved", 0)
    days = blob.get("days", 90)
    # Always use report customer as source of truth (blob may be from cache)
    customer = report.get("customer") or blob.get("customer") or "All Customers"

    entry = report.get("_current_slide") or {}
    base_title = entry.get("title") or f"{project} Tickets Resolved by Assignee"
    
    _slide(reqs, sid, idx)
    _bg(reqs, sid, _project_slide_bg(project))
    _slide_title(reqs, sid, base_title)

    if not assignees:
        _lead = _support_subtitle_matched_lead(report, customer)
        sub_empty = (
            f"{_lead}resolved in last {days} days  ·  {total_resolved} tickets  ·  0 assignees"
        )
        _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub_empty)
        _style(reqs, f"{sid}_sub", 0, len(sub_empty), size=9, color=GRAY, font=FONT)
        empty_msg = f"No {project} tickets resolved in the last {days} days."
        _box(reqs, f"{sid}_empty", sid, MARGIN, BODY_Y + 30, CONTENT_W, 40, empty_msg)
        _style(reqs, f"{sid}_empty", 0, len(empty_msg), size=10, color=NAVY, font=FONT)
        return idx + 1

    num_assignees = len(assignees)
    # Nominal row height for size math must match _table_rows_fit_span so we do not
    # claim "top 12" while Slides cell padding / clipping hides the last row. Use
    # ~22pt, not 18pt, so reserved height is closer to rendered table rows.
    table_top = BODY_Y + 24
    row_h = 22
    max_data_rows = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=BODY_BOTTOM,
        row_height_pt=row_h,
        reserved_table_rows=1,
        max_rows_cap=12,
    )
    display_assignees = assignees[:max_data_rows]
    n_shown = len(display_assignees)
    if num_assignees > n_shown:
        assignee_text = f"showing top {n_shown} of {num_assignees} assignees"
    else:
        assignee_text = f"{num_assignees} assignee{'s' if num_assignees != 1 else ''}"

    _lead = _support_subtitle_matched_lead(report, customer)
    sub = f"{_lead}resolved in last {days} days  ·  {total_resolved} tickets  ·  {assignee_text}"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)

    # Create narrower table - reduce white space
    headers = ["Assignee", "Resolved"]
    col_widths = [350, 100]  # Narrower than before (was 450, 134)
    ROW_H = row_h

    num_rows = 1 + len(display_assignees)
    table_id = f"{sid}_tbl"
    
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * ROW_H),
                "transform": _tf(MARGIN, table_top),
            },
            "rows": num_rows,
            "columns": len(headers),
        }
    })
    
    def _ct(row, col, text):
        if not text:
            return
        reqs.append({
            "insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text),
                "insertionIndex": 0,
            }
        })
    
    def _cs(row, col, text_len, bold=False, color=None, size=8, align=None, link=None):
        if text_len > 0:
            from typing import Any
            s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
            f = ["fontSize", "fontFamily"]
            if bold:
                s["bold"] = True
                f.append("bold")
            if color:
                s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
                f.append("foregroundColor")
            if link:
                s["link"] = {"url": link}
                f.append("link")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s,
                    "fields": ",".join(f),
                }
            })
        if align:
            reqs.append({
                "updateParagraphStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align},
                    "fields": "alignment",
                }
            })
    
    def _cbg(row, col, color):
        reqs.append({
            "updateTableCellProperties": {
                "objectId": table_id,
                "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                "fields": "tableCellBackgroundFill",
            }
        })
    
    _clean_table(reqs, table_id, num_rows, len(headers))
    
    # Header row
    for ci, h in enumerate(headers):
        _ct(0, ci, h)
        _cs(0, ci, len(h), bold=True, color=NAVY, size=9, align="END" if ci == 1 else None)
    
    # Data rows
    for ri, item in enumerate(display_assignees):
        row_idx = ri + 1
        
        assignee = item.get("assignee") or "—"
        count = item.get("count", 0)
        
        if len(assignee) > 80:
            assignee = assignee[:77] + "..."
        
        # Build JQL filter link for this assignee
        jql_link = None
        if jira_base and assignee != "—":
            import urllib.parse
            # JQL: project = {project} AND assignee = "{assignee}" AND resolved >= -{days}d
            jql = f'project = {project} AND assignee = "{assignee}" AND resolved >= -{days}d'
            jql_link = f"{jira_base}/issues/?jql={urllib.parse.quote(jql)}"
        
        vals = [assignee, str(count)]
        
        for ci, v in enumerate(vals):
            _ct(row_idx, ci, v)
            # Add link to count column (ci == 1) if we have a Jira base URL
            if ci == 1 and jql_link:
                _cs(row_idx, ci, len(v), size=9, color=BLUE, align="END", link=jql_link)
            else:
                _cs(row_idx, ci, len(v), size=9, align="END" if ci == 1 else None)
    
    # Subtitle already states "showing top N of M assignees" when truncated; do not
    # add a second line below the table (nominal row height != rendered height → overlap).
    return idx + 1


# ── Data Quality slide ──

_GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}   # #21a659
_RED   = {"red": 0.85, "green": 0.15, "blue": 0.15}    # #d92626


# ── CS Report slide builders ──

# ── New slides: SLA Health, Cross-Validation, Engineering Pipeline, Enhancement Requests ──


def _sla_health_slide(reqs, sid, report, idx):
    """SLA performance, sentiment distribution, and request type mix. Always appears; shows red banner when no data."""
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Jira support tickets and SLA metrics (no tickets in period or Jira unavailable)",
        )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Health & SLA")

    days = jira.get("days", 90)
    total = jira["total_issues"]
    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"

    header = f"{total} tickets  ·  {date_range}"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=12, color=GRAY, font=FONT)

    col_gap = 24
    left_x = MARGIN
    left_w = (CONTENT_W - col_gap) // 2
    right_x = MARGIN + left_w + col_gap
    right_w = CONTENT_W - left_w - col_gap
    body_top = BODY_Y + 26
    max_y = BODY_BOTTOM

    # ── LEFT: SLA gauges ──
    y = body_top

    sla_goal = {"ttfr": "48h", "ttr": "160h"}
    sla_label = {"ttfr": "First Response", "ttr": "Resolution"}
    for sla_key in ("ttfr", "ttr"):
        sla = jira.get(sla_key, {})
        measured = sla.get("measured", 0)
        if measured == 0:
            continue
        label = sla_label[sla_key]
        goal = sla_goal[sla_key]
        breached = sla.get("breached", 0)
        breach_pct = round(100 * breached / max(measured, 1))

        if breach_pct == 0:
            badge_color = {"red": 0.13, "green": 0.55, "blue": 0.13}
            badge_label = "On track"
        elif breach_pct <= 20:
            badge_color = {"red": 0.85, "green": 0.65, "blue": 0.0}
            badge_label = "Caution"
        else:
            badge_color = {"red": 0.85, "green": 0.15, "blue": 0.15}
            badge_label = "At risk"

        title_text = f"{label}  (goal: {goal})"
        _box(reqs, f"{sid}_{sla_key}_t", sid, left_x, y, left_w, 20, title_text)
        _style(reqs, f"{sid}_{sla_key}_t", 0, len(label), bold=True, size=13, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_{sla_key}_t", len(label), len(title_text), size=10, color=GRAY, font=FONT)
        y += 24

        _pill(reqs, f"{sid}_{sla_key}_b", sid, left_x, y, 88, 22, badge_label, badge_color, WHITE)

        breach_txt = "0 breaches" if breached == 0 else f"{breached} breach{'es' if breached != 1 else ''}"
        stats = f"Median {sla.get('median', '—')}  ·  Avg {sla.get('avg', '—')}  ·  {breach_txt} (of {measured} closed)"
        _box(reqs, f"{sid}_{sla_key}_s", sid, left_x + 96, y, left_w - 96, 22, stats)
        _style(reqs, f"{sid}_{sla_key}_s", 0, len(stats), size=11, color=NAVY, font=FONT)
        y += 26

        if sla.get("min") and sla.get("max"):
            range_text = f"Range {sla['min']} – {sla['max']}"
            if sla.get("waiting"):
                range_text += f"  ·  {sla['waiting']} open"
            _box(reqs, f"{sid}_{sla_key}_r", sid, left_x, y, left_w, 18, range_text)
            _style(reqs, f"{sid}_{sla_key}_r", 0, len(range_text), size=10, color=GRAY, font=FONT)
            y += 22

        y += 14

    # ── RIGHT: Sentiment + Request Type ──
    right_y = body_top

    sentiment = jira.get("by_sentiment", {})
    sentiment_clean = {k: v for k, v in sentiment.items() if k != "Unknown"}
    if sentiment_clean:
        sent_title = "Ticket sentiment"
        _box(reqs, f"{sid}_sent_t", sid, right_x, right_y, right_w, 20, sent_title)
        _style(reqs, f"{sid}_sent_t", 0, len(sent_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        color_map = {
            "Positive": {"red": 0.13, "green": 0.55, "blue": 0.13},
            "Neutral": {"red": 0.5, "green": 0.5, "blue": 0.5},
            "Negative": {"red": 0.85, "green": 0.15, "blue": 0.15},
        }
        sent_total = sum(sentiment_clean.values())
        for si, (name, count) in enumerate(sentiment_clean.items()):
            pct = round(100 * count / max(sent_total, 1))
            bar_w = max(int(pct * (right_w - 120) / 100), 6)
            fill = color_map.get(name, GRAY)
            _bar_rect(reqs, f"{sid}_sb{si}", sid, right_x, right_y, bar_w, 16, fill, outline=GRAY)
            label = f"{name}  {count} ({pct}%)"
            _box(reqs, f"{sid}_sl{si}", sid, right_x + bar_w + 8, right_y, right_w - bar_w - 8, 16, label)
            _style(reqs, f"{sid}_sl{si}", 0, len(label), size=11, color=NAVY, font=FONT)
            right_y += 22
        right_y += 14

    req_types = jira.get("by_request_type", {})
    if req_types:
        rt_title = "Request channels"
        _box(reqs, f"{sid}_rt_t", sid, right_x, right_y, right_w, 20, rt_title)
        _style(reqs, f"{sid}_rt_t", 0, len(rt_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        rt_lines = []
        for name, count in list(req_types.items())[:6]:
            rt_lines.append(f"{count:>3}  {name}")
        rt_text = "\n".join(rt_lines)
        rt_h = min(14 * len(rt_lines) + 6, max_y - right_y)
        _box(reqs, f"{sid}_rtl", sid, right_x, right_y, right_w, rt_h, rt_text)
        _style(reqs, f"{sid}_rtl", 0, len(rt_text), size=11, color=NAVY, font=FONT)

    return idx + 1


def _cross_validation_slide(reqs, sid, report, idx):
    """Pendo vs CS Report engagement comparison per site."""
    cs_ph = get_csr_section(report).get("platform_health") or {}
    pendo_sites = report.get("sites", [])

    cs_factories = cs_ph.get("factories", [])
    if not cs_factories and not pendo_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo sites and/or CS Report factories for comparison")

    engagement = report.get("engagement", {})
    pendo_rate = engagement.get("active_rate_7d")
    if pendo_rate is not None:
        pendo_rate = round(pendo_rate)

    header_parts = []
    if pendo_rate is not None:
        header_parts.append(f"Pendo 7-day active rate: {pendo_rate}%")
    cs_buyer_rates = [f["weekly_active_buyers_pct"] for f in cs_factories
                      if f.get("weekly_active_buyers_pct") is not None]
    if cs_buyer_rates:
        cs_avg = round(sum(cs_buyer_rates) / len(cs_buyer_rates))
        header_parts.append(f"CS Report avg active buyers: {cs_avg}%")
    if pendo_rate is not None and cs_buyer_rates:
        diff = abs(pendo_rate - cs_avg)
        if diff <= 15:
            header_parts.append("✓ Consistent")
        else:
            header_parts.append(f"⚠ {diff}pp gap")

    header = "  ·  ".join(header_parts) if header_parts else "Comparing Pendo usage with CS Report metrics"

    ROW_H = 20
    tbl_y = BODY_Y + 24
    max_rows = max(1, (BODY_BOTTOM - tbl_y) // ROW_H - 1)

    pendo_by_site: dict[str, dict] = {}
    for s in pendo_sites:
        name = s.get("sitename") or s.get("site_name", "")
        if name:
            pendo_by_site[name.lower()] = s

    rows: list[tuple[str, str, str, str, str]] = []
    for f in cs_factories:
        fname = f.get("factory_name", "")
        wab = f.get("weekly_active_buyers_pct")
        health = f.get("health_score")
        pendo_match = None
        for pname, ps in pendo_by_site.items():
            if fname.lower() in pname or pname in fname.lower():
                pendo_match = ps
                break

        p_users = str(pendo_match.get("total_visitors", "—")) if pendo_match else "—"
        p_events = _fmt_count(pendo_match.get("total_events", 0)) if pendo_match else "—"
        cs_wab = f"{wab:.0f}%" if wab is not None else "—"
        cs_health_str = f"{health:.0f}" if health is not None else "—"
        rows.append((fname[:22], p_users, p_events, cs_wab, cs_health_str))

    if not rows:
        _slide(reqs, sid, idx)
        _bg(reqs, sid, LIGHT)
        _slide_title(reqs, sid, "Data Cross-Validation")
        note = "No overlapping site data between Pendo and CS Report"
        _box(reqs, f"{sid}_none", sid, MARGIN, tbl_y, CONTENT_W, 30, note)
        _style(reqs, f"{sid}_none", 0, len(note), size=11, color=GRAY, font=FONT)
        return idx + 1

    row_chunks = _cap_chunk_list(
        [rows[i : i + max_rows] for i in range(0, len(rows), max_rows)]
    )
    cols = ["Site", "Pendo Users", "Pendo Events", "CS Active %", "CS Health"]
    col_widths = [150, 90, 100, 90, 90]
    num_cols = len(cols)
    oids: list[str] = []

    for pi, shown in enumerate(row_chunks):
        page_sid = f"{sid}_p{pi}" if len(row_chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        ttl = "Data Cross-Validation" if len(row_chunks) == 1 else f"Data Cross-Validation ({pi + 1} of {len(row_chunks)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

        num_rows = len(shown) + 1
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": {"width": {"magnitude": sum(col_widths), "unit": "PT"},
                             "height": {"magnitude": ROW_H * num_rows, "unit": "PT"}},
                    "transform": _tf(MARGIN, tbl_y),
                },
                "rows": num_rows, "columns": num_cols,
            }
        })
        _clean_table(reqs, table_id, num_rows, num_cols)

        for ci, hdr in enumerate(cols):
            reqs.append({"insertText": {"objectId": table_id, "text": hdr,
                                        "cellLocation": {"tableId": table_id, "rowIndex": 0, "columnIndex": ci}}})
        for ri, row in enumerate(shown, 1):
            for ci, val in enumerate(row):
                reqs.append({"insertText": {"objectId": table_id, "text": val,
                                            "cellLocation": {"tableId": table_id, "rowIndex": ri, "columnIndex": ci}}})

    return idx + len(row_chunks), oids


def _engineering_slide(reqs, sid, report, idx):
    """Dedicated slide for engineering work affecting this customer, with GPT-written ticket narratives."""
    jira = report.get("jira", {})
    eng = jira.get("engineering", {})
    eng_open = eng.get("open", [])
    eng_closed = eng.get("recent_closed", [])
    jira_base = jira.get("base_url", "")

    if not eng_open and not eng_closed:
        return _missing_data_slide(reqs, sid, report, idx, "Jira engineering pipeline (in progress / shipped)")

    open_count = eng.get("open_count", len(eng_open))
    closed_count = eng.get("closed_count", len(eng_closed))
    header = f"{eng.get('total', open_count + closed_count)} engineering tickets  ·  {open_count} open  ·  {closed_count} closed"

    TICKET_H = 58
    y0 = BODY_Y + 24
    max_y = BODY_BOTTOM
    per_page = max(1, (max_y - y0 - 24) // TICKET_H)
    seq: list[tuple[str, dict]] = [("o", t) for t in eng_open] + [("c", t) for t in eng_closed]
    pages: list[list[tuple[str, dict]]] = []
    cur: list[tuple[str, dict]] = []
    for item in seq:
        cur.append(item)
        if len(cur) >= per_page:
            pages.append(cur)
            cur = []
    if cur:
        pages.append(cur)
    pages = _cap_chunk_list(pages)

    GREEN = {"red": 0.13, "green": 0.55, "blue": 0.13}
    oids: list[str] = []

    def _render_ticket(page_sid: str, ticket: dict, label_color: dict, prefix: str, counter: int, y_ref: list[float]) -> None:
        y = y_ref[0]
        key = ticket["key"]
        status = (ticket.get("status") or "")[:14]
        assignee = ticket.get("assignee") or "unassigned"
        updated = ticket.get("updated", "")
        summary = ticket.get("summary", "")[:52]
        key_line = f"{key}  {status:14s}  {summary}  [{assignee}]"
        if updated:
            key_line += f"  ({updated})"
        _box(reqs, f"{page_sid}_{prefix}{counter}_k", page_sid, MARGIN, y, CONTENT_W, 14, key_line)
        _style(reqs, f"{page_sid}_{prefix}{counter}_k", 0, len(key_line), size=9, color=NAVY, font=FONT)
        ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
        _style(reqs, f"{page_sid}_{prefix}{counter}_k", 0, len(key), bold=True, size=9,
               color=label_color, font=MONO, link=ticket_url)
        y += 15
        narrative = (ticket.get("narrative") or "").strip()
        if narrative and y + 36 <= max_y:
            _box(reqs, f"{page_sid}_{prefix}{counter}_n", page_sid, MARGIN + 8, y, CONTENT_W - 8, 36, narrative)
            _style(reqs, f"{page_sid}_{prefix}{counter}_n", 0, len(narrative), size=8, color=GRAY, font=FONT)
            y += 38
        y += 6
        y_ref[0] = y

    for pi, page_items in enumerate(pages):
        page_sid = f"{sid}_p{pi}" if len(pages) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        ttl = "Engineering Pipeline" if len(pages) == 1 else f"Engineering Pipeline ({pi + 1} of {len(pages)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)
        y_ref = [y0]
        last_kind: str | None = None
        for j, (kind, t) in enumerate(page_items):
            if kind == "o" and last_kind != "o":
                open_title = f"In Progress ({open_count})"
                _box(reqs, f"{page_sid}_ot{j}", page_sid, MARGIN, y_ref[0], CONTENT_W, 16, open_title)
                _style(reqs, f"{page_sid}_ot{j}", 0, len(open_title), bold=True, size=11, color=BLUE, font=FONT)
                y_ref[0] += 20
                last_kind = "o"
            elif kind == "c" and last_kind != "c":
                closed_title = f"Recently Shipped ({closed_count})"
                _box(reqs, f"{page_sid}_ct{j}", page_sid, MARGIN, y_ref[0], CONTENT_W, 16, closed_title)
                _style(reqs, f"{page_sid}_ct{j}", 0, len(closed_title), bold=True, size=11, color=GREEN, font=FONT)
                y_ref[0] += 20
                last_kind = "c"
            col = BLUE if kind == "o" else GREEN
            pref = "o" if kind == "o" else "c"
            _render_ticket(page_sid, t, col, pref, pi * 200 + j, y_ref)

    return idx + len(pages), oids


def _enhancement_requests_slide(reqs, sid, report, idx):
    """Customer enhancement requests from the ER project."""
    jira = report.get("jira", {})
    er = jira.get("enhancements", {})
    er_open = er.get("open", [])
    er_shipped = er.get("shipped", [])
    er_declined = er.get("declined", [])
    jira_base = jira.get("base_url", "")

    if not er_open and not er_shipped and not er_declined:
        return _missing_data_slide(reqs, sid, report, idx, "Jira enhancement requests (open / shipped / declined)")

    open_n = er.get("open_count", len(er_open))
    shipped_n = er.get("shipped_count", len(er_shipped))
    declined_n = er.get("declined_count", len(er_declined))
    total = er.get("total", open_n + shipped_n + declined_n)
    header = f"{total} enhancement requests  ·  {open_n} open  ·  {shipped_n} shipped  ·  {declined_n} declined"

    body_top = BODY_Y + 24
    max_y = BODY_BOTTOM
    budget = max_y - body_top
    SEC_TITLE = 20
    ROW_OS = 28
    ROW_DEC = 16
    ER_GREEN = {"red": 0.13, "green": 0.55, "blue": 0.13}

    seq: list[tuple[str, dict]] = (
        [("o", t) for t in er_open] + [("s", t) for t in er_shipped] + [("d", t) for t in er_declined]
    )

    def _row_h(kind: str) -> int:
        return ROW_OS if kind in ("o", "s") else ROW_DEC

    pages: list[list[tuple[str, dict]]] = []
    page: list[tuple[str, dict]] = []
    used = 0
    last_section: str | None = None

    for kind, t in seq:
        row_h = _row_h(kind)
        extra = SEC_TITLE if last_section != kind else 0
        if page and used + extra + row_h > budget:
            pages.append(page)
            page = []
            used = 0
            last_section = None
        if last_section != kind:
            used += SEC_TITLE
            last_section = kind
        page.append((kind, t))
        used += row_h
    if page:
        pages.append(page)
    pages = _cap_chunk_list(pages)

    oids: list[str] = []
    for pi, page_items in enumerate(pages):
        page_sid = f"{sid}_p{pi}" if len(pages) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        ttl = "Enhancement Requests" if len(pages) == 1 else f"Enhancement Requests ({pi + 1} of {len(pages)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

        y = body_top
        last_kind: str | None = None
        for j, (kind, ticket) in enumerate(page_items):
            if last_kind != kind:
                if kind == "o":
                    sec = f"Open ({open_n})"
                    _box(reqs, f"{page_sid}_ot{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_ot{j}", 0, len(sec), bold=True, size=10, color=BLUE, font=FONT)
                elif kind == "s":
                    sec = f"Shipped ({shipped_n})"
                    _box(reqs, f"{page_sid}_st{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_st{j}", 0, len(sec), bold=True, size=10, color=ER_GREEN, font=FONT)
                else:
                    sec = f"Declined / Deferred ({declined_n})"
                    _box(reqs, f"{page_sid}_dt{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_dt{j}", 0, len(sec), bold=True, size=10, color=GRAY, font=FONT)
                y += SEC_TITLE
                last_kind = kind

            key = ticket["key"]
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            if kind == "o":
                prio = ticket.get("priority", "")
                prio_short = prio.split(":")[0] if ":" in prio else prio[:8]
                line1 = f"{key}  {prio_short}"
                line2 = (ticket.get("summary") or "")[:72]
                text = f"{line1}\n{line2}"
                oid = f"{page_sid}_eo{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 26, text)
                _style(reqs, oid, 0, len(text), size=8, color=NAVY, font=FONT)
                _style(reqs, oid, 0, len(key), bold=True, size=8, color=BLUE, font=MONO, link=ticket_url)
                y += ROW_OS
            elif kind == "s":
                line1 = f"{key}  ({ticket.get('updated', '')})"
                line2 = (ticket.get("summary") or "")[:72]
                text = f"{line1}\n{line2}"
                oid = f"{page_sid}_es{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 26, text)
                _style(reqs, oid, 0, len(text), size=8, color=NAVY, font=FONT)
                _style(reqs, oid, 0, len(key), bold=True, size=8, color=ER_GREEN, font=MONO, link=ticket_url)
                y += ROW_OS
            else:
                line = f"{key}  {(ticket.get('summary') or '')[:80]}"
                oid = f"{page_sid}_ed{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 14, line)
                _style(reqs, oid, 0, len(line), size=8, color=GRAY, font=MONO)
                if ticket_url:
                    _style(reqs, oid, 0, len(key), size=8, color=GRAY, font=MONO, link=ticket_url)
                y += ROW_DEC

    return idx + len(pages), oids


def _support_breakdown_slide(reqs, sid, report, idx):
    """Engineering-focused support breakdown: weekly trend, TTFR/TTR deep-dive, priority/type table, escalations."""
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Breakdown")

    days = jira.get("days", 90)
    total = jira["total_issues"]
    open_n = jira["open_issues"]
    resolved = jira["resolved_issues"]
    esc = jira["escalated"]
    bugs = jira["open_bugs"]
    jira_base = jira.get("base_url", "")

    from datetime import date, timedelta
    end_d = date.today()
    start_d = end_d - timedelta(days=days)
    date_range = f"{start_d.strftime('%b %-d')} – {end_d.strftime('%b %-d, %Y')}  ({days}d)"

    # ── Top stat bar ──
    stats_text = (
        f"Total: {total}   |   Open: {open_n}   |   Resolved: {resolved}"
        f"   |   Escalated: {esc}   |   Open bugs: {bugs}   |   {date_range}"
    )
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, stats_text)
    _style(reqs, f"{sid}_bar", 0, len(stats_text), size=9, color=GRAY, font=FONT)
    # Bold the numbers
    for label in (f"Total: {total}", f"Open: {open_n}", f"Resolved: {resolved}",
                  f"Escalated: {esc}", f"Open bugs: {bugs}"):
        pos = stats_text.find(label)
        if pos >= 0:
            _style(reqs, f"{sid}_bar", pos, pos + len(label), bold=True, color=NAVY)

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 2 // 3
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Weekly trend sparkline + SLA deep-dive ──
    left_y = body_top

    weeks = jira.get("tickets_over_time", [])
    if weeks:
        trend_title = "Weekly ticket volume"
        _box(reqs, f"{sid}_trt", sid, left_x, left_y, left_w, 16, trend_title)
        _style(reqs, f"{sid}_trt", 0, len(trend_title), bold=True, size=11, color=NAVY, font=FONT)
        left_y += 18

        # Sparkline: text bar chart using unicode blocks
        max_created = max((w["created"] for w in weeks), default=1) or 1
        BLOCKS = " ▁▂▃▄▅▆▇█"
        spark_parts = []
        label_parts = []
        for w in weeks[-12:]:  # last 12 weeks
            lvl = int(w["created"] / max_created * 8)
            spark_parts.append(BLOCKS[lvl])
            label_parts.append(w["label"])
        sparkline = "  ".join(spark_parts)
        _box(reqs, f"{sid}_spark", sid, left_x, left_y, left_w, 22, sparkline)
        _style(reqs, f"{sid}_spark", 0, len(sparkline), size=16, color=BLUE, font="Courier New")
        left_y += 24

        # Labels under sparkline
        label_text = "   ".join(w["label"] for w in weeks[-12:])
        _box(reqs, f"{sid}_sparklbl", sid, left_x, left_y, left_w, 12, label_text)
        _style(reqs, f"{sid}_sparklbl", 0, len(label_text), size=7, color=GRAY, font=FONT)
        left_y += 18
    else:
        left_y += 4

    # SLA section
    sla_goal = {"ttfr": "48h", "ttr": "160h"}
    sla_label_map = {"ttfr": "First Response (TTFR)", "ttr": "Resolution (TTR)"}
    for sla_key in ("ttfr", "ttr"):
        sla = jira.get(sla_key, {})
        if sla.get("measured", 0) == 0:
            continue
        label = sla_label_map[sla_key]
        goal = sla_goal[sla_key]
        breached = sla.get("breached", 0)
        measured = sla.get("measured", 1)
        breach_pct = round(100 * breached / max(measured, 1))

        if breach_pct == 0:
            b_color: dict = {"red": 0.13, "green": 0.55, "blue": 0.13}
            b_label = "On track"
        elif breach_pct <= 20:
            b_color = {"red": 0.85, "green": 0.65, "blue": 0.0}
            b_label = f"Caution ({breach_pct}%)"
        else:
            b_color = {"red": 0.85, "green": 0.15, "blue": 0.15}
            b_label = f"At risk ({breach_pct}%)"

        title_text = f"{label}  ·  goal {goal}"
        _box(reqs, f"{sid}_{sla_key}_t", sid, left_x, left_y, left_w, 18, title_text)
        _style(reqs, f"{sid}_{sla_key}_t", 0, len(label), bold=True, size=12, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_{sla_key}_t", len(label), len(title_text), size=9, color=GRAY, font=FONT)
        left_y += 20
        _pill(reqs, f"{sid}_{sla_key}_pill", sid, left_x, left_y, 110, 20, b_label, b_color, WHITE)
        stats_sla = (
            f"Median {sla.get('median', '—')}  ·  Avg {sla.get('avg', '—')}"
            f"  ·  Range {sla.get('min', '—')}–{sla.get('max', '—')}"
            f"  ·  {breached} breaches of {measured}"
        )
        if sla.get("waiting"):
            stats_sla += f"  ·  {sla['waiting']} open"
        _box(reqs, f"{sid}_{sla_key}_st", sid, left_x + 118, left_y, left_w - 120, 20, stats_sla)
        _style(reqs, f"{sid}_{sla_key}_st", 0, len(stats_sla), size=9, color=NAVY, font=FONT)
        left_y += 26

    # ── RIGHT: Priority + Type + Escalations ──
    right_y = body_top

    prio_short = {
        "Blocker: The platform is completely down": "Blocker",
        "Critical: Significant operational impact": "Critical",
        "Major: Workaround available, not essential": "Major",
        "Minor: Impairs non-essential functionality": "Minor",
    }
    prio_items = list(jira.get("by_priority", {}).items())[:6]
    if prio_items:
        ph = "By Priority"
        _box(reqs, f"{sid}_prt", sid, right_x, right_y, right_w, 16, ph)
        _style(reqs, f"{sid}_prt", 0, len(ph), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        for pi, (p, c) in enumerate(prio_items):
            line = f"{c:>4}  {prio_short.get(p, p[:22])}"
            _box(reqs, f"{sid}_pr{pi}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_pr{pi}", 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, f"{sid}_pr{pi}", 0, len(f"{c:>4}"), bold=True, color=BLUE)
            right_y += 14
        right_y += 8

    type_items = list(jira.get("by_type", {}).items())[:6]
    if type_items:
        th = "By Type"
        _box(reqs, f"{sid}_tyt", sid, right_x, right_y, right_w, 16, th)
        _style(reqs, f"{sid}_tyt", 0, len(th), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        for ti, (tp, c) in enumerate(type_items):
            line = f"{c:>4}  {tp[:22]}"
            _box(reqs, f"{sid}_ty{ti}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_ty{ti}", 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, f"{sid}_ty{ti}", 0, len(f"{c:>4}"), bold=True, color=BLUE)
            right_y += 14
        right_y += 8

    esc_issues = jira.get("escalated_issues", [])
    if esc_issues and right_y + 40 < BODY_BOTTOM:
        eh = "Escalated Tickets"
        _box(reqs, f"{sid}_esct", sid, right_x, right_y, right_w, 16, eh)
        _style(reqs, f"{sid}_esct", 0, len(eh), bold=True, size=11, color=_RED, font=FONT)
        right_y += 18
        for ei, esc_i in enumerate(esc_issues[:4]):
            if right_y + 14 > BODY_BOTTOM:
                break
            key = esc_i["key"]
            summary = esc_i.get("summary", "")[:28]
            line = f"{key}  {summary}"
            link = f"{jira_base}/browse/{key}" if jira_base else None
            _box(reqs, f"{sid}_esc{ei}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_esc{ei}", 0, len(key), bold=True, size=9, color=_RED, font=MONO,
                   link=link)
            _style(reqs, f"{sid}_esc{ei}", len(key) + 2, len(line), size=9, color=NAVY, font=FONT)
            right_y += 14

    return idx + 1


# ── Engineering Portfolio Slides ──────────────────────────────────────────────


# ── Composable API (agent builds deck slide by slide) ──

# Maps slide type names to builder functions and the report keys they require
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
    "data_quality": _data_quality_slide,
    "portfolio_title": _portfolio_title_slide,
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


def _get_deck_output_folder() -> str | None:
    """Return the base QBR Generator folder ID for individual deck outputs."""
    from .drive_config import get_deck_output_folder_id

    return get_deck_output_folder_id()


def create_empty_deck(customer: str, days: int = 30, deck_name: str | None = None) -> dict[str, Any]:
    """Create an empty presentation. Returns {deck_id, url} for use with add_slide."""
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    label = deck_name or "Usage Health Review"
    title = f"{customer} — {label} ({_date_range(days)})"
    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]
            
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Drive operations
            f = drive_service.files().create(body=file_meta).execute()
        finally:
            socket.setdefaulttimeout(old_timeout)
            
        deck_id = f["id"]
        logger.info("Created deck %s: %s", deck_id, title)
    except HttpError as e:
        return {"error": str(e)}

    # Delete the default blank slide
    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Slides API
            pres = slides_service.presentations().get(presentationId=deck_id).execute()
            default_id = pres["slides"][0]["objectId"]
            slides_presentations_batch_update(
                slides_service,
                deck_id,
                [{"deleteObject": {"objectId": default_id}}],
            )
        finally:
            socket.setdefaulttimeout(old_timeout)
    except Exception:
        pass

    return {
        "deck_id": deck_id,
        "url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
    }


_slide_counter: dict[str, int] = {}


def add_slide(deck_id: str, slide_type: str, data: dict[str, Any]) -> dict[str, Any]:
    """Add one slide to an existing deck.

    Args:
        deck_id: Presentation ID from create_empty_deck.
        slide_type: One of: title, health, engagement, sites, features, champions, benchmarks, exports, depth, kei, guides, custom, signals.
        data: Dict with the keys required for that slide type (see SLIDE_DATA_REQUIREMENTS).

    Returns:
        {slide_type, status} or {error}.
    """
    builder = _SLIDE_BUILDERS.get(slide_type)
    if not builder:
        return {"error": f"Unknown slide type '{slide_type}'. Valid: {', '.join(_SLIDE_BUILDERS)}"}

    try:
        slides_service, _ds, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Use local counter as insertion index to avoid an API round-trip per slide
    count = _slide_counter.get(deck_id, 0)
    _slide_counter[deck_id] = count + 1
    idx = count
    sid = _slide_object_id_base(slide_type, count)

    reqs: list[dict] = []
    try:
        ret = builder(reqs, sid, data, idx)
        new_idx, note_ids = _normalize_builder_return(ret, sid)
    except (KeyError, TypeError, IndexError) as e:
        required = SLIDE_DATA_REQUIREMENTS.get(slide_type, [])
        return {
            "error": f"Slide '{slide_type}' data is missing required key: {e}. Required keys: {required}",
            "slide_type": slide_type,
        }

    if not reqs:
        return {"slide_type": slide_type, "status": "skipped (no data)"}

    try:
        presentations_batch_update_chunked(slides_service, deck_id, reqs)
    except HttpError as e:
        return {"error": str(e), "slide_type": slide_type}

    note_entry = {
        "id": slide_type,
        "slide_type": slide_type,
        "title": data.get("title", slide_type.replace("_", " ").title()),
    }
    note_payload = dict(data)
    note_payload["_current_slide"] = note_entry
    notes = _build_slide_jql_speaker_notes(note_payload, note_entry)
    if note_ids:
        n = set_speaker_notes_batch(slides_service, deck_id, [(nid, notes) for nid in note_ids])
        if n < len(note_ids):
            logger.warning("Could not write JQL speaker notes for %d/%d slides in deck %s", len(note_ids) - n, len(note_ids), deck_id[:12])

    return {"slide_type": slide_type, "status": "added", "position": idx + 1, "pages": len(note_ids)}


# ── Monolith deck creation (deck-definition-driven) ──

def create_health_deck(
    report: dict[str, Any],
    deck_id: str = "cs_health_review",
    thumbnails: bool = True,
    output_folder_id: str | None = None,
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
        thumbnails: Whether to export slide thumbnails. Disable for batch runs.
        output_folder_id: Optional Drive folder id for the new presentation. When omitted,
            uses ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (if configured).
    """
    if "error" in report:
        return {"error": report["error"]}

    is_portfolio = report.get("type") == "portfolio"
    # Preserve None for "all customers" case; only default to "Portfolio" for actual portfolio reports
    if is_portfolio:
        customer = "Portfolio"
    else:
        customer = report.get("customer")  # Can be None for "all customers"
    days = report.get("days", 30)
    quarter_label = report.get("quarter")

    from .qa import qa
    qa.begin(customer)

    try:
        slides_service, drive_service, sheets_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Make services accessible to slide builders via the report dict
    report["_slides_svc"] = slides_service
    report["_drive_svc"] = drive_service

    from .deck_loader import resolve_deck

    # resolve_deck loads only slide YAMLs referenced by this deck (not the full slides/ catalog).
    resolved = resolve_deck(deck_id, customer)
    if resolved.get("error"):
        return {"error": resolved["error"]}

    deck_name = resolved.get("name", "Health Review")
    date_str = _date_range(days, quarter_label, report.get("quarter_start"), report.get("quarter_end"))
    
    slide_plan: list[dict[str, Any]] = list(resolved.get("slides") or [])
    
    # For support deck without customer, include full support slide lineup with all-project scope.
    if deck_id == "support" and not customer:
        title = f"{deck_name} — All Customers ({date_str})"
    elif is_portfolio:
        title = f"{deck_name} ({date_str})"
    else:
        title = f"{customer} — {deck_name} ({date_str})"

    if deck_id == "supply_chain_review":
        from datetime import datetime, timezone

        report["support_deck_generated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )

    if deck_id == "support":
        # Titles: canonical text lives in `decks/support.yaml` (and any synced Drive copy).
        # For scoping + UI (corner badge, sublines) only — do not embed customer in titles here.
        if not customer:
            # Avoid "All Customers CUSTOMER …" (Jira project + audience phrasing clash).
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
            # All-customers-only: organization ranking table (not meaningful for a single account).
            slide_plan = [
                e for e in slide_plan
                if e.get("slide_type") != "support_help_orgs_by_opened"
            ]
        else:
            report.pop("support_deck_scoped_titles", None)

        from datetime import datetime, timezone

        report["support_deck_generated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        # Cover slide is configured in decks/support.yaml + slides/support-deck-cover.yaml, not here.

    if deck_id == "salesforce_comprehensive":
        from .data_source_health import _salesforce_configured

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
            try:
                from .salesforce_client import SalesforceClient

                report["salesforce_comprehensive"] = SalesforceClient().get_customer_salesforce_comprehensive(
                    customer
                )
            except Exception as e:
                logger.warning("Salesforce comprehensive fetch failed: %s", e)
                report["salesforce_comprehensive"] = {
                    **empty_sf,
                    "error": str(e)[:500],
                }
        else:
            report["salesforce_comprehensive"] = {**empty_sf, "error": "Salesforce not configured"}

        slide_plan = _filter_salesforce_comprehensive_slide_plan(
            slide_plan, report.get("salesforce_comprehensive") or {}
        )

    if deck_id == "support":
        # Set display name for logging
        customer_display = "All Customers" if not customer else customer
            
        try:
            from .jira_client import get_shared_jira_client

            jira_client = get_shared_jira_client()
            
            # Initialize jira dict with base_url
            if "jira" not in report:
                report["jira"] = {}
            
            if "base_url" not in report["jira"]:
                report["jira"]["base_url"] = (jira_client.base_url or "").rstrip("/")
            
            # Fetch customer ticket metrics (works with None for all customers)
            if "customer_ticket_metrics" not in report["jira"]:
                logger.info("Support deck: fetching customer ticket metrics for %s", customer_display)
                customer_ticket_metrics = jira_client.get_customer_ticket_metrics(customer)
                report["jira"]["customer_ticket_metrics"] = customer_ticket_metrics

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

            # Fetch recent HELP tickets (works with None for all customers)
            logger.info("Support deck: fetching recent HELP tickets for %s", customer_display)
            customer_help_recent = jira_client.get_customer_help_recent_tickets(
                customer,
                opened_within_days=None,
                closed_within_days=None,
                max_each=200,
            )
            report["jira"]["customer_help_recent"] = customer_help_recent
            
            # Fetch resolved tickets by assignee for HELP (works with None for all customers)
            logger.info("Support deck: fetching HELP resolved tickets by assignee for %s", customer_display)
            help_resolved_by_assignee = jira_client.get_resolved_tickets_by_assignee(
                "HELP",
                customer,
                days=90,
            )
            report["jira"]["help_resolved_by_assignee"] = help_resolved_by_assignee
            
            # Fetch recent CUSTOMER project tickets (customer-scoped or all-project scope)
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

            # Fetch recent LEAN project tickets (customer-scoped or all-project scope)
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

            # Fetch resolved tickets by assignee for CUSTOMER (last 90 days)
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
            logger.warning("Support deck: Jira data fetch failed for %s: %s", customer, e)
            if "jira" not in report:
                report["jira"] = {}
            if "customer_ticket_metrics" not in report["jira"]:
                report["jira"]["customer_ticket_metrics"] = {
                    "error": str(e)[:500],
                    "customer": customer,
                }
            report["jira"]["customer_help_recent"] = {
                "error": str(e)[:500],
                "customer": customer,
                "recently_opened": [],
                "recently_closed": [],
            }
            report["jira"]["customer_project_recent"] = {
                "error": str(e)[:500],
                "project": "CUSTOMER",
                "customer": customer,
                "recently_opened": [],
                "recently_closed": [],
            }
            report["jira"]["lean_project_recent"] = {
                "error": str(e)[:500],
                "project": "LEAN",
                "customer": customer,
                "recently_opened": [],
                "recently_closed": [],
            }
            report["jira"]["customer_project_open_breakdown"] = {
                "error": str(e)[:500],
                "project": "CUSTOMER",
                "customer": customer,
                "unresolved_count": 0,
                "by_type_open": {},
                "by_status_open": {},
            }
            report["jira"]["lean_project_open_breakdown"] = {
                "error": str(e)[:500],
                "project": "LEAN",
                "customer": customer,
                "unresolved_count": 0,
                "by_type_open": {},
                "by_status_open": {},
            }
            report["jira"]["help_resolved_by_assignee"] = {
                "error": str(e)[:500],
                "project": "HELP",
                "customer": customer,
                "by_assignee": [],
                "total_resolved": 0,
            }
            report["jira"]["customer_resolved_by_assignee"] = {
                "error": str(e)[:500],
                "project": "CUSTOMER",
                "customer": customer,
                "by_assignee": [],
                "total_resolved": 0,
            }
            report["jira"]["lean_resolved_by_assignee"] = {
                "error": str(e)[:500],
                "project": "LEAN",
                "customer": customer,
                "by_assignee": [],
                "total_resolved": 0,
            }
            report["jira"]["customer_project_volume_trends"] = {
                "error": str(e)[:500],
                "all": [],
                "escalated": [],
                "non_escalated": [],
            }
            report["jira"]["lean_project_volume_trends"] = {
                "error": str(e)[:500],
                "all": [],
                "escalated": [],
                "non_escalated": [],
            }
            report["jira"]["customer_project_ticket_metrics"] = {
                "error": str(e)[:500],
                "project": "CUSTOMER",
                "customer": customer,
            }
            report["jira"]["lean_project_ticket_metrics"] = {
                "error": str(e)[:500],
                "project": "LEAN",
                "customer": customer,
            }
            report["jira"]["help_orgs_by_opened"] = {
                "error": str(e)[:500],
                "by_organization": [],
                "total_issues": 0,
                "days": 90,
            }
            report["jira"]["help_customer_escalations"] = {
                "error": str(e)[:500],
                "customer": customer,
                "tickets": [],
            }
            report["jira"]["help_escalation_metrics"] = {
                "error": str(e)[:500],
                "customer": customer,
                "not_done_escalation_count": 0,
                "escalations_opened_90d": 0,
                "escalations_closed_90d": 0,
            }

        hem_post = (report.get("jira") or {}).get("help_escalation_metrics")
        if isinstance(hem_post, dict) and not hem_post.get("error"):
            try:
                from .support_notable_llm import generate_help_escalation_nature_quote_llm

                enq = generate_help_escalation_nature_quote_llm(report)
                if enq:
                    hem_post["llm_nature_summary"] = enq
            except Exception as e:
                logger.warning("Support deck: escalation nature quote LLM failed: %s", e)

    # Material Shortage slides: QBR run_qbr_from_template() calls enrich_qbr_with_shortage_trends,
    # but standalone create_health_deck (e.g. supply_chain_review) only had get_customer_health_report
    # and never loaded LeanDNA. Fetch here when the deck plan includes those slides.
    if (
        customer
        and slide_plan
        and "leandna_shortage_trends" not in report
        and _SLIDES_NEEDING_LEANDNA_SHORTAGE
        & {str((e or {}).get("slide_type") or (e or {}).get("id") or "") for e in slide_plan}
    ):
        try:
            from .leandna_shortage_enrich import enrich_qbr_with_shortage_trends

            report = enrich_qbr_with_shortage_trends(
                report, str(customer).strip(), weeks_forward=12
            )
        except Exception as e:
            logger.warning("create_health_deck: LeanDNA shortage enrichment failed: %s", e)
            report.setdefault(
                "leandna_shortage_trends",
                {"enabled": False, "reason": str(e)[:200]},
            )

    if not slide_plan:
        logger.error(
            "create_health_deck: empty slide plan (deck_id=%s customer=%r). "
            "Check decks/*.yaml vs slides/, Drive BPO/QBR Generator sync, and per-customer slide filters.",
            deck_id,
            customer,
        )
        return {
            "error": "Deck has no slides to generate (resolved plan is empty).",
            "hint": "Verify deck YAML slide IDs exist in slides/. If using Drive config, ensure "
            "BPO/QBR Generator decks/ and slides/ on Drive match the repo. Slides with customers: [...] exclude "
            "everyone except listed customers.",
            "customer": customer,
            "deck_id": deck_id,
        }

    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Drive operations
            
            file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
            output_folder = output_folder_id if output_folder_id else _get_deck_output_folder()
            if output_folder:
                file_meta["parents"] = [output_folder]
            file = drive_service.files().create(body=file_meta).execute()
            pres_id = file["id"]
            logger.info("Created presentation %s: %s", pres_id, title)
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        err_str = str(e)
        if "rate" in err_str.lower() or "quota" in err_str.lower():
            return {"error": f"Rate limit: {err_str}. Wait and retry."}
        return {"error": err_str}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return {"error": str(e), "hint": hint, "customer": customer, "deck_id": deck_id}
        raise

    # Provide a DeckCharts instance for Slides embeds backed by Google Sheets.
    from .charts import DeckCharts
    report["_charts"] = DeckCharts(title)

    report["_slide_plan"] = slide_plan

    # Build every slide except "Notable" on the first pass; fetches are already in ``report`` for support.
    # The Notable slide (cs_notable) is inserted in a second batch at insertionIndex 1 after the LLM runs on a digest
    # of the same in-memory Jira data (so we do not refetch; bullets reflect the same dataset as the rest of the deck).
    plan_work: list[dict[str, Any]] = list(slide_plan)
    notable_deferred: dict[str, Any] | None = None
    if deck_id == "support":
        kept2: list[dict[str, Any]] = []
        for e in plan_work:
            if (e.get("slide_type") or e.get("id", "")) == "cs_notable" and notable_deferred is None:
                notable_deferred = e
            else:
                kept2.append(e)
        plan_work = kept2

    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []
    if deck_id in ("support", "supply_chain_review") and customer:
        _set_support_deck_corner_customer(str(customer).strip())

    for entry in plan_work:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            logger.warning(
                "create_health_deck: no _SLIDE_BUILDERS entry for slide_type=%r (deck %s entry id=%r)",
                slide_type,
                deck_id,
                entry.get("id"),
            )
            continue
        report["_current_slide"] = entry
        sid = _slide_object_id_base(str(entry["id"]), idx)
        ret = builder(reqs, sid, report, idx)
        next_idx, note_ids = _normalize_builder_return(ret, sid)
        if slide_type == "cohort_profiles" and note_ids:
            blks = report.get("_cohort_profile_speaker_note_blocks") or []
            for i, nid in enumerate(note_ids):
                note_entry = dict(entry)
                if i < len(blks):
                    note_entry["_cohort_profile_block"] = blks[i]
                note_targets.append((nid, note_entry))
        else:
            for nid in note_ids:
                note_targets.append((nid, dict(entry)))
        idx = next_idx

    slides_created = idx - 1

    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Slides API
            pres = slides_service.presentations().get(presentationId=pres_id).execute()
        finally:
            socket.setdefaulttimeout(old_timeout)
            
        default_id = pres["slides"][0]["objectId"]
        if slides_created > 0:
            reqs.append({"deleteObject": {"objectId": default_id}})
        else:
            logger.error(
                "create_health_deck: built 0 slides (deck_id=%s customer=%r plan_len=%d). "
                "Leaving default slide; check warnings above for missing builders.",
                deck_id,
                customer,
                len(slide_plan),
            )
    except Exception:
        pass

    try:
        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(60.0)  # 60 second timeout for batchUpdate (can be large)
            presentations_batch_update_chunked(slides_service, pres_id, reqs)
        finally:
            socket.setdefaulttimeout(old_timeout)
    except HttpError as e:
        logger.exception("Failed to build slides")
        _set_support_deck_corner_customer(None)
        return {"error": str(e), "presentation_id": pres_id}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            _set_support_deck_corner_customer(None)
            return {"error": str(e), "hint": hint, "presentation_id": pres_id, "customer": customer, "deck_id": deck_id}
        raise

    if slides_created == 0:
        _set_support_deck_corner_customer(None)
        url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        return {
            "error": "No slides were built — every slide_type may be unknown or builders returned nothing.",
            "hint": "See logs for slide_type warnings. Compare slides/*.yaml slide_type to src/slides_client.py _SLIDE_BUILDERS.",
            "presentation_id": pres_id,
            "url": url,
            "customer": customer,
            "slides_created": 0,
        }

    if deck_id == "support" and notable_deferred and slides_created > 0:
        from .support_notable_llm import (
            NotableLlmError,
            build_support_review_digest,
            generate_notable_bullets_via_llm,
        )

        titles = [e.get("title", "") for e in plan_work]
        try:
            digest = build_support_review_digest(report, slide_titles=titles)
        except Exception as e:
            logger.warning("Notable: digest build failed; LLM may have thin context. %s", e)
            digest = {}
        ne = dict(notable_deferred)
        try:
            bullets, src = generate_notable_bullets_via_llm(digest, ne)
        except NotableLlmError as e:
            _set_support_deck_corner_customer(None)
            url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
            return {
                "error": str(e),
                "presentation_id": pres_id,
                "url": url,
                "customer": customer,
                "slides_created": slides_created,
                "deck_id": deck_id,
                "hint": "Notable slide was not added. The deck is otherwise complete. Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to insert generic bullets, or fix the Notable/LLM path and regenerate.",
            }
        ne["notable_items"] = bullets
        report["support_notable_bullets"] = bullets
        report["support_notable_bullets_source"] = src
        report["_current_slide"] = ne
        nreq: list[dict] = []
        nsid = "s_snb1"
        ret_n = _cs_notable_slide(nreq, nsid, report, 1)
        _nidx, n_note_ids = _normalize_builder_return(ret_n, nsid)
        del _nidx
        try:
            import socket
            o2 = socket.getdefaulttimeout()
            try:
                socket.setdefaulttimeout(60.0)
                presentations_batch_update_chunked(slides_service, pres_id, nreq)
            finally:
                socket.setdefaulttimeout(o2)
        except HttpError as e:
            logger.error("Notable: second batch (insert at index 1) failed: %s", e)
        else:
            slides_created += 1
            for nid in n_note_ids:
                note_targets.append((nid, ne))

    _set_support_deck_corner_customer(None)
    notes_items = [(sid, _build_slide_jql_speaker_notes(report, entry)) for sid, entry in note_targets]
    if notes_items:
        n = set_speaker_notes_batch(slides_service, pres_id, notes_items)
        logger.info("Speaker notes: wrote %d/%d slide notes in single batchUpdate", n, len(notes_items))

    result = {
        "presentation_id": pres_id,
        "url": f"https://docs.google.com/presentation/d/{pres_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }
    nsrc = report.get("support_notable_bullets_source")
    if nsrc:
        result["notable_bullets_source"] = nsrc

    if thumbnails:
        try:
            thumbs = export_slide_thumbnails(pres_id)
            result["thumbnails"] = [str(p) for p in thumbs]
            logger.info("Saved %d slide thumbnails for %s", len(thumbs), customer)
        except Exception as e:
            logger.warning("Thumbnail export failed: %s", e)

    return result


def create_portfolio_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
) -> dict[str, Any]:
    """Generate a single portfolio-level deck across all customers."""
    from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

    report = try_load_portfolio_snapshot_for_request(days, max_customers)
    if report is None:
        from .pendo_client import PendoClient

        client = PendoClient()
        report = client.get_portfolio_report(days=days, max_customers=max_customers)
    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    return create_health_deck(report, deck_id="portfolio_review")


def create_cohort_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
    thumbnails: bool = False,
    output_folder_id: str | None = None,
    portfolio_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Single deck: cohort buckets from cohorts.yaml + portfolio metrics (max 10 profile slides).

    If *portfolio_report* is supplied the expensive Pendo preload + customer
    iteration is skipped entirely — the caller already computed it.

    Otherwise, when the resolved snapshot folder (``GOOGLE_QBR_GENERATOR_FOLDER_ID`` /
    ``Cache`` (QBR generator subfolder) or ``BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID``) has a fresh JSON
    file (see ``pendo_portfolio_snapshot_drive``), it is used instead of calling Pendo.
    """
    if portfolio_report is not None:
        report = portfolio_report
    else:
        from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

        report = try_load_portfolio_snapshot_for_request(days, max_customers)
        if report is None:
            from .pendo_client import PendoClient

            client = PendoClient()
            report = client.get_portfolio_report(days=days, max_customers=max_customers)

    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    logger.info(
        "cohort_review: portfolio report ready (%d customers) — sending to Google Slides",
        report.get("customer_count", 0),
    )

    try:
        from .data_source_health import _salesforce_configured
        if _salesforce_configured():
            from .salesforce_client import SalesforceClient
            sf = SalesforceClient()
            digest = report.get("cohort_digest") or {}
            all_names: list[str] = []
            for block in digest.values():
                if isinstance(block, dict):
                    all_names.extend(block.get("customers") or [])
            if all_names:
                arr_map = sf.get_arr_by_customer_names(all_names)
                report["_arr_by_customer"] = arr_map
                logger.info("cohort_review: loaded ARR for %d/%d customers from Salesforce",
                            len(arr_map), len(all_names))

                active_names = sf.get_active_customer_names(all_names)
                churned = set(all_names) - active_names
                if churned:
                    logger.info("cohort_review: filtering %d churned customer(s) from cohort slides", len(churned))
                    from .pendo_client import compute_cohort_portfolio_rollup
                    customers = report.get("customers") or []
                    active_summaries = [s for s in customers if s.get("customer") not in churned]
                    new_digest, new_findings = compute_cohort_portfolio_rollup(active_summaries)
                    report["cohort_digest"] = new_digest
                    report["cohort_findings_bullets"] = new_findings
                    report["customer_count"] = len(active_summaries)
                    report["_churned_customers"] = sorted(churned)
    except Exception as e:
        logger.warning("cohort_review: Salesforce ARR lookup failed (continuing without): %s", e)

    return create_health_deck(
        report,
        deck_id="cohort_review",
        thumbnails=thumbnails,
        output_folder_id=output_folder_id,
    )


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    deck_id: str = "cs_health_review",
    workers: int = 4,
    thumbnails: bool = False,
    quarter: "QuarterRange | None" = None,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a deck definition (parallel).

    Args:
        customer_names: List of customer names to generate decks for.
        days: Lookback window in days.
        max_customers: Cap on how many to generate.
        deck_id: Which deck definition to use (default: cs_health_review).
        workers: Concurrent deck-creation threads (default 4).
        thumbnails: Export slide thumbnails (default False for batch — saves API quota).
        quarter: Optional QuarterRange to label slides with quarter info.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names
    quarter_label = quarter.label if quarter else None
    quarter_start = quarter.start.isoformat() if quarter else None
    quarter_end = quarter.end.isoformat() if quarter else None

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.debug("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, deck_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            if quarter_label:
                report["quarter"] = quarter_label
                report["quarter_start"] = quarter_start
                report["quarter_end"] = quarter_end
            return create_health_deck(report, deck_id=deck_id, thumbnails=thumbnails)
        except Exception as e:
            return {"error": str(e), "customer": name}

    results: list[dict[str, Any]] = [{}] * len(customers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_build_one, (i, n)): i for i, n in enumerate(customers)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"error": str(e), "customer": customers[idx]}
            r = results[idx]
            if "error" in r and "403" in str(r.get("error", "")):
                logger.error("Got 403 for %s — cancelling remaining.", customers[idx])
                for f in futures:
                    f.cancel()
                break

    return results


# ── Legacy (backward compat) ──

def create_deck_for_customer(customer, sites, days=30):
    if not sites:
        return {"error": f"No sites for '{customer}'"}
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}
    title = f"{customer} - Usage Report ({_date_range(days)})"
    try:
        meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            meta["parents"] = [output_folder]
        f = drive_service.files().create(body=meta).execute()
        pid = f["id"]
    except HttpError as e:
        return {"error": str(e)}
    r = []
    ix = 1
    for i, s in enumerate(sites):
        sid = f"ls_{i}"
        r.append({"createSlide": {"objectId": sid, "insertionIndex": ix}}); ix += 1
        _box(r, f"lt_{i}", sid, 60, 40, 600, 50, s.get("sitename", "?"))
        body = f"Page views: {s.get('page_views',0)}\nFeature clicks: {s.get('feature_clicks',0)}\nEvents: {s.get('total_events',0)}\nMinutes: {s.get('total_minutes',0)}"
        _box(r, f"lb_{i}", sid, 60, 100, 600, 280, body)
    try:
        presentations_batch_update_chunked(slides_service, pid, r)
    except HttpError as e:
        return {"error": str(e), "presentation_id": pid}
    return {"presentation_id": pid, "url": f"https://docs.google.com/presentation/d/{pid}/edit", "customer": customer, "slides_created": len(sites)}


def create_decks_for_all_customers(by_customer, customer_list, days=30, delay_seconds=2.0, max_customers=None):
    cs = customer_list[:max_customers] if max_customers else customer_list
    results = []
    for i, c in enumerate(cs):
        if i > 0:
            time.sleep(delay_seconds)
        results.append(create_deck_for_customer(c, by_customer.get(c, []), days))
        if "error" in results[-1] and "403" in str(results[-1].get("error", "")):
            results.append({"error": "Stopped: 403.", "customers_attempted": i + 1}); break
    return results


# ── Slide thumbnail export ──

def export_slide_thumbnails(
    presentation_id: str,
    output_dir: str | Path | None = None,
    size: str = "LARGE",
) -> list[Path]:
    """Download PNG thumbnails for every slide in a presentation.

    Args:
        presentation_id: Google Slides presentation ID or full URL.
        output_dir: Where to save PNGs. Defaults to a temp directory.
        size: Thumbnail size — "SMALL" (default 200px) or "LARGE" (default 800px).

    Returns:
        List of saved PNG file paths.
    """
    import re
    import tempfile
    import urllib.request

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", presentation_id)
    pres_id = match.group(1) if match else presentation_id

    slides_service, _ds, _ = _get_service()
    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    title = pres.get("title", pres_id)
    slides = pres.get("slides", [])

    if not slides:
        logger.warning("Presentation %s has no slides", pres_id)
        return []

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix=f"bpo-thumbs-{pres_id[:12]}-"))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    for i, slide in enumerate(slides):
        page_id = slide["objectId"]
        thumb = slides_service.presentations().pages().getThumbnail(
            presentationId=pres_id,
            pageObjectId=page_id,
            thumbnailProperties_thumbnailSize=size,
        ).execute()
        url = thumb["contentUrl"]
        dest = out / f"slide_{i + 1:02d}.png"
        urllib.request.urlretrieve(url, str(dest))
        saved.append(dest)

    logger.info("Exported %d thumbnails for '%s' → %s", len(saved), title, out)
    return saved

