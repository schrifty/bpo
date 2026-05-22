"""Slide builders for the ``support-kpis`` HELP operational deck."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    rect as _rect,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box, append_wrapped_text_box as _wrap_box
from .slide_utils import (
    max_chars_one_line_for_table_col as _max_chars_one_line_for_table_col,
    slide_size as _sz,
    slide_transform as _tf,
    truncate_table_cell as _truncate_table_cell,
)
from .slides_theme import (
    BLUE,
    BODY_Y,
    CONTENT_W,
    FONT,
    GRAY,
    MARGIN,
    NAVY,
    SLIDE_H,
    WHITE,
    _table_rows_fit_span,
)
from .jira_client import JIRA_ESCALATED_LABEL
from .slide_jira_support import _clean_table, _table_cell_style, _table_cell_text

GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}
_AMBER = {"red": 0.96, "green": 0.71, "blue": 0.00}
_BACKLOG_STACK_SERIES: tuple[tuple[str, str, dict[str, float]], ...] = (
    ("With support", "with_support", BLUE),
    ("Waiting on engineering", "waiting_on_engineering", NAVY),
    ("Waiting on customer", "waiting_on_customer", _AMBER),
)
_BACKLOG_LEGEND_ROW_H = 16.0
_BACKLOG_LEGEND_SWATCH = 10.0
_BACKLOG_LEGEND_COL_GAP = 20.0
# Layout: business meaning under title; scope footer anchored to physical slide bottom.
_BUSINESS_BAND_H = 38.0
_CONTENT_TOP = BODY_Y + _BUSINESS_BAND_H
_SCOPE_FOOTER_MARGIN_BOTTOM = 10.0
_SCOPE_FOOTER_H = 22.0
_SCOPE_FOOTER_Y = float(SLIDE_H) - _SCOPE_FOOTER_MARGIN_BOTTOM - _SCOPE_FOOTER_H
_CONTENT_BOTTOM = _SCOPE_FOOTER_Y - 8.0
_TABLE_ROW_H = 21.0
_TABLE_BOTTOM_PAD = 6.0
_CHART_TOP_GAP = 4.0
_CHART_BOTTOM_GAP = 8.0
_INTAKE_SIDE_COL_GAP = 16.0
_INTAKE_CHART_WIDTH_RATIO = 0.62
# Max rows on tail-risk and aging-thresholds tables (must match jira_client caps).
_KPI_OPEN_TABLE_MAX_ROWS = 5
# Tail-risk table: narrow age/status/org; summary uses the remainder of CONTENT_W.
_KPI_COL_AGE_PT = 44
_KPI_COL_STATUS_PT = 108
_KPI_COL_ORG_PT = 120
_SLA_WINDOW_ROWS: tuple[tuple[str, str], ...] = (
    ("30 days", "30"),
    ("90 days", "90"),
    ("1 year", "365"),
)
_SLA_LABEL_COL_W = 72.0
_SLA_BAND_H = 56.0
_SLA_BAND_GAP = 8.0

# CEO-facing line under the slide title (slide_type → copy).
_BUSINESS_MEANING: dict[str, str] = {
    "support_kpis_intake": (
        "Shows how fast new customer issues are entering support—an early signal of "
        "rising demand, rollout pain, or accounts under stress."
    ),
    "support_kpis_flow": (
        "Compares tickets opened vs closed each week—when closes lag opens, backlog and "
        "customer wait times grow even if the team feels busy."
    ),
    "support_kpis_backlog": (
        "How long open work has been waiting—and whether support, the customer, or "
        "engineering is holding each ticket (stacked by age band)."
    ),
    "support_kpis_tail_risk": (
        "The oldest open requests by age—which customers are waiting longest and what "
        "each ticket is about."
    ),
    "support_kpis_sla": (
        "Whether we are meeting committed response and resolution targets on closed "
        "work—direct read on contractual service performance."
    ),
    "support_kpis_ttfr": (
        "How quickly customers receive a first meaningful response after opening a "
        "ticket—perceived responsiveness before the issue is fully solved."
    ),
    "support_kpis_resolution": (
        "How long issues actually take to close by type—sets realistic expectations with "
        "customers and highlights where process or dependencies slow delivery."
    ),
    "support_kpis_engineering_dependency": (
        "Engineering and Data Integration escalations from HELP—opened vs resolved each week. "
        "Mapping tickets route to Data Integration (CUSTOMER project); other escalated work "
        "routes to Engineering (LEAN project)."
    ),
    "support_kpis_escalation_backlog_engineering": (
        "Open escalated work sitting in the Engineering (LEAN) queue—how long tickets have "
        "waited and whether the customer or engineering team is holding progress."
    ),
    "support_kpis_data_integration_escalations": (
        "Mapping and Data Integration escalations from HELP—opened vs resolved each week "
        "for tickets routed to the CUSTOMER (Data Integration) project."
    ),
    "support_kpis_escalation_backlog_data_integration": (
        "Open escalated mapping work in the Data Integration (CUSTOMER) queue—how long "
        "tickets have waited and who is holding progress."
    ),
    "support_kpis_customer_health": (
        "Accounts with enough open or aged tickets to warrant proactive outreach before "
        "they become escalations or renewal risk."
    ),
    "support_kpis_csat": (
        "Directional view of customer tone on tickets—useful context alongside formal "
        "satisfaction programs, not a replacement for survey CSAT."
    ),
    "support_kpis_aging_thresholds": (
        "Tickets that have crossed service time commitments—these need management "
        "attention because promises to customers may already be at risk."
    ),
}


def _kpis(report: dict[str, Any]) -> dict[str, Any]:
    return (report.get("jira") or {}).get("support_kpis") or {}


def _kpis_or_missing(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
) -> dict[str, Any] | int:
    payload = _kpis(report)
    if payload.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"Support KPIs: {payload['error']}")
    if not payload:
        return _missing_data_slide(reqs, sid, report, idx, "Support KPIs (not in report)")
    return payload


def _current_slide_type(report: dict[str, Any]) -> str:
    entry = report.get("_current_slide") or {}
    return str(entry.get("slide_type") or entry.get("id") or "")


def _window_scope(payload: dict[str, Any]) -> str:
    days = payload.get("window_days") or 180
    open_n = payload.get("open_count")
    parts = [f"Trailing {days}d · HELP project"]
    if open_n is not None:
        parts.append(f"{open_n} open tickets")
    return "  ·  ".join(parts)


def _slide_header(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> str:
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "Support KPI").strip()
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)
    return title


def _max_single_chart_height(content_y: float, *, reserve_below: float = 0.0) -> float:
    """Height for a lone chart: fill the band between business line and scope footer."""
    available = (
        _CONTENT_BOTTOM - float(content_y) - _CHART_TOP_GAP - _CHART_BOTTOM_GAP - float(reserve_below)
    )
    return max(80.0, available)


def _sla_accent(pct: float | None) -> dict[str, float]:
    if pct is None:
        return BLUE
    if pct >= 90:
        return GREEN
    if pct < 75:
        return RED
    return BLUE


def _sla_by_window_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Normalize SLA windows; fall back to legacy single ``sla`` blob (90d)."""
    by_win = payload.get("sla_by_window")
    if isinstance(by_win, dict) and by_win:
        return by_win
    legacy = payload.get("sla") or {}
    if legacy:
        return {"90": legacy}
    return {}


def _render_stacked_age_backlog_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    content_y: float,
    *,
    backlog_age_stacked: dict[str, Any],
    backlog_age_buckets: dict[str, int],
) -> None:
    """Stacked column chart by age band (shared by HELP backlog and LEAN escalation backlog)."""
    stacked = backlog_age_stacked or {}
    labels = list(stacked.get("labels") or ["0–7", "8–14", "15–30", "30+"])
    raw_series = stacked.get("series") or {}
    buckets = backlog_age_buckets or {}
    keys = ["0-7", "8-14", "15-30", "30+"]
    total_open = sum(int(buckets.get(k, 0)) for k in keys)
    charts = report.get("_charts")
    chart_y = content_y + _CHART_TOP_GAP
    legend_h = 24.0
    chart_h = _max_single_chart_height(content_y) - legend_h

    chart_series: dict[str, list[int]] = {}
    legend_entries: list[tuple[str, dict[str, float]]] = []
    series_colors: list[dict[str, float]] = []
    for title, key, color in _BACKLOG_STACK_SERIES:
        vals = [int(x) for x in (raw_series.get(key) or [0, 0, 0, 0])[: len(labels)]]
        while len(vals) < len(labels):
            vals.append(0)
        if sum(vals) > 0:
            chart_series[title] = vals
            legend_entries.append((title, color))
            series_colors.append(color)

    if charts and total_open > 0 and chart_series:
        from .charts import embed_chart

        ss_id, chart_id = charts.add_bar_chart(
            title=f"Backlog_{sid}"[:100],
            labels=labels,
            series=chart_series,
            stacked=True,
            suppress_legend=True,
            show_title=False,
            series_colors=series_colors,
        )
        embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, MARGIN, chart_y, CONTENT_W, chart_h, linked=False)
        _render_backlog_stack_legend(
            reqs,
            sid,
            f"{sid}_bleg",
            MARGIN,
            chart_y + chart_h + 4,
            CONTENT_W,
            legend_entries,
        )
    else:
        values = [int(buckets.get(k, 0)) for k in keys]
        summary = "  ·  ".join(f"{lb}: {v}" for lb, v in zip(labels, values))
        msg = summary or "No open tickets in scope."
        _box(reqs, f"{sid}_sum", sid, MARGIN, content_y + 8, CONTENT_W, 40, msg)
        _style(reqs, f"{sid}_sum", 0, len(msg), size=12, color=NAVY, font=FONT)


def _render_backlog_stack_legend(
    reqs: list[dict[str, Any]],
    sid: str,
    oid_prefix: str,
    x: float,
    y: float,
    width: float,
    entries: list[tuple[str, dict[str, float]]],
) -> None:
    """Equal-width legend columns so long labels stay on one line and swatches align."""
    if not entries:
        return
    n = len(entries)
    gaps = _BACKLOG_LEGEND_COL_GAP * max(0, n - 1)
    col_w = max(80.0, (float(width) - gaps) / n)
    sw = _BACKLOG_LEGEND_SWATCH
    row_h = _BACKLOG_LEGEND_ROW_H
    swatch_y = y + (row_h - sw) / 2.0
    font_pt = 10.0
    for index, (label, color) in enumerate(entries):
        col_x = x + index * (col_w + _BACKLOG_LEGEND_COL_GAP)
        _rect(reqs, f"{oid_prefix}_sw{index}", sid, col_x, swatch_y, sw, sw, color)
        label_x = col_x + sw + 6.0
        label_w = col_w - sw - 6.0
        label_id = f"{oid_prefix}_lt{index}"
        _box(reqs, label_id, sid, label_x, y, label_w, row_h, label)
        _style(reqs, label_id, 0, len(label), size=font_pt, color=NAVY, font=FONT)


def _place_framing(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    payload: dict[str, Any],
    *,
    scope_detail: str = "",
    business: str | None = None,
) -> float:
    """Business line under title; scope/metadata at bottom. Returns y for main content."""
    slide_type = _current_slide_type(report)
    biz = (business or _BUSINESS_MEANING.get(slide_type) or "").strip()
    if biz:
        _wrap_box(reqs, f"{sid}_biz", sid, MARGIN, BODY_Y, CONTENT_W, _BUSINESS_BAND_H, biz)
        _style(reqs, f"{sid}_biz", 0, len(biz), size=11, color=NAVY, font=FONT)

    scope_parts = [_window_scope(payload)]
    if scope_detail.strip():
        scope_parts.append(scope_detail.strip())
    scope_text = "  ·  ".join(p for p in scope_parts if p)
    _wrap_box(reqs, f"{sid}_scope", sid, MARGIN, _SCOPE_FOOTER_Y, CONTENT_W, _SCOPE_FOOTER_H, scope_text)
    _style(reqs, f"{sid}_scope", 0, len(scope_text), size=8, color=GRAY, font=FONT)
    return _CONTENT_TOP


def _render_table(
    reqs: list[dict[str, Any]],
    sid: str,
    *,
    y_top: float,
    headers: list[str],
    col_widths: list[int],
    rows: list[list[str]],
    jira_base: str = "",
    link_col: int | None = 0,
    y_bottom: float | None = None,
    max_rows_cap: int = 25,
) -> None:
    """Render a table that must not extend into the scope footer band."""
    bottom = float(y_bottom if y_bottom is not None else _CONTENT_BOTTOM - _TABLE_BOTTOM_PAD)
    row_h = _TABLE_ROW_H
    display = rows[
        : _table_rows_fit_span(
            y_top=y_top,
            y_bottom=bottom,
            row_height_pt=row_h,
            reserved_table_rows=1,
            max_rows_cap=max_rows_cap,
        )
    ]
    if not display:
        return
    table_id = f"{sid}_tbl"
    num_rows = 1 + len(display)
    reqs.append(
        {
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": sid,
                    "size": _sz(sum(col_widths), num_rows * row_h),
                    "transform": _tf(MARGIN, y_top),
                },
                "rows": num_rows,
                "columns": len(headers),
            }
        }
    )
    _clean_table(reqs, table_id, num_rows, len(headers))
    for ci, header in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, ci, header)
        _table_cell_style(reqs, table_id, 0, ci, len(header), bold=True, color=NAVY, size=9)
    for ri, row in enumerate(display, start=1):
        for ci, value in enumerate(row):
            _table_cell_text(reqs, table_id, ri, ci, value)
            link = None
            if link_col is not None and ci == link_col and jira_base and value and value != "—":
                link = f"{jira_base}/browse/{value}"
            _table_cell_style(
                reqs,
                table_id,
                ri,
                ci,
                len(value),
                bold=bool(link),
                color=BLUE if link else None,
                size=8,
                link=link,
            )


def _project_flow_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    *,
    project: str,
    flow_block: dict[str, Any],
    chart_x: float,
    chart_y: float,
    chart_w: float,
    chart_h: float,
    oid_prefix: str,
) -> None:
    """One LEAN or CUSTOMER opened/resolved weekly chart with a project subhead."""
    header_y = chart_y - 14.0
    _box(reqs, f"{sid}_{oid_prefix}_h", sid, chart_x, header_y, chart_w, 12, project)
    _style(reqs, f"{sid}_{oid_prefix}_h", 0, len(project), bold=True, size=10, color=NAVY, font=FONT)
    weeks = list(flow_block.get("flow_weekly") or [])
    if flow_block.get("error"):
        msg = str(flow_block["error"])[:120]
        _box(reqs, f"{sid}_{oid_prefix}_err", sid, chart_x, chart_y + 8, chart_w, 36, msg)
        _style(reqs, f"{sid}_{oid_prefix}_err", 0, len(msg), size=9, color=NAVY, font=FONT)
        return
    if not weeks:
        msg = f"No {JIRA_ESCALATED_LABEL} tickets in window"
        _box(reqs, f"{sid}_{oid_prefix}_empty", sid, chart_x, chart_y + 8, chart_w, 24, msg)
        _style(reqs, f"{sid}_{oid_prefix}_empty", 0, len(msg), size=9, color=NAVY, font=FONT)
        return
    _weekly_line_chart(
        reqs,
        sid,
        report,
        weeks=weeks,
        series={
            "Opened": [w.get("created", 0) for w in weeks],
            "Resolved": [w.get("resolved", 0) for w in weeks],
        },
        chart_oid=f"{sid}_{oid_prefix}_chart",
        chart_x=chart_x,
        chart_y=chart_y,
        chart_w=chart_w,
        chart_h=chart_h,
    )


def _weekly_line_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    *,
    weeks: list[dict[str, Any]],
    series: dict[str, list[int]],
    chart_y: float,
    chart_h: float,
    chart_x: float = MARGIN,
    chart_w: float = CONTENT_W,
    chart_oid: str | None = None,
) -> None:
    charts = report.get("_charts")
    if not charts or not weeks:
        return
    from .charts import embed_chart

    labels = [w.get("label", w.get("week", "")) for w in weeks]
    ss_id, chart_id = charts.add_line_chart(title="", labels=labels, series=series)
    oid = chart_oid or f"{sid}_chart"
    embed_chart(reqs, oid, sid, ss_id, chart_id, chart_x, chart_y, chart_w, chart_h, linked=False)


def _render_intake_top_customers_panel(
    reqs: list[dict[str, Any]],
    sid: str,
    *,
    by_customer: dict[str, int],
    panel_x: float,
    panel_y: float,
    panel_w: float,
    panel_h: float,
    max_rows: int = 8,
) -> None:
    """Numbered ranked list beside the intake chart."""
    title = "Top customers (opens in window)"
    _box(reqs, f"{sid}_tc_t", sid, panel_x, panel_y, panel_w, 18, title)
    _style(reqs, f"{sid}_tc_t", 0, len(title), bold=True, size=10, color=NAVY, font=FONT)
    lines: list[str] = []
    name_chars = _max_chars_one_line_for_table_col(max(80.0, panel_w - 36.0))
    for rank, (name, count) in enumerate(list(by_customer.items())[:max_rows], start=1):
        label = _truncate_table_cell(name, name_chars)
        lines.append(f"{rank}. {label} — {count}")
    if not lines:
        lines.append("No opens in window.")
    body = "\n".join(lines)
    body_y = panel_y + 22.0
    body_h = max(40.0, panel_h - 24.0)
    _wrap_box(reqs, f"{sid}_tc_list", sid, panel_x, body_y, panel_w, body_h, body)
    _style(reqs, f"{sid}_tc_list", 0, len(body), size=10, color=NAVY, font=FONT)


def support_kpis_intake_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(reqs, sid, report, got, scope_detail="New tickets opened per ISO week")
    weeks = list(got.get("intake_weekly") or [])
    breakdown = got.get("intake_breakdown") or {}
    by_cust = breakdown.get("by_customer") or {}
    chart_y = content_y + _CHART_TOP_GAP
    chart_h = _max_single_chart_height(content_y)
    if by_cust:
        gap = _INTAKE_SIDE_COL_GAP
        chart_w = int((CONTENT_W - gap) * _INTAKE_CHART_WIDTH_RATIO)
        panel_x = MARGIN + chart_w + gap
        panel_w = CONTENT_W - chart_w - gap
        _weekly_line_chart(
            reqs,
            sid,
            report,
            weeks=weeks,
            series={"Opened": [w.get("created", 0) for w in weeks]},
            chart_y=chart_y,
            chart_h=chart_h,
            chart_x=MARGIN,
            chart_w=float(chart_w),
        )
        _render_intake_top_customers_panel(
            reqs,
            sid,
            by_customer=by_cust,
            panel_x=float(panel_x),
            panel_y=chart_y,
            panel_w=float(panel_w),
            panel_h=chart_h,
        )
    else:
        _weekly_line_chart(
            reqs,
            sid,
            report,
            weeks=weeks,
            series={"Opened": [w.get("created", 0) for w in weeks]},
            chart_y=chart_y,
            chart_h=chart_h,
        )
    return idx + 1


def support_kpis_flow_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(reqs, sid, report, got, scope_detail="Resolved vs opened per week")
    weeks = list(got.get("flow_weekly") or [])
    chart_y = content_y + _CHART_TOP_GAP
    chart_h = _max_single_chart_height(content_y)
    _weekly_line_chart(
        reqs,
        sid,
        report,
        weeks=weeks,
        series={
            "Opened": [w.get("created", 0) for w in weeks],
            "Resolved": [w.get("resolved", 0) for w in weeks],
        },
        chart_y=chart_y,
        chart_h=chart_h,
    )
    return idx + 1


def support_kpis_backlog_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            "Open tickets by age (days since created) · stacked: with support vs "
            "waiting on customer vs engineering"
        ),
    )
    _render_stacked_age_backlog_chart(
        reqs,
        sid,
        report,
        content_y,
        backlog_age_stacked=got.get("backlog_age_stacked") or {},
        backlog_age_buckets=got.get("backlog_age_buckets") or {},
    )
    return idx + 1


def support_kpis_tail_risk_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    open_n = got.get("open_count")
    if open_n is not None and int(open_n) > _KPI_OPEN_TABLE_MAX_ROWS:
        tail_scope = (
            f"Showing {_KPI_OPEN_TABLE_MAX_ROWS} of {open_n} open HELP tickets "
            f"(oldest by age, days since created)"
        )
    else:
        tail_scope = (
            f"{_KPI_OPEN_TABLE_MAX_ROWS} oldest open HELP tickets by age (days since created)"
        )
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=tail_scope,
    )
    rows_data = (got.get("tail_risk") or [])[:_KPI_OPEN_TABLE_MAX_ROWS]
    if not rows_data:
        msg = "No open tickets in scope."
        _box(reqs, f"{sid}_empty", sid, MARGIN, content_y + 8, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    col_age = _KPI_COL_AGE_PT
    col_status = _KPI_COL_STATUS_PT
    col_org = _KPI_COL_ORG_PT
    col_summary = int(CONTENT_W) - col_org - col_age - col_status
    org_max = _max_chars_one_line_for_table_col(float(col_org))
    status_max = _max_chars_one_line_for_table_col(float(col_status))
    sub_max = _max_chars_one_line_for_table_col(float(col_summary))
    rows = [
        [
            _truncate_table_cell(r.get("organization"), org_max),
            str(r.get("age_days") if r.get("age_days") is not None else "—"),
            _truncate_table_cell(r.get("status"), status_max),
            _truncate_table_cell(r.get("summary"), sub_max),
        ]
        for r in rows_data
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Organization", "Age (d)", "Status", "Summary"],
        col_widths=[col_org, col_age, col_status, col_summary],
        rows=rows,
        link_col=None,
        max_rows_cap=_KPI_OPEN_TABLE_MAX_ROWS,
    )
    return idx + 1


def support_kpis_sla_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sla_by_window = _sla_by_window_payload(got)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            "Resolved HELP tickets · % with completed first-response and resolution SLA not breached"
        ),
    )
    row_y = content_y + 6
    label_w = _SLA_LABEL_COL_W
    gap = 16.0
    card_w = int((CONTENT_W - label_w - gap) // 2)
    card_h = 48
    for ri, (win_label, win_key) in enumerate(_SLA_WINDOW_ROWS):
        band_y = row_y + ri * (_SLA_BAND_H + _SLA_BAND_GAP)
        win_sla = sla_by_window.get(win_key) or {}
        _box(reqs, f"{sid}_wl{ri}", sid, MARGIN, band_y + 14, label_w, 16, win_label)
        _style(reqs, f"{sid}_wl{ri}", 0, len(win_label), bold=True, size=10, color=NAVY, font=FONT)
        metrics_x = MARGIN + label_w + gap
        for mi, (metric_label, blob_key) in enumerate(
            (("First response SLA %", "ttfr"), ("Resolution SLA %", "ttr"))
        ):
            blob = win_sla.get(blob_key) or {}
            pct = blob.get("pct")
            val = "—" if pct is None else f"{pct:.0f}%"
            detail = f"{blob.get('met', 0)} met / {blob.get('measured', 0)} measured"
            x = metrics_x + mi * (card_w + gap)
            oid = f"{sid}_sla{ri}_{mi}"
            _kpi_metric_card(
                reqs, oid, sid, x, band_y, card_w, card_h, metric_label, val, accent=_sla_accent(pct)
            )
            _box(reqs, f"{oid}d", sid, x, band_y + card_h + 2, card_w, 14, detail)
            _style(reqs, f"{oid}d", 0, len(detail), size=8, color=GRAY, font=FONT)
    return idx + 1


def support_kpis_ttfr_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    ttfr = got.get("ttfr") or {}
    measured = ttfr.get("measured", 0)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=f"JSM first-response SLA on resolved tickets · {measured} measured",
    )
    row_y = content_y + 8
    card_w = (CONTENT_W - 36) // 3
    items = [
        ("Median", ttfr.get("median", "—")),
        ("Average", ttfr.get("avg", "—")),
        ("Breaches", str(ttfr.get("breached", 0))),
    ]
    for i, (label, val) in enumerate(items):
        _kpi_metric_card(reqs, f"{sid}_t{i}", sid, MARGIN + i * (card_w + 18), row_y, card_w, 64, label, str(val), accent=BLUE)
    return idx + 1


def support_kpis_resolution_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail="Calendar time to resolution · median and p90 by ticket type",
    )
    rows = [
        [r.get("type", "—"), str(r.get("count", 0)), r.get("median", "—"), r.get("p90", "—")]
        for r in (got.get("resolution_by_type") or [])
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Type", "Count", "Median TTR", "p90 TTR"],
        col_widths=[200, 80, 120, 120],
        rows=rows,
        link_col=None,
    )
    return idx + 1


def support_kpis_engineering_dependency_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    esc = got.get("escalation_flow") or {}
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            f"HELP {JIRA_ESCALATED_LABEL} · mapping → CUSTOMER (Data Integration); "
            f"other escalations → LEAN (Engineering) · opened vs resolved per week"
        ),
    )
    gap = _INTAKE_SIDE_COL_GAP
    half_w = (CONTENT_W - gap) / 2.0
    subhead_h = 14.0
    chart_y = content_y + _CHART_TOP_GAP + subhead_h
    chart_h = _max_single_chart_height(content_y) - subhead_h
    charts = report.get("_charts")
    if not charts:
        msg = "Charts unavailable."
        _box(reqs, f"{sid}_nochart", sid, MARGIN, content_y + 8, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_nochart", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    _project_flow_chart(
        reqs,
        sid,
        report,
        project="Engineering (LEAN)",
        flow_block=esc.get("LEAN") or {},
        chart_x=MARGIN,
        chart_y=chart_y,
        chart_w=half_w,
        chart_h=chart_h,
        oid_prefix="lean",
    )
    _project_flow_chart(
        reqs,
        sid,
        report,
        project="Data Integration (CUSTOMER)",
        flow_block=esc.get("CUSTOMER") or {},
        chart_x=MARGIN + half_w + gap,
        chart_y=chart_y,
        chart_w=half_w,
        chart_h=chart_h,
        oid_prefix="cust",
    )
    return idx + 1


def support_kpis_escalation_backlog_engineering_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    blob = got.get("escalation_backlog_engineering") or {}
    if blob.get("error"):
        return _missing_data_slide(
            reqs,
            sid,
            report,
            idx,
            f"LEAN escalation backlog: {blob['error']}",
        )
    _slide_header(reqs, sid, report, idx)
    open_n = blob.get("open_count")
    scope_extra = f"{open_n} open LEAN escalations" if open_n is not None else "Open LEAN escalations"
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            f"{scope_extra} · {JIRA_ESCALATED_LABEL} · by age (days since created) · "
            "stacked: with support vs waiting on customer vs engineering"
        ),
    )
    _render_stacked_age_backlog_chart(
        reqs,
        sid,
        report,
        content_y,
        backlog_age_stacked=blob.get("backlog_age_stacked") or {},
        backlog_age_buckets=blob.get("backlog_age_buckets") or {},
    )
    return idx + 1


def support_kpis_data_integration_escalations_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    esc = got.get("escalation_flow") or {}
    flow_block = esc.get("CUSTOMER") or {}
    if flow_block.get("error"):
        return _missing_data_slide(
            reqs,
            sid,
            report,
            idx,
            f"Data Integration escalations: {flow_block['error']}",
        )
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            f"CUSTOMER project · {JIRA_ESCALATED_LABEL} · mapping tickets from HELP · "
            "opened vs resolved per week"
        ),
    )
    subhead_h = 14.0
    chart_y = content_y + _CHART_TOP_GAP + subhead_h
    chart_h = _max_single_chart_height(content_y) - subhead_h
    charts = report.get("_charts")
    if not charts:
        msg = "Charts unavailable."
        _box(reqs, f"{sid}_nochart", sid, MARGIN, content_y + 8, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_nochart", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    _project_flow_chart(
        reqs,
        sid,
        report,
        project="Data Integration (CUSTOMER)",
        flow_block=flow_block,
        chart_x=MARGIN,
        chart_y=chart_y,
        chart_w=CONTENT_W,
        chart_h=chart_h,
        oid_prefix="di",
    )
    return idx + 1


def support_kpis_escalation_backlog_data_integration_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    blob = got.get("escalation_backlog_data_integration") or {}
    if blob.get("error"):
        return _missing_data_slide(
            reqs,
            sid,
            report,
            idx,
            f"Data Integration escalation backlog: {blob['error']}",
        )
    _slide_header(reqs, sid, report, idx)
    open_n = blob.get("open_count")
    scope_extra = (
        f"{open_n} open CUSTOMER escalations" if open_n is not None else "Open CUSTOMER escalations"
    )
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            f"{scope_extra} · {JIRA_ESCALATED_LABEL} · mapping tickets · by age (days since created) · "
            "stacked: with support vs waiting on customer vs engineering"
        ),
    )
    _render_stacked_age_backlog_chart(
        reqs,
        sid,
        report,
        content_y,
        backlog_age_stacked=blob.get("backlog_age_stacked") or {},
        backlog_age_buckets=blob.get("backlog_age_buckets") or {},
    )
    return idx + 1


def support_kpis_customer_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail="JSM orgs with 3+ open tickets or any ticket open 30+ days",
    )
    rows = [
        [
            _truncate_table_cell(r.get("organization"), 40),
            str(r.get("open_count", 0)),
            str(r.get("oldest_days", "—")),
        ]
        for r in (got.get("customer_health") or [])
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Organization", "Open", "Oldest (d)"],
        col_widths=[360, 80, 100],
        rows=rows,
        link_col=None,
    )
    return idx + 1


def support_kpis_csat_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    csat = got.get("csat") or {}
    note = (csat.get("note") or "Jira AI sentiment on HELP tickets").strip()
    content_y = _place_framing(reqs, sid, report, got, scope_detail=note)
    by_sent = csat.get("by_sentiment") or {}
    y = content_y + 8
    if not by_sent:
        msg = "No sentiment labels on tickets in this scope."
        _box(reqs, f"{sid}_empty", sid, MARGIN, y, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    total = sum(by_sent.values()) or 1
    for name, count in by_sent.items():
        if y + 22 >= _SCOPE_FOOTER_Y:
            break
        pct = round(100 * count / total)
        line = f"{name}: {count} ({pct}%)"
        _box(reqs, f"{sid}_s{hash(name) % 9999}", sid, MARGIN, y, CONTENT_W, 18, line)
        _style(reqs, f"{sid}_s{hash(name) % 9999}", 0, len(line), size=11, color=NAVY, font=FONT)
        y += 22
    return idx + 1


def support_kpis_aging_thresholds_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    aging = got.get("aging_beyond_thresholds") or {}
    ttfr_h = aging.get("ttfr_goal_hours", 48)
    ttr_h = aging.get("ttr_goal_hours", 160)
    total = int(aging.get("count") or 0)
    if total > _KPI_OPEN_TABLE_MAX_ROWS:
        aging_scope = (
            f"Showing {_KPI_OPEN_TABLE_MAX_ROWS} of {total} open beyond thresholds "
            f"(oldest by age · no first response >{ttfr_h}h and/or open >{ttr_h}h)"
        )
    else:
        aging_scope = (
            f"{total} open beyond thresholds "
            f"(no first response >{ttfr_h}h and/or open >{ttr_h}h)"
        )
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=aging_scope,
    )
    jira_base = ((report.get("jira") or {}).get("base_url") or "").rstrip("/")
    rows_data = (aging.get("tickets") or [])[:_KPI_OPEN_TABLE_MAX_ROWS]
    if not rows_data:
        msg = "No open tickets beyond service thresholds."
        _box(reqs, f"{sid}_empty", sid, MARGIN, content_y + 8, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    rows = [
        [
            t.get("key") or "—",
            str(t.get("age_days", "—")),
            _truncate_table_cell(t.get("reasons"), 36),
            _truncate_table_cell(t.get("assignee"), 16),
            _truncate_table_cell(t.get("summary"), 40),
        ]
        for t in rows_data
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Key", "Age (d)", "Threshold", "Assignee", "Summary"],
        col_widths=[56, 48, 160, 100, 216],
        rows=rows,
        jira_base=jira_base,
        max_rows_cap=_KPI_OPEN_TABLE_MAX_ROWS,
    )
    return idx + 1
