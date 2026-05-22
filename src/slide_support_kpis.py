"""Slide builders for the ``support-kpis`` HELP operational deck."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
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
        "Open requests still in the queue—who raised each issue, what they need, and when it was logged."
    ),
    "support_kpis_tail_risk": (
        "The oldest issues still open—these are the tickets most likely to erode trust, "
        "delay outcomes, or surface in executive conversations."
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
        "Tickets escalated from support into engineering work (LEAN and CUSTOMER projects, "
        "jira_escalated label)—opened vs resolved each week shows whether engineering "
        "throughput is keeping up with new escalations."
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
    days = payload.get("window_days") or 90
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
            max_rows_cap=25,
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
    open_n = got.get("open_count")
    scope_extra = f"{open_n} open" if open_n is not None else ""
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=scope_extra or "Open HELP tickets",
    )
    rows_data = got.get("backlog_open") or []
    if not rows_data:
        msg = "No open tickets in scope."
        _box(reqs, f"{sid}_empty", sid, MARGIN, content_y + 8, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    col_customer = 132
    col_created = 76
    col_subject = int(CONTENT_W) - col_customer - col_created
    sub_max = _max_chars_one_line_for_table_col(float(col_subject))
    rows = [
        [
            _truncate_table_cell(r.get("customer"), 22),
            _truncate_table_cell(r.get("summary"), sub_max),
            r.get("created") or "—",
        ]
        for r in rows_data
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Customer", "Subject", "Created"],
        col_widths=[col_customer, col_subject, col_created],
        rows=rows,
        link_col=None,
    )
    return idx + 1


def support_kpis_tail_risk_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail="Oldest 10 active (not Done) tickets · owner and status",
    )
    jira_base = ((report.get("jira") or {}).get("base_url") or "").rstrip("/")
    rows_data = got.get("tail_risk") or []
    headers = ["Key", "Age (d)", "Assignee", "Status", "Blocker", "Summary"]
    col_widths = [56, 48, 100, 100, 80, 236]
    t_sum = _max_chars_one_line_for_table_col(float(col_widths[5]))
    rows = [
        [
            r.get("key") or "—",
            str(r.get("age_days") if r.get("age_days") is not None else "—"),
            _truncate_table_cell(r.get("assignee"), 20),
            _truncate_table_cell(r.get("status"), 18),
            _truncate_table_cell(r.get("blocker"), 14),
            _truncate_table_cell(r.get("summary"), t_sum),
        ]
        for r in rows_data
    ]
    _render_table(reqs, sid, y_top=content_y + 4, headers=headers, col_widths=col_widths, rows=rows, jira_base=jira_base)
    return idx + 1


def support_kpis_sla_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sla = got.get("sla") or {}
    ttfr = sla.get("ttfr") or {}
    ttr = sla.get("ttr") or {}
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail="% of resolved tickets with completed SLA not breached",
    )
    row_y = content_y + 8
    card_w = (CONTENT_W - 24) // 2
    for i, (label, blob) in enumerate((("First response SLA %", ttfr), ("Resolution SLA %", ttr))):
        pct = blob.get("pct")
        val = "—" if pct is None else f"{pct:.0f}%"
        accent = GREEN if pct is not None and pct >= 90 else RED if pct is not None and pct < 75 else BLUE
        detail = f"{blob.get('met', 0)} met / {blob.get('measured', 0)} measured"
        x = MARGIN + i * (card_w + 24)
        _kpi_metric_card(reqs, f"{sid}_sla{i}", sid, x, row_y, card_w, 72, label, val, accent=accent)
        _box(reqs, f"{sid}_sla{i}d", sid, x, row_y + 76, card_w, 18, detail)
        _style(reqs, f"{sid}_sla{i}d", 0, len(detail), size=9, color=GRAY, font=FONT)
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
        scope_detail=f"LEAN & CUSTOMER · label {JIRA_ESCALATED_LABEL} · opened vs resolved per week",
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
        project="LEAN",
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
        project="CUSTOMER",
        flow_block=esc.get("CUSTOMER") or {},
        chart_x=MARGIN + half_w + gap,
        chart_y=chart_y,
        chart_w=half_w,
        chart_h=chart_h,
        oid_prefix="cust",
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
    content_y = _place_framing(
        reqs,
        sid,
        report,
        got,
        scope_detail=(
            f"{aging.get('count', 0)} open beyond thresholds "
            f"(no first response >{ttfr_h}h and/or open >{ttr_h}h)"
        ),
    )
    jira_base = ((report.get("jira") or {}).get("base_url") or "").rstrip("/")
    rows = [
        [
            t.get("key") or "—",
            str(t.get("age_days", "—")),
            _truncate_table_cell(t.get("reasons"), 36),
            _truncate_table_cell(t.get("assignee"), 16),
            _truncate_table_cell(t.get("summary"), 40),
        ]
        for t in (aging.get("tickets") or [])
    ]
    _render_table(
        reqs,
        sid,
        y_top=content_y + 4,
        headers=["Key", "Age (d)", "Threshold", "Assignee", "Summary"],
        col_widths=[56, 48, 160, 100, 216],
        rows=rows,
        jira_base=jira_base,
    )
    return idx + 1
