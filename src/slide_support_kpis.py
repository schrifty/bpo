"""Slide builders for the ``support-kpis`` HELP operational deck."""

from __future__ import annotations

from typing import Any

from .slide_jira_support import project_slide_bg
from .slide_primitives import (
    background as _bg,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import (
    max_chars_one_line_for_table_col as _max_chars_one_line_for_table_col,
    slide_size as _sz,
    slide_transform as _tf,
    truncate_table_cell as _truncate_table_cell,
)
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    GRAY,
    MARGIN,
    NAVY,
    _table_rows_fit_span,
)
from .slide_jira_support import _clean_table, _table_cell_style, _table_cell_text

GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}


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


def _slide_header(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
) -> str:
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "Support KPI").strip()
    _slide(reqs, sid, idx)
    _bg(reqs, sid, project_slide_bg("HELP"))
    _slide_title(reqs, sid, title)
    return title


def _window_subtitle(payload: dict[str, Any]) -> str:
    days = payload.get("window_days") or 90
    open_n = payload.get("open_count")
    parts = [f"Trailing {days}d window · HELP project"]
    if open_n is not None:
        parts.append(f"{open_n} open tickets")
    return "  ·  ".join(parts)


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
) -> None:
    row_h = 22.0
    display = rows[: _table_rows_fit_span(y_top=y_top, y_bottom=BODY_BOTTOM, row_height_pt=row_h, reserved_table_rows=1, max_rows_cap=25)]
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


def _weekly_line_chart(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    *,
    weeks: list[dict[str, Any]],
    series: dict[str, list[int]],
    chart_y: float,
    chart_h: float,
) -> None:
    charts = report.get("_charts")
    if not charts or not weeks:
        return
    from .charts import embed_chart

    labels = [w.get("label", w.get("week", "")) for w in weeks]
    ss_id, chart_id = charts.add_line_chart(title="", labels=labels, series=series)
    embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, MARGIN, chart_y, CONTENT_W, chart_h, linked=False)


def support_kpis_intake_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sub = _window_subtitle(got) + "  ·  New tickets opened per ISO week"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    weeks = list(got.get("intake_weekly") or [])
    chart_y = BODY_Y + 22
    _weekly_line_chart(
        reqs,
        sid,
        report,
        weeks=weeks,
        series={"Opened": [w.get("created", 0) for w in weeks]},
        chart_y=chart_y,
        chart_h=140,
    )
    breakdown = got.get("intake_breakdown") or {}
    by_cust = breakdown.get("by_customer") or {}
    if by_cust:
        lines = ["Top customers (opens in window):"]
        for name, count in list(by_cust.items())[:6]:
            lines.append(f"  {name}: {count}")
        text = "\n".join(lines)
        _box(reqs, f"{sid}_bd", sid, MARGIN, chart_y + 148, CONTENT_W, 80, text)
        _style(reqs, f"{sid}_bd", 0, len(text), size=9, color=NAVY, font=FONT)
    return idx + 1


def support_kpis_flow_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sub = _window_subtitle(got) + "  ·  Resolved vs opened per week"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    weeks = list(got.get("flow_weekly") or [])
    _weekly_line_chart(
        reqs,
        sid,
        report,
        weeks=weeks,
        series={
            "Opened": [w.get("created", 0) for w in weeks],
            "Resolved": [w.get("resolved", 0) for w in weeks],
        },
        chart_y=BODY_Y + 22,
        chart_h=200,
    )
    return idx + 1


def support_kpis_backlog_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    buckets = got.get("backlog_age_buckets") or {}
    sub = _window_subtitle(got) + "  ·  Open tickets by age (days since created)"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    charts = report.get("_charts")
    labels = ["0–7", "8–14", "15–30", "30+"]
    keys = ["0-7", "8-14", "15-30", "30+"]
    values = [int(buckets.get(k, 0)) for k in keys]
    if charts and any(values):
        from .charts import embed_chart

        ss_id, chart_id = charts.add_bar_chart(title="", labels=labels, series={"Open": values})
        embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, MARGIN, BODY_Y + 28, CONTENT_W, 200, linked=False)
    else:
        summary = "  ·  ".join(f"{lb}: {v}" for lb, v in zip(labels, values))
        _box(reqs, f"{sid}_sum", sid, MARGIN, BODY_Y + 40, CONTENT_W, 40, summary or "No open tickets")
        _style(reqs, f"{sid}_sum", 0, len(summary or "No open tickets"), size=12, color=NAVY, font=FONT)
    return idx + 1


def support_kpis_tail_risk_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sub = "Oldest 10 active (not Done) HELP tickets — owner and status/blocker hint"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
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
    _render_table(reqs, sid, y_top=BODY_Y + 24, headers=headers, col_widths=col_widths, rows=rows, jira_base=jira_base)
    return idx + 1


def support_kpis_sla_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sla = got.get("sla") or {}
    ttfr = sla.get("ttfr") or {}
    ttr = sla.get("ttr") or {}
    sub = _window_subtitle(got) + "  ·  % of resolved tickets with completed SLA not breached"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    row_y = BODY_Y + 36
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
    sub = _window_subtitle(got) + "  ·  Time to first response (JSM SLA, resolved in window)"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    row_y = BODY_Y + 32
    card_w = (CONTENT_W - 36) // 3
    items = [
        ("Median", ttfr.get("median", "—")),
        ("Average", ttfr.get("avg", "—")),
        ("Breaches", str(ttfr.get("breached", 0))),
    ]
    for i, (label, val) in enumerate(items):
        _kpi_metric_card(reqs, f"{sid}_t{i}", sid, MARGIN + i * (card_w + 18), row_y, card_w, 64, label, str(val), accent=BLUE)
    measured = ttfr.get("measured", 0)
    foot = f"Measured on {measured} resolved tickets with completed first-response SLA"
    _box(reqs, f"{sid}_ft", sid, MARGIN, row_y + 72, CONTENT_W, 16, foot)
    _style(reqs, f"{sid}_ft", 0, len(foot), size=9, color=GRAY, font=FONT)
    return idx + 1


def support_kpis_resolution_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sub = _window_subtitle(got) + "  ·  Calendar time to resolution — median and p90 by ticket type"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    rows = [
        [r.get("type", "—"), str(r.get("count", 0)), r.get("median", "—"), r.get("p90", "—")]
        for r in (got.get("resolution_by_type") or [])
    ]
    _render_table(
        reqs,
        sid,
        y_top=BODY_Y + 24,
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
    eng = got.get("engineering_dependency") or {}
    count = eng.get("count", 0)
    avg_age = eng.get("avg_age_days")
    avg_s = f"{avg_age:.1f}d" if avg_age is not None else "—"
    sub = f"Open HELP tickets blocked by / in Engineering — {count} tickets · avg age {avg_s}"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
    jira_base = ((report.get("jira") or {}).get("base_url") or "").rstrip("/")
    rows = [
        [
            t.get("key") or "—",
            str(t.get("age_days", "—")),
            _truncate_table_cell(t.get("status"), 22),
            _truncate_table_cell(t.get("assignee"), 18),
            _truncate_table_cell(t.get("summary"), 44),
        ]
        for t in (eng.get("tickets") or [])
    ]
    _render_table(
        reqs,
        sid,
        y_top=BODY_Y + 24,
        headers=["Key", "Age (d)", "Status", "Assignee", "Summary"],
        col_widths=[56, 48, 110, 100, 246],
        rows=rows,
        jira_base=jira_base,
    )
    return idx + 1


def support_kpis_customer_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    got = _kpis_or_missing(reqs, sid, report, idx)
    if isinstance(got, int):
        return got
    _slide_header(reqs, sid, report, idx)
    sub = "Customers (JSM org) with 3+ open HELP tickets or any ticket open 30+ days"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
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
        y_top=BODY_Y + 24,
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
    note = csat.get("note") or ""
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 28, note)
    _style(reqs, f"{sid}_sub", 0, len(note), size=9, color=GRAY, font=FONT)
    by_sent = csat.get("by_sentiment") or {}
    y = BODY_Y + 36
    if not by_sent:
        msg = "No Jira sentiment labels on HELP tickets in this scope."
        _box(reqs, f"{sid}_empty", sid, MARGIN, y, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1
    total = sum(by_sent.values()) or 1
    for name, count in by_sent.items():
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
    sub = (
        f"Open tickets beyond service thresholds "
        f"(no first response >{ttfr_h}h and/or open >{ttr_h}h) — {aging.get('count', 0)} total"
    )
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 20, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)
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
        y_top=BODY_Y + 28,
        headers=["Key", "Age (d)", "Threshold", "Assignee", "Summary"],
        col_widths=[56, 48, 160, 100, 216],
        rows=rows,
        jira_base=jira_base,
    )
    return idx + 1
