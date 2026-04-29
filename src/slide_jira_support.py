"""Jira support-ticket slide builders."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_primitives import (
    CHART_LEGEND_PT,
    align as _align,
    background as _bg,
    bar_rect as _bar_rect,
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    pill as _pill,
    slide_chart_legend_vertical as _slide_chart_legend_vertical,
    slide_title as _slide_title,
    style as _style,
    support_subtitle_matched_lead as _support_subtitle_matched_lead,
    support_title_includes_project as _support_title_includes_project,
)
from .slide_requests import append_slide as _slide, append_text_box as _box, append_wrapped_text_box as _wrap_box
from .slide_utils import (
    blob_recent_tickets_window_days as _blob_recent_tickets_window_days,
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
    FONT_SERIF,
    GRAY,
    LIGHT,
    MARGIN,
    MAX_PAGINATED_SLIDE_PAGES,
    MONO,
    NAVY,
    WHITE,
    _cap_chunk_list,
    _table_rows_fit_span,
)


GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}


def _format_count(value: Any) -> str:
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return "—"
    return f"{number:,}"


def project_slide_bg(project: str) -> dict[str, float]:
    """Subtle project tint backgrounds for project-specific slides."""
    proj = (project or "").strip().upper()
    if proj == "CUSTOMER":
        return {"red": 0.95, "green": 0.98, "blue": 1.0}
    if proj == "LEAN":
        return {"red": 0.95, "green": 1.0, "blue": 0.97}
    if proj == "HELP":
        return {"red": 1.0, "green": 0.96, "blue": 0.96}
    return WHITE


def _table_cell_text(reqs: list[dict[str, Any]], table_id: str, row: int, col: int, text: str) -> None:
    if not text:
        return
    reqs.append(
        {
            "insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text),
                "insertionIndex": 0,
            }
        }
    )


def _table_cell_style(
    reqs: list[dict[str, Any]],
    table_id: str,
    row: int,
    col: int,
    text_len: int,
    *,
    bold: bool = False,
    color: dict[str, float] | None = None,
    size: int = 8,
    align: str | None = None,
    link: str | None = None,
) -> None:
    if text_len > 0:
        style: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
        fields = ["fontSize", "fontFamily"]
        if bold:
            style["bold"] = True
            fields.append("bold")
        if color:
            style["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
            fields.append("foregroundColor")
        if link:
            style["link"] = {"url": link}
            fields.append("link")
        reqs.append(
            {
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": style,
                    "fields": ",".join(fields),
                }
            }
        )
    if align:
        reqs.append(
            {
                "updateParagraphStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align},
                    "fields": "alignment",
                }
            }
        )


def customer_ticket_metrics_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Support ticket KPI dashboard for HELP."""
    jira = report.get("jira") or {}
    snapshot = jira.get("customer_ticket_metrics") or {}
    charts = report.get("_charts")
    if snapshot.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"Customer ticket metrics: {snapshot.get('error')}")
    if not snapshot or not charts:
        return _missing_data_slide(reqs, sid, report, idx, "Customer ticket metrics and chart service")

    customer = report.get("customer") or snapshot.get("customer") or "All Customers"
    entry = report.get("_current_slide") or {}
    configured_title = (entry.get("title") or "").strip()
    if configured_title:
        title = configured_title
    elif report.get("support_deck_scoped_titles") and report.get("customer"):
        title = "HELP Ticket Metrics"
    else:
        title = f"{customer} Ticket Metrics"

    return _ticket_kpi_cards(reqs, sid, report, idx, snapshot, "HELP", title)


def non_help_project_ticket_kpi_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    snap_key: str,
    project: str,
) -> int:
    """KPI dashboard for CUSTOMER or LEAN."""
    jira = report.get("jira") or {}
    snapshot = jira.get(snap_key) or {}
    charts = report.get("_charts")
    if snapshot.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"{project} ticket metrics: {snapshot.get('error')}")
    if not snapshot or not charts:
        return _missing_data_slide(reqs, sid, report, idx, f"{project} ticket metrics and chart service")

    customer = report.get("customer") or snapshot.get("customer") or "All Customers"
    entry = report.get("_current_slide") or {}
    configured_title = (entry.get("title") or "").strip()
    if configured_title:
        title = configured_title
    elif report.get("support_deck_scoped_titles") and report.get("customer"):
        title = f"{project} Ticket Metrics"
    else:
        title = f"{customer} {project} Ticket Metrics"
    return _ticket_kpi_cards(reqs, sid, report, idx, snapshot, project, title)


def _ticket_kpi_cards(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    snapshot: dict[str, Any],
    project: str,
    title: str,
) -> int:
    unresolved = int(snapshot.get("unresolved_count") or 0)
    resolved_6mo = int(snapshot.get("resolved_in_6mo_count") or 0)
    ttfr = snapshot.get("ttfr_1y") or {}
    ttr = snapshot.get("ttr_1y") or {}
    adherence = snapshot.get("sla_adherence_1y") or {}

    _slide(reqs, sid, idx)
    _bg(reqs, sid, project_slide_bg(project))
    _slide_title(reqs, sid, title)
    title_mentions_project = _support_title_includes_project(title, project)
    defs = (
        "TTR: age of open, not-done backlog. TTFR: JSM first-response SLA (elapsed)."
        if title_mentions_project
        else "TTR = now − created for not-done tickets.  TTFR = JSM first-response SLA elapsed time."
    )
    _box(reqs, f"{sid}_defs", sid, MARGIN, BODY_Y, CONTENT_W, 14, defs)
    _style(reqs, f"{sid}_defs", 0, len(defs), size=8, color=GRAY, font=FONT)

    row_gap = 14
    col_gap = 18
    top_card_w = (CONTENT_W - 2 * col_gap) / 3
    bot_card_w = (CONTENT_W - col_gap) / 2
    card_h = 54
    row1_y = BODY_Y + 18
    row2_y = row1_y + card_h + row_gap

    adherence_pct = adherence.get("pct")
    adherence_value = "—" if adherence_pct is None else f"{adherence_pct:.0f}%"
    k3_accent = BLUE if adherence_pct is None else GREEN if adherence_pct >= 90 else BLUE if adherence_pct >= 75 else RED

    prefix = "" if project == "HELP" else f"{project} "
    labels = [
        "Unresolved" if title_mentions_project else f"{prefix or 'HELP '}unresolved tickets",
        "Resolved (6 mo)" if title_mentions_project else f"{prefix or 'HELP '}resolved (last 6mo)",
        "SLA adherence (1y)" if title_mentions_project else f"{prefix or 'HELP '}SLA adherence (1y)",
        "TTR — open backlog (median)" if title_mentions_project else f"{prefix or 'HELP '}TTR (Open Backlog Age, median)",
        "TTFR — median (1y)" if title_mentions_project else f"{prefix or 'HELP '}TTFR (1y median)",
        "TTR — open backlog (avg.)" if title_mentions_project else f"{prefix or 'HELP '}TTR (Open Backlog Age, average)",
        "TTFR — average (1y)" if title_mentions_project else f"{prefix or 'HELP '}TTFR (1y average)",
    ]

    _kpi_metric_card(reqs, f"{sid}_k1", sid, MARGIN, row1_y, top_card_w, card_h, labels[0], f"{unresolved}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_k2", sid, MARGIN + top_card_w + col_gap, row1_y, top_card_w, card_h, labels[1], f"{resolved_6mo}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_k3", sid, MARGIN + 2 * (top_card_w + col_gap), row1_y, top_card_w, card_h, labels[2], adherence_value, accent=k3_accent)
    _kpi_metric_card(reqs, f"{sid}_k4", sid, MARGIN, row2_y, bot_card_w, card_h, labels[3], ttr.get("median", "—"), accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_k5", sid, MARGIN + bot_card_w + col_gap, row2_y, bot_card_w, card_h, labels[4], ttfr.get("median", "—"), accent=BLUE)
    row3_y = row2_y + card_h + row_gap
    _kpi_metric_card(reqs, f"{sid}_k6", sid, MARGIN, row3_y, bot_card_w, card_h, labels[5], ttr.get("avg", "—"), accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_k7", sid, MARGIN + bot_card_w + col_gap, row3_y, bot_card_w, card_h, labels[6], ttfr.get("avg", "—"), accent=BLUE)
    return idx + 1


def customer_project_ticket_metrics_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return non_help_project_ticket_kpi_slide(reqs, sid, report, idx, snap_key="customer_project_ticket_metrics", project="CUSTOMER")


def lean_project_ticket_metrics_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return non_help_project_ticket_kpi_slide(reqs, sid, report, idx, snap_key="lean_project_ticket_metrics", project="LEAN")


def project_ticket_metrics_breakdown_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    *,
    snap_key: str,
    project: str,
    default_title: str,
) -> int:
    """Pie-chart breakdown slide for unresolved tickets by type/status."""
    jira = report.get("jira") or {}
    snapshot = jira.get(snap_key) or {}
    charts = report.get("_charts")
    if snapshot.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"{project} ticket metrics breakdown: {snapshot.get('error')}")
    if not snapshot or not charts:
        return _missing_data_slide(reqs, sid, report, idx, f"{project} ticket metrics breakdown and chart service")

    customer = report.get("customer") or snapshot.get("customer") or "All Customers"
    entry = report.get("_current_slide") or {}
    configured_title = (entry.get("title") or "").strip()
    if configured_title:
        title = configured_title
    elif report.get("support_deck_scoped_titles") and report.get("customer"):
        title = f"{project} — {default_title}"
    else:
        title = f"{customer} — {default_title}"

    _slide(reqs, sid, idx)
    slide_bg = project_slide_bg(project)
    _bg(reqs, sid, slide_bg)
    _slide_title(reqs, sid, title)

    def chart_rows(items: dict[str, int], limit: int = 6) -> tuple[list[str], list[int]]:
        pairs = list(items.items())
        shown = pairs[: limit - 1] + [("Other", sum(int(value) for _, value in pairs[limit - 1:]))] if len(pairs) > limit else pairs
        labels = [_truncate_table_cell(str(name) if name is not None else "—", 48) for name, _ in shown]
        values = [int(count) for _, count in shown]
        return labels, values

    type_labels, type_values = chart_rows(snapshot.get("by_type_open") or {})
    status_labels, status_values = chart_rows(snapshot.get("by_status_open") or {})
    if not type_labels and not status_labels:
        msg = f"No open {project} tickets to chart."
        _box(reqs, f"{sid}_em", sid, MARGIN, BODY_Y + 42, CONTENT_W, 24, msg)
        _style(reqs, f"{sid}_em", 0, len(msg), size=10, color=NAVY, font=FONT)
        return idx + 1

    from .charts import PIE_SLICE_COLORS, embed_chart

    col_gap = 16
    col_w = (CONTENT_W - col_gap) / 2
    title_y = BODY_Y + 18
    chart_y = title_y + 24
    chart_h = int(float(BODY_BOTTOM) - float(chart_y) - 4.0)
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap

    def embed_pie_plus_legend(oid: str, x: float, labels: list[str], values: list[int]) -> None:
        ncols = len(PIE_SLICE_COLORS) if PIE_SLICE_COLORS else 1
        rows = [
            (f"{_truncate_table_cell(str(label), 44)}  —  {int(values[index])} open", PIE_SLICE_COLORS[index % ncols] if PIE_SLICE_COLORS else RED)
            for index, label in enumerate(labels)
        ]
        legend_h = min(16.0 * float(max(1, len(labels))) + 12.0, chart_h * 0.40)
        pie_h = max(90.0, float(chart_h) - legend_h - 4.0)
        ss_id, chart_id = charts.add_pie_chart(title="", labels=labels, values=values, donut=False, suppress_legend=True, show_title=False, background=slide_bg)
        embed_chart(reqs, oid, sid, ss_id, chart_id, x, chart_y, col_w, pie_h, linked=True)
        _slide_chart_legend_vertical(reqs, sid, f"{oid}leg", x, chart_y + pie_h + 4.0, col_w - 2, rows, font_pt=CHART_LEGEND_PT, max_label_chars=64, row_h=16.0, swatch_size=10.0, gap=6.0)

    if type_labels:
        header = "Unresolved by type"
        _box(reqs, f"{sid}_th", sid, left_x, title_y, col_w, 14, header)
        _style(reqs, f"{sid}_th", 0, len(header), bold=True, size=13, color=NAVY, font=FONT)
        _align(reqs, f"{sid}_th", "CENTER")
        embed_pie_plus_legend(f"{sid}_t", left_x, type_labels, type_values)
    if status_labels:
        header = "Unresolved by status"
        _box(reqs, f"{sid}_sh", sid, right_x, title_y, col_w, 14, header)
        _style(reqs, f"{sid}_sh", 0, len(header), bold=True, size=13, color=NAVY, font=FONT)
        _align(reqs, f"{sid}_sh", "CENTER")
        embed_pie_plus_legend(f"{sid}_s", right_x, status_labels, status_values)
    return idx + 1


def customer_ticket_metrics_charts_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return project_ticket_metrics_breakdown_slide(reqs, sid, report, idx, snap_key="customer_ticket_metrics", project="HELP", default_title="Ticket Metrics Breakdown")


def customer_project_ticket_metrics_breakdown_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return project_ticket_metrics_breakdown_slide(reqs, sid, report, idx, snap_key="customer_project_open_breakdown", project="CUSTOMER", default_title="CUSTOMER Ticket Metrics Breakdown")


def customer_help_recent_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int, *, closed: bool) -> int:
    """Table slide for HELP tickets opened or resolved."""
    jira = report.get("jira") or {}
    blob = jira.get("customer_help_recent")
    if not isinstance(blob, dict):
        return _missing_data_slide(reqs, sid, report, idx, "customer HELP recent tickets (not in report — use support deck data fetch)")
    if blob.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"customer HELP recent tickets: {blob['error']}")
    return project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "HELP", closed=closed)


def support_recent_opened_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return customer_help_recent_slide(reqs, sid, report, idx, closed=False)


def support_recent_closed_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    return customer_help_recent_slide(reqs, sid, report, idx, closed=True)


def customer_project_recent_opened_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    blob = _project_recent_blob(reqs, sid, report, idx, "customer_project_recent", "CUSTOMER")
    return blob if isinstance(blob, int) else project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "CUSTOMER", closed=False)


def customer_project_recent_closed_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    blob = _project_recent_blob(reqs, sid, report, idx, "customer_project_recent", "CUSTOMER")
    return blob if isinstance(blob, int) else project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "CUSTOMER", closed=True)


def lean_project_recent_opened_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    blob = _project_recent_blob(reqs, sid, report, idx, "lean_project_recent", "LEAN")
    return blob if isinstance(blob, int) else project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "LEAN", closed=False)


def _project_recent_blob(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    key: str,
    project: str,
) -> dict[str, Any] | int:
    jira = report.get("jira") or {}
    blob = jira.get(key)
    if not isinstance(blob, dict):
        return _missing_data_slide(reqs, sid, report, idx, f"{project} project recent tickets (not in report)")
    if blob.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"{project} project recent tickets: {blob['error']}")
    return blob


def project_recent_tickets_table_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    blob: dict[str, Any],
    project: str,
    *,
    closed: bool,
) -> int:
    """Generic table slide for any project's recent tickets."""
    jira_base = (report.get("jira", {}).get("base_url") or "").rstrip("/")
    items: list[dict[str, Any]] = list(blob.get("recently_closed" if closed else "recently_opened") or [])
    window_days = _blob_recent_tickets_window_days(blob, closed)
    customer = report.get("customer") or blob.get("customer") or "All Customers"
    is_all_customers = report.get("customer") is None
    entry = report.get("_current_slide") or {}
    title = entry.get("title") or (f"Recently closed {project} tickets" if closed else f"Recently opened {project} tickets")
    kind = "Resolved" if closed else "Created"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, project_slide_bg(project))
    _slide_title(reqs, sid, title)

    table_top = BODY_Y + 24
    row_h = 19.0
    max_data_rows = _table_rows_fit_span(y_top=table_top, y_bottom=BODY_BOTTOM, row_height_pt=row_h, reserved_table_rows=1, max_rows_cap=30)
    display_items = items[:max_data_rows]
    count_text = f"showing {len(display_items)} of {len(items)} tickets (most recent)" if len(items) > len(display_items) else f"{len(items)} ticket{'s' if len(items) != 1 else ''}"
    port_note = " ·  no org column (portfolio scope)" if is_all_customers else ""
    time_phrase = f"Most recently {kind.lower()}" if window_days is None else f"{kind} in the last {window_days} days"
    subtitle = f"{_support_subtitle_matched_lead(report, customer)}{time_phrase}  ·  {count_text}{port_note}"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 16, subtitle)
    _style(reqs, f"{sid}_sub", 0, len(subtitle), size=9, color=GRAY, font=FONT)

    if not items:
        empty = f"No matching {project} tickets."
        _box(reqs, f"{sid}_empty", sid, MARGIN, BODY_Y + 30, CONTENT_W, 40, empty)
        _style(reqs, f"{sid}_empty", 0, len(empty), size=10, color=NAVY, font=FONT)
        return idx + 1

    headers = ["ID", "Title", "Status", "Priority", "Created", "Resolved"]
    col_widths = [60, 236, 100, 100, 64, 64] if is_all_customers else [60, 200, 100, 100, 64, 64]
    title_chars = _max_chars_one_line_for_table_col(float(col_widths[1]))
    status_chars = _max_chars_one_line_for_table_col(float(col_widths[2]))
    priority_chars = _max_chars_one_line_for_table_col(float(col_widths[3]))
    table_id = f"{sid}_tbl"
    num_rows = 1 + len(display_items)
    reqs.append({"createTable": {"objectId": table_id, "elementProperties": {"pageObjectId": sid, "size": _sz(sum(col_widths), num_rows * row_h), "transform": _tf(MARGIN, table_top)}, "rows": num_rows, "columns": len(headers)}})
    _clean_table(reqs, table_id, num_rows, len(headers))

    for col_index, header in enumerate(headers):
        _table_cell_text(reqs, table_id, 0, col_index, header)
        _table_cell_style(reqs, table_id, 0, col_index, len(header), bold=True, color=NAVY, size=9)

    for row_index, item in enumerate(display_items, start=1):
        key = item.get("key") or "—"
        values = [
            key,
            _truncate_table_cell(item.get("summary"), title_chars),
            _truncate_table_cell(item.get("status"), status_chars),
            _truncate_table_cell(item.get("priority"), priority_chars),
            item.get("created_short") or "—",
            item.get("resolved_short") or "—",
        ]
        for col_index, value in enumerate(values):
            _table_cell_text(reqs, table_id, row_index, col_index, value)
            link = f"{jira_base}/browse/{key}" if col_index == 0 and jira_base and key and key != "—" else None
            _table_cell_style(reqs, table_id, row_index, col_index, len(value), bold=bool(link), color=BLUE if link else None, size=8, link=link)

    return idx + 1


def support_help_customer_escalations_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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
    _bg(reqs, sid, project_slide_bg("HELP"))
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


def support_help_escalation_metrics_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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
    _bg(reqs, sid, project_slide_bg("HELP"))
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


def support_help_orgs_by_opened_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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
    _bg(reqs, sid, project_slide_bg("HELP"))
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


def lean_project_recent_closed_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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

    return project_recent_tickets_table_slide(reqs, sid, report, idx, blob, "LEAN", closed=True)


def lean_project_ticket_metrics_breakdown_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """LEAN ticket breakdown (pie charts)."""
    return project_ticket_metrics_breakdown_slide(
        reqs,
        sid,
        report,
        idx,
        snap_key="lean_project_open_breakdown",
        project="LEAN",
        default_title="LEAN Ticket Metrics Breakdown",
    )


def help_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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

    return resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "HELP")


def customer_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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

    return resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "CUSTOMER")


def lean_resolved_by_assignee_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
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

    return resolved_by_assignee_table_slide(reqs, sid, report, idx, blob, "LEAN")


def resolved_by_assignee_table_slide(
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
    _bg(reqs, sid, project_slide_bg(project))
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


def sla_health_slide(reqs, sid, report, idx):
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


def support_breakdown_slide(reqs, sid, report, idx):
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
        _style(reqs, f"{sid}_esct", 0, len(eh), bold=True, size=11, color=RED, font=FONT)
        right_y += 18
        for ei, esc_i in enumerate(esc_issues[:4]):
            if right_y + 14 > BODY_BOTTOM:
                break
            key = esc_i["key"]
            summary = esc_i.get("summary", "")[:28]
            line = f"{key}  {summary}"
            link = f"{jira_base}/browse/{key}" if jira_base else None
            _box(reqs, f"{sid}_esc{ei}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_esc{ei}", 0, len(key), bold=True, size=9, color=RED, font=MONO,
                   link=link)
            _style(reqs, f"{sid}_esc{ei}", len(key) + 2, len(line), size=9, color=NAVY, font=FONT)
            right_y += 14

    return idx + 1


def jira_slide(reqs, sid, report, idx):
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")
    jira_base = jira.get("base_url", "")

    total = jira["total_issues"]
    esc = jira["escalated"]
    days = jira.get("days", 90)

    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}  ({days}d)"
    header = date_range

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
                f"{reconcile_header_total} tickets in this summary.",
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
            st_footer = f"\nNote: status rows sum to {sum_status}; total issues in scope: {total}."
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
            pr_footer = f"\nNote: priority rows sum to {sum_priority}; total issues in scope: {total}."
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


def cross_validation_slide(reqs, sid, report, idx):
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
        p_events = _format_count(pendo_match.get("total_events", 0)) if pendo_match else "—"
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


def engineering_slide(reqs, sid, report, idx):
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


def enhancement_requests_slide(reqs, sid, report, idx):
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
