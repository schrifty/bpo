"""Jira support-ticket slide builders."""

from __future__ import annotations

from typing import Any

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
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, FONT_SERIF, GRAY, MARGIN, MONO, NAVY, WHITE, _table_rows_fit_span


GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}
RED = {"red": 0.85, "green": 0.15, "blue": 0.15}


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
