"""Jira support-ticket slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    CHART_LEGEND_PT,
    align as _align,
    background as _bg,
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_chart_legend_vertical as _slide_chart_legend_vertical,
    slide_title as _slide_title,
    style as _style,
    support_subtitle_matched_lead as _support_subtitle_matched_lead,
    support_title_includes_project as _support_title_includes_project,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import (
    blob_recent_tickets_window_days as _blob_recent_tickets_window_days,
    max_chars_one_line_for_table_col as _max_chars_one_line_for_table_col,
    slide_size as _sz,
    slide_transform as _tf,
    truncate_table_cell as _truncate_table_cell,
)
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY, WHITE, _table_rows_fit_span


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
