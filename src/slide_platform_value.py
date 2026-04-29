"""CS Report Platform Value & ROI slide builder."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_pipeline_traces import fmt_platform_value_count as _fmt_platform_value_count
from .slide_pipeline_traces import fmt_platform_value_dollar as _fmt_platform_value_dollar
from .slide_primitives import (
    background as _bg,
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box, append_wrapped_text_box as _wrap_box
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    GRAY,
    LIGHT,
    MARGIN,
    NAVY,
    WHITE,
    _cap_chunk_list,
    _date_range,
)


def platform_value_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    cs = get_csr_section(report).get("platform_value") or {}
    total_savings = cs.get("total_savings", 0)
    total_open = cs.get("total_open_ia_value", 0)
    total_recs = cs.get("total_recs_created_30d", 0)
    site_list = cs.get("sites", [])

    total_pos = cs.get("total_pos_placed_30d", 0)
    total_overdue = cs.get("total_overdue_tasks", 0)
    ops = f"{total_pos:,} POs placed  ·  {total_overdue:,} overdue tasks"

    card_h = 58
    gap = 18.0

    def _render_kpi(page_sid: str) -> None:
        row_y = BODY_Y + 8
        card_w = (CONTENT_W - 2 * gap) / 3
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k0",
            page_sid,
            MARGIN,
            row_y,
            card_w,
            card_h,
            "Savings achieved",
            _fmt_platform_value_dollar(total_savings),
            accent=BLUE,
            value_pt=22,
        )
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k1",
            page_sid,
            MARGIN + card_w + gap,
            row_y,
            card_w,
            card_h,
            "Open IA pipeline",
            _fmt_platform_value_dollar(total_open),
            accent=BLUE,
            value_pt=22,
        )
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k2",
            page_sid,
            MARGIN + 2 * (card_w + gap),
            row_y,
            card_w,
            card_h,
            "Recs created (30d)",
            _fmt_platform_value_count(total_recs),
            accent=BLUE,
            value_pt=22,
        )
        ops_y = row_y + card_h + 10
        _box(reqs, f"{page_sid}_ops", page_sid, MARGIN, ops_y, CONTENT_W, 16, ops)
        _style(reqs, f"{page_sid}_ops", 0, len(ops), size=9, color=GRAY, font=FONT)

    factory_rows = [site for site in site_list if site.get("savings_current_period") or site.get("recs_created_30d")]
    row_h = 28
    table_y_kpi = BODY_Y + 8 + card_h + 10 + 16 + 12
    max_rows_first = max(1, (BODY_BOTTOM - table_y_kpi) // row_h - 1)
    table_y_cont = BODY_Y + 24
    max_rows_cont = max(1, (BODY_BOTTOM - table_y_cont) // row_h - 1)

    chunks_planned: list[list[Any]] = []
    if factory_rows:
        remaining = list(factory_rows)
        chunks_planned.append(remaining[:max_rows_first])
        remaining = remaining[max_rows_first:]
        while remaining:
            chunks_planned.append(remaining[:max_rows_cont])
            remaining = remaining[max_rows_cont:]
    chunks_planned = _cap_chunk_list(chunks_planned)

    object_ids: list[str] = []
    if not chunks_planned:
        _slide(reqs, sid, idx)
        _slide_title(reqs, sid, "Platform Value & ROI")
        _render_kpi(sid)
        return idx + 1, [sid]

    for page_index, show in enumerate(chunks_planned):
        page_sid = f"{sid}_p{page_index}" if len(chunks_planned) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        if page_index == 0:
            _slide_title(reqs, page_sid, "Platform Value & ROI")
            _render_kpi(page_sid)
            table_y = table_y_kpi
        else:
            _slide_title(
                reqs,
                page_sid,
                f"Platform Value & ROI — factory detail ({page_index + 1} of {len(chunks_planned)})",
            )
            table_y = table_y_cont

        headers_list = ["Factory", "Savings", "Recs (30d)"]
        col_widths = [180, 120, 80]
        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * row_h),
                    "transform": _tf(MARGIN, table_y),
                },
                "rows": num_rows,
                "columns": len(headers_list),
            }
        })

        def _ct(row: int, col: int, text: str) -> None:
            if not text:
                return
            reqs.append({
                "insertText": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "text": text,
                    "insertionIndex": 0,
                }
            })

        def _cs(
            row: int,
            col: int,
            text_len: int,
            bold: bool = False,
            color: dict[str, float] | None = None,
            size: int = 8,
            align: str | None = None,
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
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": style,
                        "fields": ",".join(fields),
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

        def _cbg(row: int, col: int, color: dict[str, float]) -> None:
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_id,
                    "tableRange": {
                        "location": {"rowIndex": row, "columnIndex": col},
                        "rowSpan": 1,
                        "columnSpan": 1,
                    },
                    "tableCellProperties": {
                        "tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}
                    },
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_id, num_rows, len(headers_list))
        for col_index, header in enumerate(headers_list):
            _ct(0, col_index, header)
            _cs(0, col_index, len(header), bold=True, color=NAVY, size=9, align="END" if col_index >= 1 else None)
            _cbg(0, col_index, WHITE)
        for row_index, site in enumerate(show):
            row = row_index + 1
            savings_value = site.get("savings_current_period", 0)
            recs_value = site.get("recs_created_30d", 0)
            values = [
                site.get("factory", "?")[:24],
                f"${savings_value:,.0f}" if savings_value else "-",
                f"{recs_value:,}" if recs_value else "-",
            ]
            for col_index, value in enumerate(values):
                _ct(row, col_index, value)
                _cs(row, col_index, len(value), color=NAVY, size=8, align="END" if col_index >= 1 else None)
                _cbg(row, col_index, WHITE)

    return idx + len(chunks_planned), object_ids


_SKIP_PV_SUMMARY_TOC_TYPES = frozenset({"platform_value_summary_cover", "platform_value_summary_toc"})


def platform_value_summary_cover_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    """Title slide for the Platform Value & ROI Summary companion deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Platform Value & ROI Summary"
    _slide_title(reqs, sid, title)
    customer = (report.get("customer") or "").strip() or "Customer"
    acct = report.get("account") or {}
    days = int(report.get("days") or 90)
    dr = _date_range(days, report.get("quarter"), report.get("quarter_start"), report.get("quarter_end"))
    csm = acct.get("csm") or "—"
    sites = acct.get("total_sites", "—")
    visitors = acct.get("total_visitors", "—")
    parts = [
        customer,
        "",
        f"Reporting window: {dr}",
        f"CSM: {csm}  ·  {sites} sites  ·  {visitors} users",
        "",
        "Hard-dollar ROI, supply chain metrics, Lean Projects savings (when applicable), "
        "platform health, and data quality — from the Customer Success Report and related exports.",
    ]
    body = "\n".join(parts)
    oid = f"{sid}_body"
    body_h = max(80.0, BODY_BOTTOM - BODY_Y - 8)
    _wrap_box(reqs, oid, sid, MARGIN, BODY_Y, CONTENT_W, body_h, body)
    _style(reqs, oid, 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def _pv_summary_toc_label(entry: dict[str, Any]) -> str:
    t = str(entry.get("title") or "").strip()
    return t or str(entry.get("slide_type") or entry.get("id") or "Section").replace("_", " ").title()


def platform_value_summary_toc_slide(
    reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int
) -> int:
    """Table of contents for the resolved Platform Value & ROI Summary slide plan."""
    plan = list(report.get("_slide_plan") or [])
    entries = [e for e in plan if e.get("slide_type") not in _SKIP_PV_SUMMARY_TOC_TYPES]
    if not entries:
        return _missing_data_slide(reqs, sid, report, idx, "Platform Value deck table of contents entries")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Table of Contents"
    _slide_title(reqs, sid, title)
    customer = (report.get("customer") or "").strip()
    subtitle = (
        f"Sections in this deck for {customer}."
        if customer
        else "Sections in this deck."
    )
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y, CONTENT_W, 20, subtitle)
    _style(reqs, f"{sid}_sub", 0, len(subtitle), size=10, color=GRAY, font=FONT)

    max_items = 20
    labels = [_pv_summary_toc_label(e) for e in entries[:max_items]]
    left = labels[:10]
    right = labels[10:]
    col_gap = 28.0
    col_w = (CONTENT_W - col_gap) / 2
    y0 = BODY_Y + 38
    row_h = 20.0

    def _render_col(items: list[str], start_num: int, x: float) -> None:
        for row_index, label in enumerate(items):
            n = start_num + row_index
            line = f"{n}.   {label}"
            oid = f"{sid}_toc_{n}"
            _box(reqs, oid, sid, x, y0 + row_index * row_h, col_w, row_h, line)
            _style(reqs, oid, 0, len(line), size=11, color=NAVY, font=FONT)
            _style(reqs, oid, 0, len(str(n)) + 1, bold=True)

    _render_col(left, 1, MARGIN)
    _render_col(right, 1 + len(left), MARGIN + col_w + col_gap)

    if len(entries) > max_items:
        note = f"{len(entries) - max_items} additional section(s) omitted from this contents slide."
        _box(reqs, f"{sid}_more", sid, MARGIN, BODY_BOTTOM - 18, CONTENT_W, 16, note)
        _style(reqs, f"{sid}_more", 0, len(note), size=8, color=GRAY, font=FONT)

    return idx + 1
