"""CS Report Supply Chain Overview slide builder."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_primitives import (
    clean_table as _clean_table,
    kpi_metric_card as _kpi_metric_card,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import BLUE, BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, MARGIN, NAVY, WHITE, _cap_chunk_list

ORANGE = {"red": 0.95, "green": 0.55, "blue": 0.13}


def _fmtk(value: Any) -> str:
    if value is None or value == 0:
        return "-"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}"


def supply_chain_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    cs = get_csr_section(report).get("supply_chain") or {}
    site_list = cs.get("sites", [])
    if not site_list:
        err = (cs.get("error") or "").strip()
        desc = err or "CS Report supply chain / site list"
        return _missing_data_slide(reqs, sid, report, idx, desc)

    totals = cs.get("totals", {})
    on_hand = totals.get("on_hand", 0)
    on_order = totals.get("on_order", 0)
    excess = totals.get("excess_on_hand", 0)

    ldna = report.get("leandna_item_master") or {}
    ldna_enabled = ldna.get("enabled", False)
    doi_bwd_data = ldna.get("doi_backwards") or {}
    items_over_60 = doi_bwd_data.get("items_over_60_days", 0) if ldna_enabled else 0

    kpi_h = 58
    gap = 18.0
    num_kpis = 4 if ldna_enabled else 3
    table_top = BODY_Y + 8 + kpi_h + 12

    row_h = 28
    max_rows = max(1, (BODY_BOTTOM - table_top) // row_h - 1)

    if ldna_enabled:
        headers_list = ["Factory", "On-Hand", "On-Order", "Excess", "DOI Fwd", "DOI Bwd", "Late POs"]
        col_widths = [140, 85, 85, 75, 50, 50, 55]
    else:
        headers_list = ["Factory", "On-Hand", "On-Order", "Excess", "DOI", "Late POs"]
        col_widths = [150, 90, 90, 80, 55, 55]

    chunks = _cap_chunk_list([site_list[i: i + max_rows] for i in range(0, len(site_list), max_rows)])
    object_ids: list[str] = []

    for page_index, show in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        title = "Supply Chain Overview" if len(chunks) == 1 else f"Supply Chain Overview ({page_index + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, title)

        card_w = (CONTENT_W - (num_kpis - 1) * gap) / num_kpis
        kpi_y = BODY_Y + 8
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k0",
            page_sid,
            MARGIN,
            kpi_y,
            card_w,
            kpi_h,
            "Inventory on-hand",
            _fmtk(on_hand),
            accent=BLUE,
            value_pt=20,
        )
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k1",
            page_sid,
            MARGIN + card_w + gap,
            kpi_y,
            card_w,
            kpi_h,
            "On-order",
            _fmtk(on_order),
            accent=BLUE,
            value_pt=20,
        )
        _kpi_metric_card(
            reqs,
            f"{page_sid}_k2",
            page_sid,
            MARGIN + 2 * (card_w + gap),
            kpi_y,
            card_w,
            kpi_h,
            "Excess on-hand",
            _fmtk(excess),
            accent=BLUE,
            value_pt=20,
        )

        if num_kpis == 4:
            card_accent = ORANGE if items_over_60 > 10 else BLUE
            _kpi_metric_card(
                reqs,
                f"{page_sid}_k3",
                page_sid,
                MARGIN + 3 * (card_w + gap),
                kpi_y,
                card_w,
                kpi_h,
                "Items >60d DOI Bwd",
                f"{items_over_60:,}",
                accent=card_accent,
                value_pt=20,
            )

        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * row_h),
                    "transform": _tf(MARGIN, table_top),
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
            if ldna_enabled:
                doi_bwd_val = doi_bwd_data.get("mean")
                values = [
                    site.get("factory", "?")[:22],
                    _fmtk(site.get("on_hand_value")),
                    _fmtk(site.get("on_order_value")),
                    _fmtk(site.get("excess_on_hand")),
                    f'{site["doi_days"]:.0f}d' if "doi_days" in site else "-",
                    f"{doi_bwd_val:.0f}d" if doi_bwd_val else "-",
                    f'{site.get("late_pos", 0):,}' if "late_pos" in site else "-",
                ]
            else:
                values = [
                    site.get("factory", "?")[:22],
                    _fmtk(site.get("on_hand_value")),
                    _fmtk(site.get("on_order_value")),
                    _fmtk(site.get("excess_on_hand")),
                    f'{site["doi_days"]:.0f}d' if "doi_days" in site else "-",
                    f'{site.get("late_pos", 0):,}' if "late_pos" in site else "-",
                ]
            for col_index, value in enumerate(values):
                _ct(row, col_index, value)
                _cs(row, col_index, len(value), color=NAVY, size=8, align="END" if col_index >= 1 else None)
                _cbg(row, col_index, WHITE)

    return idx + len(chunks), object_ids
