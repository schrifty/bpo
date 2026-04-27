"""Site Comparison slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    clean_table as _clean_table,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import (
    BODY_BOTTOM,
    BODY_Y,
    FONT,
    GRAY,
    MARGIN,
    NAVY,
    WHITE,
    _cap_page_count,
    _table_rows_fit_span,
)


def sites_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    all_sites = report["sites"]
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site/list data")

    customer_prefix = report.get("account", {}).get("customer", "").strip()
    has_entity = any(site.get("entity") for site in all_sites)

    def _short_site(name: str) -> str:
        short = name
        if customer_prefix and short.lower().startswith(customer_prefix.lower()):
            short = short[len(customer_prefix):].lstrip(" -·")
        return short[:18] if len(short) > 18 else short

    row_h = 26
    font_pt = 7
    table_top = BODY_Y

    if has_entity:
        headers = [
            "Site",
            "Entity",
            "Users",
            "Page views",
            "Feature clicks",
            "Events",
            "Minutes",
            "Last active",
        ]
        col_widths = [96, 72, 44, 56, 72, 48, 52, 64]
        end_col_start, end_col_end = 2, 6
    else:
        headers = [
            "Site",
            "Users",
            "Page views",
            "Feature clicks",
            "Events",
            "Minutes",
            "Last active",
        ]
        col_widths = [128, 44, 56, 72, 48, 52, 64]
        end_col_start, end_col_end = 1, 5

    num_cols = len(headers)
    rows_per_page = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=BODY_BOTTOM,
        row_height_pt=row_h,
        reserved_table_rows=2,
        max_rows_cap=40,
    )
    show_total = len(all_sites) > 1
    num_pages = ((len(all_sites) + rows_per_page - 1) // rows_per_page) if rows_per_page else 1
    num_pages = _cap_page_count(num_pages)

    def _add_site_table(page_sid: str, table_sid: str, sites_chunk: list[dict[str, Any]], add_total: bool) -> None:
        num_rows = 1 + len(sites_chunk) + (1 if add_total else 0)
        table_w = sum(col_widths)
        table_h = num_rows * row_h
        reqs.append({
            "createTable": {
                "objectId": table_sid,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(table_w, table_h),
                    "transform": _tf(MARGIN, table_top),
                },
                "rows": num_rows,
                "columns": num_cols,
            }
        })

        def _cell_loc(row: int, col: int) -> dict[str, int]:
            return {"rowIndex": row, "columnIndex": col}

        def _cell_text(row: int, col: int, text: str) -> None:
            reqs.append({
                "insertText": {
                    "objectId": table_sid,
                    "cellLocation": _cell_loc(row, col),
                    "text": text,
                    "insertionIndex": 0,
                }
            })

        def _cell_style(
            row: int,
            col: int,
            text_len: int,
            bold: bool = False,
            color: dict[str, float] | None = None,
            size: int = font_pt,
            align: str | None = None,
        ) -> None:
            if text_len > 0:
                style: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}}
                fields = ["fontSize"]
                if bold:
                    style["bold"] = True
                    fields.append("bold")
                if color:
                    style["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
                    fields.append("foregroundColor")
                if FONT:
                    style["fontFamily"] = FONT
                    fields.append("fontFamily")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_sid,
                        "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": style,
                        "fields": ",".join(fields),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_sid,
                        "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align},
                        "fields": "alignment",
                    }
                })

        def _cell_bg(row: int, col: int, color: dict[str, float]) -> None:
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_sid,
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

        _clean_table(reqs, table_sid, num_rows, num_cols)

        for col_index, header in enumerate(headers):
            _cell_text(0, col_index, header)
            align = "END" if end_col_start <= col_index <= end_col_end else None
            _cell_style(0, col_index, len(header), bold=True, color=GRAY, align=align)
            _cell_bg(0, col_index, WHITE)

        for row_index, site in enumerate(sites_chunk):
            row = row_index + 1
            values = [
                _short_site(site["sitename"]),
                (site.get("entity", "") or "")[:14] if has_entity else None,
                f'{site["visitors"]:,}',
                f'{site["page_views"]:,}',
                f'{site["feature_clicks"]:,}',
                f'{site["total_events"]:,}',
                f'{site["total_minutes"]:,}',
                (site.get("last_active") or "")[:10],
            ]
            if not has_entity:
                values.pop(1)
            for col_index, value in enumerate(values):
                text = str(value)
                _cell_text(row, col_index, text)
                align = "END" if end_col_start <= col_index <= end_col_end else None
                _cell_style(row, col_index, len(text), color=NAVY, align=align)
                _cell_bg(row, col_index, WHITE)

        if add_total:
            total_row_idx = len(sites_chunk) + 1
            reqs.append({
                "updateTableBorderProperties": {
                    "objectId": table_sid,
                    "tableRange": {
                        "location": {"rowIndex": total_row_idx, "columnIndex": 0},
                        "rowSpan": 1,
                        "columnSpan": num_cols,
                    },
                    "borderPosition": "TOP",
                    "tableBorderProperties": {
                        "tableBorderFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                        "weight": {"magnitude": 0.5, "unit": "PT"},
                        "dashStyle": "SOLID",
                    },
                    "fields": "tableBorderFill,weight,dashStyle",
                }
            })
            totals = [
                "Total",
                "" if has_entity else None,
                f'{sum(site["visitors"] for site in all_sites):,}',
                f'{sum(site["page_views"] for site in all_sites):,}',
                f'{sum(site["feature_clicks"] for site in all_sites):,}',
                f'{sum(site["total_events"] for site in all_sites):,}',
                f'{sum(site["total_minutes"] for site in all_sites):,}',
                "",
            ]
            if not has_entity:
                totals.pop(1)
            for col_index, value in enumerate(totals):
                text = value if value is not None else ""
                if text or col_index == 0:
                    _cell_text(total_row_idx, col_index, text)
                align = "END" if end_col_start <= col_index <= end_col_end else None
                _cell_style(total_row_idx, col_index, len(text), bold=True, color=NAVY, align=align)
                _cell_bg(total_row_idx, col_index, WHITE)

    for page in range(num_pages):
        page_sid = f"{sid}_p{page}" if num_pages > 1 else sid
        _slide(reqs, page_sid, idx + page)
        title = f"Site Comparison ({page + 1} of {num_pages})" if num_pages > 1 else "Site Comparison"
        _slide_title(reqs, page_sid, title)

        start = page * rows_per_page
        chunk = all_sites[start: start + rows_per_page]
        add_total = show_total and (page == num_pages - 1)
        _add_site_table(page_sid, f"{page_sid}_table", chunk, add_total)

    slide_oids = [f"{sid}_p{i}" for i in range(num_pages)] if num_pages > 1 else [sid]
    return idx + num_pages, slide_oids
