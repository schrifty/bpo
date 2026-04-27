"""CS Report Platform Health slide builder."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_primitives import clean_table as _clean_table, missing_data_slide as _missing_data_slide, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY, WHITE, _cap_chunk_list

HEALTH_BADGE = {
    "GREEN": ({"red": 0.10, "green": 0.55, "blue": 0.28}, "\u2705"),
    "YELLOW": ({"red": 0.9, "green": 0.65, "blue": 0.0}, "\u26a0"),
    "RED": ({"red": 0.78, "green": 0.18, "blue": 0.18}, "\u2716"),
}


def platform_health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int | tuple[int, list[str]]:
    cs = get_csr_section(report).get("platform_health") or {}
    site_list = cs.get("sites", [])
    if not site_list:
        err = (cs.get("error") or "").strip()
        desc = err or "CS Report platform health / site list"
        return _missing_data_slide(reqs, sid, report, idx, desc)

    dist = cs.get("health_distribution", {})
    total_short = cs.get("total_shortages", 0)
    total_crit = cs.get("total_critical_shortages", 0)

    ldna = report.get("leandna_item_master") or {}
    ldna_enabled = ldna.get("enabled", False)
    high_risk_count = len(ldna.get("high_risk_items", [])) if ldna_enabled else 0

    parts = [f"{value} {key}" for key, value in dist.items() if value > 0]
    parts.append(f"{total_short:,} shortages ({total_crit:,} critical)")

    if ldna_enabled and high_risk_count > 0:
        parts.append(f"{high_risk_count} high-risk items")

    summary_hdr = "  ·  ".join(parts)

    row_h = 28
    max_rows = max(1, (BODY_BOTTOM - BODY_Y - 24) // row_h - 1)
    headers_list = ["Factory", "Health", "CTB%", "CTC%", "Comp Avail%", "Shortages", "Critical"]
    col_widths = [170, 60, 55, 55, 75, 65, 60]
    chunks = _cap_chunk_list([site_list[i: i + max_rows] for i in range(0, len(site_list), max_rows)])
    object_ids: list[str] = []

    for page_index, show in enumerate(chunks):
        page_sid = f"{sid}_p{page_index}" if len(chunks) > 1 else sid
        object_ids.append(page_sid)
        _slide(reqs, page_sid, idx + page_index)
        title = "Platform Health" if len(chunks) == 1 else f"Platform Health ({page_index + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, title)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, summary_hdr)
        _style(reqs, f"{page_sid}_hdr", 0, len(summary_hdr), size=10, color=GRAY, font=FONT)

        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * row_h),
                    "transform": _tf(MARGIN, BODY_Y + 24),
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
            _cs(0, col_index, len(header), bold=True, color=NAVY, size=9, align="END" if col_index >= 2 else None)
            _cbg(0, col_index, WHITE)

        for row_index, site in enumerate(show):
            row = row_index + 1
            health_score = site.get("health_score") or "NONE"
            badge_info = HEALTH_BADGE.get(health_score)
            badge = badge_info[1] + " " + health_score if badge_info else health_score
            values = [
                site.get("factory", "?")[:24],
                badge,
                f'{site.get("clear_to_build_pct", 0):.1f}' if "clear_to_build_pct" in site else "-",
                f'{site.get("clear_to_commit_pct", 0):.1f}' if "clear_to_commit_pct" in site else "-",
                f'{site.get("component_availability_pct", 0):.1f}' if "component_availability_pct" in site else "-",
                f'{site.get("shortages", 0):,}' if "shortages" in site else "-",
                f'{site.get("critical_shortages", 0):,}' if "critical_shortages" in site else "-",
            ]
            for col_index, value in enumerate(values):
                _ct(row, col_index, value)
                _cs(row, col_index, len(value), color=NAVY, size=8, align="END" if col_index >= 2 else None)
                _cbg(row, col_index, WHITE)

    return idx + len(chunks), object_ids
