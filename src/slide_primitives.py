"""Shared drawing primitives for app-built Google Slides decks."""

from __future__ import annotations

from typing import Any

from .slide_requests import (
    append_slide as _slide,
    append_text_box as _box,
)
from .slide_utils import slide_size as _sz, slide_transform as _tf
from .slides_theme import (
    BLACK,
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    FONT_SERIF,
    GRAY,
    KPI_METRIC_LABEL_PT,
    LIGHT,
    MARGIN,
    NAVY,
    SLIDE_H,
    SLIDE_W,
    TITLE_Y,
    WHITE,
    _fit_kpi_label,
)

_SUPPORT_DECK_CORNER_CUSTOMER: str | None = None


def set_support_deck_corner_customer(name: str | None) -> None:
    """Set the account label rendered in the upper-right of support deck slides."""
    global _SUPPORT_DECK_CORNER_CUSTOMER
    _SUPPORT_DECK_CORNER_CUSTOMER = (name or "").strip() or None


def support_subtitle_matched_lead(report: dict[str, Any], customer: str) -> str:
    """Prefix for support table subtitles, or empty when the title/corner already shows the account."""
    if report.get("support_deck_scoped_titles") and report.get("customer") is not None:
        return ""
    return f"Matched to {customer}  ·  "


def support_title_includes_project(title: str, project: str) -> bool:
    """True when the slide title already names *project*, so body copy can omit a repeat."""
    title = (title or "").strip()
    project = (project or "").strip().upper()
    if not title or not project:
        return False
    upper = title.upper()
    if upper.startswith(project + " "):
        return True
    for separator in ("—", "–", "-", ":"):
        if upper.startswith(project + separator):
            return True
    if f"({project})" in upper:
        return True
    return upper.endswith(f"({project})".upper())


def background(reqs: list[dict[str, Any]], sid: str, color: dict[str, float]) -> None:
    reqs.append({
        "updatePageProperties": {
            "objectId": sid,
            "pageProperties": {"pageBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
            "fields": "pageBackgroundFill",
        }
    })


def rect(reqs: list[dict[str, Any]], oid: str, sid: str, x: float, y: float, w: float, h: float, fill: dict[str, float]) -> None:
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": fill}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })


def bar_rect(
    reqs: list[dict[str, Any]],
    oid: str,
    sid: str,
    x: float,
    y: float,
    w: float,
    h: float,
    fill: dict[str, float],
    outline: dict[str, float] = NAVY,
) -> None:
    """Rectangle for chart bars with a visible outline."""
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": fill}}},
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": outline}}},
                    "weight": {"magnitude": 1, "unit": "PT"},
                },
            },
            "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
        }
    })


def kpi_metric_card(
    reqs: list[dict[str, Any]],
    oid_base: str,
    sid: str,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
    *,
    accent: dict[str, float] | None = None,
    label_pt: float = KPI_METRIC_LABEL_PT,
    value_pt: float = 18,
) -> None:
    """Outlined KPI tile for app-built slides."""
    accent = accent or BLUE
    bar_rect(reqs, oid_base, sid, x, y, w, h, LIGHT, outline=GRAY)
    pad = 10.0
    inner_w = max(40.0, w - 2 * pad)
    label, label_pt = _fit_kpi_label(label, inner_w, label_pt)
    _box(reqs, f"{oid_base}_l", sid, x + pad, y + 8, inner_w, 12, label)
    if label:
        reqs.append({
            "updateTextStyle": {
                "objectId": f"{oid_base}_l",
                "textRange": {"type": "ALL"},
                "style": {
                    "fontSize": {"magnitude": label_pt, "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": BLACK}},
                    "fontFamily": FONT,
                },
                "fields": "fontSize,foregroundColor,fontFamily",
            }
        })
    value_h = max(22.0, h - 28.0)
    _box(reqs, f"{oid_base}_v", sid, x + pad, y + 22, inner_w, value_h, value)
    if value:
        reqs.append({
            "updateTextStyle": {
                "objectId": f"{oid_base}_v",
                "textRange": {"type": "ALL"},
                "style": {
                    "bold": True,
                    "fontSize": {"magnitude": value_pt, "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": accent}},
                    "fontFamily": FONT,
                },
                "fields": "bold,fontSize,foregroundColor,fontFamily",
            }
        })


CHART_LEGEND_PT = 12.0


def slide_chart_legend(
    reqs: list[dict[str, Any]],
    sid: str,
    oid_prefix: str,
    x: float,
    y: float,
    entries: list[tuple[str, dict[str, float]]],
    *,
    font_pt: float = CHART_LEGEND_PT,
    swatch_size: float = 10.0,
    gap: float = 6.0,
    entry_gap: float = 18.0,
) -> float:
    """Render a horizontal slide-level chart legend and return y + height consumed."""
    cursor_x = x
    for index, (label, color) in enumerate(entries):
        swatch_id = f"{oid_prefix}_sw{index}"
        rect(reqs, swatch_id, sid, cursor_x, y + 2, swatch_size, swatch_size, color)
        cursor_x += swatch_size + gap
        label_id = f"{oid_prefix}_lt{index}"
        _box(reqs, label_id, sid, cursor_x, y, 120, swatch_size + 6, label)
        style(reqs, label_id, 0, len(label), size=font_pt, color=NAVY, font=FONT)
        cursor_x += len(label) * font_pt * 0.52 + entry_gap
    return y + swatch_size + 8


def slide_chart_legend_vertical(
    reqs: list[dict[str, Any]],
    sid: str,
    oid_prefix: str,
    x: float,
    y: float,
    max_w: float,
    entries: list[tuple[str, dict[str, float]]],
    *,
    font_pt: float = 10.0,
    swatch_size: float = 8.0,
    gap: float = 4.0,
    row_h: float = 14.0,
    max_label_chars: int = 22,
) -> float:
    """Render a vertical slide-level chart legend and return y + height consumed."""
    cursor_y = y
    for index, (label, color) in enumerate(entries):
        display = label if len(label) <= max_label_chars else label[: max_label_chars - 1] + "…"
        swatch_id = f"{oid_prefix}_sw{index}"
        rect(reqs, swatch_id, sid, x, cursor_y + 2, swatch_size, swatch_size, color)
        label_id = f"{oid_prefix}_lt{index}"
        _box(reqs, label_id, sid, x + swatch_size + gap, cursor_y, max_w - swatch_size - gap, row_h, display)
        style(reqs, label_id, 0, len(display), size=font_pt, color=NAVY, font=FONT)
        cursor_y += row_h
    return cursor_y


def pill(reqs: list[dict[str, Any]], oid: str, sid: str, x: float, y: float, w: float, h: float, text: str, bg: dict[str, float], fg: dict[str, float]) -> None:
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bg}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    style(reqs, oid, 0, len(text), bold=True, size=11, color=fg)
    align(reqs, oid, "CENTER")


def style(
    reqs: list[dict[str, Any]],
    oid: str,
    start: int,
    end: int,
    bold: bool = False,
    size: float | None = None,
    color: dict[str, float] | None = None,
    font: str | None = None,
    italic: bool = False,
    link: str | None = None,
) -> None:
    if start >= end:
        return
    style_body: dict[str, Any] = {}
    fields: list[str] = []
    if bold:
        style_body["bold"] = True
        fields.append("bold")
    if italic:
        style_body["italic"] = True
        fields.append("italic")
    if size:
        style_body["fontSize"] = {"magnitude": size, "unit": "PT"}
        fields.append("fontSize")
    if color:
        style_body["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
        fields.append("foregroundColor")
    if font:
        style_body["fontFamily"] = font
        fields.append("fontFamily")
    if link:
        style_body["link"] = {"url": link}
        fields.append("link")
    if fields:
        reqs.append({
            "updateTextStyle": {
                "objectId": oid,
                "textRange": {"type": "FIXED_RANGE", "startIndex": start, "endIndex": end},
                "style": style_body,
                "fields": ",".join(fields),
            }
        })


def align(reqs: list[dict[str, Any]], oid: str, alignment: str) -> None:
    reqs.append({
        "updateParagraphStyle": {
            "objectId": oid,
            "textRange": {"type": "ALL"},
            "style": {"alignment": alignment},
            "fields": "alignment",
        }
    })


BANNER_RED = {"red": 0.9, "green": 0.2, "blue": 0.2}


def red_banner(reqs: list[dict[str, Any]], oid: str, sid: str, x: float, y: float, w: float, h: float, text: str) -> None:
    """Create a red rectangle with white bold centered text."""
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": BANNER_RED}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    style(reqs, oid, 0, len(text), bold=True, size=12, color=WHITE, font=FONT)
    align(reqs, oid, "CENTER")


def missing_data_slide(
    reqs: list[dict[str, Any]],
    sid: str,
    report: dict[str, Any],
    idx: int,
    missing_description: str,
) -> int:
    """Render a slide with title + red banner when required data is unavailable."""
    from .qa import qa

    entry = report.get("_current_slide") or {}
    slide_type = entry.get("slide_type", entry.get("id", "slide"))
    slide_title_text = entry.get("title", slide_type.replace("_", " ").title())
    report.setdefault("_missing_slide_data", []).append({
        "slide_type": slide_type,
        "slide_title": slide_title_text,
        "missing": missing_description,
    })
    qa.flag(
        f"Slide \"{slide_title_text}\": {missing_description} not available",
        severity="warning",
        internal=False,
    )

    _slide(reqs, sid, idx)
    background(reqs, sid, LIGHT)
    slide_title(reqs, sid, slide_title_text)
    banner_text = f"Data not available: {missing_description}"
    if len(banner_text) > 90:
        banner_text = banner_text[:87] + "..."
    red_banner(reqs, f"{sid}_banner", sid, MARGIN, BODY_Y - 8, CONTENT_W, 28, banner_text)
    return idx + 1


def internal_footer(reqs: list[dict[str, Any]], sid: str) -> None:
    label = "INTERNAL ONLY"
    footer_id = f"{sid}_iof"
    _box(reqs, footer_id, sid, SLIDE_W - MARGIN - 80, SLIDE_H - 16, 80, 12, label)
    style(reqs, footer_id, 0, len(label), size=6, color=GRAY, font=FONT)
    reqs.append({
        "updateParagraphStyle": {
            "objectId": footer_id,
            "textRange": {"type": "ALL"},
            "style": {"alignment": "END"},
            "fields": "alignment",
        }
    })


def clean_table(reqs: list[dict[str, Any]], table_id: str, num_rows: int, num_cols: int) -> None:
    """Strip all borders from a table, then add a thin blue header separator."""
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": num_rows,
                "columnSpan": num_cols,
            },
            "borderPosition": "ALL",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": WHITE}}},
                "weight": {"magnitude": 0.01, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": 1,
                "columnSpan": num_cols,
            },
            "borderPosition": "BOTTOM",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": BLUE}}},
                "weight": {"magnitude": 1, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })


def simple_table(
    reqs: list[dict[str, Any]],
    table_id: str,
    sid: str,
    x: float,
    y: float,
    col_widths: list[float],
    row_h: float,
    headers: list[str],
    rows: list[list[Any]],
) -> float:
    """Create a styled table with headers and data rows."""
    num_rows = 1 + len(rows)
    num_cols = len(headers)
    table_width = sum(col_widths)
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(table_width, num_rows * row_h),
                "transform": _tf(x, y),
            },
            "rows": num_rows,
            "columns": num_cols,
        }
    })

    def insert_cell_text(row: int, col: int, text: Any) -> None:
        if text:
            reqs.append({"insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text),
                "insertionIndex": 0,
            }})

    def style_cell(row: int, col: int, length: int, **kwargs: Any) -> None:
        if length <= 0:
            return
        reqs.append({"updateTextStyle": {
            "objectId": table_id,
            "cellLocation": {"rowIndex": row, "columnIndex": col},
            "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": length},
            "style": {key: value for key, value in {
                "bold": kwargs.get("bold"),
                "fontSize": {"magnitude": kwargs.get("size", 9), "unit": "PT"},
                "foregroundColor": {"opaqueColor": {"rgbColor": kwargs.get("color", NAVY)}} if kwargs.get("color") else None,
                "fontFamily": kwargs.get("font", FONT),
            }.items() if value is not None},
            "fields": ",".join(
                field
                for field in ["bold", "fontSize", "foregroundColor", "fontFamily"]
                if kwargs.get(
                    field.replace("fontSize", "size").replace("foregroundColor", "color").replace("fontFamily", "font"),
                    None,
                ) is not None
                or field in ("fontSize", "fontFamily")
            ),
        }})

    for col_index, header in enumerate(headers):
        insert_cell_text(0, col_index, header)
        style_cell(0, col_index, len(str(header)), bold=True, size=9, color=NAVY, font=FONT)

    for row_index, row in enumerate(rows):
        for col_index, value in enumerate(row):
            insert_cell_text(row_index + 1, col_index, str(value))
            style_cell(row_index + 1, col_index, len(str(value)), size=9, color=NAVY, font=FONT)

    for col_index, width in enumerate(col_widths):
        reqs.append({"updateTableColumnProperties": {
            "objectId": table_id,
            "columnIndices": [col_index],
            "tableColumnProperties": {"columnWidth": {"magnitude": width, "unit": "PT"}},
            "fields": "columnWidth",
        }})

    clean_table(reqs, table_id, num_rows, num_cols)
    return num_rows * row_h


def table_cell_bg(reqs: list[dict[str, Any]], table_id: str, row: int, col: int, color: dict[str, float]) -> None:
    """Set background color on a single table cell."""
    reqs.append({"updateTableCellProperties": {
        "objectId": table_id,
        "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
        "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
        "fields": "tableCellBackgroundFill",
    }})


def omission_note(reqs: list[dict[str, Any]], sid: str, omitted_names: list[str], label: str = "Not shown") -> None:
    """Add a small italic note near the bottom listing items omitted for space."""
    if not omitted_names:
        return
    names = ", ".join(omitted_names[:8])
    if len(omitted_names) > 8:
        names += f", +{len(omitted_names) - 8} more"
    note = f"{label}: {names}"
    object_id = f"{sid}_omit"
    _box(reqs, object_id, sid, MARGIN, BODY_BOTTOM - 2, CONTENT_W, 14, note)
    style(reqs, object_id, 0, len(note), size=7, color=GRAY, font=FONT, italic=True)


def slide_title(reqs: list[dict[str, Any]], sid: str, text: str) -> None:
    """Standard content-slide title: navy text + teal underline + internal footer."""
    title_len = len(text or "")
    if title_len > 100:
        title_size = 12
    elif title_len > 85:
        title_size = 13
    elif title_len > 72:
        title_size = 14
    elif title_len > 60:
        title_size = 16
    else:
        title_size = 20
    object_id = f"{sid}_ttl"
    corner = _SUPPORT_DECK_CORNER_CUSTOMER
    corner_w = 200.0
    title_w = (CONTENT_W - corner_w - 8) if corner else float(CONTENT_W)
    _box(reqs, object_id, sid, MARGIN, TITLE_Y, title_w, 36, text)
    style(reqs, object_id, 0, len(text), bold=True, size=title_size, color=NAVY, font=FONT_SERIF)
    rect(reqs, f"{sid}_ul", sid, MARGIN, TITLE_Y + 38, 56, 2.5, BLUE)
    if corner:
        corner_id = f"{sid}_sdcorner"
        corner_x = MARGIN + title_w + 8.0
        _box(reqs, corner_id, sid, corner_x, TITLE_Y, corner_w, 40, corner)
        style(reqs, corner_id, 0, len(corner), size=12, color=NAVY, font=FONT, bold=True)
        align(reqs, corner_id, "END")
    internal_footer(reqs, sid)
