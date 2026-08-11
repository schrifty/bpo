"""Intermediate representation for Claude-designed slides → Google Slides batchUpdate.

No LeanDNA Python design-standards: Claude owns layout. This module only translates a
small element vocabulary into Slides API requests on a 720×405 pt canvas.
"""

from __future__ import annotations

import re
from typing import Any

from .slide_primitives import background as _bg
from .slide_primitives import rect as _rect
from .slide_requests import append_slide as _slide
from .slide_requests import append_text_box as _box
from .slide_utils import slide_size as _sz
from .slide_utils import slide_transform as _tf
from .slides_theme import BLACK, FONT, SLIDE_H, SLIDE_W, WHITE

# Canvas Claude should design against (same aspect as existing eng decks).
CANVAS_W = float(SLIDE_W)
CANVAS_H = float(SLIDE_H)

_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")


def rgb_from_hex(value: str | None, default: dict[str, float] | None = None) -> dict[str, float]:
    """Parse ``#RRGGBB`` / ``RRGGBB`` into Slides rgbColor floats."""
    if value is None or value == "":
        return dict(default or BLACK)
    s = str(value).strip()
    m = _HEX_RE.match(s)
    if not m:
        named = {
            "white": WHITE,
            "black": BLACK,
        }
        return dict(named.get(s.lower(), default or BLACK))
    hx = m.group(1)
    return {
        "red": int(hx[0:2], 16) / 255.0,
        "green": int(hx[2:4], 16) / 255.0,
        "blue": int(hx[4:6], 16) / 255.0,
    }


def _clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# Rough advance width of one character as a fraction of font size, for the
# proportional fonts used in these decks.
_CHAR_W_RATIO = 0.52
# Bold display numbers set wider than body copy.
_BOLD_CHAR_W_RATIO = 0.62
_LINE_H_RATIO = 1.3
_TABLE_CELL_PAD = 10.0
_TABLE_FONT_SIZES = (10.0, 9.0, 8.0, 7.0)
_MIN_COL_W = 44.0
# Measured: Slides draws no table row shorter than this, whatever the font size.
_MIN_TABLE_ROW_H = 26.0
# Vertical padding Slides adds inside a table cell, on top of the text height.
_TABLE_ROW_PAD = 12.0


def _wrapped_lines(
    text: str, width: float, font_size: float, *, char_ratio: float = _CHAR_W_RATIO
) -> int:
    """Lines a string needs at *font_size* inside *width* points."""
    chars_per_line = max(1, int(width / (font_size * char_ratio)))
    lines = 0
    for para in (text or "").split("\n"):
        lines += max(1, -(-len(para) // chars_per_line))
    return max(1, lines)


def _wrapped_height(text: str, width: float, font_size: float) -> float:
    return _wrapped_lines(text, width, font_size) * font_size * _LINE_H_RATIO


def _fit_number_size(text: str, width: float, start_size: float) -> float:
    """Shrink a KPI number until it sits on one line (never wrap a figure)."""
    size = start_size
    while size > 10.0 and _wrapped_lines(
        text, width, size, char_ratio=_BOLD_CHAR_W_RATIO
    ) > 1:
        size -= 1.0
    return size


def _table_column_widths(
    rows: list[list[str]], total_w: float, n_cols: int
) -> list[float]:
    """Split table width across columns in proportion to their longest cell."""
    weights = [
        max(4, max((len(r[c]) for r in rows if c < len(r)), default=4))
        for c in range(n_cols)
    ]
    # First column carries labels; cap the rest so numbers do not hog width.
    weights = [min(w, 40) for w in weights]
    total_weight = float(sum(weights)) or 1.0
    widths = [max(_MIN_COL_W, total_w * (w / total_weight)) for w in weights]
    overflow = sum(widths) - total_w
    if overflow > 0:
        # Shave the excess off the widest columns so the table still fits.
        order = sorted(range(n_cols), key=lambda i: widths[i], reverse=True)
        for i in order:
            if overflow <= 0:
                break
            take = min(overflow, widths[i] - _MIN_COL_W)
            widths[i] -= take
            overflow -= take
    return widths


def _table_height(rows: list[list[str]], widths: list[float], font_size: float) -> float:
    """Slides grows rows to fit wrapped text; estimate that height."""
    total = 0.0
    for row in rows:
        lines = max(
            _wrapped_lines(cell, max(8.0, widths[c] - _TABLE_CELL_PAD), font_size)
            for c, cell in enumerate(row)
        )
        total += max(
            _MIN_TABLE_ROW_H, lines * font_size * _LINE_H_RATIO + _TABLE_ROW_PAD
        )
    return total


def _space_below(
    elements: list[dict[str, Any]], index: int, *, x: float, y: float, w: float
) -> float:
    """Height available under an element before it would collide with another.

    A table grows downward as its text wraps, so it has to stop short of whatever
    the designer placed beneath it (usually a takeaway bar or rule).
    """
    limit = CANVAS_H - 10.0
    for j, other in enumerate(elements):
        if j == index:
            continue
        oy = _f(other.get("y"), 0.0)
        ox = _f(other.get("x"), 0.0)
        ow = _f(other.get("w"), 0.0)
        if oy <= y or ox + ow <= x or ox >= x + w:
            continue
        limit = min(limit, oy - 6.0)
    return max(40.0, limit - y)


def _fit_table_rows(
    rows: list[list[str]], widths: list[float], *, available_h: float
) -> tuple[float, list[list[str]]]:
    """Pick a font size (and drop trailing rows if needed) so the table fits."""
    for size in _TABLE_FONT_SIZES:
        if _table_height(rows, widths, size) <= available_h:
            return size, rows
    size = _TABLE_FONT_SIZES[-1]
    kept = list(rows)
    while len(kept) > 2 and _table_height(kept, widths, size) > available_h:
        kept.pop()
    return size, kept


def normalize_slide_ir(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize/validate a Claude slide IR dict (raises ValueError on hard failures)."""
    if not isinstance(raw, dict):
        raise ValueError("slide IR must be a JSON object")
    elements = raw.get("elements")
    if not isinstance(elements, list) or not elements:
        raise ValueError("slide IR requires a non-empty 'elements' array")
    out_els: list[dict[str, Any]] = []
    for i, el in enumerate(elements):
        if not isinstance(el, dict):
            raise ValueError(f"elements[{i}] must be an object")
        et = str(el.get("type") or "").strip().lower()
        if not et:
            raise ValueError(f"elements[{i}] missing type")
        if et not in {"rect", "text", "kpi_row", "bullets", "table", "takeaway", "rule"}:
            raise ValueError(f"elements[{i}] unsupported type {et!r}")
        out_els.append(dict(el))
        out_els[-1]["type"] = et
    notes = raw.get("speaker_notes")
    return {
        "background": raw.get("background") or "#FFFFFF",
        "elements": out_els,
        "speaker_notes": (str(notes).strip() if notes is not None else ""),
    }


def render_slide_ir(
    reqs: list[dict[str, Any]],
    sid: str,
    ir: dict[str, Any],
    idx: int,
) -> int:
    """Append createSlide + IR elements for one page. Returns next slide index."""
    normalized = normalize_slide_ir(ir)
    _slide(reqs, sid, idx)
    _bg(reqs, sid, rgb_from_hex(normalized.get("background"), WHITE))

    for i, el in enumerate(normalized["elements"]):
        oid = f"{sid}_e{i}"
        et = el["type"]
        x = _clamp(_f(el.get("x"), 24), 0, CANVAS_W - 4)
        y = _clamp(_f(el.get("y"), 24), 0, CANVAS_H - 4)
        w = _clamp(_f(el.get("w"), 100), 4, CANVAS_W - x)
        h = _clamp(_f(el.get("h"), 20), 4, CANVAS_H - y)

        if et == "rect":
            _rect(reqs, oid, sid, x, y, w, h, rgb_from_hex(el.get("fill"), WHITE))
            continue

        if et == "rule":
            _rect(reqs, oid, sid, x, y, w, max(1.0, min(h, 4.0)), rgb_from_hex(el.get("fill"), BLACK))
            continue

        if et in ("text", "takeaway"):
            text = str(el.get("text") or "").strip()
            if not text:
                continue
            size = _clamp(_f(el.get("size"), 14 if et == "text" else 11), 8, 48)
            color = rgb_from_hex(el.get("color"), BLACK)
            bold = bool(el.get("bold"))
            if et == "takeaway" and el.get("fill"):
                _rect(reqs, f"{oid}_bg", sid, x, y, w, h, rgb_from_hex(el.get("fill"), WHITE))
            _box(reqs, oid, sid, x, y, w, h, text)
            reqs.append(
                {
                    "updateTextStyle": {
                        "objectId": oid,
                        "style": {
                            "fontFamily": str(el.get("font") or FONT),
                            "fontSize": {"magnitude": size, "unit": "PT"},
                            "bold": bold,
                            "foregroundColor": {"opaqueColor": {"rgbColor": color}},
                        },
                        "textRange": {"type": "ALL"},
                        "fields": "fontFamily,fontSize,bold,foregroundColor",
                    }
                }
            )
            continue

        if et == "bullets":
            items = [str(t).strip() for t in (el.get("items") or []) if str(t).strip()]
            if not items:
                continue
            body = "\n".join(f"• {t}" for t in items)
            size = _clamp(_f(el.get("size"), 12), 8, 28)
            _box(reqs, oid, sid, x, y, w, h, body)
            reqs.append(
                {
                    "updateTextStyle": {
                        "objectId": oid,
                        "style": {
                            "fontFamily": FONT,
                            "fontSize": {"magnitude": size, "unit": "PT"},
                            "foregroundColor": {"opaqueColor": {"rgbColor": rgb_from_hex(el.get("color"), BLACK)}},
                        },
                        "textRange": {"type": "ALL"},
                        "fields": "fontFamily,fontSize,foregroundColor",
                    }
                }
            )
            continue

        if et == "kpi_row":
            items = [it for it in (el.get("items") or []) if isinstance(it, dict)]
            if not items:
                continue
            gap = 8.0
            cell_w = (w - gap * (len(items) - 1)) / max(1, len(items))
            for j, it in enumerate(items):
                cx = x + j * (cell_w + gap)
                fill = rgb_from_hex(it.get("fill") or el.get("fill"), {"red": 0.95, "green": 0.95, "blue": 0.97})
                _rect(reqs, f"{oid}_k{j}", sid, cx, y, cell_w, h, fill)
                label = str(it.get("label") or "").strip()
                value = str(it.get("value") or "").strip()
                label_size = 10.0 if len(label) <= 18 else 8.5
                label_h = _wrapped_height(label, cell_w - 12, label_size) if label else 0.0
                value_y = y + 6 + label_h + (3 if label else 0)
                if label:
                    _box(reqs, f"{oid}_kl{j}", sid, cx + 6, y + 6, cell_w - 12, label_h, label)
                    reqs.append(
                        {
                            "updateTextStyle": {
                                "objectId": f"{oid}_kl{j}",
                                "style": {
                                    "fontFamily": FONT,
                                    "fontSize": {"magnitude": label_size, "unit": "PT"},
                                    "foregroundColor": {
                                        "opaqueColor": {"rgbColor": rgb_from_hex(it.get("label_color"), {"red": 0.4, "green": 0.4, "blue": 0.45})}
                                    },
                                },
                                "textRange": {"type": "ALL"},
                                "fields": "fontFamily,fontSize,foregroundColor",
                            }
                        }
                    )
                if value:
                    value_h = max(14.0, y + h - value_y - 4)
                    # Shrink the number until it sits on one line inside what the
                    # label left behind.
                    value_size = _fit_number_size(
                        value, cell_w - 12, _clamp(_f(it.get("value_size"), 20), 12, 36)
                    )
                    while value_size > 10 and (
                        _wrapped_height(value, cell_w - 12, value_size) > value_h
                    ):
                        value_size -= 2
                    _box(reqs, f"{oid}_kv{j}", sid, cx + 6, value_y, cell_w - 12, value_h, value)
                    reqs.append(
                        {
                            "updateTextStyle": {
                                "objectId": f"{oid}_kv{j}",
                                "style": {
                                    "fontFamily": FONT,
                                    "fontSize": {"magnitude": value_size, "unit": "PT"},
                                    "bold": True,
                                    "foregroundColor": {
                                        "opaqueColor": {"rgbColor": rgb_from_hex(it.get("color"), BLACK)}
                                    },
                                },
                                "textRange": {"type": "ALL"},
                                "fields": "fontFamily,fontSize,bold,foregroundColor",
                            }
                        }
                    )
            continue

        if et == "table":
            rows = el.get("rows") or []
            if not isinstance(rows, list) or not rows:
                continue
            norm_rows: list[list[str]] = []
            for row in rows:
                if isinstance(row, (list, tuple)):
                    norm_rows.append([str(c) for c in row])
                else:
                    norm_rows.append([str(row)])
            n_cols = max(len(r) for r in norm_rows)
            for r in norm_rows:
                while len(r) < n_cols:
                    r.append("")
            col_widths = _table_column_widths(norm_rows, w, n_cols)
            font_size, norm_rows = _fit_table_rows(
                norm_rows,
                col_widths,
                available_h=_space_below(normalized["elements"], i, x=x, y=y, w=w),
            )
            n_rows = len(norm_rows)
            table_id = f"{oid}_t"
            reqs.append(
                {
                    "createTable": {
                        "objectId": table_id,
                        "elementProperties": {
                            "pageObjectId": sid,
                            "size": _sz(w, h),
                            "transform": _tf(x, y),
                        },
                        "rows": n_rows,
                        "columns": n_cols,
                    }
                }
            )
            for c_i, cw in enumerate(col_widths):
                reqs.append(
                    {
                        "updateTableColumnProperties": {
                            "objectId": table_id,
                            "columnIndices": [c_i],
                            "tableColumnProperties": {
                                "columnWidth": {"magnitude": cw, "unit": "PT"}
                            },
                            "fields": "columnWidth",
                        }
                    }
                )
            for r_i, row in enumerate(norm_rows):
                for c_i, cell in enumerate(row):
                    if not cell:
                        continue
                    reqs.append(
                        {
                            "insertText": {
                                "objectId": table_id,
                                "cellLocation": {"rowIndex": r_i, "columnIndex": c_i},
                                "text": cell,
                            }
                        }
                    )
                    reqs.append(
                        {
                            "updateTextStyle": {
                                "objectId": table_id,
                                "cellLocation": {"rowIndex": r_i, "columnIndex": c_i},
                                "style": {
                                    "fontFamily": FONT,
                                    "fontSize": {"magnitude": font_size, "unit": "PT"},
                                    "bold": r_i == 0,
                                    "foregroundColor": {"opaqueColor": {"rgbColor": BLACK}},
                                },
                                "textRange": {"type": "ALL"},
                                "fields": "fontFamily,fontSize,bold,foregroundColor",
                            }
                        }
                    )
            continue

    return idx + 1


IR_SCHEMA_FOR_PROMPT = """
Return ONLY a single JSON object (no markdown fence, no commentary) with this shape.
Put "elements" BEFORE "speaker_notes" so layout is not truncated.
{
  "background": "#FFFFFF",
  "elements": [
    {"type": "rect", "x": 0, "y": 0, "w": 720, "h": 48, "fill": "#0B1F33"},
    {"type": "text", "x": 48, "y": 12, "w": 624, "h": 28, "text": "Title", "size": 22, "bold": true, "color": "#FFFFFF"},
    {"type": "kpi_row", "x": 48, "y": 72, "w": 624, "h": 70, "items": [{"label": "Closed", "value": "42", "fill": "#E8F4FC", "color": "#009AFF"}]},
    {"type": "bullets", "x": 48, "y": 160, "w": 300, "h": 160, "items": ["Point one", "Point two"], "size": 12},
    {"type": "table", "x": 360, "y": 160, "w": 312, "h": 140, "rows": [["Col A", "Col B"], ["1", "2"]]},
    {"type": "takeaway", "x": 48, "y": 360, "w": 624, "h": 32, "text": "So what", "size": 11, "fill": "#EEF0F3"},
    {"type": "rule", "x": 48, "y": 350, "w": 624, "h": 1, "fill": "#38C0CE"}
  ],
  "speaker_notes": "Optional; max 160 characters."
}

Brand palette (use for visual variety and hierarchy):
- Navy header/dark text: #0B1F33
- Primary accent (KPI values, highlights): #009AFF
- Secondary accents: #7BC4FA, #38C0CE (teal)
- Highlight/callout: #AEFFF6 (mint)
- Light fills (KPI tiles, takeaway, alt rows): #EEF0F3, #E8F4FC
- White: #FFFFFF — use for text on dark backgrounds

Hard limits:
- Canvas 720×405 points; coordinates (x,y,w,h) in points from top-left
- At most 10 elements
- Keep every string short (titles ≤60 chars; bullet lines ≤90 chars; takeaway ≤140 chars)
- speaker_notes ≤160 chars or omit it
- Element types: rect, text, kpi_row, bullets, table, takeaway, rule

Table guidance:
- Every row is 26pt tall (more if a cell wraps) and tables grow downward, so
  budget 26pt × (header + data rows). A table at y=200 fits 6 rows total;
  at y=140 it fits 8. Rows past the space available are dropped
- Max 8 data rows; max 4 columns; keep cells ≤22 chars so they do not wrap
- Anything you place below a table (takeaway, rule, bullets) shortens it — leave
  26pt per row of clearance or move the table up

KPI row guidance:
- Use "fill" on items for colored tile backgrounds (#E8F4FC, #EEF0F3, #AEFFF6)
- Use "color" on items for accent-colored values (#009AFF, #38C0CE)
- Keep labels ≤22 chars; long labels wrap and squeeze the number
- 4-6 tiles per row; give the row h≥72 so label and value both breathe

Invent layout freely; use only facts/numbers present in the data digest.
""".strip()
