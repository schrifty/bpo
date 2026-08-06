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
                if label:
                    _box(reqs, f"{oid}_kl{j}", sid, cx + 6, y + 6, cell_w - 12, 16, label)
                    reqs.append(
                        {
                            "updateTextStyle": {
                                "objectId": f"{oid}_kl{j}",
                                "style": {
                                    "fontFamily": FONT,
                                    "fontSize": {"magnitude": 10, "unit": "PT"},
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
                    _box(reqs, f"{oid}_kv{j}", sid, cx + 6, y + 24, cell_w - 12, h - 30, value)
                    reqs.append(
                        {
                            "updateTextStyle": {
                                "objectId": f"{oid}_kv{j}",
                                "style": {
                                    "fontFamily": FONT,
                                    "fontSize": {"magnitude": _clamp(_f(it.get("value_size"), 20), 12, 36), "unit": "PT"},
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
            n_rows = len(norm_rows)
            n_cols = max(len(r) for r in norm_rows)
            for r in norm_rows:
                while len(r) < n_cols:
                    r.append("")
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
- Size tables to fit content: ~20pt height per row
- Max 8 data rows; max 5 columns; keep cells ≤35 chars
- For readability use compact dimensions, not oversized

KPI row guidance:
- Use "fill" on items for colored tile backgrounds (#E8F4FC, #EEF0F3, #AEFFF6)
- Use "color" on items for accent-colored values (#009AFF, #38C0CE)

Invent layout freely; use only facts/numbers present in the data digest.
""".strip()
