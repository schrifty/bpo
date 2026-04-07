"""Extract yellow/orange template styling hints from Google Slides and summarize via LLM (QBR flow).

Convention (authoring guide):
  • Yellow(-ish) text or **highlight** (foreground or background on a run) → fields to refresh.
  • Orange **filled** shapes/cells, or orange **foreground** coaching lines → remove after hints are logged.

After logging + LLM analysis, orange coaching shapes are removed from the deck (entire box), orange table
cells are cleared, fill-in (yellow-styled) text is replaced with ``[???]``, and a red banner summarizes what
actually changed on that slide. Surface cleanup runs for **every** slide slated for adaptation, not only
slides where extraction found strings (so we never skip removals when the LLM batch is empty).

Yellow→``[???]`` inserts clear text highlight immediately (Slides otherwise keeps highlight on the new run).
After ``adapt_custom_slides``, ``replaceAllText`` preserves character styles, so a second pass strips any
remaining yellow highlight / orange coaching text color and clears orange table-cell fills / deletes any
orange coaching shapes the first pass missed.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from googleapiclient.errors import HttpError

from .config import LLM_MODEL_FAST, logger
from .evaluate import (
    _add_incomplete_banner,
    _extract_text,
    _llm_create_with_retry,
    _strip_json_code_fence,
)
from .slides_client import slides_presentations_batch_update

YELLOW_FIELD_PLACEHOLDER = "[???]"


# Zero-width / format chars Slides often keeps on styled runs (no visible [???] when "replaced").
_SPAN_INVISIBLE_CHARS = re.compile(
    r"[\u200b-\u200f\u202a-\u202e\u2060\u2066-\u2069\ufeff\u00ad]+"
)
# Collapse exotic Unicode spaces (thin/hair/figure/NNBSP/ideographic, etc.) — slides 28–29 often use these.
_WS_RUN = re.compile(
    r"[\s\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+",
    re.UNICODE,
)


def _clean_segment_for_hints(s: str) -> str:
    """NBSP → space; drop ZWSP/BOM/etc.; collapse all Unicode spacing; strip."""
    t = _SPAN_INVISIBLE_CHARS.sub("", (s or "").replace("\u00a0", " "))
    t = _WS_RUN.sub(" ", t)
    return t.strip()


def _span_has_visible_text(content: str) -> bool:
    """True if the run has characters that are visibly more than padding (not NBSP/ZWSP-only)."""
    return bool(_clean_segment_for_hints(content))


def _rgb_tuple(rgb: dict | None) -> tuple[float, float, float] | None:
    if not rgb or not isinstance(rgb, dict):
        return None
    return (
        float(rgb.get("red", 0)),
        float(rgb.get("green", 0)),
        float(rgb.get("blue", 0)),
    )


def _foreground_rgb_from_run_style(style: dict | None) -> tuple[float, float, float] | None:
    if not style:
        return None
    fc = style.get("foregroundColor") or {}
    oc = fc.get("opaqueColor") or {}
    if "rgbColor" in oc:
        return _rgb_tuple(oc["rgbColor"])
    return None


# When the API omits rgbColor, themeColor still marks template styling (varies by master theme).
_ORANGE_COACHING_FILL_THEMES = frozenset({"ACCENT2", "ACCENT3", "ACCENT6"})
_ORANGE_COACHING_TEXT_THEMES = frozenset({"ACCENT2", "ACCENT3", "ACCENT6"})
# Table/shape fill + text highlight theme slots Slides uses for “marker” yellow (varies by master).
_YELLOW_FIELD_THEME_HINTS = frozenset({"ACCENT4", "ACCENT5", "ACCENT6", "LIGHT2"})


def _solid_fill_color_rgb_and_theme(fill: dict | None) -> tuple[tuple[float, float, float] | None, str | None]:
    """RGB and/or theme from shapeBackgroundFill / tableCellBackgroundFill solidFill."""
    if not fill:
        return None, None
    sf = fill.get("solidFill") or {}
    col = sf.get("color") or {}
    if "rgbColor" in col:
        return _rgb_tuple(col["rgbColor"]), None
    tc = col.get("themeColor")
    if tc:
        return None, str(tc)
    return None, None


def _solid_fill_rgb(fill: dict | None) -> tuple[float, float, float] | None:
    rgb, _ = _solid_fill_color_rgb_and_theme(fill)
    return rgb


def _shape_fill_suggests_orange_coaching(shape: dict) -> bool:
    sp = shape.get("shapeProperties") or {}
    bg = sp.get("shapeBackgroundFill") or {}
    if bg.get("propertyState") == "NOT_RENDERED":
        return False
    rgb, theme = _solid_fill_color_rgb_and_theme(bg)
    if rgb and is_orange_fill(rgb):
        return True
    if theme and theme in _ORANGE_COACHING_FILL_THEMES:
        return True
    return False


def _cell_fill_suggests_orange_coaching(cell: dict) -> bool:
    tcp = cell.get("tableCellProperties") or {}
    tbf = tcp.get("tableCellBackgroundFill") or {}
    rgb, theme = _solid_fill_color_rgb_and_theme(tbf)
    if rgb and is_orange_fill(rgb):
        return True
    if theme and theme in _ORANGE_COACHING_FILL_THEMES:
        return True
    return False


def _fill_suggests_yellow_template(
    rgb: tuple[float, float, float] | None, theme: str | None
) -> bool:
    """Yellow table/shape cell fill (not the same as orange coaching)."""
    if rgb and is_yellow_foreground(rgb):
        return True
    if theme and theme in _YELLOW_FIELD_THEME_HINTS:
        return True
    return False


def _shape_fill_suggests_yellow_template(shape: dict) -> bool:
    sp = shape.get("shapeProperties") or {}
    bg = sp.get("shapeBackgroundFill") or {}
    if bg.get("propertyState") == "NOT_RENDERED":
        return False
    rgb, theme = _solid_fill_color_rgb_and_theme(bg)
    return _fill_suggests_yellow_template(rgb, theme)


def _cell_fill_suggests_yellow_template(cell: dict) -> bool:
    tcp = cell.get("tableCellProperties") or {}
    tbf = tcp.get("tableCellBackgroundFill") or {}
    rgb, theme = _solid_fill_color_rgb_and_theme(tbf)
    return _fill_suggests_yellow_template(rgb, theme)


def is_yellow_foreground(rgb: tuple[float, float, float]) -> bool:
    """Heuristic for template 'adapt this value' yellow text or highlight (tolerates theme variance)."""
    r, g, b = rgb
    # Strong yellow: high R+G, B clearly lower (includes many Slides highlight swatches)
    if r >= 0.72 and g >= 0.72 and b <= 0.55:
        return True
    # Pale / lemon highlighter (Slides default — blue channel often 0.55–0.75)
    if r >= 0.78 and g >= 0.78 and b <= 0.82 and (r + g - b) >= 0.85:
        return True
    # Gold / dark yellow
    if r >= 0.85 and g >= 0.65 and b <= 0.35:
        return True
    # Cream / soft highlight
    if r >= 0.82 and g >= 0.78 and b <= 0.72 and b < min(r, g) - 0.08:
        return True
    return False


def is_orange_fill(rgb: tuple[float, float, float]) -> bool:
    """Heuristic for orange instruction boxes."""
    r, g, b = rgb
    if r < 0.72:
        return False
    if g < 0.22 or g > 0.82:
        return False
    if b > 0.38:
        return False
    return True


def is_orange_foreground(rgb: tuple[float, float, float]) -> bool:
    """Orange / amber **text** (coaching captions), slightly wider than fill heuristic."""
    r, g, b = rgb
    if r < 0.55:
        return False
    if g < 0.12 or g > 0.72:
        return False
    if b > 0.55:
        return False
    return r > g and r > b


def _run_is_template_yellow_field(style: dict) -> bool:
    """Yellow fill-in field: RGB on foreground or highlight, or common theme slots when rgb omitted."""
    for key in ("foregroundColor", "backgroundColor"):
        bucket = style.get(key) or {}
        oc = bucket.get("opaqueColor") or {}
        if "rgbColor" in oc:
            t = _rgb_tuple(oc["rgbColor"])
            if t and is_yellow_foreground(t):
                return True
        tc = oc.get("themeColor")
        if tc and str(tc) in _YELLOW_FIELD_THEME_HINTS:
            return True
    return False


def _run_has_any_opaque_text_highlight(style: dict) -> bool:
    """True if this run has any text background (RGB or theme). Used post-adapt: replaceAllText keeps highlights."""
    bg = style.get("backgroundColor") or {}
    oc = bg.get("opaqueColor") or {}
    if not oc:
        return False
    if "rgbColor" in oc:
        return True
    tc = oc.get("themeColor")
    return bool(tc) and str(tc) != "THEME_COLOR_TYPE_UNSPECIFIED"


def _run_should_strip_text_highlight_post_adapt(style: dict) -> bool:
    return _run_is_template_yellow_field(style) or _run_has_any_opaque_text_highlight(style)


def _run_is_orange_coaching_text(style: dict) -> bool:
    """Orange coaching as **text color** (not only filled boxes)."""
    bucket = style.get("foregroundColor") or {}
    oc = bucket.get("opaqueColor") or {}
    if "rgbColor" in oc:
        t = _rgb_tuple(oc["rgbColor"])
        if t and is_orange_foreground(t):
            return True
    tc = oc.get("themeColor")
    if tc and str(tc) in _ORANGE_COACHING_TEXT_THEMES:
        return True
    return False


def _orange_coaching_text_segments(text_body: dict) -> list[str]:
    out: list[str] = []
    for _a, _b, content, style in iter_text_run_spans(text_body):
        if _run_is_orange_coaching_text(style):
            t = (content or "").strip()
            if t:
                out.append(t)
    return out


def _utf16_code_units(s: str) -> int:
    """Length in UTF-16 code units (Google Slides text indices)."""
    return len(s.encode("utf-16-le")) // 2


def iter_text_run_spans(text_body: dict) -> list[tuple[int, int, str, dict]]:
    """(start, end, content, style) per textRun.

    Must use the same index walk as ``_text_body_max_exclusive_index`` (paragraph markers
    consume one UTF-16 index each). Skipping markers but advancing ``pos`` only on runs
    produced spans past the real document end and caused deleteText 400s on short cells.
    """
    spans: list[tuple[int, int, str, dict]] = []
    pos = 0
    for te in text_body.get("textElements", []):
        si = te.get("startIndex")
        if si is not None:
            pos = int(si)
        tr = te.get("textRun")
        if tr:
            content = tr.get("content") or ""
            clen = _utf16_code_units(content)
            start = pos
            end = start + clen
            spans.append((start, end, content, tr.get("style") or {}))
            pos = end
        elif te.get("paragraphMarker") is not None:
            pos += 1
    return spans


def _req_clear_template_text_cue_style(
    object_id: str,
    cell_location: dict[str, int] | None,
    start: int,
    end: int,
    *,
    orange_text: bool,
) -> dict[str, Any]:
    """Strip yellow highlight / orange coaching from a text range (post-replaceAllText safe)."""
    style: dict[str, Any] = {"backgroundColor": {}}
    fields = ["backgroundColor"]
    if orange_text:
        style["foregroundColor"] = {
            "opaqueColor": {"rgbColor": {"red": 0.12, "green": 0.16, "blue": 0.22}}
        }
        fields.append("foregroundColor")
    uts: dict[str, Any] = {
        "objectId": object_id,
        "textRange": {"type": "FIXED_RANGE", "startIndex": start, "endIndex": end},
        "style": style,
        "fields": ",".join(fields),
    }
    if cell_location is not None:
        uts["cellLocation"] = {
            "rowIndex": int(cell_location["rowIndex"]),
            "columnIndex": int(cell_location["columnIndex"]),
        }
    return {"updateTextStyle": uts}


@dataclass
class _HintMutation:
    object_id: str
    cell_location: dict[str, int] | None  # rowIndex, columnIndex for tables
    action: str  # "delete_shape" | "clear_all" | "clear_shape_fill" | "clear_cell_fill" | "replace"
    start: int = 0
    end: int = 0
    replacement: str = YELLOW_FIELD_PLACEHOLDER


def _mutations_from_shape(shape_el: dict) -> list[_HintMutation]:
    oid = shape_el.get("objectId", "")
    if not oid:
        return []
    shape = shape_el.get("shape") or {}
    if _shape_fill_suggests_orange_coaching(shape):
        return [_HintMutation(oid, None, "delete_shape")]

    out: list[_HintMutation] = []
    if _shape_fill_suggests_yellow_template(shape):
        out.append(_HintMutation(oid, None, "clear_shape_fill"))

    text_body = shape.get("text") or {}
    if not text_body.get("textElements"):
        return out

    for start, end, content, style in iter_text_run_spans(text_body):
        if not _span_has_visible_text(content):
            continue
        if _run_is_orange_coaching_text(style) and end > start:
            out.append(_HintMutation(oid, None, "replace", start, end, ""))
            continue
        if _run_is_template_yellow_field(style) and end > start:
            out.append(_HintMutation(oid, None, "replace", start, end, YELLOW_FIELD_PLACEHOLDER))
    return out


def _mutations_from_table(table_el: dict) -> list[_HintMutation]:
    oid = table_el.get("objectId", "")
    if not oid:
        return []
    out: list[_HintMutation] = []
    table = table_el.get("table") or {}
    for ri, row in enumerate(table.get("tableRows", [])):
        for ci, cell in enumerate(row.get("tableCells", [])):
            text_body = cell.get("text") or {}
            cell_loc = {"rowIndex": ri, "columnIndex": ci}
            if _cell_fill_suggests_orange_coaching(cell):
                out.append(_HintMutation(oid, cell_loc, "clear_all"))
                continue
            if _cell_fill_suggests_yellow_template(cell):
                out.append(_HintMutation(oid, cell_loc, "clear_cell_fill"))
            if not text_body.get("textElements"):
                continue
            for start, end, content, style in iter_text_run_spans(text_body):
                if not _span_has_visible_text(content):
                    continue
                if _run_is_orange_coaching_text(style) and end > start:
                    out.append(_HintMutation(oid, cell_loc, "replace", start, end, ""))
                    continue
                if _run_is_template_yellow_field(style) and end > start:
                    out.append(
                        _HintMutation(oid, cell_loc, "replace", start, end, YELLOW_FIELD_PLACEHOLDER)
                    )
    return out


def collect_hint_mutations_from_slide(slide: dict) -> list[_HintMutation]:
    """Structural edits: remove orange shapes, clear orange table cells; replace yellow runs."""
    muts: list[_HintMutation] = []

    def walk(elements: list[dict]) -> None:
        for el in elements:
            if el.get("elementGroup"):
                walk(el["elementGroup"].get("children") or [])
                continue
            if el.get("shape"):
                muts.extend(_mutations_from_shape(el))
            if el.get("table"):
                muts.extend(_mutations_from_table(el))

    walk(slide.get("pageElements") or [])
    return muts


def qbr_hint_banner_text_for_mutations(muts: list[_HintMutation]) -> str:
    """Banner line that matches edits actually queued (avoid claiming [???] when none)."""
    n_placeholder = sum(
        1 for m in muts
        if m.action == "replace" and m.replacement == YELLOW_FIELD_PLACEHOLDER
    )
    n_orange = sum(
        1 for m in muts
        if m.action in ("delete_shape", "clear_all", "clear_shape_fill", "clear_cell_fill")
        or (m.action == "replace" and m.replacement == "")
    )
    if n_placeholder and n_orange:
        return (
            "MODIFIED — orange coaching removed; fill-in fields replaced with [???] — verify before presenting"
        )
    if n_placeholder:
        return "MODIFIED — fill-in fields replaced with [???] — verify before presenting"
    if n_orange:
        return "MODIFIED — orange coaching text/boxes removed — verify before presenting"
    return "MODIFIED — template cues processed — verify before presenting"


def _cell_key(cell: dict[str, int] | None) -> tuple[tuple[str, int], ...]:
    if not cell:
        return ()
    return tuple(sorted(cell.items()))


def _merge_touching_replace_mutations(repl: list[_HintMutation]) -> list[_HintMutation]:
    """Merge overlapping or adjacent replace spans that share the same replacement string.

    ``batchUpdate`` applies requests in order; overlapping ``deleteText`` ranges built from the
    same snapshot cause index drift and Slides 400s (end index past length, invalid start).
    """
    if len(repl) <= 1:
        return repl
    by_rep: dict[str, list[_HintMutation]] = defaultdict(list)
    for m in repl:
        by_rep[m.replacement].append(m)
    out: list[_HintMutation] = []
    for rep, group in by_rep.items():
        group.sort(key=lambda m: (m.start, m.end))
        cur_s = group[0].start
        cur_e = group[0].end
        oid = group[0].object_id
        cell = group[0].cell_location
        for m in group[1:]:
            if m.start <= cur_e:
                cur_e = max(cur_e, m.end)
            else:
                out.append(_HintMutation(oid, cell, "replace", cur_s, cur_e, rep))
                cur_s, cur_e = m.start, m.end
        out.append(_HintMutation(oid, cell, "replace", cur_s, cur_e, rep))
    return out


def _find_text_body_for_hint_target(
    slide: dict,
    object_id: str,
    cell_location: dict[str, int] | None,
) -> dict | None:
    """Return shape.text or table cell text dict for mutation target."""

    def walk(elements: list[dict]) -> dict | None:
        for el in elements or []:
            if el.get("elementGroup"):
                found = walk(el["elementGroup"].get("children") or [])
                if found is not None:
                    return found
            sh = el.get("shape") or {}
            # Mutations use the page element's objectId (see ``_mutations_from_shape(el)``);
            # Slides often omit objectId on the nested ``shape`` dict — match both.
            shape_oid = el.get("objectId") or sh.get("objectId")
            if shape_oid == object_id and cell_location is None:
                return sh.get("text") or {}
            tb = el.get("table") or {}
            table_oid = el.get("objectId") or tb.get("objectId")
            if table_oid == object_id and cell_location is not None:
                ri = cell_location["rowIndex"]
                ci = cell_location["columnIndex"]
                rows = tb.get("tableRows") or []
                if ri < len(rows):
                    cells = rows[ri].get("tableCells") or []
                    if ci < len(cells):
                        return cells[ci].get("text") or {}
        return None

    return walk(slide.get("pageElements") or [])


def _text_body_walk_end_exclusive(text_body: dict) -> int:
    """Exclusive end index from the same walk as ``iter_text_run_spans`` (UTF-16).

    ``startIndex`` resets the cursor; each ``textRun`` advances by its content length;
    each ``paragraphMarker`` advances by 1.  This matches how we compute mutation spans.
    """
    pos = 0
    for te in text_body.get("textElements", []):
        si = te.get("startIndex")
        if si is not None:
            pos = int(si)
        if te.get("textRun"):
            c = te["textRun"].get("content") or ""
            pos += _utf16_code_units(c)
        elif te.get("paragraphMarker") is not None:
            pos += 1
    return pos


def _text_body_last_text_run_exclusive_end(text_body: dict) -> int:
    """Position after the last textRun (UTF-16 walk); ignores trailing paragraphMarker tail.

    Slides' ``deleteText`` "existing text length" often matches this value while our full
    ``walk_end`` includes a terminal structural paragraph index (+1), causing 400s when
    merged replace spans end at ``walk_end`` (e.g. end 150 vs length 149).
    """
    pos = 0
    last_run_end = 0
    for te in text_body.get("textElements") or []:
        si = te.get("startIndex")
        if si is not None:
            pos = int(si)
        if te.get("textRun"):
            c = te["textRun"].get("content") or ""
            pos += _utf16_code_units(c)
            last_run_end = pos
        elif te.get("paragraphMarker") is not None:
            pos += 1
    return last_run_end


def _text_body_max_exclusive_index(text_body: dict) -> int:
    """Exclusive end index upper bound safe for ``deleteText`` / ``updateTextStyle`` (UTF-16).

    Google sometimes returns ``endIndex`` values that match our positional ``walk_end`` while
    ``deleteText`` still rejects the range (400: end index > existing text length). Typical
    cause: a **terminal paragraph marker** (or an extra structural index) is counted in
    ``walk_end`` / ``endIndex`` but is not part of the deletable UTF-16 span Slides reports
    as "text length".

    We take the minimum of: walk end, max API ``endIndex``, and the **end of the last
    textRun span** from the same walk as ``iter_text_run_spans`` — that last value matches
    deletable content even when the JSON omits a trailing ``paragraphMarker`` element.
    """
    elements = text_body.get("textElements") or []
    walk_end = _text_body_walk_end_exclusive(text_body)
    spans = iter_text_run_spans(text_body)
    run_tail = max((e for _, e, _, _ in spans), default=0)
    api_max = 0
    for te in elements:
        ei = te.get("endIndex")
        if ei is not None:
            api_max = max(api_max, int(ei))
    if api_max > 0 and walk_end > 0:
        cap = min(api_max, walk_end)
    elif walk_end > 0:
        cap = walk_end
    else:
        cap = api_max
    if run_tail > 0:
        cap = min(cap, run_tail)
    if elements and elements[-1].get("paragraphMarker") is not None and not elements[-1].get("textRun"):
        cap = min(cap, max(0, walk_end - 1))
    # When every textRun includes endIndex, the largest matches Slides' document tail (helps 400s
    # where walk_end and a lone textRun disagree by one UTF-16 unit).
    run_ends: list[int] = []
    for te in elements:
        if te.get("textRun") is not None and te.get("endIndex") is not None:
            run_ends.append(int(te["endIndex"]))
    n_text_runs = sum(1 for te in elements if te.get("textRun"))
    if run_ends and len(run_ends) == n_text_runs:
        cap = min(cap, max(run_ends))
    lr = _text_body_last_text_run_exclusive_end(text_body)
    if lr > 0:
        cap = min(cap, lr)
    return cap


def _clamp_utf16_range_to_text_body(
    text_body: dict | None, start: int, end: int
) -> tuple[int, int] | None:
    """Clamp [start, end) for deleteText/updateTextStyle; return None if empty after clamp."""
    if start >= end:
        return None
    # Without structural text from the Slides payload we cannot bound indices — emitting
    # raw ranges caused batchUpdate 400s (e.g. start past cell length).
    if not text_body or not text_body.get("textElements"):
        return None
    mx = _text_body_max_exclusive_index(text_body)
    end = min(int(end), mx)
    start = max(0, min(int(start), end))
    if start >= end:
        return None
    return start, end


def hint_mutations_to_batch_requests(
    page_object_id: str,
    muts: list[_HintMutation],
    *,
    add_banner: bool = True,
    slide: dict | None = None,
) -> tuple[list[list[dict[str, Any]]], int]:
    """Return (batches, content_request_count excluding banner).

    ``batches`` must be applied **in order** via separate ``batchUpdate`` calls. Slides applies
    each request sequentially; multiple ``deleteText``/``insertText`` pairs in one batch leave
    later indices stale vs the snapshot (400: end index past length / start past length).
    """
    if not muts:
        return [], 0
    groups: dict[tuple[str, tuple], list[_HintMutation]] = defaultdict(list)
    for m in muts:
        key = (m.object_id, _cell_key(m.cell_location))
        groups[key].append(m)

    prefix_reqs: list[dict[str, Any]] = []
    replace_batches: list[list[dict[str, Any]]] = []
    for (oid, cell_tup), items in groups.items():
        cell = dict(cell_tup) if cell_tup else None
        if any(x.action == "delete_shape" for x in items):
            prefix_reqs.append({"deleteObject": {"objectId": oid}})
            continue
        if any(x.action == "clear_all" for x in items):
            text_body_clear = (
                _find_text_body_for_hint_target(slide, oid, cell)
                if slide is not None
                else None
            )
            mx_clear = (
                _text_body_max_exclusive_index(text_body_clear)
                if text_body_clear and text_body_clear.get("textElements")
                else 0
            )
            if mx_clear <= 0:
                logger.debug(
                    "QBR hint skip clear_all (empty cell): oid=%s cell=%s",
                    (oid or "")[:16],
                    cell,
                )
                continue
            dt: dict[str, Any] = {
                "deleteText": {
                    "objectId": oid,
                    "textRange": {"type": "ALL"},
                }
            }
            if cell is not None:
                dt["deleteText"]["cellLocation"] = {
                    "rowIndex": cell["rowIndex"],
                    "columnIndex": cell["columnIndex"],
                }
            prefix_reqs.append(dt)
            continue

        for x in items:
            if x.action == "clear_shape_fill":
                prefix_reqs.append({
                    "updateShapeProperties": {
                        "objectId": oid,
                        "shapeProperties": {
                            "shapeBackgroundFill": {"propertyState": "NOT_RENDERED"},
                        },
                        "fields": "shapeBackgroundFill",
                    }
                })
            elif x.action == "clear_cell_fill" and cell is not None:
                prefix_reqs.append({
                    "updateTableCellProperties": {
                        "objectId": oid,
                        "tableRange": {
                            "location": {
                                "rowIndex": cell["rowIndex"],
                                "columnIndex": cell["columnIndex"],
                            },
                            "rowSpan": 1,
                            "columnSpan": 1,
                        },
                        "tableCellProperties": {
                            "tableCellBackgroundFill": {
                                "propertyState": "NOT_RENDERED",
                            },
                        },
                        "fields": "tableCellBackgroundFill",
                    }
                })

        repl = _merge_touching_replace_mutations([x for x in items if x.action == "replace"])
        repl.sort(key=lambda x: x.start, reverse=True)
        text_body = (
            _find_text_body_for_hint_target(slide, oid, cell)
            if slide is not None
            else None
        )
        for x in repl:
            cl = _clamp_utf16_range_to_text_body(text_body, x.start, x.end)
            if cl is None:
                logger.debug(
                    "QBR hint skip replace (empty or out-of-range): oid=%s cell=%s range was %s-%s",
                    oid[:12],
                    cell,
                    x.start,
                    x.end,
                )
                continue
            start_i, end_i = cl
            base_del: dict[str, Any] = {
                "objectId": oid,
                "textRange": {
                    "type": "FIXED_RANGE",
                    "startIndex": start_i,
                    "endIndex": end_i,
                },
            }
            del_req: dict[str, Any] = {"deleteText": dict(base_del)}
            if cell is not None:
                del_req["deleteText"]["cellLocation"] = {
                    "rowIndex": cell["rowIndex"],
                    "columnIndex": cell["columnIndex"],
                }
            ins_req: dict[str, Any] = {
                "insertText": {
                    "objectId": oid,
                    "insertionIndex": start_i,
                    "text": x.replacement,
                }
            }
            if cell is not None:
                ins_req["insertText"]["cellLocation"] = {
                    "rowIndex": cell["rowIndex"],
                    "columnIndex": cell["columnIndex"],
                }
            one_replace: list[dict[str, Any]] = [del_req]
            if x.replacement:
                one_replace.append(ins_req)
                ins_len = _utf16_code_units(x.replacement)
                if ins_len > 0 and x.replacement == YELLOW_FIELD_PLACEHOLDER:
                    style_end = start_i + ins_len
                    if text_body is not None:
                        mx0 = _text_body_max_exclusive_index(text_body)
                        new_len = mx0 - (end_i - start_i) + ins_len
                        style_end = min(style_end, new_len)
                    if start_i < style_end:
                        one_replace.append(
                            _req_clear_template_text_cue_style(
                                oid, cell, start_i, style_end, orange_text=False
                            )
                        )
            replace_batches.append(one_replace)

    batches: list[list[dict[str, Any]]] = []
    if prefix_reqs:
        batches.append(prefix_reqs)
    batches.extend(replace_batches)
    content_n = sum(len(b) for b in batches)
    if add_banner:
        banner = _add_incomplete_banner(
            page_object_id,
            banner_text=qbr_hint_banner_text_for_mutations(muts),
        )
        if batches:
            batches[-1].extend(banner)
        else:
            batches.append(list(banner))
    return batches, content_n


def build_post_adapt_template_style_strip_requests(slide: dict) -> list[dict[str, Any]]:
    """Clear yellow highlight / orange coaching text styles; drop orange coaching shapes; clear orange cell fills.

    ``adapt_custom_slides`` uses ``replaceAllText``, which keeps the old run's highlight and colors — this
    pass removes those template cues without changing the adapted wording.
    """

    def walk(elements: list[dict], out: list[dict[str, Any]]) -> None:
        for el in elements or []:
            if el.get("elementGroup"):
                walk(el["elementGroup"].get("children") or [], out)
                continue
            if el.get("shape"):
                oid = el.get("objectId") or (el.get("shape") or {}).get("objectId")
                if not oid:
                    continue
                shape = el.get("shape") or {}
                if _shape_fill_suggests_orange_coaching(shape):
                    out.append({"deleteObject": {"objectId": oid}})
                    continue
                if _shape_fill_suggests_yellow_template(shape):
                    out.append({
                        "updateShapeProperties": {
                            "objectId": oid,
                            "shapeProperties": {
                                "shapeBackgroundFill": {"propertyState": "NOT_RENDERED"},
                            },
                            "fields": "shapeBackgroundFill",
                        }
                    })
                text_body = shape.get("text") or {}
                for start, end, content, style in iter_text_run_spans(text_body):
                    if not _span_has_visible_text(content):
                        continue
                    cl = _clamp_utf16_range_to_text_body(text_body, start, end)
                    if cl is None:
                        continue
                    s0, e0 = cl
                    if _run_is_orange_coaching_text(style):
                        out.append(
                            _req_clear_template_text_cue_style(
                                oid, None, s0, e0, orange_text=True
                            )
                        )
                    elif _run_should_strip_text_highlight_post_adapt(style):
                        out.append(
                            _req_clear_template_text_cue_style(
                                oid, None, s0, e0, orange_text=False
                            )
                        )
            if el.get("table"):
                oid = el.get("objectId") or (el.get("table") or {}).get("objectId")
                if not oid:
                    continue
                table = el.get("table") or {}
                for ri, row in enumerate(table.get("tableRows", [])):
                    for ci, cell in enumerate(row.get("tableCells", [])):
                        cell_loc = {"rowIndex": ri, "columnIndex": ci}
                        if _cell_fill_suggests_orange_coaching(cell):
                            out.append({
                                "updateTableCellProperties": {
                                    "objectId": oid,
                                    "tableRange": {
                                        "location": {
                                            "rowIndex": ri,
                                            "columnIndex": ci,
                                        },
                                        "rowSpan": 1,
                                        "columnSpan": 1,
                                    },
                                    "tableCellProperties": {
                                        "tableCellBackgroundFill": {
                                            "propertyState": "NOT_RENDERED",
                                        },
                                    },
                                    "fields": "tableCellBackgroundFill",
                                }
                            })
                        elif _cell_fill_suggests_yellow_template(cell):
                            out.append({
                                "updateTableCellProperties": {
                                    "objectId": oid,
                                    "tableRange": {
                                        "location": {
                                            "rowIndex": ri,
                                            "columnIndex": ci,
                                        },
                                        "rowSpan": 1,
                                        "columnSpan": 1,
                                    },
                                    "tableCellProperties": {
                                        "tableCellBackgroundFill": {
                                            "propertyState": "NOT_RENDERED",
                                        },
                                    },
                                    "fields": "tableCellBackgroundFill",
                                }
                            })
                        text_body = cell.get("text") or {}
                        for start, end, content, style in iter_text_run_spans(text_body):
                            if not _span_has_visible_text(content):
                                continue
                            cl = _clamp_utf16_range_to_text_body(text_body, start, end)
                            if cl is None:
                                continue
                            s0, e0 = cl
                            if _run_is_orange_coaching_text(style):
                                out.append(
                                    _req_clear_template_text_cue_style(
                                        oid, cell_loc, s0, e0, orange_text=True
                                    )
                                )
                            elif _run_should_strip_text_highlight_post_adapt(style):
                                out.append(
                                    _req_clear_template_text_cue_style(
                                        oid, cell_loc, s0, e0, orange_text=False
                                    )
                                )

    out_reqs: list[dict[str, Any]] = []
    walk(slide.get("pageElements") or [], out_reqs)
    return out_reqs


def apply_qbr_template_style_strip_after_adapt(
    slides_svc: Any,
    pres_id: str,
    page_object_ids: list[str],
) -> int:
    """Re-read the deck and strip template authoring colors after data adaptation."""
    if not page_object_ids:
        return 0
    try:
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    except HttpError as e:
        logger.warning("QBR post-adapt style strip: get presentation failed: %s", e)
        return 0
    by_id = {s["objectId"]: s for s in pres.get("slides", [])}
    total_reqs = 0
    slides_touched = 0
    for pid in page_object_ids:
        slide = by_id.get(pid)
        if not slide:
            continue
        slide_reqs = build_post_adapt_template_style_strip_requests(slide)
        if not slide_reqs:
            continue
        try:
            slides_presentations_batch_update(slides_svc, pres_id, slide_reqs)
            total_reqs += len(slide_reqs)
            slides_touched += 1
        except HttpError as e:
            logger.warning(
                "QBR post-adapt style strip: batchUpdate failed for slide %s (%d req): %s",
                pid[:16],
                len(slide_reqs),
                e,
            )
    if total_reqs == 0:
        logger.warning(
            "QBR post-adapt style strip: generated 0 successful Slides requests across %d slide(s) — "
            "if yellow/orange cues remain, the API may be omitting colors or using an untracked style",
            len(page_object_ids),
        )
        return 0
    logger.info(
        "QBR post-adapt: stripped template yellow/orange styling (%d request(s)) on %d slide(s)",
        total_reqs,
        slides_touched,
    )
    return total_reqs


def apply_hint_mutations_to_presentation(
    slides_svc: Any,
    pres_id: str,
    hint_rows: list[dict[str, Any]],
    slide_by_id: dict[str, dict],
    *,
    title_slide_object_id: str | None = None,
) -> int:
    """Apply orange shape removal, orange cell text clears, yellow→[???], and optional banner.

    Pass **every** adapt slide row (same order as ``build_hint_rows_for_adapt_slides``), not only
    rows that had extracted yellow/orange strings. Extraction can miss cues the API still exposes
    in structure we use for ``collect_hint_mutations_from_slide``; scanning all slides keeps
    cleanup consistent.
    """
    n_slides = 0
    total_reqs = 0
    total_batch_calls = 0
    for row in hint_rows:
        pid = row.get("object_id")
        slide = slide_by_id.get(pid) if pid else None
        if not slide:
            continue
        muts = collect_hint_mutations_from_slide(slide)
        if not muts:
            logger.debug("QBR adapt hints — slide %s: no structural mutations (extraction had text but no API spans?)",
                         row.get("slide_num", "?"))
            continue
        add_banner = title_slide_object_id is None or pid != title_slide_object_id
        batches, content_n = hint_mutations_to_batch_requests(
            pid, muts, add_banner=add_banner, slide=slide
        )
        if not batches:
            continue
        slide_reqs = 0
        slide_batches = 0
        try:
            for sub in batches:
                if not sub:
                    continue
                slides_presentations_batch_update(slides_svc, pres_id, sub)
                slide_reqs += len(sub)
                slide_batches += 1
            n_slides += 1
            total_reqs += slide_reqs
            total_batch_calls += slide_batches
            logger.debug(
                "QBR adapt hints — slide %s: applied %d text mutation(s)%s (%d batchUpdate call(s))",
                row.get("slide_num", "?"),
                content_n,
                " + red banner" if add_banner else " (no banner — title slide)",
                slide_batches,
            )
        except HttpError as e:
            logger.warning(
                "QBR adapt hints: batchUpdate failed for slide %s (slide_num=%s, %d req in failed batch): %s",
                (pid or "")[:16],
                row.get("slide_num", "?"),
                slide_reqs or sum(len(s) for s in batches),
                e,
            )

    if total_reqs == 0:
        return 0
    logger.info(
        "QBR adapt hints: applied surface changes (%d request(s)) across %d slide(s) (%d batchUpdate call(s))",
        total_reqs,
        n_slides,
        total_batch_calls,
    )
    return n_slides


def _merge_runs_by_yellow(text_body: dict) -> tuple[list[str], list[str]]:
    """Split text into yellow segments vs other (non-yellow) from textRuns.

    Returns (yellow_segments, other_segments) as stripped non-empty strings.
    """
    yellow_parts: list[str] = []
    other_parts: list[str] = []
    cur_y: list[str] = []
    cur_o: list[str] = []

    def flush_y():
        nonlocal cur_y
        if cur_y:
            cleaned = _clean_segment_for_hints("".join(cur_y))
            if cleaned:
                yellow_parts.append(cleaned)
            cur_y = []

    def flush_o():
        nonlocal cur_o
        if cur_o:
            cleaned = _clean_segment_for_hints("".join(cur_o))
            if cleaned:
                other_parts.append(cleaned)
            cur_o = []

    for te in text_body.get("textElements", []):
        tr = te.get("textRun")
        if not tr:
            flush_y()
            flush_o()
            continue
        content = tr.get("content") or ""
        style = tr.get("style") or {}
        is_y = _run_is_template_yellow_field(style)
        if is_y:
            flush_o()
            cur_y.append(content)
        else:
            flush_y()
            cur_o.append(content)
    flush_y()
    flush_o()
    return yellow_parts, other_parts


def _text_from_body(text_body: dict) -> str:
    parts: list[str] = []
    for te in text_body.get("textElements", []):
        tr = te.get("textRun")
        if tr:
            parts.append(tr.get("content") or "")
    return "".join(parts).strip()


_ORANGE_SHAPE_EMPTY_MARKER = "[empty orange coaching box]"


def _extract_from_shape(shape_el: dict) -> tuple[list[str], list[str]]:
    """Return (yellow_segments, orange_segments) for one shape element."""
    shape = shape_el.get("shape") or {}
    text_body = shape.get("text") or {}
    if _shape_fill_suggests_orange_coaching(shape):
        full = _text_from_body(text_body) if text_body.get("textElements") else ""
        stripped = full.strip()
        orange_segs = [stripped] if stripped else [_ORANGE_SHAPE_EMPTY_MARKER]
        return [], orange_segs

    if not text_body.get("textElements"):
        return [], []

    yellow_parts, _ = _merge_runs_by_yellow(text_body)
    orange_txt = _orange_coaching_text_segments(text_body)
    return yellow_parts, orange_txt


def _extract_from_table(table_el: dict) -> tuple[list[str], list[str]]:
    yellow_all: list[str] = []
    orange_all: list[str] = []
    table = table_el.get("table") or {}
    for ri, row in enumerate(table.get("tableRows", [])):
        for ci, cell in enumerate(row.get("tableCells", [])):
            text_body = cell.get("text") or {}
            if _cell_fill_suggests_orange_coaching(cell):
                full = _text_from_body(text_body) if text_body.get("textElements") else ""
                orange_all.append(full.strip() if full.strip() else "[empty orange table cell]")
                continue
            if not text_body.get("textElements"):
                continue
            yparts, _ = _merge_runs_by_yellow(text_body)
            yellow_all.extend(yparts)
            orange_all.extend(_orange_coaching_text_segments(text_body))
    return yellow_all, orange_all


def extract_template_adapt_hints_from_slide(slide: dict) -> dict[str, Any]:
    """Scan pageElements for yellow fields (incl. highlight), orange fills, and orange text.

    Returns {"yellow_segments": [...], "orange_segments": [...]} (deduped, order preserved).
    """
    yellow: list[str] = []
    orange: list[str] = []

    def walk(elements: list[dict]) -> None:
        for el in elements:
            if el.get("elementGroup"):
                ch = el["elementGroup"].get("children") or []
                walk(ch)
                continue
            if el.get("shape"):
                y, o = _extract_from_shape(el)
                yellow.extend(y)
                orange.extend(o)
            if el.get("table"):
                y, o = _extract_from_table(el)
                yellow.extend(y)
                orange.extend(o)

    walk(slide.get("pageElements") or [])

    def _dedupe(seq: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for s in seq:
            k = s.strip()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    return {
        "yellow_segments": _dedupe(yellow),
        "orange_segments": _dedupe(orange),
    }


def slide_title_guess(slide: dict) -> str:
    """Short title string for logging (same spirit as QBR inventory)."""
    parts: list[str] = []
    for el in slide.get("pageElements") or []:
        for t in _extract_text(el):
            if t.strip():
                parts.append(t.strip())
                break
        if parts:
            break
    s = " ".join(parts)[:120] if parts else ""
    return s or "(no text)"


_ADAPT_HINTS_SYSTEM = """You help an automated QBR deck hydration pipeline interpret author cues embedded in a Google Slides template.

Conventions:
• Yellow (or yellow-ish) text in the template usually marks numeric or data fields the author intends to refresh with live metrics — but not always (could be emphasis).
• Black or dark text placed inside orange-filled boxes or table cells is editorial context: how to read the slide, what to prioritize, or caveats for adaptation.

You receive JSON with one entry per slide: slide_num, title_guess, yellow_segments, orange_segments. Segments may be empty, partial, or noisy if the Slides API did not return colors.

For EACH slide with at least one non-empty segment list, respond with concise adaptation advice (1–4 sentences) for engineers or the adapt LLM: what to refresh, what to treat as static, and any risks.

Return ONLY valid JSON (one object, no markdown):
{
  "slides": [
    {
      "slide_num": <int>,
      "advice": "<string>",
      "useful": <true|false>,
      "reason": "<short — why useful or why not>"
    }
  ],
  "overall_useful": <true|false>,
  "overall_summary": "<one sentence>"
}

JSON string rules (required — output is parsed with Python json.loads):
• Escape every double-quote inside a string as \\" .
• Do not put raw line breaks inside "advice" or "reason"; use \\n if needed.
• Keep "advice" and "reason" short (aim under 200 characters each) to avoid truncation.

If no slide has any yellow or orange segments, return slides: [], overall_useful: false, overall_summary explaining there was nothing to analyze.
"""

_LLM_SEG_CHAR_CAP = 420
_LLM_USER_JSON_CAP = 24000


def _trim_hint_rows_for_llm(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shorten segment strings so the model is less likely to echo broken quotes/newlines."""
    out: list[dict[str, Any]] = []
    for r in rows:
        cp = dict(r)
        for key in ("yellow_segments", "orange_segments"):
            segs = cp.get(key)
            if not isinstance(segs, list):
                continue
            trimmed: list[str] = []
            for s in segs:
                t = s if isinstance(s, str) else str(s)
                if len(t) > _LLM_SEG_CHAR_CAP:
                    t = t[: _LLM_SEG_CHAR_CAP] + "…"
                trimmed.append(t)
            cp[key] = trimmed
        out.append(cp)
    return out


def _coerce_adapt_hints_llm_result(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {
            "slides": [],
            "overall_useful": False,
            "overall_summary": "Invalid LLM response shape",
        }
    slides = data.get("slides")
    if not isinstance(slides, list):
        slides = []
    return {
        "slides": slides,
        "overall_useful": bool(data.get("overall_useful", False)),
        "overall_summary": str(data.get("overall_summary", "") or "")[:500],
    }


def analyze_adapt_hints_with_llm(
    oai: Any,
    rows: list[dict[str, Any]],
    customer: str,
) -> dict[str, Any]:
    """One batched LLM call over slides that had extracted segments."""
    payload = {"customer": customer, "slides": _trim_hint_rows_for_llm(rows)}
    user_json = json.dumps(payload, indent=0, default=str)[:_LLM_USER_JSON_CAP]
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _ADAPT_HINTS_SYSTEM},
        {"role": "user", "content": user_json},
    ]
    resp: Any = None
    for attempt in range(2):
        try:
            resp = _llm_create_with_retry(
                oai,
                model=LLM_MODEL_FAST,
                temperature=0,
                max_tokens=8192,
                response_format={"type": "json_object"},
                messages=messages,
            )
            raw = _strip_json_code_fence(resp.choices[0].message.content or "")
            data = json.loads(raw)
            return _coerce_adapt_hints_llm_result(data)
        except json.JSONDecodeError as e:
            if attempt == 0 and resp is not None:
                bad = (resp.choices[0].message.content or "").strip()
                if len(bad) > 1600:
                    bad = bad[:1600] + "…"
                logger.warning("QBR adapt hints: invalid JSON from LLM (%s); retrying once with repair prompt", e)
                messages = [
                    {
                        "role": "system",
                        "content": _ADAPT_HINTS_SYSTEM
                        + "\n\nYour previous reply was not valid JSON. This attempt must be a single JSON object only.",
                    },
                    {"role": "user", "content": user_json},
                    {"role": "assistant", "content": bad},
                    {
                        "role": "user",
                        "content": (
                            f"That output failed json.loads with: {e}. "
                            "Reply with ONLY one corrected JSON object (same schema). "
                            "Escape any literal double-quote inside string values with a backslash. "
                            "Keep each advice and reason under 120 characters."
                        ),
                    },
                ]
                continue
            logger.warning("QBR adapt hints: LLM analysis failed: %s", e)
            return {
                "slides": [],
                "overall_useful": False,
                "overall_summary": f"LLM analysis failed: {e}",
                "error": str(e),
            }
        except Exception as e:
            logger.warning("QBR adapt hints: LLM analysis failed: %s", e)
            return {
                "slides": [],
                "overall_useful": False,
                "overall_summary": f"LLM analysis failed: {e}",
                "error": str(e),
            }

    raise RuntimeError("analyze_adapt_hints_with_llm: exhausted retries without return")


def log_extracted_hints(rows: list[dict[str, Any]]) -> None:
    """Log yellow/orange extraction per slide (truncated)."""
    max_seg = 400
    any_content = False
    for row in rows:
        y = row.get("yellow_segments") or []
        o = row.get("orange_segments") or []
        if not y and not o:
            continue
        any_content = True
        sn = row.get("slide_num", "?")
        title = (row.get("title_guess") or "")[:80]
        logger.debug("QBR adapt hints — slide %s %r: yellow=%d segment(s), orange=%d segment(s)",
                     sn, title, len(y), len(o))
        for i, seg in enumerate(y):
            t = seg if len(seg) <= max_seg else seg[:max_seg] + "…"
            logger.debug("QBR adapt hints — slide %s yellow[%d]: %s", sn, i, t.replace("\n", " "))
        for i, seg in enumerate(o):
            t = seg if len(seg) <= max_seg else seg[:max_seg] + "…"
            logger.debug("QBR adapt hints — slide %s orange[%d]: %s", sn, i, t.replace("\n", " "))
    if not any_content:
        logger.info(
            "QBR adapt hints: no yellow/orange-styled text detected on slides slated for adaptation "
            "(API may omit rgb colors, or template uses different fills)."
        )


def log_llm_hints_result(result: dict[str, Any]) -> None:
    """Log whether LLM produced useful adaptation guidance."""
    summary = result.get("overall_summary", "") or ""
    useful = result.get("overall_useful", False)
    logger.info(
        "QBR adapt hints — LLM overall_useful=%s summary=%s",
        useful,
        summary[:300] + ("…" if len(summary) > 300 else ""),
    )
    slides = result.get("slides") or []
    if not isinstance(slides, list):
        return
    for ent in slides:
        if not isinstance(ent, dict):
            continue
        sn = ent.get("slide_num", "?")
        u = ent.get("useful", False)
        adv = str(ent.get("advice", "") or "")
        adv_t = adv if len(adv) <= 350 else adv[:350] + "…"
        reason = str(ent.get("reason", "") or "")
        logger.debug(
            "QBR adapt hints — slide %s LLM useful=%s reason=%r advice=%s",
            sn,
            u,
            reason[:200],
            adv_t.replace("\n", " "),
        )


def build_hint_rows_for_adapt_slides(
    final_slides: list[dict],
    adapt_page_ids: list[str],
) -> list[dict[str, Any]]:
    """Map adapt objectIds to slide index + extraction payload for the LLM."""
    order = [s["objectId"] for s in final_slides]
    slide_by_id = {s["objectId"]: s for s in final_slides}
    rows: list[dict[str, Any]] = []
    for oid in adapt_page_ids:
        slide = slide_by_id.get(oid)
        if not slide:
            continue
        try:
            idx = order.index(oid) + 1
        except ValueError:
            idx = -1
        hints = extract_template_adapt_hints_from_slide(slide)
        rows.append({
            "slide_num": idx,
            "object_id": oid,
            "title_guess": slide_title_guess(slide),
            "yellow_segments": hints["yellow_segments"],
            "orange_segments": hints["orange_segments"],
        })
    return rows


def run_qbr_adapt_hints_phase(
    oai: Any,
    slides_svc: Any,
    pres_id: str,
    final_slides: list[dict],
    adapt_page_ids: list[str],
    customer: str,
    *,
    title_slide_object_id: str | None = None,
) -> dict[str, Any]:
    """Extract template hints, log them, LLM analysis, then strip orange / yellow→[???] / banner."""
    rows = build_hint_rows_for_adapt_slides(final_slides, adapt_page_ids)
    log_extracted_hints(rows)

    nonempty = [
        r for r in rows
        if (r.get("yellow_segments") or r.get("orange_segments"))
    ]
    slide_by_id = {s["objectId"]: s for s in final_slides}

    if nonempty:
        analysis = analyze_adapt_hints_with_llm(oai, nonempty, customer)
    else:
        analysis = {
            "slides": [],
            "overall_useful": False,
            "overall_summary": "No yellow/orange segments extracted; skipped LLM.",
        }
    log_llm_hints_result(analysis)

    try:
        n_mod = apply_hint_mutations_to_presentation(
            slides_svc, pres_id, rows, slide_by_id, title_slide_object_id=title_slide_object_id
        )
        analysis["slides_surface_updated"] = n_mod
    except Exception as e:
        logger.warning("QBR adapt hints: surface mutation phase failed: %s", e)
        analysis["slides_surface_updated"] = 0
        analysis["surface_mutation_error"] = str(e)

    return analysis
