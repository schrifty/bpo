"""Slides API request builders for hydrate/adapt mutation."""

from __future__ import annotations

import re
import secrets
from typing import Any

from .config import logger

PLACEHOLDER_MARKERS = ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
STATIC_IMAGE_MARKER = "[STATIC IMAGE"
EMBEDDED_CHART_TEXT = "(embedded chart — contains data that cannot be auto-updated)"
EMBEDDED_IMAGE_TEXTS = ("(embedded image)", "(image in shape)")


def replacement_row_is_static_visual_incomplete(row: dict[str, Any]) -> bool:
    """True when the row is a static chart/image rather than text replacement."""
    original = row.get("original", "")
    new_value = row.get("new_value", "")
    return (
        original in EMBEDDED_IMAGE_TEXTS + (EMBEDDED_CHART_TEXT,)
        or STATIC_IMAGE_MARKER in (new_value or "")
        or "[CHART —" in (new_value or "")
    )


def has_text_placeholder_incomplete(replacements: list[dict[str, Any]]) -> bool:
    """True if some row is incomplete for non-visual text reasons."""
    for row in replacements:
        if replacement_row_is_static_visual_incomplete(row):
            continue
        original = row.get("original", "")
        new_value = row.get("new_value", "")
        mapped = row.get("mapped", True)
        if not original or original == new_value:
            continue
        if not mapped:
            return True
    return False


METRICISH_IN_ORIGINAL = re.compile(r"[\d%$€£]|Q[1-4]\b", re.I)


def unmapped_nonvisual_rows_all_editorial_headings(replacements: list[dict[str, Any]]) -> bool:
    """True when every unmapped non-visual row looks like prose/section copy."""
    found = False
    for row in replacements:
        if replacement_row_is_static_visual_incomplete(row):
            continue
        original = (row.get("original") or "").strip()
        new_value = row.get("new_value", "")
        mapped = row.get("mapped", True)
        if not original or original == new_value or mapped:
            continue
        found = True
        if len(original) < 12:
            return False
        if METRICISH_IN_ORIGINAL.search(original):
            return False
    return found


HYDRATE_SKIP_TEXT_ADAPT_TYPES = frozenset({"title", "qbr_cover", "qbr_divider"})


def should_add_incomplete_banner(
    page_id: str,
    replacements: list[dict[str, Any]],
    title_slide_object_id: str | None = None,
    analysis: dict[str, Any] | None = None,
) -> bool:
    """Skip banner on title/divider/cover slides, prose-only unmapped, and static-only slides."""
    if title_slide_object_id and page_id == title_slide_object_id:
        return False
    if analysis:
        slide_type = (analysis.get("slide_type") or "").strip()
        if slide_type in HYDRATE_SKIP_TEXT_ADAPT_TYPES:
            return False
    if not has_text_placeholder_incomplete(replacements):
        return False
    if unmapped_nonvisual_rows_all_editorial_headings(replacements):
        return False
    return True


def apply_adaptations(
    slides_svc, pres_id: str, page_id: str, replacements: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], bool, bool]:
    """Build Slides API requests to replace data values on a slide."""
    reqs: list[dict[str, Any]] = []
    has_unmapped = False
    has_static_images = False

    for row in replacements:
        original = row.get("original", "")
        new_value = row.get("new_value", "")
        mapped = row.get("mapped", True)

        if replacement_row_is_static_visual_incomplete(row):
            has_static_images = True
            has_unmapped = True
            continue

        if not original or original == new_value:
            continue
        if not mapped:
            has_unmapped = True

        reqs.append({
            "replaceAllText": {
                "containsText": {"text": original, "matchCase": True},
                "replaceText": new_value,
                "pageObjectIds": [page_id],
            }
        })

    return reqs, has_unmapped, has_static_images


def mapped_new_values_for_font_clamp(replacements: list[dict[str, Any]]) -> set[str]:
    """New values actually swapped in, used to target font clamping."""
    out: set[str] = set()
    for row in replacements:
        if replacement_row_is_static_visual_incomplete(row):
            continue
        new_value = str(row.get("new_value") or "").strip()
        original = str(row.get("original") or "").strip()
        if not new_value or new_value == original:
            continue
        if new_value in ("[???]", "[?]"):
            continue
        out.add(new_value)
    return out


def slide_metric_font_clamp_requests(
    slide: dict[str, Any],
    replacements: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """After replaceAllText, clamp runs that inherited headline-sized fonts."""
    from .qbr_adapt_hints import iter_text_run_spans

    mapped_values = mapped_new_values_for_font_clamp(replacements)
    max_metric_pt = 28.0
    min_body_pt = 8.0
    max_body_for_ref_pt = 24.0
    absolute_fallback_pt = 14.0
    aggressive_mag_pt = 36.0

    def body_reference_font_pt(text_body: dict[str, Any]) -> float:
        mags: list[float] = []
        for _start, _end, _content, style in iter_text_run_spans(text_body):
            font_size = style.get("fontSize") or {}
            magnitude = font_size.get("magnitude")
            if magnitude is not None and min_body_pt <= magnitude <= max_body_for_ref_pt:
                mags.append(float(magnitude))
        if mags:
            mags.sort()
            return mags[len(mags) // 2]
        mags2: list[float] = []
        for _start, _end, _content, style in iter_text_run_spans(text_body):
            font_size = style.get("fontSize") or {}
            magnitude = font_size.get("magnitude")
            if magnitude is not None and magnitude <= max_metric_pt + 6:
                mags2.append(float(magnitude))
        if mags2:
            return float(min(mags2))
        return absolute_fallback_pt

    def looks_like_metricish(value: str) -> bool:
        text = (value or "").strip()
        if len(text) > 72:
            return False
        return bool(re.search(r"[\d%$€£]", text))

    def run_matches_mapped_value(content: str, mapped: set[str]) -> bool:
        cleaned = content.strip()
        if cleaned in mapped:
            return True
        return any(len(mapped_value) >= 4 and mapped_value in cleaned for mapped_value in mapped)

    def clamp_text_body(
        object_id: str,
        text_body: dict[str, Any],
        cell_loc: dict[str, int] | None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        ref = min(body_reference_font_pt(text_body), 22.0)
        for start, end, content, style in iter_text_run_spans(text_body):
            font_size = style.get("fontSize") or {}
            magnitude = font_size.get("magnitude")
            if magnitude is None:
                continue
            magnitude = float(magnitude)
            if magnitude <= max_metric_pt:
                continue
            if not looks_like_metricish(content):
                continue
            matched = run_matches_mapped_value(content, mapped_values)
            if not matched and (magnitude <= aggressive_mag_pt or len(content.strip()) > 48):
                continue
            target = min(magnitude, max(ref, 12.0))
            if abs(target - magnitude) < 0.4:
                continue
            update_text_style: dict[str, Any] = {
                "objectId": object_id,
                "textRange": {"type": "FIXED_RANGE", "startIndex": start, "endIndex": end},
                "style": {"fontSize": {"magnitude": round(target, 1), "unit": "PT"}},
                "fields": "fontSize",
            }
            if cell_loc is not None:
                update_text_style["cellLocation"] = {
                    "rowIndex": int(cell_loc["rowIndex"]),
                    "columnIndex": int(cell_loc["columnIndex"]),
                }
            out.append({"updateTextStyle": update_text_style})
        return out

    reqs: list[dict[str, Any]] = []

    def walk(elements: list[dict[str, Any]]) -> None:
        for element in elements or []:
            if element.get("elementGroup"):
                walk(element["elementGroup"].get("children") or [])
                continue
            object_id = element.get("objectId") or ""
            if element.get("shape"):
                text_body = element.get("shape", {}).get("text") or {}
                if text_body.get("textElements"):
                    reqs.extend(clamp_text_body(object_id, text_body, None))
            if element.get("table"):
                table = element.get("table") or {}
                for row_index, row in enumerate(table.get("tableRows", [])):
                    for col_index, cell in enumerate(row.get("tableCells", [])):
                        text_body = cell.get("text") or {}
                        if text_body.get("textElements"):
                            reqs.extend(
                                clamp_text_body(
                                    object_id,
                                    text_body,
                                    {"rowIndex": row_index, "columnIndex": col_index},
                                )
                            )

    walk(slide.get("pageElements") or [])
    if reqs:
        logger.debug("adapt: font clamp %d run(s) (replaceAllText headline inheritance)", len(reqs))
    return reqs


def red_style_placeholders(slides_svc, pres_id: str, page_id: str) -> list[dict[str, Any]]:
    """Re-read a slide and return updateTextStyle requests to make placeholders red."""
    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    target_slide = None
    for slide in pres.get("slides", []):
        if slide["objectId"] == page_id:
            target_slide = slide
            break
    if not target_slide:
        return []

    red_color = {
        "foregroundColor": {
            "opaqueColor": {"rgbColor": {"red": 0.9, "green": 0.1, "blue": 0.1}}
        }
    }
    reqs: list[dict[str, Any]] = []

    def scan_text_body(
        element_id: str,
        text_body: dict[str, Any],
        cell_location: dict[str, int] | None = None,
    ) -> None:
        full = ""
        for text_element in text_body.get("textElements", []):
            full += text_element.get("textRun", {}).get("content", "")
        for marker in PLACEHOLDER_MARKERS:
            start = 0
            while True:
                idx = full.find(marker, start)
                if idx == -1:
                    break
                req: dict[str, Any] = {
                    "updateTextStyle": {
                        "objectId": element_id,
                        "textRange": {
                            "type": "FIXED_RANGE",
                            "startIndex": idx,
                            "endIndex": idx + len(marker),
                        },
                        "style": {**red_color, "bold": True},
                        "fields": "foregroundColor,bold",
                    }
                }
                if cell_location:
                    req["updateTextStyle"]["cellLocation"] = cell_location
                reqs.append(req)
                start = idx + len(marker)

    for element in target_slide.get("pageElements", []):
        object_id = element.get("objectId", "")
        shape_text = element.get("shape", {}).get("text", {})
        if shape_text:
            scan_text_body(object_id, shape_text)
        table = element.get("table", {})
        if table:
            for row_index, row in enumerate(table.get("tableRows", [])):
                for col_index, cell in enumerate(row.get("tableCells", [])):
                    cell_text = cell.get("text", {})
                    if cell_text:
                        scan_text_body(
                            object_id,
                            cell_text,
                            cell_location={"rowIndex": row_index, "columnIndex": col_index},
                        )

    return reqs


def add_incomplete_banner(
    page_id: str,
    slide_w: int = 720,
    slide_h: int = 405,
    has_static_images: bool = False,
    banner_text: str | None = None,
) -> list[dict[str, Any]]:
    """Create a prominent red INCOMPLETE banner across the top of a slide."""
    banner_id = f"incomplete_{page_id[:12]}_{secrets.token_hex(4)}"
    emu = 12700
    banner_w = slide_w - 40
    banner_h = 28
    banner_x = 20
    banner_y = 4
    if banner_text:
        text = banner_text
    elif has_static_images:
        text = "INCOMPLETE — contains static image(s) with data that cannot be auto-updated"
    else:
        text = "INCOMPLETE — red values need manual update"
    return [
        {
            "createShape": {
                "objectId": banner_id,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {
                        "width": {"magnitude": banner_w * emu, "unit": "EMU"},
                        "height": {"magnitude": banner_h * emu, "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1,
                        "scaleY": 1,
                        "translateX": banner_x * emu,
                        "translateY": banner_y * emu,
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "updateShapeProperties": {
                "objectId": banner_id,
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "solidFill": {
                            "color": {"rgbColor": {"red": 0.95, "green": 0.2, "blue": 0.2}},
                            "alpha": 0.92,
                        }
                    },
                    "outline": {
                        "outlineFill": {
                            "solidFill": {
                                "color": {"rgbColor": {"red": 0.8, "green": 0.1, "blue": 0.1}},
                            }
                        }
                    },
                },
                "fields": "shapeBackgroundFill,outline",
            }
        },
        {"insertText": {"objectId": banner_id, "text": text}},
        {
            "updateTextStyle": {
                "objectId": banner_id,
                "textRange": {"type": "ALL"},
                "style": {
                    "foregroundColor": {
                        "opaqueColor": {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}
                    },
                    "bold": True,
                    "fontSize": {"magnitude": 14, "unit": "PT"},
                },
                "fields": "foregroundColor,bold,fontSize",
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": banner_id,
                "textRange": {"type": "ALL"},
                "style": {"alignment": "CENTER"},
                "fields": "alignment",
            }
        },
    ]
