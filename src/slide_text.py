"""Text helpers for Google Slides API ranges and page elements."""

from __future__ import annotations

from typing import Any


def utf16_code_unit_len(value: str) -> int:
    """Length in UTF-16 code units, as expected by the Slides API text ranges."""
    return len(value.encode("utf-16-le")) // 2 if value else 0


def slides_shape_text_plain(text_body: dict[str, Any]) -> str:
    """Plain concatenated text from a Slides shape text body."""
    parts: list[str] = []
    for text_element in text_body.get("textElements") or []:
        text_run = text_element.get("textRun")
        if isinstance(text_run, dict):
            parts.append(str(text_run.get("content") or ""))
    return "".join(parts)


def utf16_ranges_for_phrases(full: str, phrases: tuple[str, ...]) -> list[tuple[int, int]]:
    """Find phrase ranges in UTF-16 code units for Slides API text styling."""
    ranges: list[tuple[int, int]] = []
    for phrase in phrases:
        if not phrase:
            continue
        pos = 0
        while True:
            idx = full.find(phrase, pos)
            if idx < 0:
                break
            start = utf16_code_unit_len(full[:idx])
            end = start + utf16_code_unit_len(phrase)
            ranges.append((start, end))
            pos = idx + len(phrase)
    return ranges


def iter_flat_page_elements(elements: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Flatten Slides page elements, including nested element groups."""
    out: list[dict[str, Any]] = []
    for element in elements or []:
        group = element.get("elementGroup")
        if isinstance(group, dict):
            out.extend(iter_flat_page_elements(group.get("children")))
        else:
            out.append(element)
    return out
