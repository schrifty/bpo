"""Flexible custom slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, BODY_Y, CONTENT_W, FONT, MARGIN, NAVY


def custom_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Flexible slide renderer for agent-composed content."""
    title = report.get("title", "")
    sections = report.get("sections", [])
    if not title and not sections:
        return _missing_data_slide(reqs, sid, report, idx, "deck title / section list")

    _slide(reqs, sid, idx)
    if title:
        _slide_title(reqs, sid, title)

    y = BODY_Y
    col_w = CONTENT_W
    if len(sections) == 2:
        col_w = 300
    elif len(sections) >= 3:
        col_w = 195

    for index, section in enumerate(sections[:3]):
        header = section.get("header", "")
        body = section.get("body", "")
        x = MARGIN + index * (col_w + 16)

        if header:
            _box(reqs, f"{sid}_h{index}", sid, x, y, col_w, 18, header)
            _style(reqs, f"{sid}_h{index}", 0, len(header), bold=True, size=11, color=BLUE, font=FONT)

        if body:
            body_y = y + (22 if header else 0)
            _box(reqs, f"{sid}_b{index}", sid, x, body_y, col_w, 280, body)
            _style(reqs, f"{sid}_b{index}", 0, len(body), size=10, color=NAVY, font=FONT)

    return idx + 1
