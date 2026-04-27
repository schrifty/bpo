"""QBR template framing slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import background as _bg, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import CONTENT_W, FONT, FONT_SERIF, GRAY, MARGIN, MINT, NAVY, SLIDE_H, SLIDE_W, WHITE


def qbr_divider_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Section divider slide with LeanDNA tagline and section title."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    entry = report.get("_current_slide", {})
    section_title = entry.get("title", entry.get("note", ""))

    lines = [
        ("THE RIGHT PART.", 28, True),
        ("In the right place.", 28, False),
        ("AT THE RIGHT TIME.", 26, False),
    ]
    text_y = SLIDE_H * 0.18
    for line_index, (text, size, bold) in enumerate(lines):
        _box(reqs, f"{sid}_tl{line_index}", sid, MARGIN, text_y, 400, 36, text)
        _style(reqs, f"{sid}_tl{line_index}", 0, len(text), size=size, color=WHITE, font=FONT, bold=bold)
        text_y += 40

    if section_title:
        _box(reqs, f"{sid}_sec", sid, MARGIN, SLIDE_H * 0.65, CONTENT_W, 50, section_title)
        _style(reqs, f"{sid}_sec", 0, len(section_title), size=32, color=MINT, font=FONT_SERIF)

    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1
