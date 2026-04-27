"""QBR template framing slide builders."""

from __future__ import annotations

import datetime
from typing import Any

from .slide_primitives import background as _bg, rect as _rect, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import CONTENT_W, FONT, FONT_SERIF, GRAY, MARGIN, MINT, NAVY, SLIDE_H, SLIDE_W, WHITE


BESPOKE_NAVY = {"red": 0.031, "green": 0.239, "blue": 0.471}  # #083d78 accent navy


def qbr_cover_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Branded cover slide: customer name, deck title, date."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    raw_date = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_date, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_date or datetime.datetime.now().strftime("%B %-d, %Y")

    tagline = "THE RIGHT PART.\nIN THE RIGHT PLACE.\nAT THE RIGHT TIME."
    _box(reqs, f"{sid}_tag", sid, SLIDE_W - 240, 30, 220, 120, tagline)
    _style(reqs, f"{sid}_tag", 0, len(tagline), size=11, color=BESPOKE_NAVY, font=FONT, bold=True)

    title = "Executive business review"
    title_top = SLIDE_H * 0.22
    _box(reqs, f"{sid}_t", sid, MARGIN + 6, title_top, 560, 130, title)
    _style(reqs, f"{sid}_t", 0, len(title), size=50, color=WHITE, font=FONT_SERIF)

    cust_top = title_top + 140
    _box(reqs, f"{sid}_c", sid, MARGIN + 6, cust_top, 500, 36, customer)
    _style(reqs, f"{sid}_c", 0, len(customer), size=24, color=MINT, font=FONT, bold=True)

    _box(reqs, f"{sid}_d", sid, MARGIN + 6, cust_top + 42, 500, 28, generated)
    _style(reqs, f"{sid}_d", 0, len(generated), size=19, color=MINT, font=FONT)

    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1


def qbr_agenda_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Numbered agenda slide generated from the deck's slide plan."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    _rect(reqs, f"{sid}_accent", sid, SLIDE_W * 0.48, 0, SLIDE_W * 0.52, SLIDE_H, BESPOKE_NAVY)

    title = "Agenda"
    _box(reqs, f"{sid}_t", sid, MARGIN, MARGIN, 300, 50, title)
    _style(reqs, f"{sid}_t", 0, len(title), size=38, color=WHITE, font=FONT_SERIF)

    slide_plan = report.get("_slide_plan", [])
    divider_items = [
        entry.get("title", "")
        for entry in slide_plan
        if entry.get("slide_type", entry.get("id", "")) == "qbr_divider" and entry.get("title")
    ]
    if divider_items:
        items = divider_items
    else:
        skip_types = {"qbr_cover", "qbr_agenda", "title", "data_quality", "skip"}
        items = [
            entry.get("title", entry.get("id", "").replace("_", " ").title())
            for entry in slide_plan
            if entry.get("slide_type", entry.get("id", "")) not in skip_types
        ]

    x = SLIDE_W * 0.52
    y_start = MARGIN + 20
    avail_h = SLIDE_H - MARGIN * 2 - 20
    n_items = len(items)
    line_h = max(28, min(42, avail_h // max(n_items, 1)))
    font_sz = 18 if n_items > 8 else 20
    num_sz = 20 if n_items > 8 else 22
    max_items = min(n_items, avail_h // line_h)

    y = y_start
    for item_index, item in enumerate(items[:max_items]):
        num = f"{item_index + 1:02d}"
        label = item[:50] + "…" if len(item) > 50 else item
        _box(reqs, f"{sid}_n{item_index}", sid, x, y, 40, line_h, num)
        _style(reqs, f"{sid}_n{item_index}", 0, len(num), size=num_sz, color=MINT, font=FONT, bold=True)

        _box(reqs, f"{sid}_i{item_index}", sid, x + 48, y, 280, line_h, label)
        _style(reqs, f"{sid}_i{item_index}", 0, len(label), size=font_sz, color=WHITE, font=FONT)
        y += line_h

    return idx + 1


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
