"""Support deck cover and intro slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    background as _bg,
    internal_footer as _internal_footer,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import (
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    FONT_SERIF,
    GRAY,
    LTBLUE,
    MARGIN,
    NAVY,
    WHITE,
    _date_range,
)


def support_deck_cover_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Title slide for support / supply-chain scoped decks (one shared hero layout)."""
    entry = report.get("_current_slide") or {}
    customer = (report.get("customer") or "").strip() or "All Customers"
    generated = (report.get("support_deck_generated_at") or "").strip() or "—"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    title = (entry.get("title") or "Support Review").strip() or "Support Review"
    days = int(report.get("days") or 30)
    date_range = _date_range(
        days,
        report.get("quarter"),
        report.get("quarter_start"),
        report.get("quarter_end"),
    )
    subtitle = f"{customer}  ·  {date_range}"

    _box(reqs, f"{sid}_h1", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_h1", 0, len(title), bold=True, size=36, color=WHITE, font=FONT_SERIF)
    _box(reqs, f"{sid}_cust", sid, MARGIN, 190, CONTENT_W, 36, subtitle)
    _style(reqs, f"{sid}_cust", 0, len(subtitle), size=15, color=LTBLUE, font=FONT)
    generated_text = f"Generated {generated}"
    _box(reqs, f"{sid}_gen", sid, MARGIN, 340, CONTENT_W, 24, generated_text)
    _style(reqs, f"{sid}_gen", 0, len(generated_text), size=10, color=GRAY, font=FONT)

    _internal_footer(reqs, sid)
    return idx + 1


def support_intro_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """First slide: deck title, audience/timeframe, and short Support Review context."""
    entry = report.get("_current_slide") or {}
    days = int(report.get("days") or 30)
    date_range = _date_range(
        days,
        report.get("quarter"),
        report.get("quarter_start"),
        report.get("quarter_end"),
    )
    customer = report.get("customer") or "All Customers"
    title = entry.get("title") or "Support Review"
    blurb = (entry.get("intro_blurb") or "").strip() or (
        "Jira ticket volume, backlog, response-time KPIs, and recent activity—organized by project "
        "(HELP, CUSTOMER, LEAN) for operations and customer success."
    )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    meta = f"{customer}  ·  {date_range}"
    _box(reqs, f"{sid}_meta", sid, MARGIN, BODY_Y, CONTENT_W, 22, meta)
    _style(reqs, f"{sid}_meta", 0, len(meta), size=10, color=GRAY, font=FONT)

    body_y = BODY_Y + 28
    body_h = max(64.0, float(BODY_BOTTOM) - body_y - 4.0)
    _box(reqs, f"{sid}_b", sid, MARGIN, body_y, CONTENT_W, body_h, blurb)
    _style(reqs, f"{sid}_b", 0, len(blurb), size=12, color=NAVY, font=FONT)
    return idx + 1
