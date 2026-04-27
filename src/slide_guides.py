"""Guide Engagement slide builders."""

from __future__ import annotations

from typing import Any

from .slide_primitives import (
    bar_rect as _bar_rect,
    missing_data_slide as _missing_data_slide,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box, append_wrapped_text_box as _wrap_box
from .slides_theme import BLUE, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY


def guides_no_usage_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int, guides: dict[str, Any]) -> int:
    """Guide engagement succeeded but zero events: explicit signal, not missing data."""
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Guide Engagement")

    days = guides.get("days")
    # Do not echo "0 visitors" under the title; the headline already says "No usage".
    scope = f"{int(days)}-day lookback" if days is not None else ""
    no_usage_y = BODY_Y
    if scope:
        _box(reqs, f"{sid}_scope", sid, MARGIN, BODY_Y, CONTENT_W, 16, scope)
        _style(reqs, f"{sid}_scope", 0, len(scope), size=10, color=GRAY, font=FONT)
        no_usage_y = BODY_Y + 22

    headline = "No usage"
    _box(reqs, f"{sid}_nu", sid, MARGIN, no_usage_y, CONTENT_W, 36, headline)
    _style(reqs, f"{sid}_nu", 0, len(headline), bold=True, size=22, color=NAVY, font=FONT)

    detail = (
        "No in-app guide events (views, continue/next, or dismiss) were recorded for this "
        "customer in this period — an adoption signal worth reviewing with the account team."
    )
    _wrap_box(reqs, f"{sid}_nu_d", sid, MARGIN, no_usage_y + 44, CONTENT_W, 120, detail)
    _style(reqs, f"{sid}_nu_d", 0, len(detail), size=11, color=NAVY, font=FONT)

    return idx + 1


def guides_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    guides = report.get("guides")
    if not isinstance(guides, dict):
        return _missing_data_slide(reqs, sid, report, idx, "guide engagement data")
    error = guides.get("error")
    if error:
        return _missing_data_slide(reqs, sid, report, idx, f"guide engagement: {error}")

    total_events = int(guides.get("total_guide_events") or 0)
    if total_events == 0:
        return guides_no_usage_slide(reqs, sid, report, idx, guides)

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Guide Engagement")

    seen = guides.get("seen", 0)
    advanced = guides.get("advanced", 0)
    dismissed = guides.get("dismissed", 0)
    reach = guides.get("guide_reach", 0)
    dismiss_rate = guides.get("dismiss_rate", 0)
    advance_rate = guides.get("advance_rate", 0)

    metrics = (
        f"{seen:,} guide views (Pendo)  ·  {reach}% of tracked users saw at least one guide\n"
        f"{advance_rate}% of views included a continue (next step)  ·  "
        f"{dismiss_rate}% of views included a dismiss"
    )
    _box(reqs, f"{sid}_met", sid, MARGIN, BODY_Y, CONTENT_W, 34, metrics)
    _style(reqs, f"{sid}_met", 0, len(metrics), size=10, color=GRAY, font=FONT)

    # Continue vs dismiss: bar is split of those two event types only; many views have neither.
    bar_y = BODY_Y + 42
    total_responses = advanced + dismissed
    if total_responses > 0:
        advanced_width = int(advanced / total_responses * 400)
        dismissed_width = int(dismissed / total_responses * 400)
        _bar_rect(reqs, f"{sid}_adv", sid, MARGIN, bar_y, max(advanced_width, 4), 18, BLUE)
        _bar_rect(reqs, f"{sid}_dis", sid, MARGIN + advanced_width, bar_y, max(dismissed_width, 4), 18, GRAY)
        advanced_label = f"Continue / next step ({advanced:,})"
        _box(reqs, f"{sid}_alab", sid, MARGIN, bar_y + 20, 220, 14, advanced_label)
        _style(reqs, f"{sid}_alab", 0, len(advanced_label), size=8, color=BLUE, font=FONT)
        dismissed_label = f"Dismissed ({dismissed:,})"
        _box(reqs, f"{sid}_dlab", sid, MARGIN + advanced_width, bar_y + 20, 220, 14, dismissed_label)
        _style(reqs, f"{sid}_dlab", 0, len(dismissed_label), size=8, color=GRAY, font=FONT)
        bar_note = "Bar = share of continue vs dismiss events only; other guide views did not continue or dismiss."
        _box(reqs, f"{sid}_bnote", sid, MARGIN, bar_y + 36, CONTENT_W, 22, bar_note)
        _style(reqs, f"{sid}_bnote", 0, len(bar_note), size=7, color=GRAY, font=FONT)
        bar_y += 62

    top_guides = guides.get("top_guides", [])
    section_title = "Most active guides"
    section_legend = (
        "Each bullet: views = times the guide was shown; continue = user went to the next step; "
        "dismiss = user closed the guide without continuing."
    )
    bullet_lines: list[str] = []
    for guide in top_guides[:6]:
        name = str(guide.get("guide") or "")
        if len(name) > 52:
            name = name[:49] + "..."
        bullet_lines.append(
            f"• {name}: {guide['seen']} views, {guide['advanced']} continue, {guide['dismissed']} dismiss"
        )
    if not bullet_lines:
        bullet_lines.append("• No guide interactions in this period.")
    body = "\n".join(bullet_lines)
    text = f"{section_title}\n{section_legend}\n\n{body}"
    _box(reqs, f"{sid}_guides", sid, MARGIN, bar_y + 4, CONTENT_W, 220, text)
    _style(reqs, f"{sid}_guides", 0, len(text), size=10, color=NAVY, font=FONT)
    title_len = len(section_title)
    _style(reqs, f"{sid}_guides", 0, title_len, bold=True, size=11, color=BLUE, font=FONT)
    legend_start = title_len + 1
    legend_end = legend_start + len(section_legend)
    _style(reqs, f"{sid}_guides", legend_start, legend_end, size=8, color=GRAY, font=FONT)

    return idx + 1
