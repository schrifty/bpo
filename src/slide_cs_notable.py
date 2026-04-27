"""Customer Success notable slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import background as _bg, missing_data_slide as _missing_data_slide, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BODY_BOTTOM, BODY_Y, CONTENT_W, FONT, GRAY, MARGIN, NAVY, WHITE


def cs_notable_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Six focus areas of interest to Customer Success leaders."""
    entry = report.get("_current_slide") or {}
    title = entry.get("title") or "Notable"
    default_items = [
        "Adoption and depth: Are the right people using the product in the ways that matter for business outcomes?",
        "Account health and risk: Churn, renewal, adoption trends, and what would worry you on this account.",
        "Value proof: Concrete metrics and outcomes the customer and their execs would recognize as progress or ROI.",
        "Champions and executive coverage: Sponsors, power users, and access at the right level.",
        "Support, friction, and product gaps: Ticket patterns, training vs. real gaps, and recurring blockers to value.",
        "Expectations and follow-through: What was committed, what shipped, what is still open, and what is next.",
    ]
    llm_bullets = report.get("support_notable_bullets")
    if isinstance(llm_bullets, list) and llm_bullets:
        items = [str(item).strip() for item in llm_bullets if str(item).strip()][:6]
    else:
        items = list(entry.get("notable_items") or default_items)
    items = [str(item).strip() for item in items if str(item).strip()][:6]
    if not items:
        return _missing_data_slide(reqs, sid, report, idx, "notable_items")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    y = float(BODY_Y)
    subtitle = (entry.get("notable_subtitle") or entry.get("subtitle") or "").strip()
    if subtitle:
        _box(reqs, f"{sid}_st", sid, MARGIN, y, CONTENT_W, 20, subtitle)
        _style(reqs, f"{sid}_st", 0, len(subtitle), size=9, color=GRAY, font=FONT)
        y += 24.0

    body = "\n\n".join(f"• {item}" for item in items)
    height = max(40.0, float(BODY_BOTTOM) - y - 4.0)
    _box(reqs, f"{sid}_li", sid, MARGIN, y, CONTENT_W, height, body)
    _style(reqs, f"{sid}_li", 0, len(body), size=10, color=NAVY, font=FONT)
    return idx + 1
