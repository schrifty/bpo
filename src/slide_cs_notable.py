"""Customer Success notable slide builder."""

from __future__ import annotations

from typing import Any

from .slide_primitives import missing_data_slide as _missing_data_slide
from .slide_signals import MAX_SIGNAL_BULLETS, render_signal_list_slide


def _canonical_notable_signals_title(raw: str | None) -> str:
    t = (raw or "").strip()
    if not t or t.casefold() == "notable":
        return "Notable Signals"
    return t


def cs_notable_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Support-review Notable slide — same layout as Notable Signals (exec summary)."""
    entry = report.get("_current_slide") or {}
    title = _canonical_notable_signals_title(entry.get("title"))
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
        items = [str(item).strip() for item in llm_bullets if str(item).strip()][:MAX_SIGNAL_BULLETS]
    else:
        items = [str(x).strip() for x in (entry.get("notable_items") or default_items) if str(x).strip()][
            :MAX_SIGNAL_BULLETS
        ]
    if not items:
        return _missing_data_slide(reqs, sid, report, idx, "notable_items")

    subtitle = (entry.get("notable_subtitle") or entry.get("subtitle") or "").strip()
    return render_signal_list_slide(
        reqs,
        sid,
        report,
        idx,
        signals=items,
        title=title,
        missing_label="notable_items",
        trend_banner=subtitle,
    )
