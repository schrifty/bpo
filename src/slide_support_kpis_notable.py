"""Notable Findings slide for the support-kpis HELP operational deck."""

from __future__ import annotations

from typing import Any

from .slide_primitives import missing_data_slide as _missing_data_slide
from .slide_signals import render_signal_list_slide
from .support_notable_llm import SUPPORT_KPIS_NOTABLE_BULLET_COUNT


def support_kpis_notable_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    """Executive findings list — same layout as Notable Signals, up to 10 bullets."""
    entry = report.get("_current_slide") or {}
    title = (entry.get("title") or "").strip() or "Notable Findings"
    llm_bullets = report.get("support_kpis_notable_bullets")
    if isinstance(llm_bullets, list) and llm_bullets:
        items = [str(item).strip() for item in llm_bullets if str(item).strip()][:SUPPORT_KPIS_NOTABLE_BULLET_COUNT]
    else:
        items = [str(x).strip() for x in (entry.get("notable_items") or []) if str(x).strip()][
            :SUPPORT_KPIS_NOTABLE_BULLET_COUNT
        ]
    if not items:
        return _missing_data_slide(reqs, sid, report, idx, "notable findings")

    subtitle = (entry.get("notable_subtitle") or entry.get("subtitle") or "").strip()
    return render_signal_list_slide(
        reqs,
        sid,
        report,
        idx,
        signals=items,
        title=title,
        missing_label="notable findings",
        trend_banner=subtitle,
        max_bullets=SUPPORT_KPIS_NOTABLE_BULLET_COUNT,
    )
