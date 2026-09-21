"""Claude designs the opening slides of the Engineering KPIs deck.

The per-KPI slides stay hand-built: their point is the stored-history chart, and
the slide IR has no chart element. Claude gets the two slides that were a wall of
text — where engineering stands, and what moved period to period.

Strict by default (see ``fail-loud-integrations``): a Claude or parse failure
raises unless ``CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK`` is set, in which case the
caller reverts to the hand-built cover and notable-changes slides.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .claude_slide_ir import render_slide_ir
from .config import CORTEX_METRICS_CLAUDE_MODEL, logger
from .metrics_claude_slides import (
    MetricsClaudeError,
    generate_metrics_slide_irs,
    metrics_claude_allow_fallback,
    metrics_claude_slides_enabled,
    row_fact,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .eng_kpi_deck import EngKpiCard

__all__ = [
    "EngKpiClaudeError",
    "build_eng_kpi_claude_digest",
    "build_eng_kpi_claude_plan",
    "card_fact",
    "eng_kpi_claude_allow_fallback",
    "eng_kpi_claude_enabled",
    "render_eng_kpi_claude_slides",
]


class EngKpiClaudeError(MetricsClaudeError):
    """Claude did not return usable slide IR for the Engineering KPIs deck."""


def eng_kpi_claude_enabled() -> bool:
    return metrics_claude_slides_enabled()


def eng_kpi_claude_allow_fallback() -> bool:
    return metrics_claude_allow_fallback()


def card_fact(card: EngKpiCard) -> dict[str, Any]:
    """One KPI as facts Claude may cite, including its period-to-period move."""
    fact = row_fact(card.row)
    fact["cadence"] = "month-close" if card.grain == "month" else "trailing window"
    if card.history:
        fact["history_points"] = len(card.history)
        fact["history_span"] = f"{card.history[0].period_key}..{card.history[-1].period_key}"
    if card.notable:
        fact["move"] = card.notable.summary
        fact["move_direction"] = {
            "success": "better",
            "failure": "worse",
            "watch": "moved",
        }[card.notable.kind]
    elif len(card.history) >= 2:
        fact["move"] = "flat vs prior period"
        fact["move_direction"] = "flat"
    return fact


def build_eng_kpi_claude_digest(
    cards: list[EngKpiCard], *, as_of: str
) -> dict[str, Any]:
    """Facts for the opener slides: standing, movement counts, and every KPI."""
    facts = [card_fact(c) for c in cards]
    off = [f for f in facts if f["status"] == "off_target"]
    unavailable = [f for f in facts if f["status"] in ("error", "no_data")]
    counted = [f for f in facts if f["status"] in ("on_target", "off_target")]
    worse = [f for f in facts if f.get("move_direction") == "worse"]
    better = [f for f in facts if f.get("move_direction") == "better"]
    return {
        "function": "Engineering",
        "as_of": as_of,
        "kpi_count": len(facts),
        "on_target_count": len(counted) - len(off),
        "off_target_count": len(off),
        "measured_count": len(counted),
        "unavailable_count": len(unavailable),
        "worse_count": len(worse),
        "better_count": len(better),
        "deck_purpose": (
            "This deck is the engineering KPI review as of "
            f"{as_of}. Every KPI gets its own chart slide later in the deck, so "
            "these opening slides must not restate the full list: slide one gives "
            "the verdict on engineering delivery and AI adoption, slide two says "
            "what actually moved since the prior period and what it implies. "
            f"Scope: {len(facts)} KPIs, {len(off)} off target, "
            f"{len(worse)} moved worse, {len(better)} moved better, "
            f"{len(unavailable)} unavailable."
        ),
        "kpis": facts,
    }


def build_eng_kpi_claude_plan(digest: dict[str, Any]) -> list[dict[str, Any]]:
    """Two opener slides: where engineering stands, then what moved."""
    facts: list[dict[str, Any]] = list(digest.get("kpis") or [])
    moved = [f for f in facts if f.get("move_direction") in ("better", "worse", "moved")]
    return [
        {
            "id": "standing",
            "slide_type": "standing",
            "title": "Engineering KPIs — Where We Stand",
            "purpose": (
                "The cover. A reader who stops here should know whether "
                "engineering delivery and AI adoption are healthy, and why."
            ),
            "must_include": [
                "A headline sentence naming the overall state of engineering "
                "delivery and the single biggest reason for it.",
                "A kpi_row of 4-6 tiles for the most decision-relevant KPIs, with "
                "the target in the label (e.g. 'PRs Merged (target 500)').",
                "Color tiles so off-target reads red-ish and on-target blue/teal.",
                f"A line noting the as-of date {digest.get('as_of')} and that each "
                "following slide is one KPI with its stored-history chart.",
                "One takeaway line: the so-what for an engineering leader.",
            ],
            "kpis": facts,
        },
        {
            "id": "movement",
            "slide_type": "movement",
            "title": "What Moved Since Last Period",
            "purpose": (
                "Separate real movement from noise. Only KPIs whose move cleared "
                "the 10% / 3-point band or crossed a target are supplied."
            ),
            "must_include": [
                "Group the moves: what got worse first, then what got better. "
                "Never repeat a status word at the start of every line.",
                "A table with columns KPI, Prior, Current, Move — worst first, "
                "using the supplied move strings for the numbers.",
                "Two or three bullets on what the biggest moves imply, using only "
                "the supplied definition / how_computed facts.",
                "A takeaway naming the one thing to look at first."
                if moved
                else "State plainly that no KPI moved beyond the noise band.",
            ],
            "kpis": moved or facts,
        },
    ]


def render_eng_kpi_claude_slides(
    reqs: list[dict[str, Any]],
    cards: list[EngKpiCard],
    *,
    as_of: str,
    start_index: int = 0,
) -> tuple[int, list[str]]:
    """Append Claude-designed opener slides. Returns (next index, slide ids)."""
    digest = build_eng_kpi_claude_digest(cards, as_of=as_of)
    plan = build_eng_kpi_claude_plan(digest)
    try:
        irs = generate_metrics_slide_irs(plan, digest)
    except MetricsClaudeError as e:
        raise EngKpiClaudeError(str(e)) from e

    idx = start_index
    sids: list[str] = []
    for entry, ir in zip(plan, irs):
        sid = f"eng_kpis_c{idx}_{str(entry.get('id') or 'slide')}"
        idx = render_slide_ir(reqs, sid, ir, idx)
        sids.append(sid)
    logger.info(
        "Engineering KPI Claude slides: model=%s slides=%d kpis=%d",
        CORTEX_METRICS_CLAUDE_MODEL,
        len(sids),
        len(cards),
    )
    return idx, sids
