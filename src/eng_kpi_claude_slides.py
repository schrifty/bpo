"""Claude designs pages 2–3 of the four-page Engineering KPIs deck.

Page 1 is a deterministic title page and page 4 is the deterministic KPI list.
Claude gets the summary of the three largest moves and the Engineering/DevOps
and Support team narrative.

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


def _movement_score(card: EngKpiCard) -> float:
    if not card.notable:
        return -1.0
    previous = card.notable.previous
    delta = abs(card.notable.current - previous)
    return delta / abs(previous) if previous else delta


def _team_fact(card: EngKpiCard, team: str) -> dict[str, Any]:
    fact = card_fact(card)
    fact["team"] = team
    return fact


def build_eng_kpi_claude_digest(
    engineering_cards: list[EngKpiCard],
    support_cards: list[EngKpiCard] | None = None,
    *,
    as_of: str,
) -> dict[str, Any]:
    """Facts for the three-change summary and two-team narrative."""
    support = support_cards or []
    engineering_facts = [_team_fact(c, "Engineering/DevOps") for c in engineering_cards]
    support_facts = [_team_fact(c, "Support") for c in support]
    facts = engineering_facts + support_facts
    off = [f for f in facts if f["status"] == "off_target"]
    unavailable = [f for f in facts if f["status"] in ("error", "no_data")]
    counted = [f for f in facts if f["status"] in ("on_target", "off_target")]
    worse = [f for f in facts if f.get("move_direction") == "worse"]
    better = [f for f in facts if f.get("move_direction") == "better"]
    changed = sorted(
        [c for c in engineering_cards + support if c.notable],
        key=lambda c: (-_movement_score(c), c.row.name.casefold()),
    )[:3]
    return {
        "function": "Engineering/DevOps & Support",
        "as_of": as_of,
        "kpi_count": len(facts),
        "on_target_count": len(counted) - len(off),
        "off_target_count": len(off),
        "measured_count": len(counted),
        "unavailable_count": len(unavailable),
        "worse_count": len(worse),
        "better_count": len(better),
        "engineering_kpis": engineering_facts,
        "support_kpis": support_facts,
        "top_changes": [
            _team_fact(
                c,
                "Support" if "support" in {str(t).lower() for t in c.row.tags} else "Engineering/DevOps",
            )
            for c in changed
        ],
        "deck_purpose": (
            "This is a four-page KPI review as of "
            f"{as_of}. Page 1 is the title. Page 2 summarizes no more than the "
            "three KPIs that changed most. Page 3 gives separate factual narratives "
            "for Engineering/DevOps and Support. Implementation is intentionally "
            "omitted because it has no instrumented KPI sources. Page 4 is a fixed "
            "list of every Engineering/DevOps KPI. "
            f"Scope: {len(facts)} KPIs, {len(off)} off target, "
            f"{len(worse)} moved worse, {len(better)} moved better, "
            f"{len(unavailable)} unavailable."
        ),
        "kpis": facts,
    }


def build_eng_kpi_claude_plan(digest: dict[str, Any]) -> list[dict[str, Any]]:
    """Exactly two Claude pages: top-three summary and team narratives."""
    changed: list[dict[str, Any]] = list(digest.get("top_changes") or [])[:3]
    team_facts = list(digest.get("engineering_kpis") or []) + list(
        digest.get("support_kpis") or []
    )
    return [
        {
            "id": "summary",
            "slide_type": "summary",
            "title": "Summary — Largest Changes",
            "purpose": (
                "Show only the KPIs with the largest period-over-period movement. "
                "This is a summary, not a scorecard."
            ),
            "must_include": [
                "Show no more than three KPI tiles or rows—exactly the supplied KPIs.",
                "For each show name, team, prior value, current value, and direction.",
                "Preserve each KPI name exactly and use its supplied move string; "
                "do not shorten or relabel one KPI as another.",
                "Use red-ish styling for a move that is worse and blue/teal for better.",
                "Add one short takeaway that connects the largest changes without "
                "inventing causes.",
            ],
            "kpis": changed,
        },
        {
            "id": "teams",
            "slide_type": "team_narrative",
            "title": "How Each Team Is Doing",
            "purpose": (
                "Give a concise, evidence-backed health narrative for each team. "
                "Implementation is omitted by design."
            ),
            "must_include": [
                "Use exactly two clearly separated sections: Engineering/DevOps and Support.",
                "For each team write 2–3 short sentences: overall health, strongest "
                "evidence, and largest risk. Cite KPI names and values.",
                "Judge only against supplied targets and changes. Do not infer causes.",
                "Do not mention Implementation or missing slides.",
            ],
            "kpis": team_facts,
        },
    ]


def render_eng_kpi_claude_slides(
    reqs: list[dict[str, Any]],
    engineering_cards: list[EngKpiCard],
    support_cards: list[EngKpiCard] | None = None,
    *,
    as_of: str,
    start_index: int = 1,
) -> tuple[int, list[str]]:
    """Append Claude-designed pages 2–3. Returns (next index, slide ids)."""
    digest = build_eng_kpi_claude_digest(
        engineering_cards, support_cards, as_of=as_of
    )
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
        len(engineering_cards) + len(support_cards or []),
    )
    return idx, sids
