"""Tests for the Claude-designed Engineering KPI opener slides."""

from __future__ import annotations

import pytest

from src.eng_kpi_claude_slides import (
    EngKpiClaudeError,
    build_eng_kpi_claude_digest,
    build_eng_kpi_claude_plan,
    card_fact,
    render_eng_kpi_claude_slides,
)
from src.eng_kpi_deck import EngKpiCard, HistoryPoint, NotableChange
from src.metrics_claude_slides import MetricsClaudeError, deck_purpose_brief
from src.metrics_digest import DigestRow


def _card(
    name: str,
    value: float | None,
    target: float | None,
    *,
    direction: str = "higher",
    off_target: bool = False,
    grain: str = "month",
    history: tuple[HistoryPoint, ...] = (),
    notable: NotableChange | None = None,
    error: str | None = None,
    tags: tuple[str, ...] = ("engineering",),
) -> EngKpiCard:
    row = DigestRow(
        name=name,
        metric_id=None,
        value=value,
        target=target,
        direction=direction,
        off_target=off_target,
        error=error,
        description=f"{name} definition",
        tags=tags,
    )
    return EngKpiCard(row=row, grain=grain, history=history, notable=notable)


def _cards() -> list[EngKpiCard]:
    hist = (HistoryPoint("2026-06", 349.0), HistoryPoint("2026-07", 424.0), HistoryPoint("2026-08", 280.0))
    return [
        _card(
            "PRs Merged",
            280.0,
            500.0,
            off_target=True,
            history=hist,
            notable=NotableChange(
                kind="failure",
                summary="PRs Merged 424 → 280 (-34%)",
                previous=424.0,
                current=280.0,
            ),
        ),
        _card(
            "Customer-Reported Bugs",
            13.0,
            15.0,
            direction="lower",
            history=(HistoryPoint("2026-07", 19.0), HistoryPoint("2026-08", 13.0)),
            notable=NotableChange(
                kind="success",
                summary="Customer-Reported Bugs 19 → 13 (-32%) — now on target",
                previous=19.0,
                current=13.0,
            ),
        ),
        _card("AI Code Share", 88.85, 80.0, history=(HistoryPoint("2026-07", 88.14),)),
        _card("AI Token Usage", None, None, grain="daily", error="Cursor API 401"),
    ]


def test_card_fact_carries_cadence_and_move() -> None:
    fact = card_fact(_cards()[0])
    assert fact["cadence"] == "month-close"
    assert fact["move_direction"] == "worse"
    assert fact["move"] == "PRs Merged 424 → 280 (-34%)"
    assert fact["history_span"] == "2026-06..2026-08"


def test_card_fact_marks_unavailable_kpis() -> None:
    fact = card_fact(_cards()[3])
    assert fact["status"] == "error"
    assert fact["cadence"] == "trailing window"
    assert "move" not in fact


def test_digest_counts_movement_and_standing() -> None:
    support = [_card("TTFR", 2.0, 48.0, direction="lower", tags=("support",))]
    digest = build_eng_kpi_claude_digest(_cards(), support, as_of="2026-09-20")
    assert digest["function"] == "Engineering/DevOps & Support"
    assert digest["kpi_count"] == 5
    assert digest["off_target_count"] == 1
    assert digest["unavailable_count"] == 1
    assert digest["worse_count"] == 1
    assert digest["better_count"] == 1
    assert digest["support_kpis"][0]["team"] == "Support"


def test_deck_purpose_states_four_page_structure() -> None:
    digest = build_eng_kpi_claude_digest(_cards(), as_of="2026-09-20")
    brief = deck_purpose_brief(digest)
    assert "four-page KPI review" in brief
    assert "Implementation is intentionally omitted" in brief
    assert "2026-09-20" in brief


def test_plan_is_summary_then_team_narrative() -> None:
    digest = build_eng_kpi_claude_digest(_cards(), as_of="2026-09-20")
    plan = build_eng_kpi_claude_plan(digest)
    assert [p["slide_type"] for p in plan] == ["summary", "team_narrative"]
    assert [k["name"] for k in plan[0]["kpis"]] == [
        "PRs Merged",
        "Customer-Reported Bugs",
    ]
    assert len(plan[0]["kpis"]) <= 3


def test_plan_summary_handles_a_quiet_period() -> None:
    quiet = [_card("PRs Merged", 280.0, 500.0, off_target=True)]
    plan = build_eng_kpi_claude_plan(
        build_eng_kpi_claude_digest(quiet, as_of="2026-09-20")
    )
    assert plan[0]["kpis"] == []
    assert plan[1]["title"] == "How Each Team Is Doing"


def test_render_raises_eng_specific_error_on_claude_failure(monkeypatch) -> None:
    def _boom(_plan, _digest, **_kwargs):
        raise MetricsClaudeError("Claude API failed")

    monkeypatch.setattr("src.eng_kpi_claude_slides.generate_metrics_slide_irs", _boom)
    with pytest.raises(EngKpiClaudeError):
        render_eng_kpi_claude_slides([], _cards(), as_of="2026-09-20")


def test_render_appends_one_slide_per_plan_entry(monkeypatch) -> None:
    ir = {"background": "#FFFFFF", "elements": [{"type": "text", "x": 48, "y": 12, "w": 600, "h": 28, "text": "Engineering is mixed"}]}
    monkeypatch.setattr(
        "src.eng_kpi_claude_slides.generate_metrics_slide_irs",
        lambda plan, _digest, **_kwargs: [ir for _ in plan],
    )
    reqs: list = []
    idx, sids = render_eng_kpi_claude_slides(reqs, _cards(), as_of="2026-09-20")
    assert idx == 3
    assert [s.split("_")[-1] for s in sids] == ["summary", "teams"]
    assert reqs
