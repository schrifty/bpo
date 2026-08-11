"""Tests for Claude-designed metrics scorecard slides."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.metrics_claude_slides import (
    MetricsClaudeError,
    build_metrics_deck_plan,
    build_metrics_digest,
    deck_purpose_brief,
    function_label,
    generate_metrics_slide_ir,
    render_metrics_claude_slides,
    row_fact,
)
from src.metrics_digest import DigestRow


def _row(
    name: str,
    value: float | None,
    target: float | None,
    *,
    direction: str = "lower",
    off_target: bool = False,
    unit: str | None = None,
    context: str | None = None,
    error: str | None = None,
) -> DigestRow:
    return DigestRow(
        name=name,
        metric_id=None,
        value=value,
        target=target,
        direction=direction,
        off_target=off_target,
        error=error,
        description=f"{name} definition",
        tags=("support",),
        unit=unit,
        context=context,
    )


def _rows() -> list[DigestRow]:
    return [
        _row("TTFR (30 Days)", 72.0, 48.0, off_target=True, context="120 tickets"),
        _row("Median TTR (30 Days)", 30.0, 40.0),
        _row(
            "SLA Adherence (30 Days)",
            92.0,
            90.0,
            direction="higher",
            unit="percent",
        ),
        _row("P90 TTR (30 Days)", None, 120.0, error="Jira unavailable"),
    ]


def test_function_label_falls_back_to_title_case() -> None:
    assert function_label("support") == "Customer Support"
    assert function_label("data_platform") == "Data Platform"
    assert function_label(None) == "The Business"


def test_row_fact_signs_gap_by_direction() -> None:
    worse_when_high = row_fact(_row("TTFR", 72.0, 48.0, off_target=True))
    assert worse_when_high["status"] == "off_target"
    assert worse_when_high["pct_vs_target"] == -50.0

    better_when_high = row_fact(
        _row("Adherence", 99.0, 90.0, direction="higher", unit="percent")
    )
    assert better_when_high["points_vs_target"] == 9.0


def test_row_fact_uses_points_for_percent_kpis() -> None:
    """A percent-of-a-percent gap reads as nonsense (163% vs 15% is not -989%)."""
    fact = row_fact(
        _row("Escalation Rate", 163.33, 15.0, unit="percent", off_target=True)
    )
    assert "pct_vs_target" not in fact
    assert fact["points_vs_target"] == -148.3


def test_row_fact_reports_unavailable_metrics() -> None:
    fact = row_fact(_row("P90 TTR", None, 120.0, error="Jira unavailable"))
    assert fact["status"] == "error"
    assert fact["value"] == "error"
    assert "Jira unavailable" in fact["error"]


def test_build_metrics_digest_counts_standing() -> None:
    digest = build_metrics_digest(_rows(), tag="support", as_of="2026-08-07")
    assert digest["function"] == "Customer Support"
    assert digest["kpi_count"] == 4
    assert digest["off_target_count"] == 1
    assert digest["on_target_count"] == 2
    assert digest["unavailable_count"] == 1


def test_deck_purpose_states_the_60_second_goal() -> None:
    digest = build_metrics_digest(_rows(), tag="support", as_of="2026-08-07")
    brief = deck_purpose_brief(digest)
    assert "where Customer Support stands" in brief
    assert "60 seconds" in brief


def test_plan_leads_with_standing_then_off_target() -> None:
    digest = build_metrics_digest(_rows(), tag="support", as_of="2026-08-07")
    plan = build_metrics_deck_plan(digest)
    assert [p["slide_type"] for p in plan] == ["standing", "attention", "detail"]
    assert plan[0]["title"] == "Where Customer Support Stands"
    assert [k["name"] for k in plan[1]["kpis"]] == ["TTFR (30 Days)"]
    # Detail pages carry every KPI, off-target first.
    assert plan[2]["kpis"][0]["name"] == "TTFR (30 Days)"
    assert len(plan[2]["kpis"]) == 4


def test_plan_omits_attention_slide_when_all_on_target() -> None:
    rows = [_row("Median TTR", 30.0, 40.0)]
    plan = build_metrics_deck_plan(
        build_metrics_digest(rows, tag="support", as_of="2026-08-07")
    )
    assert [p["slide_type"] for p in plan] == ["standing", "detail"]


def test_plan_balances_detail_pages() -> None:
    rows = [_row(f"KPI {i}", float(i), 10.0) for i in range(8)]
    plan = build_metrics_deck_plan(
        build_metrics_digest(rows, tag="support", as_of="2026-08-07")
    )
    details = [p for p in plan if p["slide_type"] == "detail"]
    assert [len(p["kpis"]) for p in details] == [4, 4]


def test_plan_paginates_long_scorecards() -> None:
    rows = [_row(f"KPI {i}", float(i), 10.0) for i in range(15)]
    plan = build_metrics_deck_plan(
        build_metrics_digest(rows, tag="support", as_of="2026-08-07")
    )
    details = [p for p in plan if p["slide_type"] == "detail"]
    assert len(details) == 3
    assert details[0]["title"].endswith("(1 of 3)")
    assert sum(len(p["kpis"]) for p in details) == 15


def _resp(content: str) -> Any:
    class _Msg:
        pass

    msg = _Msg()
    msg.content = content

    class _Choice:
        message = msg

    class _Resp:
        choices = [_Choice()]

    return _Resp()


_GOOD_IR = {
    "background": "#FFFFFF",
    "elements": [
        {
            "type": "text",
            "x": 48,
            "y": 12,
            "w": 600,
            "h": 28,
            "text": "Support is under pressure",
            "size": 20,
        }
    ],
}


def test_generate_metrics_slide_ir_sends_purpose_and_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, Any]] = []

    def _fake(_c, **kwargs):
        captured.append(kwargs)
        return _resp(json.dumps(_GOOD_IR))

    monkeypatch.setattr("src.metrics_claude_slides._llm_create_with_retry", _fake)
    digest = build_metrics_digest(_rows(), tag="support", as_of="2026-08-07")
    plan = build_metrics_deck_plan(digest)

    ir = generate_metrics_slide_ir(entry=plan[0], digest=digest, client=MagicMock())

    assert ir["elements"][0]["text"] == "Support is under pressure"
    user_msg = captured[0]["messages"][-1]["content"]
    assert "DECK PURPOSE" in user_msg
    assert "where Customer Support stands" in user_msg
    assert "TTFR (30 Days)" in user_msg
    assert "temperature" not in captured[0]


def test_generate_metrics_slide_ir_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.metrics_claude_slides._llm_create_with_retry",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(MetricsClaudeError, match="Claude API failed"):
        generate_metrics_slide_ir(
            entry={"id": "standing", "slide_type": "standing", "kpis": []},
            digest={"function": "Customer Support", "as_of": "2026-08-07"},
            client=MagicMock(),
        )


def test_render_metrics_claude_slides_appends_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.metrics_claude_slides._llm_create_with_retry",
        lambda *_a, **_k: _resp(json.dumps(_GOOD_IR)),
    )
    monkeypatch.setattr(
        "src.metrics_claude_slides.metrics_llm_client", lambda: MagicMock()
    )

    reqs: list[dict[str, Any]] = []
    next_idx, sids = render_metrics_claude_slides(
        reqs, _rows(), tag="support", as_of="2026-08-07", start_index=1
    )
    assert len(sids) == 3
    assert next_idx == 4
    assert sum(1 for r in reqs if "createSlide" in r) == 3
    assert sids[0].endswith("standing")
