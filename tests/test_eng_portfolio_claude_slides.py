"""Tests for Claude-designed eng-portfolio slides (IR + generator wiring)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml

from src.claude_slide_ir import (
    CANVAS_H,
    _fit_table_rows,
    _table_column_widths,
    _table_height,
    normalize_slide_ir,
    render_slide_ir,
    rgb_from_hex,
    unify_kpi_row_accents,
)
from src.eng_portfolio_claude_slides import (
    EngPortfolioClaudeError,
    _extract_json_object,
    _llm_slide_completion,
    _warn_slides_without_takeaways,
    build_eng_portfolio_digest,
    digest_for_slide,
    generate_slide_ir_via_claude,
)
from src.slide_metadata import SLIDE_DATA_REQUIREMENTS


def test_rgb_from_hex() -> None:
    assert rgb_from_hex("#FFFFFF")["red"] == 1.0
    assert abs(rgb_from_hex("0B1F33")["blue"] - (0x33 / 255.0)) < 1e-6


def test_normalize_slide_ir_requires_elements() -> None:
    with pytest.raises(ValueError, match="elements"):
        normalize_slide_ir({"background": "#fff"})


def test_normalize_and_render_slide_ir() -> None:
    ir = normalize_slide_ir(
        {
            "background": "#FFFFFF",
            "speaker_notes": "Talk to velocity.",
            "elements": [
                {"type": "rect", "x": 0, "y": 0, "w": 720, "h": 40, "fill": "#0B1F33"},
                {
                    "type": "text",
                    "x": 48,
                    "y": 8,
                    "w": 600,
                    "h": 24,
                    "text": "Executive Summary",
                    "size": 20,
                    "bold": True,
                    "color": "#FFFFFF",
                },
                {
                    "type": "kpi_row",
                    "x": 48,
                    "y": 60,
                    "w": 624,
                    "h": 64,
                    "items": [{"label": "Closed", "value": "12"}],
                },
                {
                    "type": "bullets",
                    "x": 48,
                    "y": 140,
                    "w": 300,
                    "h": 120,
                    "items": ["Risk A", "Risk B"],
                },
                {
                    "type": "takeaway",
                    "x": 48,
                    "y": 360,
                    "w": 624,
                    "h": 30,
                    "text": "Clear the blockers this sprint.",
                    "fill": "#F0F0F0",
                },
            ],
        }
    )
    reqs: list[dict[str, Any]] = []
    next_idx = render_slide_ir(reqs, "s_test_1", ir, 1)
    assert next_idx == 2
    assert any("createSlide" in r for r in reqs)
    assert any("createShape" in r for r in reqs)
    assert any("insertText" in r for r in reqs)


def test_table_column_widths_favor_wide_columns_and_fit() -> None:
    rows = [
        ["KPI", "Value", "Target"],
        ["Escalation Rate (30 Days)", "163%", "15%"],
    ]
    widths = _table_column_widths(rows, 400.0, 3)
    assert abs(sum(widths) - 400.0) < 1.0
    assert widths[0] > widths[1] > 0


def test_fit_table_rows_shrinks_font_before_dropping_rows() -> None:
    rows = [["KPI", "Value"]] + [[f"Metric number {i}", "100"] for i in range(8)]
    widths = _table_column_widths(rows, 400.0, 2)
    size, kept = _fit_table_rows(rows, widths, available_h=CANVAS_H - 160)
    assert len(kept) == len(rows)
    assert size <= 10.0
    assert _table_height(kept, widths, size) <= CANVAS_H - 160


def test_fit_table_rows_trims_when_no_font_fits() -> None:
    rows = [["KPI", "Value"]] + [[f"Metric {i}", "100"] for i in range(20)]
    widths = _table_column_widths(rows, 400.0, 2)
    size, kept = _fit_table_rows(rows, widths, available_h=100.0)
    assert len(kept) < len(rows)
    assert _table_height(kept, widths, size) <= 100.0


def test_table_stops_short_of_a_takeaway_below_it() -> None:
    rows = [["KPI", "Value"]] + [[f"Metric {i}", "100"] for i in range(6)]
    widths = _table_column_widths(rows, 440.0, 2)
    ir = normalize_slide_ir(
        {
            "elements": [
                {"type": "table", "x": 500, "y": 190, "w": 440, "h": 120, "rows": rows},
                {"type": "takeaway", "x": 500, "y": 356, "w": 440, "h": 32, "text": "So what"},
            ]
        }
    )
    reqs: list[dict[str, Any]] = []
    render_slide_ir(reqs, "s_tbl", ir, 1)
    created = next(r["createTable"] for r in reqs if "createTable" in r)
    sizes = {
        r["updateTextStyle"]["style"]["fontSize"]["magnitude"]
        for r in reqs
        if "updateTextStyle" in r and "cellLocation" in r["updateTextStyle"]
    }
    font = min(sizes)
    # Only 160pt of clearance to the takeaway, so the table stops above it
    # instead of rendering rows underneath the bar.
    assert created["rows"] < len(rows)
    assert _table_height(rows[: created["rows"]], widths, font) <= 160.0


def test_kpi_row_value_clears_a_wrapped_label() -> None:
    ir = normalize_slide_ir(
        {
            "elements": [
                {
                    "type": "kpi_row",
                    "x": 48,
                    "y": 60,
                    "w": 110,
                    "h": 76,
                    "items": [
                        {"label": "Aged backlog >30d (target 20%)", "value": "83.3%"}
                    ],
                }
            ]
        }
    )
    reqs: list[dict[str, Any]] = []
    render_slide_ir(reqs, "s_kpi", ir, 1)
    boxes = {
        r["createShape"]["objectId"]: r["createShape"]["elementProperties"]["transform"]
        for r in reqs
        if "createShape" in r and r["createShape"]["shapeType"] == "TEXT_BOX"
    }
    label_y = boxes["s_kpi_e0_kl0"]["translateY"]
    value_y = boxes["s_kpi_e0_kv0"]["translateY"]
    # Two-line label pushes the number below it instead of overlapping.
    assert value_y - label_y > 24


def test_build_eng_portfolio_digest_trims() -> None:
    report = {
        "days": 30,
        "eng_portfolio": {
            "closed_count": 10,
            "open_bugs": [{"key": f"B-{i}"} for i in range(40)],
        },
        "cursor_usage": {"configured": True, "totals": {"tokens": 1}},
    }
    digest = build_eng_portfolio_digest(report)
    assert digest["eng_portfolio"]["closed_count"] == 10
    assert len(digest["eng_portfolio"]["open_bugs"]) == 15
    assert digest["cursor_usage"]["totals"]["tokens"] == 1


def test_llm_slide_completion_omits_temperature(monkeypatch: pytest.MonkeyPatch) -> None:
    """Claude Opus rejects temperature; eng-portfolio must not send it."""
    captured: list[dict[str, Any]] = []

    class _Msg:
        content = '{"background":"#fff","elements":[]}'

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    def _fake(_c, **kwargs):
        captured.append(kwargs)
        return _Resp()

    monkeypatch.setattr("src.eng_portfolio_claude_slides._llm_create_with_retry", _fake)
    text = _llm_slide_completion(
        MagicMock(),
        model="claude-opus-5",
        messages=[{"role": "user", "content": "hi"}],
    )
    assert text.startswith("{")
    assert captured, "expected at least one LLM call"
    assert "temperature" not in captured[0]
    assert captured[0]["model"] == "claude-opus-5"
    assert captured[0]["max_tokens"] > 0


def test_generate_slide_ir_via_claude_parses_json(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "background": "#FFFFFF",
        "elements": [
            {"type": "text", "x": 40, "y": 40, "w": 400, "h": 30, "text": "Hello", "size": 18},
        ],
    }

    class _Msg:
        content = json.dumps(payload)

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    client = MagicMock()
    client.chat.completions.create.return_value = _Resp()

    monkeypatch.setattr(
        "src.eng_portfolio_claude_slides._llm_create_with_retry",
        lambda _c, **_kw: _Resp(),
    )

    ir = generate_slide_ir_via_claude(
        entry={"id": "eng_exec_summary", "slide_type": "eng_exec_summary", "title": "Exec"},
        digest={"days": 30, "eng_portfolio": {"closed_count": 3}},
        client=client,
        model="claude-opus-5",
    )
    assert ir["elements"][0]["text"] == "Hello"


def test_extract_json_object_from_noise() -> None:
    raw = 'Sure.\n```json\n{"background":"#fff","elements":[{"type":"text","x":1,"y":1,"w":10,"h":10,"text":"Hi"}]}\n```\n'
    blob = _extract_json_object(raw)
    data = json.loads(blob)
    assert data["elements"][0]["text"] == "Hi"


def test_long_takeaway_shrinks_to_fit_its_box() -> None:
    """Copy that would overflow a short band is set smaller, not left to run off-page."""
    long_text = (
        "Intake is balanced at 147 in and 146 out, so the drag is aged inventory: "
        "clearing the 83 abandoned items frees more capacity than hiring would."
    )
    reqs: list[dict[str, Any]] = []
    render_slide_ir(
        reqs,
        "s_fit",
        {
            "elements": [
                {
                    "type": "takeaway",
                    "x": 48,
                    "y": 360,
                    "w": 300,
                    "h": 24,
                    "text": long_text,
                    "size": 14,
                }
            ]
        },
        0,
    )
    sizes = [
        r["updateTextStyle"]["style"]["fontSize"]["magnitude"]
        for r in reqs
        if "updateTextStyle" in r
    ]
    assert sizes and sizes[0] < 14


def test_deck_plan_opens_with_takeaways_slide() -> None:
    root = Path(__file__).resolve().parents[1]
    deck = yaml.safe_load((root / "decks" / "engineering-portfolio.yaml").read_text())
    plan = [str(e.get("slide")) for e in deck["slides"]]
    assert plan[:2] == ["eng_portfolio_title", "eng_takeaways"]

    slide = yaml.safe_load((root / "slides" / "eng-10ab-takeaways.yaml").read_text())
    assert slide["id"] == "eng_takeaways"
    assert slide["slide_type"] == "eng_takeaways"
    assert SLIDE_DATA_REQUIREMENTS["eng_takeaways"]


def test_unify_kpi_row_accents_collapses_decorative_colors() -> None:
    items = [
        {"label": "Closed", "value": "42", "fill": "#E8F4FC", "color": "#009AFF"},
        {"label": "Open", "value": "18", "fill": "#AEFFF6", "color": "#38C0CE"},
        {"label": "Merged", "value": "31", "fill": "#EEF0F3", "color": "#7BC4FA"},
    ]
    out = unify_kpi_row_accents(items)
    assert {(it["fill"], it["color"]) for it in out} == {("#E8F4FC", "#009AFF")}


def test_unify_kpi_row_accents_keeps_status_and_single_callout() -> None:
    items = [
        {"label": "Velocity", "value": "42", "fill": "#E8F4FC", "color": "#009AFF"},
        {"label": "Escalations", "value": "7", "fill": "#FDECEA", "color": "#C0392B"},
        {"label": "AI share", "value": "n/a", "fill": "#EEF0F3", "color": "#6B7280"},
        {"label": "Reactive load", "value": "31%", "fill": "#AEFFF6", "color": "#0B1F33"},
    ]
    assert unify_kpi_row_accents(items) == items


def test_normalize_slide_ir_unifies_kpi_row_colors() -> None:
    ir = normalize_slide_ir(
        {
            "elements": [
                {
                    "type": "kpi_row",
                    "x": 48,
                    "y": 72,
                    "w": 624,
                    "h": 72,
                    "items": [
                        {"label": "A", "value": "1", "color": "#009AFF"},
                        {"label": "B", "value": "2", "color": "#38C0CE"},
                    ],
                }
            ]
        }
    )
    assert {it["color"] for it in ir["elements"][0]["items"]} == {"#009AFF"}


def test_warn_slides_without_takeaways_skips_exempt_slides(
    caplog: pytest.LogCaptureFixture,
) -> None:
    plan = [
        {"id": "eng_portfolio_title", "slide_type": "eng_portfolio_title"},
        {"id": "eng_takeaways", "slide_type": "eng_takeaways"},
        {"id": "eng_divider_2", "slide_type": "eng_divider"},
        {"id": "eng_velocity", "slide_type": "eng_velocity"},
    ]
    irs: list[dict[str, Any]] = [
        {"elements": [{"type": "text", "text": "Cover"}]},
        {"elements": [{"type": "bullets", "items": ["One"]}]},
        {"elements": [{"type": "text", "text": "Quality"}]},
        {"elements": [{"type": "takeaway", "text": "Velocity held flat."}]},
    ]
    with caplog.at_level("WARNING"):
        _warn_slides_without_takeaways(plan, irs)
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "eng_divider_2" in messages
    assert "eng_portfolio_title" not in messages
    assert "eng_velocity" not in messages


def test_build_digest_keeps_integrations_beside_a_huge_jira_blob() -> None:
    """A 700 KB Jira payload must not push the small Cursor/GitHub facts out."""
    report = {
        "days": 30,
        "eng_portfolio": {
            "in_flight_count": 1083,
            "closed_count": 346,
            "by_assignee": [{"name": f"dev{i}", "detail": "x" * 400} for i in range(300)],
            "open_bugs": [{"key": f"B-{i}", "summary": "y" * 300} for i in range(200)],
        },
        "cursor_usage": {"configured": True, "totals": {"tokens": 580799806, "seats": 57}},
        "github_productivity": {"configured": True, "totals": {"prs": 214}},
    }
    digest = build_eng_portfolio_digest(report)
    assert digest["cursor_usage"]["totals"]["tokens"] == 580799806
    assert digest["github_productivity"]["totals"]["prs"] == 214

    scoped = digest_for_slide(digest, "cursor_cost")
    assert scoped["cursor_usage"]["totals"]["seats"] == 57


def test_digest_for_slide_scopes_eng_sections_per_slide() -> None:
    eng = {
        "sprint": {"name": "S1"},
        "in_flight_count": 5,
        "closed_count": 3,
        "sprint_velocity": [{"sprint": "S1", "points": 20}],
        "support_pressure": {"reactive_pct": 31},
        "team_scorecard": [{"name": "dev1"}],
    }
    velocity = digest_for_slide({"days": 30, "eng_portfolio": eng}, "eng_velocity")
    assert "sprint_velocity" in velocity["eng_portfolio"]
    assert "support_pressure" not in velocity["eng_portfolio"]

    support = digest_for_slide({"days": 30, "eng_portfolio": eng}, "eng_support_pressure")
    assert "support_pressure" in support["eng_portfolio"]
    assert "sprint_velocity" not in support["eng_portfolio"]


def test_digest_for_slide_gives_takeaways_slide_full_context() -> None:
    full = {
        "days": 30,
        "eng_portfolio": {"closed_count": 9, "flow": {"active_count": 3}},
        "cursor_usage": {"totals": {"tokens": 1}},
        "github_productivity": {"totals": {"prs": 2}},
    }
    scoped = digest_for_slide(full, "eng_takeaways")
    assert scoped["cursor_usage"]["totals"]["tokens"] == 1
    assert scoped["github_productivity"]["totals"]["prs"] == 2
    assert (scoped["eng_portfolio"] or {}).get("flow") == {"active_count": 3}


def test_digest_for_slide_scopes_cursor() -> None:
    full = {
        "days": 30,
        "eng_portfolio": {"closed_count": 9, "flow": {"active_count": 3}},
        "cursor_usage": {"totals": {"tokens": 1}},
        "github_productivity": {"totals": {"prs": 2}},
    }
    scoped = digest_for_slide(full, "cursor_cost")
    assert "cursor_usage" in scoped
    assert "flow" not in (scoped.get("eng_portfolio") or {})


def test_generate_slide_ir_retries_on_bad_json(monkeypatch: pytest.MonkeyPatch) -> None:
    good = {
        "background": "#FFFFFF",
        "elements": [
            {"type": "text", "x": 40, "y": 40, "w": 400, "h": 30, "text": "Fixed", "size": 18},
        ],
    }
    calls: list[str] = []

    def _fake(_c, **kwargs):
        calls.append("x")

        class _Msg:
            content = "{bad" if len(calls) == 1 else json.dumps(good)

        class _Choice:
            message = _Msg()

        class _Resp:
            choices = [_Choice()]

        return _Resp()

    monkeypatch.setattr("src.eng_portfolio_claude_slides._llm_create_with_retry", _fake)
    ir = generate_slide_ir_via_claude(
        entry={"id": "eng_toc", "slide_type": "eng_toc"},
        digest={"days": 30},
        client=MagicMock(),
        model="claude-opus-5",
    )
    assert ir["elements"][0]["text"] == "Fixed"
    assert len(calls) == 2


def test_generate_slide_ir_via_claude_fail_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.eng_portfolio_claude_slides._llm_create_with_retry",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with pytest.raises(EngPortfolioClaudeError, match="Claude API failed"):
        generate_slide_ir_via_claude(
            entry={"id": "eng_toc", "slide_type": "eng_toc"},
            digest={},
            client=MagicMock(),
        )


def test_default_eng_portfolio_model_is_opus() -> None:
    from src.config import CORTEX_ENG_PORTFOLIO_LLM_MODEL

    assert CORTEX_ENG_PORTFOLIO_LLM_MODEL == "claude-opus-5" or CORTEX_ENG_PORTFOLIO_LLM_MODEL  # env may override
    # When unset in the test env, config default should be opus.
    import os
    if not (
        os.environ.get("CORTEX_ENG_PORTFOLIO_LLM_MODEL")
        or os.environ.get("CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL")
    ):
        assert CORTEX_ENG_PORTFOLIO_LLM_MODEL == "claude-opus-5"


def test_render_slide_plan_routes_to_claude(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import deck_renderer
    import src.config as cfg

    called: dict[str, Any] = {}

    def _fake(report, slide_plan, deck_id, *, deck_purpose=""):
        called["deck_id"] = deck_id
        called["n"] = len(slide_plan)
        return [], 0, [], None, list(slide_plan)

    monkeypatch.setattr(cfg, "CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES", True)
    monkeypatch.setattr(
        "src.eng_portfolio_claude_slides.render_eng_portfolio_claude_slide_plan",
        _fake,
    )

    _reqs, n, _notes, deferred, _plan = deck_renderer.render_slide_plan(
        {},
        [{"id": "eng_portfolio_title", "slide_type": "eng_portfolio_title"}],
        "engineering-portfolio",
    )
    assert called["deck_id"] == "engineering-portfolio"
    assert called["n"] == 1
    assert n == 0
    assert deferred is None
