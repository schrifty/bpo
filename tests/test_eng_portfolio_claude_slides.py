"""Tests for Claude-designed eng-portfolio slides (IR + generator wiring)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.claude_slide_ir import (
    CANVAS_H,
    _fit_table_rows,
    _table_column_widths,
    _table_height,
    normalize_slide_ir,
    render_slide_ir,
    rgb_from_hex,
)
from src.eng_portfolio_claude_slides import (
    EngPortfolioClaudeError,
    _extract_json_object,
    _llm_slide_completion,
    build_eng_portfolio_digest,
    digest_for_slide,
    generate_slide_ir_via_claude,
)


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
