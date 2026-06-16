"""Render tests for Cursor AI-coding slides (cost / usage / users / efficiency).

Without ``_charts`` in the report, chart panels are omitted. Chart-specific tests use a
MagicMock ``_charts`` and assert embed requests plus bordered panel shapes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.slide_engineering_portfolio import (
    _clamp_eng_takeaway,
    cursor_cost_slide,
    cursor_efficiency_slide,
    cursor_model_usage_slide,
    cursor_users_slide,
    cursor_users_non_engineers_slide,
    cursor_usage_slide,
    cursor_usage_non_engineers_slide,
)


def _cursor_report() -> dict:
    cu = {
        "configured": True,
        "window_days": 30,
        "members": {"total": 10, "active_window": 4},
        "totals": {
            "total_tokens": 1_200_000,
            "input_tokens": 900_000,
            "output_tokens": 300_000,
            "event_count": 5_000,
            "charged_cents_window": 978_700,
            "spend_cents_cycle": 41_000,
            "included_spend_cents_cycle": 500_000,
        },
        "daily": [
            {"date": "2026-04-01", "label": "4/1", "input_tokens": 400_000, "output_tokens": 120_000,
             "total_tokens": 520_000, "cents": 11_000, "events": 2_100, "active_users": 4},
            {"date": "2026-04-02", "label": "4/2", "input_tokens": 500_000, "output_tokens": 180_000,
             "total_tokens": 680_000, "cents": 14_000, "events": 2_900, "active_users": 3},
        ],
        "model_mix": [
            {"model": "claude-4.5-sonnet", "tokens": 800_000, "cents": 18_000, "share": 0.6667},
            {"model": "gpt-5", "tokens": 400_000, "cents": 7_000, "share": 0.3333},
        ],
        "cost_engineers": {
            "configured": True,
            "headcount": 8,
            "seats": 8,
            "active_window": 4,
            "totals": {
                "charged_cents_window": 25_000,
                "spend_cents_cycle": 41_000,
                "included_spend_cents_cycle": 400_000,
            },
            "daily": [
                {"date": "2026-04-01", "label": "4/1", "cents": 11_000, "active_users": 4},
                {"date": "2026-04-02", "label": "4/2", "cents": 14_000, "active_users": 3},
            ],
            "model_mix": [
                {"model": "claude-4.5-sonnet", "tokens": 800_000, "cents": 18_000, "share": 0.6667},
                {"model": "gpt-5", "tokens": 400_000, "cents": 7_000, "share": 0.3333},
            ],
        },
        "usage_engineers": {
            "configured": True,
            "seats": 8,
            "active_window": 4,
            "totals": {
                "total_tokens": 1_000_000,
                "input_tokens": 750_000,
                "output_tokens": 250_000,
                "event_count": 4_200,
            },
            "daily": [
                {"date": "2026-04-01", "label": "4/1", "input_tokens": 350_000, "output_tokens": 100_000},
                {"date": "2026-04-02", "label": "4/2", "input_tokens": 400_000, "output_tokens": 150_000},
            ],
            "model_mix": [
                {"model": "claude-4.5-sonnet", "tokens": 700_000, "events": 3_000, "cents": 18_000, "share": 0.7},
                {"model": "gpt-5", "tokens": 300_000, "events": 1_200, "cents": 7_000, "share": 0.3},
            ],
        },
        "usage_non_engineers": {
            "configured": True,
            "seats": 2,
            "active_window": 2,
            "totals": {
                "total_tokens": 200_000,
                "input_tokens": 150_000,
                "output_tokens": 50_000,
                "event_count": 800,
            },
            "daily": [
                {"date": "2026-04-01", "label": "4/1", "input_tokens": 50_000, "output_tokens": 20_000},
                {"date": "2026-04-02", "label": "4/2", "input_tokens": 100_000, "output_tokens": 30_000},
            ],
            "model_mix": [
                {"model": "gpt-5", "tokens": 120_000, "events": 500, "cents": 4_000, "share": 0.6},
                {"model": "Auto (default)", "tokens": 80_000, "events": 300, "cents": 2_000, "share": 0.4},
            ],
        },
        "users_engineers": {
            "configured": True,
            "seats": 8,
            "active_window": 4,
            "totals": {"total_tokens": 1_000_000},
            "top_users": [
                {"email": "ada@x.com", "tokens": 700_000, "events": 3_000,
                 "models": [{"model": "claude-4.5-sonnet", "tokens": 600_000, "share": 0.86}]},
                {"email": "linus@x.com", "tokens": 300_000, "events": 2_000,
                 "models": [{"model": "gpt-5", "tokens": 300_000, "share": 1.0}]},
            ],
            "bottom_users": [
                {"email": "grace@x.com", "tokens": 12_000, "events": 40},
                {"email": "dan@x.com", "tokens": 8_500, "events": 22},
            ],
            "user_model_matrix": {
                "users": ["ada@x.com", "linus@x.com"],
                "models": ["claude-4.5-sonnet", "gpt-5"],
                "series": {"claude-4.5-sonnet": [600_000, 0], "gpt-5": [100_000, 300_000]},
            },
        },
        "users_non_engineers": {
            "configured": True,
            "seats": 2,
            "active_window": 2,
            "totals": {"total_tokens": 200_000},
            "top_users": [
                {"email": "pm@x.com", "tokens": 120_000, "events": 400,
                 "models": [{"model": "gpt-5", "tokens": 120_000, "share": 1.0}]},
                {"email": "cs@x.com", "tokens": 80_000, "events": 300,
                 "models": [{"model": "Auto (default)", "tokens": 80_000, "share": 1.0}]},
            ],
            "bottom_users": [
                {"email": "cs@x.com", "tokens": 80_000, "events": 300},
            ],
            "user_model_matrix": {
                "users": ["pm@x.com", "cs@x.com"],
                "models": ["gpt-5", "Auto (default)"],
                "series": {"gpt-5": [120_000, 0], "Auto (default)": [0, 80_000]},
            },
        },
        "top_users": [
            {"email": "ada@x.com", "tokens": 700_000, "input_tokens": 520_000, "output_tokens": 180_000,
             "events": 3_000, "window_cents": 15_000, "spend_cents": 22_000,
             "models": [{"model": "claude-4.5-sonnet", "tokens": 600_000, "share": 0.86}]},
            {"email": "linus@x.com", "tokens": 500_000, "input_tokens": 380_000, "output_tokens": 120_000,
             "events": 2_000, "window_cents": 10_000, "spend_cents": 19_000,
             "models": [{"model": "gpt-5", "tokens": 400_000, "share": 0.8}]},
        ],
        "bottom_users": [
            {"email": "grace@x.com", "tokens": 12_000, "events": 40,
             "models": [{"model": "gpt-5", "tokens": 12_000, "share": 1.0}]},
            {"email": "dan@x.com", "tokens": 8_500, "events": 22,
             "models": [{"model": "Auto (default)", "tokens": 8_500, "share": 1.0}]},
        ],
        "user_model_matrix": {
            "users": ["ada@x.com", "linus@x.com"],
            "models": ["claude-4.5-sonnet", "gpt-5"],
            "series": {"claude-4.5-sonnet": [600_000, 100_000], "gpt-5": [100_000, 400_000]},
        },
        "efficiency": {
            "accepted_lines": 120_000,
            "total_lines": 150_000,
            "lines_kept": 0.8,
            "total_tokens": 1_200_000,
            "charged_cents_window": 25_000,
            "accepted_lines_per_1k_tokens": 100.0,
            "cost_per_accepted_line_cents": 0.21,
            "daily": [
                {"date": "2026-04-01", "label": "4/1", "accepted_lines": 60_000, "total_lines": 75_000, "cents": 11_000},
                {"date": "2026-04-02", "label": "4/2", "accepted_lines": 60_000, "total_lines": 75_000, "cents": 14_000},
            ],
            "top_efficiency": [
                {"email": "ada@x.com", "accepted_lines": 70_000, "tokens": 700_000,
                 "cents": 15_000, "lines_per_1k_tokens": 100.0, "cents_per_line": 0.21},
                {"email": "linus@x.com", "accepted_lines": 40_000, "tokens": 500_000,
                 "cents": 10_000, "lines_per_1k_tokens": 80.0, "cents_per_line": 0.25},
            ],
        },
        "takeaways": {"cost": "Cost up.", "usage": "Tokens up.", "usage_non_engineers": "Non-eng tokens up.",
                      "users": "Concentrated.", "users_non_engineers": "Non-eng concentrated.",
                      "efficiency": "Efficient."},
        "errors": [],
    }
    return {"cursor_usage": cu}


def _texts(reqs: list) -> str:
    return " ".join(
        r["insertText"]["text"] for r in reqs if isinstance(r, dict) and "insertText" in r
    )


def _title(reqs: list, sid: str) -> str:
    for r in reqs:
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == f"{sid}_ttl":
            return r["insertText"]["text"]
    return ""


def _chart_embeds(reqs: list) -> list[dict]:
    return [r["createSheetsChart"] for r in reqs if isinstance(r, dict) and "createSheetsChart" in r]


def _chart_panel_ids(reqs: list, sid: str) -> list[str]:
    return [
        r["createShape"]["objectId"]
        for r in reqs
        if isinstance(r, dict)
        and r.get("createShape", {}).get("shapeType") == "RECTANGLE"
        and str(r["createShape"].get("objectId", "")).startswith(f"{sid}_")
        and str(r["createShape"]["objectId"]).endswith("_pnl")
    ]


def _mock_charts() -> MagicMock:
    charts = MagicMock()
    charts.add_bar_chart.return_value = ("spreadsheet_id", 101)
    charts.add_combo_chart.return_value = ("spreadsheet_id", 102)
    return charts


def test_clamp_eng_takeaway_strips_stray_quote_and_truncates() -> None:
    assert _clamp_eng_takeaway('Assign a team to clear bugs."') == "Assign a team to clear bugs."
    long = "word " * 80
    clipped = _clamp_eng_takeaway(long)
    assert clipped.endswith("…")
    assert len(clipped) < len(long)


def test_cost_slide_renders_spend_and_model_cost() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_cost_slide(reqs, "sid_c", rep, 0)
    assert _title(reqs, "sid_c") == "Cursor AI Coding Spend"
    text = _texts(reqs)
    assert "30d run rate" in text
    assert "Cost / active eng" in text
    assert "Active engineers" in text
    assert "Total included usage" in text
    assert "$9,787" in text  # org-wide 30d run rate (978_700 cents)
    assert "$5,000" in text  # org-wide included usage (500_000 cents)
    assert "Usage cost" not in text
    assert "Cycle overage" not in text
    # Engineer-scoped slide must not show seat-based metrics.
    assert "Idle" not in text
    assert "30d" in text
    assert "Spend by model" in text
    combo_title = rep["_charts"].add_combo_chart.call_args.kwargs["title"]
    assert "Cost over time" in combo_title
    assert "dev-* engineers" in combo_title
    assert "Cost over time" in text
    assert len(_chart_embeds(reqs)) == 1
    assert _chart_panel_ids(reqs, "sid_c")
    # Per-model cost dollar value appears.
    assert "$180" in text  # claude cost (18,000 cents)


def test_cost_slide_missing_included_usage_shows_dash() -> None:
    rep = _cursor_report()
    rep["cursor_usage"]["totals"]["included_spend_cents_cycle"] = None
    reqs: list = []
    cursor_cost_slide(reqs, "sid_c0", rep, 0)
    text = _texts(reqs)
    assert "Total included usage" in text
    assert "—" in text  # missing includedSpendCents → dash on KPI card


def test_users_slide_renders_token_volume_columns() -> None:
    rep = _cursor_report()
    for u in rep["cursor_usage"]["users_engineers"]["top_users"]:
        u["spend_cents"] = 0
    reqs: list = []
    cursor_users_slide(reqs, "sid_w0", rep, 0)
    text = _texts(reqs)
    # Tokens still render; cost column was removed from the volume lists.
    assert "700K" in text or "700" in text
    assert "grace" in text
    assert "dan" in text


def test_usage_slide_renders_tokens_and_chart_only() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_usage_slide(reqs, "sid_u", rep, 0)
    assert _title(reqs, "sid_u") == "Cursor AI Token Usage"
    text = _texts(reqs)
    assert "Input tokens" in text and "Output tokens" in text
    assert "30d" in text
    # Chart title is in the embedded Sheets chart, not a duplicate slide-level header.
    title_arg = rep["_charts"].add_bar_chart.call_args.kwargs["title"]
    assert "Tokens over time" in title_arg
    assert "dev-* engineers" in title_arg
    assert "Tokens over time" in text  # slide-level panel title
    assert len(_chart_embeds(reqs)) == 1
    assert _chart_panel_ids(reqs, "sid_u")
    # The per-model mix panel moved to the dedicated model-usage slide.
    assert "Model usage (by tokens)" not in text


def test_usage_non_engineers_slide_renders_scoped_tokens() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_usage_non_engineers_slide(reqs, "sid_un", rep, 0)
    assert _title(reqs, "sid_un") == "Cursor AI Token Usage — Non-Engineering"
    text = _texts(reqs)
    assert "Input tokens" in text and "Output tokens" in text
    assert "30d" in text
    title_arg = rep["_charts"].add_bar_chart.call_args.kwargs["title"]
    assert "Tokens over time" in title_arg
    assert "non-engineering users" in title_arg
    assert len(_chart_embeds(reqs)) == 1


def test_model_usage_slide_renders_both_audiences_and_percentages() -> None:
    reqs: list = []
    cursor_model_usage_slide(reqs, "sid_m", _cursor_report(), 0)
    assert _title(reqs, "sid_m") == "Cursor AI Model Usage"
    text = _texts(reqs)
    assert "Model mix by audience" in text
    assert "30d" in text
    # Both audiences are labeled (grouped in the combined table's Audience column).
    assert "Engineering" in text
    assert "Non-engineering" in text
    # Model names appear (engineering + non-engineering, friendly relabel kept).
    assert "claude-4.5-sonnet" in text
    assert "Auto (default)" in text
    # Both percentage columns are present.
    assert "% of tokens" in text
    assert "% of volume" in text
    # Token share (70% of 1.0M) and request-volume share (3,000 / 4,200 = 71%) both render.
    assert "70%" in text  # claude tokens / total tokens
    assert "71%" in text  # claude requests / total requests


def test_users_slide_renders_power_users_and_concentration() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_users_slide(reqs, "sid_w", rep, 0)
    assert _title(reqs, "sid_w") == "Cursor AI Power Users"
    text = _texts(reqs)
    assert "Top-user share" in text
    assert "Top-3 share" in text
    # Scoped slide must not surface seat-based adoption/idle metrics.
    assert "Adoption" not in text
    assert "Idle" not in text
    assert "Highest volume" in text
    assert "Lowest volume" in text
    assert "30d" in text
    title_arg = rep["_charts"].add_bar_chart.call_args.kwargs["title"]
    assert "dev-* engineers" in title_arg
    assert len(_chart_embeds(reqs)) == 1
    assert "ada" in text  # short email of top user
    assert "grace" in text  # short email of low-volume user


def test_users_non_engineers_slide_renders_scoped_power_users() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_users_non_engineers_slide(reqs, "sid_wn", rep, 0)
    assert _title(reqs, "sid_wn") == "Cursor AI Power Users — Non-Engineering"
    text = _texts(reqs)
    assert "Active users" in text
    assert "30d" in text
    title_arg = rep["_charts"].add_bar_chart.call_args.kwargs["title"]
    assert "non-engineering users" in title_arg
    assert len(_chart_embeds(reqs)) == 1
    assert "pm" in text
    assert "Highest volume" in text


def test_efficiency_slide_renders_ratios_and_engineers() -> None:
    rep = _cursor_report()
    rep["_charts"] = _mock_charts()
    reqs: list = []
    cursor_efficiency_slide(reqs, "sid_e", rep, 0)
    assert _title(reqs, "sid_e") == "Cursor AI Coding Efficiency"
    text = _texts(reqs)
    assert "Lines kept" in text
    assert "Lines / 1K tokens" in text
    assert "Most efficient engineers" in text
    assert "30d" in text
    combo_title = rep["_charts"].add_combo_chart.call_args.kwargs["title"]
    assert "Output vs. cost over time" in combo_title
    assert len(_chart_embeds(reqs)) == 1
    assert "ada" in text  # short email of most-efficient engineer
    assert "0.21" in text  # cost per accepted line shown in cents (0.21¢)


def test_cursor_slides_emit_missing_data_when_unconfigured() -> None:
    rep = {"cursor_usage": {"configured": False}}
    for builder in (
        cursor_cost_slide, cursor_usage_slide, cursor_usage_non_engineers_slide,
        cursor_model_usage_slide, cursor_efficiency_slide,
        cursor_users_slide, cursor_users_non_engineers_slide,
    ):
        reqs: list = []
        builder(reqs, "sid_x", rep, 0)
        assert _texts(reqs)  # renders a missing-data slide rather than crashing
