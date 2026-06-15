"""Render tests for the three Cursor AI-coding slides (cost / usage / users).

Charts are skipped (no ``_charts`` in the report), so these assert the text surfaces:
title, subtitle, KPI cards, share bars, and tables.
"""

from __future__ import annotations

from src.slide_engineering_portfolio import (
    cursor_cost_slide,
    cursor_efficiency_slide,
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
            "charged_cents_window": 25_000,
            "spend_cents_cycle": 41_000,
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
                {"model": "claude-4.5-sonnet", "tokens": 700_000, "share": 0.7},
                {"model": "gpt-5", "tokens": 300_000, "share": 0.3},
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
                {"model": "gpt-5", "tokens": 120_000, "share": 0.6},
                {"model": "Auto (default)", "tokens": 80_000, "share": 0.4},
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


def _subtitle(reqs: list, sid: str) -> str:
    for r in reqs:
        if isinstance(r, dict) and "insertText" in r and r["insertText"].get("objectId") == f"{sid}_sub":
            return r["insertText"]["text"]
    return ""


def test_cost_slide_renders_spend_and_model_cost() -> None:
    reqs: list = []
    cursor_cost_slide(reqs, "sid_c", _cursor_report(), 0)
    assert _title(reqs, "sid_c") == "Cursor AI Coding Spend"
    sub = _subtitle(reqs, "sid_c")
    # Leads with usage cost ($250 = 25,000 cents); overage ($410) shown as overage.
    assert "$250" in sub and "$410" in sub
    text = _texts(reqs)
    assert "Usage cost" in text
    assert "Cost / active eng" in text
    assert "Idle eng seats" in text
    assert "dev-* team members only" in text
    assert "Where the spend goes" in text
    # Per-model cost dollar value appears.
    assert "$180" in text  # claude cost (18,000 cents)


def test_cost_slide_no_overage_leads_with_usage_cost() -> None:
    rep = _cursor_report()
    rep["cursor_usage"]["totals"]["spend_cents_cycle"] = 0
    rep["cursor_usage"]["cost_engineers"]["totals"]["spend_cents_cycle"] = 0
    reqs: list = []
    cursor_cost_slide(reqs, "sid_c0", rep, 0)
    sub = _subtitle(reqs, "sid_c0")
    assert "$250" in sub  # usage cost still leads
    text = _texts(reqs)
    # Context states there is no overage rather than a bare $0 spend card.
    assert "no cycle overage" in text


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


def test_usage_slide_renders_tokens_and_models() -> None:
    reqs: list = []
    cursor_usage_slide(reqs, "sid_u", _cursor_report(), 0)
    assert _title(reqs, "sid_u") == "Cursor AI Token Usage"
    text = _texts(reqs)
    assert "Input tokens" in text and "Output tokens" in text
    assert "Model usage (by tokens)" in text
    assert "claude-4.5-sonnet" in text
    assert "dev-* team members only" in text
    # input/output ratio in subtitle.
    assert "in/out" in _subtitle(reqs, "sid_u")


def test_usage_non_engineers_slide_renders_scoped_tokens() -> None:
    reqs: list = []
    cursor_usage_non_engineers_slide(reqs, "sid_un", _cursor_report(), 0)
    assert _title(reqs, "sid_un") == "Cursor AI Token Usage — Non-Engineering"
    text = _texts(reqs)
    assert "Input tokens" in text and "Output tokens" in text
    assert "outside dev-* teams" in text
    assert "Auto (default)" in text
    sub = _subtitle(reqs, "sid_un")
    assert "200K" in sub or "200" in sub
    assert "users active" in sub


def test_users_slide_renders_power_users_and_concentration() -> None:
    reqs: list = []
    cursor_users_slide(reqs, "sid_w", _cursor_report(), 0)
    assert _title(reqs, "sid_w") == "Cursor AI Power Users"
    text = _texts(reqs)
    assert "Top-user share" in text
    assert "Idle seats" in text
    assert "Highest volume" in text
    assert "Lowest volume" in text
    assert "dev-* team members only" in text
    assert "ada" in text  # short email of top user
    assert "grace" in text  # short email of low-volume user
    # idle seats = 8 - 4 = 4
    assert "4" in _subtitle(reqs, "sid_w") or "idle" in _subtitle(reqs, "sid_w")


def test_users_non_engineers_slide_renders_scoped_power_users() -> None:
    reqs: list = []
    cursor_users_non_engineers_slide(reqs, "sid_wn", _cursor_report(), 0)
    assert _title(reqs, "sid_wn") == "Cursor AI Power Users — Non-Engineering"
    text = _texts(reqs)
    assert "Active users" in text
    assert "outside dev-* teams" in text
    assert "pm" in text
    assert "Highest volume" in text


def test_efficiency_slide_renders_ratios_and_engineers() -> None:
    reqs: list = []
    cursor_efficiency_slide(reqs, "sid_e", _cursor_report(), 0)
    assert _title(reqs, "sid_e") == "Cursor AI Coding Efficiency"
    text = _texts(reqs)
    assert "Lines kept" in text
    assert "Lines / 1K tokens" in text
    assert "Most efficient engineers" in text
    assert "ada" in text  # short email of most-efficient engineer
    assert "0.21" in text  # cost per accepted line shown in cents (0.21¢)
    # Subtitle leads with accepted lines and lines-kept ratio.
    assert "kept" in _subtitle(reqs, "sid_e")


def test_cursor_slides_emit_missing_data_when_unconfigured() -> None:
    rep = {"cursor_usage": {"configured": False}}
    for builder in (
        cursor_cost_slide, cursor_usage_slide, cursor_usage_non_engineers_slide,
        cursor_efficiency_slide, cursor_users_slide, cursor_users_non_engineers_slide,
    ):
        reqs: list = []
        builder(reqs, "sid_x", rep, 0)
        assert _texts(reqs)  # renders a missing-data slide rather than crashing
