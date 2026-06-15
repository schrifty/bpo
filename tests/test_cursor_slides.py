"""Render tests for the three Cursor AI-coding slides (cost / usage / users).

Charts are skipped (no ``_charts`` in the report), so these assert the text surfaces:
title, subtitle, KPI cards, share bars, and tables.
"""

from __future__ import annotations

from src.slide_engineering_portfolio import (
    cursor_cost_slide,
    cursor_users_slide,
    cursor_usage_slide,
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
        "top_users": [
            {"email": "ada@x.com", "tokens": 700_000, "input_tokens": 520_000, "output_tokens": 180_000,
             "events": 3_000, "window_cents": 15_000, "spend_cents": 22_000,
             "models": [{"model": "claude-4.5-sonnet", "tokens": 600_000, "share": 0.86}]},
            {"email": "linus@x.com", "tokens": 500_000, "input_tokens": 380_000, "output_tokens": 120_000,
             "events": 2_000, "window_cents": 10_000, "spend_cents": 19_000,
             "models": [{"model": "gpt-5", "tokens": 400_000, "share": 0.8}]},
        ],
        "user_model_matrix": {
            "users": ["ada@x.com", "linus@x.com"],
            "models": ["claude-4.5-sonnet", "gpt-5"],
            "series": {"claude-4.5-sonnet": [600_000, 100_000], "gpt-5": [100_000, 400_000]},
        },
        "takeaways": {"cost": "Cost up.", "usage": "Tokens up.", "users": "Concentrated."},
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
    assert _title(reqs, "sid_c") == "AI Coding Spend"
    sub = _subtitle(reqs, "sid_c")
    # Leads with usage cost ($250 = 25,000 cents); overage ($410) shown as overage.
    assert "$250" in sub and "$410" in sub
    text = _texts(reqs)
    assert "Usage cost" in text
    assert "Cost / active eng" in text
    assert "Idle seats" in text
    assert "Where the spend goes" in text
    # Per-model cost dollar value appears.
    assert "$180" in text  # claude cost (18,000 cents)


def test_cost_slide_no_overage_leads_with_usage_cost() -> None:
    rep = _cursor_report()
    rep["cursor_usage"]["totals"]["spend_cents_cycle"] = 0
    reqs: list = []
    cursor_cost_slide(reqs, "sid_c0", rep, 0)
    sub = _subtitle(reqs, "sid_c0")
    assert "$250" in sub  # usage cost still leads
    text = _texts(reqs)
    # Context states there is no overage rather than a bare $0 spend card.
    assert "no cycle overage" in text


def test_users_slide_falls_back_to_window_cost_when_spend_zero() -> None:
    rep = _cursor_report()
    for u in rep["cursor_usage"]["top_users"]:
        u["spend_cents"] = 0
    reqs: list = []
    cursor_users_slide(reqs, "sid_w0", rep, 0)
    text = _texts(reqs)
    # ada window_cents=15,000 -> $150 (not $0) via fallback.
    assert "$150" in text


def test_usage_slide_renders_tokens_and_models() -> None:
    reqs: list = []
    cursor_usage_slide(reqs, "sid_u", _cursor_report(), 0)
    assert _title(reqs, "sid_u") == "AI Token Usage"
    text = _texts(reqs)
    assert "Input tokens" in text and "Output tokens" in text
    assert "Model usage (by tokens)" in text
    assert "claude-4.5-sonnet" in text
    # input/output ratio in subtitle.
    assert "in/out" in _subtitle(reqs, "sid_u")


def test_users_slide_renders_power_users_and_concentration() -> None:
    reqs: list = []
    cursor_users_slide(reqs, "sid_w", _cursor_report(), 0)
    assert _title(reqs, "sid_w") == "AI Power Users"
    text = _texts(reqs)
    assert "Top-user share" in text
    assert "Idle seats" in text
    assert "Highest-volume users" in text
    assert "ada" in text  # short email of top user
    # idle seats = 10 - 4 = 6
    assert "6" in _subtitle(reqs, "sid_w") or "idle" in _subtitle(reqs, "sid_w")


def test_cursor_slides_emit_missing_data_when_unconfigured() -> None:
    rep = {"cursor_usage": {"configured": False}}
    for builder in (cursor_cost_slide, cursor_usage_slide, cursor_users_slide):
        reqs: list = []
        builder(reqs, "sid_x", rep, 0)
        assert _texts(reqs)  # renders a missing-data slide rather than crashing
