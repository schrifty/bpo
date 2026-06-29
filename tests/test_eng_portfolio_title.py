"""Cover slide for the engineering portfolio deck."""

from __future__ import annotations

from src.slide_engineering_portfolio import eng_portfolio_title_slide


def _texts(reqs: list) -> str:
    parts: list[str] = []
    for r in reqs:
        if isinstance(r, dict) and "insertText" in r:
            parts.append(r["insertText"]["text"])
    return "\n".join(parts)


def test_title_slide_shows_sprint_and_cursor_run_rate() -> None:
    report = {
        "eng_portfolio": {"sprint": {"name": "Sprint 597", "end": "2026-06-20"}},
        "cursor_usage": {"configured": True, "totals": {"charged_cents_window": 996_300}},
    }
    reqs: list = []
    eng_portfolio_title_slide(reqs, "sid", report, 0)
    text = _texts(reqs)
    assert "Sprint: Sprint 597" in text
    assert "ends Jun 20, 2026" in text
    assert "Cursor tokens - 30d run rate: $9,963" in text


def test_title_slide_omits_cursor_line_when_unconfigured() -> None:
    report = {
        "eng_portfolio": {"sprint": {"name": "Sprint 597", "end": "2026-06-20"}},
        "cursor_usage": {"configured": False},
    }
    reqs: list = []
    eng_portfolio_title_slide(reqs, "sid", report, 0)
    text = _texts(reqs)
    assert "Sprint:" in text
    assert "Cursor tokens - 30d run rate" not in text
