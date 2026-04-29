"""Notable/Critical Signals slides share the executive-summary visual pattern."""

from src.slide_cs_notable import cs_notable_slide
from src.slides_client import _portfolio_signals_slide, _signals_slide


def test_signals_slide_uses_configured_title_and_numbered_list():
    report = {
        "_current_slide": {"title": "Executive Signals"},
        "signals": ["Usage is up", "Support risk is down"],
    }
    reqs: list[dict] = []

    _signals_slide(reqs, "sig", report, 0)

    rendered = str(reqs)
    assert "Executive Signals" in rendered
    assert "1.   Usage is up" in rendered
    assert "2.   Support risk is down" in rendered


def test_portfolio_signals_use_same_numbered_style_without_severity_dots():
    report = {
        "_current_slide": {"title": "Critical Signals"},
        "portfolio_signals": [
            {"customer": "Acme", "signal": "Adoption risk", "severity": 2},
            {"customer": "Beta", "signal": "Expansion opportunity", "severity": 1},
        ],
    }
    reqs: list[dict] = []

    _portfolio_signals_slide(reqs, "crit", report, 0)

    rendered = str(reqs)
    assert "Critical Signals" in rendered
    assert "1.   Acme:  Adoption risk" in rendered
    assert "2.   Beta:  Expansion opportunity" in rendered
    assert "\u25cf" not in rendered


def test_cs_notable_uses_numbered_signals_layout_and_normalizes_notable_title():
    report = {
        "_current_slide": {
            "title": "Notable",
            "notable_items": ["Portfolio signal one", "Portfolio signal two"],
        },
    }
    reqs: list[dict] = []

    cs_notable_slide(reqs, "snb", report, 0)

    rendered = str(reqs)
    assert "Notable Signals" in rendered
    assert "1.   Portfolio signal one" in rendered
    assert "2.   Portfolio signal two" in rendered
    assert "\u2022 " not in rendered


def test_signal_slides_cap_at_eight_bullets_and_one_slide():
    report = {
        "_current_slide": {"title": "Notable Signals"},
        "signals": [f"Signal {i}" for i in range(1, 11)],
    }
    reqs: list[dict] = []

    result = _signals_slide(reqs, "sig_cap", report, 0)

    rendered = str(reqs)
    assert result == 1
    assert sum(1 for req in reqs if "createSlide" in req) == 1
    assert "8.   Signal 8" in rendered
    assert "9.   Signal 9" not in rendered
    assert "Notable Signals (2 of" not in rendered
