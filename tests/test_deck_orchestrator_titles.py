"""Presentation title naming for Drive file creation."""

from src.deck_orchestrator import _health_deck_presentation_title


def test_portfolio_review_title_uses_consistent_prefix():
    t = _health_deck_presentation_title(
        deck_id="portfolio_review",
        deck_name="Portfolio Health Review",
        date_str="Last 30 days",
        customer="Portfolio",
        report={"type": "portfolio"},
        is_portfolio=True,
    )
    assert t == "Portfolio - Health Review (Last 30 days)"


def test_cohort_and_engineering_portfolio_prefix():
    cohort = _health_deck_presentation_title(
        deck_id="cohort_review",
        deck_name="Manufacturing Cohort Review",
        date_str="Q1 2026",
        customer="Portfolio",
        report={"type": "portfolio"},
        is_portfolio=True,
    )
    assert cohort == "Portfolio - Cohort Review (Q1 2026)"

    eng = _health_deck_presentation_title(
        deck_id="engineering-portfolio",
        deck_name="Engineering Portfolio Review",
        date_str="Last 14 days",
        customer="Spirit",
        report={"customer": "Spirit", "days": 14},
        is_portfolio=False,
    )
    assert eng == "Portfolio - Engineering Review (Last 14 days)"


def test_engineering_portfolio_cursor_only_title(monkeypatch):
    monkeypatch.setattr("src.config.BPO_CURSOR_SLIDES_ONLY", True)
    t = _health_deck_presentation_title(
        deck_id="engineering-portfolio",
        deck_name="Engineering Portfolio Review",
        date_str="Last 14 days",
        customer="Engineering",
        report={"customer": "Engineering", "days": 14},
        is_portfolio=False,
    )
    assert t == "Portfolio - Engineering Review — Cursor (Last 14 days)"


def test_csm_book_prefix_includes_owner():
    t = _health_deck_presentation_title(
        deck_id="csm_book_of_business",
        deck_name="CSM Book of Business",
        date_str="Last 30 days",
        customer="Portfolio",
        report={"csm_owner": "Alex Smith", "type": "portfolio"},
        is_portfolio=True,
    )
    assert t == "Portfolio - Alex Smith — Book of Business (Last 30 days)"


def test_per_customer_deck_unchanged():
    t = _health_deck_presentation_title(
        deck_id="cs_health_review",
        deck_name="CS Health Review",
        date_str="Last 30 days",
        customer="Acme Corp",
        report={"customer": "Acme Corp"},
        is_portfolio=False,
    )
    assert t == "Acme Corp — CS Health Review (Last 30 days)"
