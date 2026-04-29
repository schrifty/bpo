"""Support Review — Portfolio deck definition and resolution."""

from src.deck_loader import load_deck, resolve_deck


def test_support_review_portfolio_deck_yaml_loads():
    d = load_deck("support_review_portfolio")
    assert d is not None
    assert d.get("id") == "support_review_portfolio"
    assert "Portfolio" in (d.get("name") or "")


def test_support_review_portfolio_resolves_all_customers_slides():
    r = resolve_deck("support_review_portfolio", None)
    assert not r.get("error")
    slides = r.get("slides") or []
    ids = [s.get("slide_type") or s.get("id") for s in slides]
    assert "support_deck_cover" in ids
    assert "support_help_orgs_by_opened" in ids
    assert "data_quality" in ids
    assert len(slides) >= 10
