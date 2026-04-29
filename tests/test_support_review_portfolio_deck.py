"""Support Review — Portfolio deck definition and resolution."""

from src.deck_loader import load_deck, resolve_deck
from src.slide_support_intro import support_deck_cover_slide
from src.slides_theme import NAVY


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


def test_support_deck_cover_uses_shared_hero_background():
    """support_deck_cover always uses the navy hero (support, portfolio, supply-chain)."""
    reqs: list = []
    report = {
        "customer": "Acme Corp",
        "support_deck_generated_at": "2026-04-29T12:00:00Z",
        "days": 30,
        "_current_slide": {"id": "support_deck_cover", "title": "Support Review"},
    }
    support_deck_cover_slide(reqs, "s_cover_1", report, 1)
    bg_reqs = [
        r
        for r in reqs
        if r.get("updatePageProperties", {})
        .get("pageProperties", {})
        .get("pageBackgroundFill", {})
        .get("solidFill", {})
        .get("color", {})
        .get("rgbColor")
    ]
    assert bg_reqs, "expected updatePageProperties with solidFill background"
    rgb = bg_reqs[0]["updatePageProperties"]["pageProperties"]["pageBackgroundFill"]["solidFill"]["color"]["rgbColor"]
    assert rgb == NAVY
