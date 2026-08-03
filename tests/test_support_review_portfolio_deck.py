"""Support Review — Portfolio deck definition and resolution."""

from src.deck_loader import load_deck, resolve_deck
from src.slide_support_intro import support_deck_cover_slide
from src.slides_theme import NAVY, WHITE


def test_support_review_portfolio_deck_yaml_loads():
    d = load_deck("support_review_portfolio")
    assert d is not None
    assert d.get("id") == "support_review_portfolio"
    assert "Portfolio" in (d.get("name") or "")
    assert d.get("extends") is None
    cover = next(
        e for e in (d.get("slides") or []) if (e.get("slide") or e.get("recipe")) == "support_deck_cover"
    )
    assert cover.get("title") == "Support Review — Portfolio"


def test_support_review_portfolio_resolves_all_customers_slides():
    r = resolve_deck("support_review_portfolio", None)
    assert not r.get("error")
    slides = r.get("slides") or []
    ids = [s.get("slide_type") or s.get("id") for s in slides]
    assert "support_deck_cover" in ids
    assert "support_help_factory_start_buckets" in ids
    assert "support_help_monthly_operational" in ids
    assert "support_help_orgs_by_opened" in ids
    assert "data_quality" in ids
    assert len(slides) >= 10
    fi = ids.index("support_help_factory_start_buckets")
    mi = ids.index("support_help_monthly_operational")
    assert mi == fi + 1
    cover = next(s for s in slides if (s.get("slide_type") or s.get("id")) == "support_deck_cover")
    assert cover.get("title") == "Support Review — Portfolio"


def test_support_review_portfolio_matches_support_slide_lineup():
    """Portfolio deck inherits support.yaml slides; only cover title differs."""
    r_support = resolve_deck("support", None)
    r_portfolio = resolve_deck("support_review_portfolio", None)
    assert not r_support.get("error")
    assert not r_portfolio.get("error")
    support_ids = [s.get("slide_type") or s.get("id") for s in r_support.get("slides") or []]
    portfolio_ids = [s.get("slide_type") or s.get("id") for s in r_portfolio.get("slides") or []]
    assert support_ids == portfolio_ids
    support_titles = [s.get("title") for s in r_support.get("slides") or []]
    portfolio_titles = [s.get("title") for s in r_portfolio.get("slides") or []]
    assert support_titles[1:] == portfolio_titles[1:]
    assert portfolio_titles[0] == "Support Review — Portfolio"
    assert support_titles[0] == "Support Review"


def _cover_bg_rgb(reqs: list) -> dict:
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
    return bg_reqs[0]["updatePageProperties"]["pageProperties"]["pageBackgroundFill"]["solidFill"]["color"]["rgbColor"]


def test_support_deck_cover_uses_shared_hero_background():
    """support_deck_cover uses navy hero for support/portfolio decks; white for support-kpis."""
    base = {
        "customer": "Acme Corp",
        "support_deck_generated_at": "2026-04-29T12:00:00Z",
        "days": 30,
        "_current_slide": {"id": "support_deck_cover", "title": "Support Review"},
    }
    reqs: list = []
    support_deck_cover_slide(reqs, "s_cover_1", dict(base), 1)
    assert _cover_bg_rgb(reqs) == NAVY

    reqs_kpi: list = []
    support_deck_cover_slide(reqs_kpi, "s_cover_2", {**base, "type": "support_kpis"}, 1)
    assert _cover_bg_rgb(reqs_kpi) == WHITE
