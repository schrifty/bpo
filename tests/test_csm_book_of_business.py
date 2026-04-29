"""CSM book-of-business portfolio filter."""

from src.deck_variants import csm_book_cli_argv_anchor, portfolio_row_matches_csm_owner


def test_portfolio_row_matches_csm_owner_substring():
    row = {"pendo_csm": "Josh Fox, Jane Doe"}
    assert portfolio_row_matches_csm_owner(row, "josh")
    assert portfolio_row_matches_csm_owner(row, "Fox")
    assert portfolio_row_matches_csm_owner(row, "jane")


def test_portfolio_row_matches_csm_owner_unknown_no_match():
    row = {"pendo_csm": "Unknown"}
    assert not portfolio_row_matches_csm_owner(row, "Josh")


def test_portfolio_row_matches_csm_owner_empty_needle():
    row = {"pendo_csm": "Anyone"}
    assert not portfolio_row_matches_csm_owner(row, "")


def test_csm_book_argv_anchor_detects_phrase():
    assert csm_book_cli_argv_anchor(["csm", "book", "--csm", "x"]) == 1
    assert csm_book_cli_argv_anchor(["foo", "csm_book_of_business", "--csm", "x"]) == 1
    assert csm_book_cli_argv_anchor(["portfolio"]) == -1


def test_csm_book_deck_yaml_loads():
    from src.deck_loader import load_deck, resolve_deck

    d = load_deck("csm_book_of_business")
    assert d is not None
    assert d.get("id") == "csm_book_of_business"
    r = resolve_deck("csm_book_of_business", "Portfolio")
    assert not r.get("error")
    types = [s.get("slide_type") for s in (r.get("slides") or [])]
    assert "csm_book_title" in types
    assert "portfolio_signals" in types
    assert "data_quality" in types
