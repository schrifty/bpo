"""Support deck Jira data product registry and selective enrichment."""

from src import deck_data_enrichment
from src.deck_loader import resolve_deck
from src.support_jira_data_products import (
    JIRA_SUPPORT_PRODUCT_IDS,
    NOTABLE_DIGEST_JIRA_PRODUCTS,
    SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE,
    SUPPORT_SLIDE_TYPES_NO_JIRA,
    collect_support_jira_product_ids,
)


def test_support_deck_slide_types_are_registered():
    """Every slide_type in canonical support decks maps to products or a no-Jira bucket."""
    for deck_id in ("support", "support_review_portfolio"):
        resolved = resolve_deck(deck_id, None)
        assert not resolved.get("error"), resolved
        for row in resolved.get("slides") or []:
            st = (row.get("slide_type") or "").strip()
            assert st, row
            assert (
                st in SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE
                or st in SUPPORT_SLIDE_TYPES_NO_JIRA
                or st == "cs_notable"
            ), f"unregistered slide_type {st!r} in {deck_id}"

    resolved_scoped = resolve_deck("support", "Acme Corp")
    assert not resolved_scoped.get("error"), resolved_scoped
    for row in resolved_scoped.get("slides") or []:
        st = (row.get("slide_type") or "").strip()
        assert (
            st in SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE
            or st in SUPPORT_SLIDE_TYPES_NO_JIRA
            or st == "cs_notable"
        ), f"unregistered slide_type {st!r} in support (scoped)"


def test_full_support_plan_union_covers_all_jira_products():
    resolved = resolve_deck("support", None)
    products, fallback = collect_support_jira_product_ids(resolved["slides"], customer=None)
    assert not fallback
    assert products == JIRA_SUPPORT_PRODUCT_IDS


def test_scoped_support_plan_drops_help_orgs_product():
    resolved = resolve_deck("support", "Acme Corp")
    products, fallback = collect_support_jira_product_ids(resolved["slides"], customer="Acme Corp")
    assert not fallback
    assert "help_orgs_by_opened" not in products


def test_slide_type_registry_union_matches_canonical_product_set():
    """Every ``JIRA_SUPPORT_PRODUCT_IDS`` entry is required by at least one slide or Notable digest."""
    union: set[str] = set(NOTABLE_DIGEST_JIRA_PRODUCTS)
    for prod_set in SUPPORT_JIRA_PRODUCTS_BY_SLIDE_TYPE.values():
        union |= set(prod_set)
    assert union == set(JIRA_SUPPORT_PRODUCT_IDS)


def test_unknown_slide_type_triggers_full_fallback():
    plan = [{"slide_type": "support_recent_opened", "id": "a", "title": "A"}, {"slide_type": "unknown_xyz", "id": "b", "title": "B"}]
    products, fallback = collect_support_jira_product_ids(plan, customer=None)
    assert fallback is True
    assert products == JIRA_SUPPORT_PRODUCT_IDS


def test_cs_notable_adds_digest_products():
    plan = [{"slide_type": "support_deck_cover", "id": "c", "title": "Cover"}]
    without, _ = collect_support_jira_product_ids(plan, customer=None)
    with_notable, _ = collect_support_jira_product_ids(
        plan + [{"slide_type": "cs_notable", "id": "n", "title": "Notable"}],
        customer=None,
    )
    assert without == frozenset()
    assert NOTABLE_DIGEST_JIRA_PRODUCTS.issubset(with_notable)


class _FakeSelectiveJira:
    base_url = "https://jira.example"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_customer_help_recent_tickets(self, customer, **_kwargs):
        self.calls.append("get_customer_help_recent_tickets")
        return {"customer": customer, "recently_opened": [], "recently_closed": []}


def test_selective_enrichment_skips_unneeded_fetches(monkeypatch):
    fake = _FakeSelectiveJira()
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: fake)

    report: dict = {"customer": "Acme"}
    plan = [{"slide_type": "support_recent_opened", "id": "ro", "title": "Opened"}]
    deck_data_enrichment.enrich_support_jira_data(report, "Acme", plan)

    assert fake.calls == ["get_customer_help_recent_tickets"]
    assert "customer_help_recent" in report["jira"]
    assert "customer_ticket_metrics" not in report["jira"]
