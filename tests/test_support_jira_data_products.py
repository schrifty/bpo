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


class _FakeSupportJiraClient:
    base_url = "https://example.atlassian.net"

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def _record(self, name: str, *args):
        self.calls.append((name, args))

    def get_customer_ticket_metrics(self, customer):
        self._record("get_customer_ticket_metrics", customer)
        return {"customer": customer}

    def get_help_ticket_volume_trends(self, customer):
        self._record("get_help_ticket_volume_trends", customer)
        return {"customer": customer, "all": [], "escalated": [], "non_escalated": []}

    def get_help_factory_start_day_buckets(self, customer):
        self._record("get_help_factory_start_day_buckets", customer)
        return {"customer": customer, "buckets": []}

    def get_help_monthly_operational_table(self, customer):
        self._record("get_help_monthly_operational_table", customer)
        return {"customer": customer, "rows": []}

    def get_help_customer_escalations(self, customer):
        self._record("get_help_customer_escalations", customer)
        return {"customer": customer, "tickets": []}

    def get_help_escalation_metrics(self, customer):
        self._record("get_help_escalation_metrics", customer)
        return {"customer": customer, "error": "skip LLM in unit test"}

    def get_customer_help_recent_tickets(self, customer, **_kwargs):
        self._record("get_customer_help_recent_tickets", customer)
        return {"customer": customer, "recently_opened": [], "recently_closed": []}

    def get_resolved_tickets_by_assignee(self, project, customer, **_kwargs):
        self._record("get_resolved_tickets_by_assignee", project, customer)
        return {"project": project, "customer": customer, "by_assignee": [], "total_resolved": 0}

    def get_customer_project_recent_tickets(self, project, customer, **_kwargs):
        self._record("get_customer_project_recent_tickets", project, customer)
        return {"project": project, "customer": customer, "recently_opened": [], "recently_closed": []}

    def get_customer_project_open_breakdown(self, project, customer):
        self._record("get_customer_project_open_breakdown", project, customer)
        return {"project": project, "customer": customer}

    def get_project_ticket_volume_trends(self, project, customer):
        self._record("get_project_ticket_volume_trends", project, customer)
        return {"project": project, "customer": customer, "all": [], "escalated": [], "non_escalated": []}

    def get_project_ticket_metrics(self, project, customer):
        self._record("get_project_ticket_metrics", project, customer)
        return {"project": project, "customer": customer}


def test_support_enrichment_fetches_full_jira_bundle_for_customer(monkeypatch):
    fake = _FakeSupportJiraClient()
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: fake)

    report = {"customer": "Acme"}
    deck_data_enrichment.enrich_support_jira_data(report, "Acme")

    assert report["jira"]["help_ticket_volume_trends"]["customer"] == "Acme"
    assert report["jira"]["customer_help_recent"]["customer"] == "Acme"
    assert ("get_help_ticket_volume_trends", ("Acme",)) in fake.calls
    assert ("get_customer_help_recent_tickets", ("Acme",)) in fake.calls
