"""Support deck Jira data should stay scoped to the requested customer."""

import threading

from src import deck_data_enrichment
from src.jira_client import JiraClient


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


def test_support_enrichment_fetches_help_volume_trends_for_customer(monkeypatch):
    fake = _FakeSupportJiraClient()
    monkeypatch.setattr(deck_data_enrichment, "get_shared_jira_client", lambda: fake, raising=False)
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: fake)

    report = {"customer": "Acme"}
    deck_data_enrichment.enrich_support_jira_data(report, "Acme")

    assert report["jira"]["help_ticket_volume_trends"]["customer"] == "Acme"
    customer_calls = [call for call in fake.calls if call[0] != "get_help_organizations_by_opened"]
    assert ("get_help_ticket_volume_trends", ("Acme",)) in customer_calls
    assert all("Acme" in args for _, args in customer_calls)


def test_help_volume_trends_jql_uses_customer_scope():
    client = JiraClient.__new__(JiraClient)
    client._jql_log = []
    client._jql_lock = threading.Lock()
    client._help_project_customer_filter = lambda customer, match_terms=None: (
        'Organizations = "Acme"',
        ["Acme"],
    )

    seen: dict[str, str] = {}

    def fake_search(jql: str, *args, **kwargs):
        seen["jql"] = jql
        client._record_jql(jql, description=kwargs.get("data_description"))
        return []

    client._search = fake_search

    result = client.get_help_ticket_volume_trends("Acme")

    assert result["customer"] == "Acme"
    assert result["jsm_organizations_resolved"] == ["Acme"]
    assert 'project = HELP AND Organizations = "Acme"' in seen["jql"]
    assert "key is not EMPTY" not in seen["jql"]
    assert result["jql_queries"][0]["jql"] == seen["jql"]
