"""engineering-portfolio deck must load Jira portfolio data when not pre-filled."""

from src import deck_data_enrichment


class _FakePortfolioJira:
    def __init__(self) -> None:
        self.portfolio_calls: list[int] = []

    def get_engineering_portfolio(self, days: int = 30) -> dict:
        self.portfolio_calls.append(days)
        return {"days": days, "sprint": {"name": "Unit Sprint"}, "in_flight_count": 0}


def test_engineering_portfolio_enrichment_fetches_when_missing(monkeypatch):
    fake = _FakePortfolioJira()
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: fake)

    report: dict = {"customer": "Acme Corp", "days": 45}
    out, _ = deck_data_enrichment.enrich_deck_report_data(
        "engineering-portfolio",
        report,
        [],
        "Acme Corp",
    )
    assert fake.portfolio_calls == [45]
    assert out["eng_portfolio"]["sprint"]["name"] == "Unit Sprint"
    assert out["eng_portfolio"]["days"] == 45


def test_engineering_portfolio_enrichment_skips_when_prefilled(monkeypatch):
    fake = _FakePortfolioJira()
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: fake)

    report: dict = {"eng_portfolio": {"prefilled": True}}
    out, _ = deck_data_enrichment.enrich_deck_report_data(
        "engineering-portfolio",
        report,
        [],
        None,
    )
    assert fake.portfolio_calls == []
    assert out["eng_portfolio"]["prefilled"] is True
