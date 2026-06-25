"""Tests for engineering portfolio Drive cache loader."""

from __future__ import annotations

from src.engineering_portfolio_cache import load_or_fetch_engineering_portfolio


def test_load_or_fetch_uses_jira_when_drive_miss(monkeypatch) -> None:
    calls: list[int] = []

    class _FakeJira:
        def get_engineering_portfolio(self, days: int = 30) -> dict:
            calls.append(days)
            return {"days": days, "sprint": {"name": "S1"}}

    monkeypatch.setattr(
        "src.engineering_portfolio_cache.integration_drive_cache_reads_enabled",
        lambda: False,
    )
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: _FakeJira())

    out = load_or_fetch_engineering_portfolio(days=45)
    assert calls == [45]
    assert out["sprint"]["name"] == "S1"


def test_load_or_fetch_drive_hit_skips_jira(monkeypatch) -> None:
    calls: list[int] = []
    sprint_calls: list[int] = []

    class _FakeJira:
        def get_engineering_portfolio(self, days: int = 30) -> dict:
            calls.append(days)
            return {"days": days}

        def fetch_active_board_sprint(self, board_id: int = 44) -> dict | None:
            sprint_calls.append(board_id)
            return {"name": "Sprint598", "end": "2026-06-27"}

    monkeypatch.setattr(
        "src.engineering_portfolio_cache.integration_drive_cache_reads_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.engineering_portfolio_cache.try_load_integration_payload",
        lambda kind, customer: {"days": 30, "sprint": {"name": "Cached597"}},
    )
    monkeypatch.setattr("src.jira_client.get_shared_jira_client", lambda: _FakeJira())

    out = load_or_fetch_engineering_portfolio(days=30)
    assert calls == []
    assert sprint_calls == [44]
    assert out["sprint"]["name"] == "Sprint598"
