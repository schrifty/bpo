"""Tests for Cursor Admin API LeanDNA metric generators."""

from __future__ import annotations

from typing import Any

from src.cursor_ai_usage_metrics import get_ai_token_usage_value, get_monthly_ai_spend


class _FakeCursor:
    def __init__(
        self,
        *,
        events: list[dict[str, Any]] | None = None,
        spend_rows: list[dict[str, Any]] | None = None,
        fail_events: bool = False,
        fail_spend: bool = False,
    ) -> None:
        self.events = events or []
        self.spend_rows = spend_rows or []
        self.fail_events = fail_events
        self.fail_spend = fail_spend

    def get_usage_events(self, start: object, end: object, **kwargs: object) -> list[dict[str, Any]]:
        if self.fail_events:
            raise RuntimeError("usage events down")
        return self.events

    def get_spend(self, **kwargs: object) -> list[dict[str, Any]]:
        if self.fail_spend:
            raise RuntimeError("spend down")
        return self.spend_rows


def test_get_monthly_ai_spend_sums_billing_cycle_usd() -> None:
    client = _FakeCursor(
        spend_rows=[
            {"email": "a@ex.com", "overallSpendCents": 12_345},
            {"email": "b@ex.com", "overallSpendCents": 655},
            {"email": "c@ex.com", "overallSpendCents": 0},
        ]
    )
    out = get_monthly_ai_spend(client)
    assert out["value"] == 130.0
    assert out["spend_usd"] == 130.0
    assert out["spend_cents"] == 13_000.0
    assert out["members_count"] == 3
    assert out["billing_cycle"] == "current"


def test_get_monthly_ai_spend_empty_team() -> None:
    out = get_monthly_ai_spend(_FakeCursor(spend_rows=[]))
    assert out["value"] == 0.0
    assert out["members_count"] == 0


def test_get_monthly_ai_spend_fails_loud() -> None:
    out = get_monthly_ai_spend(_FakeCursor(fail_spend=True))
    assert "error" in out
    assert "value" not in out
    assert "spend" in out["error"].lower()


def test_get_ai_token_usage_value_sums_tokens() -> None:
    client = _FakeCursor(
        events=[
            {"tokenUsage": {"inputTokens": 10, "outputTokens": 5}},
            {"tokenUsage": {"inputTokens": 2, "outputTokens": 3, "cacheReadTokens": 1}},
            {"tokenUsage": None},
        ]
    )
    out = get_ai_token_usage_value(client, days=30)
    assert out["value"] == 20
    assert out["input_tokens"] == 12
    assert out["output_tokens"] == 8
    assert out["event_count"] == 3


def test_registry_wires_monthly_ai_spend() -> None:
    from src.metrics_registry import load_metrics_registry
    from src.metrics_upsert import _GENERATORS

    registry = load_metrics_registry()
    entry = registry["metrics"]["Monthly AI Spend"]
    assert entry["metric-generator"] == "get_monthly_ai_spend"
    assert entry["tags"] == ["enterprise", "finance"]
    assert "get_monthly_ai_spend" in _GENERATORS
