"""Tests for Cursor Admin API LeanDNA metric generators."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.cursor_ai_usage_metrics import (
    get_ai_token_usage_value,
    get_monthly_ai_spend,
    project_monthly_ai_spend_cents,
)


def _ms(year: int, month: int, day: int, hour: int = 12) -> int:
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000)


class _FakeCursor:
    def __init__(
        self,
        *,
        events: list[dict[str, Any]] | None = None,
        fail_events: bool = False,
    ) -> None:
        self.events = events or []
        self.fail_events = fail_events
        self.last_range: tuple[object, object] | None = None

    def get_usage_events(self, start: object, end: object, **kwargs: object) -> list[dict[str, Any]]:
        self.last_range = (start, end)
        if self.fail_events:
            raise RuntimeError("usage events down")
        return self.events


def test_project_mtd_plus_window_daily() -> None:
    # Mid-April: 15 elapsed, 15 remaining in a 30-day month.
    # 90d window = $90 → $1/day; MTD $15 stays real; remaining = $15.
    as_of = datetime(2026, 4, 15, 12, tzinfo=timezone.utc)
    out = project_monthly_ai_spend_cents(
        mtd_cents=1500.0,
        window_cents=9000.0,
        window_days=90,
        as_of=as_of,
    )
    assert out["method"] == "mtd_plus_window_daily"
    assert out["mtd_cents"] == 1500.0
    assert out["days_elapsed"] == 15
    assert out["days_remaining"] == 15
    assert out["window_avg_daily_cents"] == 100.0
    assert out["extrapolated_cents"] == 1500.0
    assert out["projected_cents"] == 3000.0


def test_project_prefers_actual_when_month_complete() -> None:
    as_of = datetime(2026, 4, 30, 23, tzinfo=timezone.utc)
    out = project_monthly_ai_spend_cents(
        mtd_cents=4210.0,
        window_cents=50_000.0,
        window_days=90,
        as_of=as_of,
    )
    assert out["method"] == "actual_month_complete"
    assert out["projected_cents"] == 4210.0
    assert out["extrapolated_cents"] == 0.0
    assert out["days_remaining"] == 0


def test_project_falls_back_to_mtd_pace_without_window_spend() -> None:
    as_of = datetime(2026, 4, 10, tzinfo=timezone.utc)
    out = project_monthly_ai_spend_cents(
        mtd_cents=1000.0,
        window_cents=0.0,
        window_days=90,
        as_of=as_of,
    )
    assert out["method"] == "mtd_pace"
    assert out["mtd_cents"] == 1000.0
    # 1000 * 30/10 = 3000
    assert out["projected_cents"] == 3000.0


def test_get_monthly_ai_spend_from_events() -> None:
    as_of = datetime(2026, 4, 15, 12, tzinfo=timezone.utc)
    # Window total $90 over 90d → $1/day; April MTD $15; remaining 15d → +$15.
    events = [
        {"timestamp": _ms(2026, 1, 20), "chargedCents": 2500},
        {"timestamp": _ms(2026, 2, 10), "chargedCents": 2500},
        {"timestamp": _ms(2026, 3, 10), "chargedCents": 2500},
        {"timestamp": _ms(2026, 4, 2), "chargedCents": 700},
        {"timestamp": _ms(2026, 4, 14), "chargedCents": 800},
    ]
    client = _FakeCursor(events=events)
    out = get_monthly_ai_spend(client, as_of=as_of, lookback_days=90)
    assert out["value"] == 30.0
    assert out["mtd_usd"] == 15.0
    assert out["extrapolated_usd"] == 15.0
    assert out["window_usd"] == 90.0
    assert out["window_days"] == 90
    assert out["method"] == "mtd_plus_window_daily"
    assert out["month"] == "2026-04"
    assert client.last_range is not None


def test_get_monthly_ai_spend_fails_loud() -> None:
    out = get_monthly_ai_spend(_FakeCursor(fail_events=True))
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
    assert "Cursor AI spend" in (entry.get("description") or "")
    assert "get_monthly_ai_spend" in _GENERATORS
