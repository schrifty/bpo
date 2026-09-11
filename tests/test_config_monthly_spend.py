"""Monthly department spend env parsing."""

from __future__ import annotations

import pytest

from src.config import _parse_monthly_spend_usd, require_monthly_spend_usd


def test_parse_monthly_spend_usd_empty_and_number(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CORTEX_MONTHLY_SPEND_USD_SUPPORT", raising=False)
    assert _parse_monthly_spend_usd("CORTEX_MONTHLY_SPEND_USD_SUPPORT") is None
    monkeypatch.setenv("CORTEX_MONTHLY_SPEND_USD_SUPPORT", "85,000")
    assert _parse_monthly_spend_usd("CORTEX_MONTHLY_SPEND_USD_SUPPORT") == 85000.0
    monkeypatch.setenv("CORTEX_MONTHLY_SPEND_USD_SUPPORT", "not-a-number")
    assert _parse_monthly_spend_usd("CORTEX_MONTHLY_SPEND_USD_SUPPORT") is None


def test_require_monthly_spend_usd_fail_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CORTEX_MONTHLY_SPEND_USD_SUPPORT", raising=False)
    missing = require_monthly_spend_usd(
        env_name="CORTEX_MONTHLY_SPEND_USD_SUPPORT",
        configured=None,
        required_for="Support Spend / Ticket",
    )
    assert "not set" in missing["error"]
    monkeypatch.setenv("CORTEX_MONTHLY_SPEND_USD_SUPPORT", "abc")
    invalid = require_monthly_spend_usd(
        env_name="CORTEX_MONTHLY_SPEND_USD_SUPPORT",
        configured=None,
        required_for="Support Spend / Ticket",
    )
    assert "not a valid number" in invalid["error"]
    assert require_monthly_spend_usd(
        env_name="CORTEX_MONTHLY_SPEND_USD_SUPPORT",
        configured=0,
        required_for="Support Spend / Ticket",
    )["error"].startswith("CORTEX_MONTHLY_SPEND_USD_SUPPORT must be > 0")
    ok = require_monthly_spend_usd(
        env_name="CORTEX_MONTHLY_SPEND_USD_SUPPORT",
        configured=12000.0,
        required_for="Support Spend / Ticket",
    )
    assert ok == {"value": 12000.0}
