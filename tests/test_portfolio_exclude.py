"""Portfolio customer exclusion (cohorts.yaml exclude + env)."""

import os
from unittest.mock import patch

from src.pendo_client import customer_is_excluded_from_portfolio


def test_customer_is_excluded_from_portfolio_support_in_yaml():
    assert customer_is_excluded_from_portfolio("Support") is True


def test_customer_is_excluded_from_portfolio_automated_in_yaml():
    assert customer_is_excluded_from_portfolio("Automated") is True


def test_customer_is_excluded_from_portfolio_by_false_prefix():
    assert customer_is_excluded_from_portfolio("By") is True


def test_customer_is_excluded_from_portfolio_false_prefix_tokens():
    for name in ("LOB", "Manual", "Override", "Prefixed", "Professional"):
        assert customer_is_excluded_from_portfolio(name) is True


def test_customer_is_excluded_from_portfolio_env_extra():
    with patch.dict(os.environ, {"BPO_PORTFOLIO_EXCLUDE_CUSTOMERS": "FooBar, Baz"}):
        assert customer_is_excluded_from_portfolio("FooBar") is True
        assert customer_is_excluded_from_portfolio("Baz") is True
        assert customer_is_excluded_from_portfolio("NotListed") is False
