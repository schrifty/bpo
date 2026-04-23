"""Pytest configuration and shared fixtures.

Fast pre-push: ``pytest -m "not slow"`` (skips tests marked ``@pytest.mark.slow``,
see ``pytest.ini``). Run the full ``tests/`` tree before release or in CI.
"""
import sys
from pathlib import Path

import pytest

# Ensure src is on the path when running tests from project root or tests/
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


@pytest.fixture(autouse=True)
def _isolated_salesforce_read_cache():
    """Module-level SF read cache must not leak mocked responses between tests."""
    from src.salesforce_client import clear_salesforce_read_cache

    clear_salesforce_read_cache()
    yield
    clear_salesforce_read_cache()
