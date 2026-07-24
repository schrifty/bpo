"""Pytest configuration and shared fixtures.

Fast pre-push: ``pytest -m "not slow and not jira_live and not leandna_data_api"``
(see ``pytest.ini``). Run the full ``tests/`` tree before release or in CI.
"""
import sys
from pathlib import Path

import pytest

# Ensure src is on the path when running tests from project root or tests/
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


@pytest.fixture(autouse=True)
def _leandna_data_api_staging_only(request: pytest.FixtureRequest):
    """Live LeanDNA HTTP tests may only run against staging; Production fails loud."""
    if request.node.get_closest_marker("leandna_data_api") is None:
        yield
        return
    from tests.leandna_integration_env import require_leandna_staging_integration_env

    require_leandna_staging_integration_env()
    yield


@pytest.fixture(autouse=True)
def _disable_speaker_notes_llm_by_default(monkeypatch):
    """Existing speaker-notes tests expect deterministic output without live LLM calls."""
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "false")


@pytest.fixture(autouse=True)
def _deterministic_test_env(monkeypatch):
    """Keep unit tests independent of developer .env toggles and live integration caches."""
    monkeypatch.setenv("CORTEX_CURSOR_SLIDES_ONLY", "false")
    monkeypatch.setenv("CORTEX_CURSOR_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("src.config.CORTEX_CURSOR_SLIDES_ONLY", False)
    monkeypatch.setattr("src.deck_data_enrichment.CORTEX_CURSOR_SLIDES_ONLY", False)
    monkeypatch.setattr(
        "src.integration_drive_cache.integration_drive_cache_reads_enabled",
        lambda: False,
    )
    yield


@pytest.fixture(autouse=True)
def _isolated_process_caches():
    """Module-level integration/config caches must not leak mocked responses between tests."""
    from src import drive_config, salesforce_client, slide_loader

    salesforce_client.reset_for_tests()
    drive_config.reset_for_tests()
    slide_loader.reset_for_tests()
    yield
    salesforce_client.reset_for_tests()
    drive_config.reset_for_tests()
    slide_loader.reset_for_tests()
