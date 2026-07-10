"""Salesforce configured detection."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src import data_source_health as dsh


def test_salesforce_not_configured_when_key_path_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / "missing.key"
    monkeypatch.setattr(dsh, "SF_LOGIN_URL", "https://login.salesforce.com")
    monkeypatch.setattr(dsh, "SF_CONSUMER_KEY", "consumer")
    monkeypatch.setattr(dsh, "SF_USERNAME", "user@example.com")
    monkeypatch.setattr(dsh, "SF_PRIVATE_KEY", None)
    monkeypatch.setattr(dsh, "SF_PRIVATE_KEY_PATH", str(missing))
    assert dsh._salesforce_configured() is False


def test_check_jira_backed_deck_required_skips_pendo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dsh, "check_jira", lambda: (True, None))
    monkeypatch.setattr(dsh, "check_github", lambda: (True, None))
    monkeypatch.setattr(
        dsh,
        "check_pendo",
        lambda: pytest.fail("Pendo should not be checked for Jira-backed decks"),
    )
    assert dsh.check_jira_backed_deck_required() == []


def test_check_jira_backed_deck_required_reports_jira_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dsh, "check_jira", lambda: (False, "Jira: timeout"))
    monkeypatch.setattr(dsh, "check_github", lambda: (True, None))
    assert dsh.check_jira_backed_deck_required() == ["Jira: timeout"]


def test_integration_freshness_metadata_reports_slack(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "SLACK_BOT_TOKEN", "xoxb-test", raising=False)
    meta = dsh.integration_freshness_metadata()
    assert meta["slack_configured"] is True
    assert "slack_cache_ttl_h" in meta

    monkeypatch.setattr(cfg, "SLACK_BOT_TOKEN", None, raising=False)
    assert dsh.integration_freshness_metadata()["slack_configured"] is False


def test_salesforce_configured_when_key_file_exists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    key_file = tmp_path / "server.key"
    key_file.write_text("-----BEGIN PRIVATE KEY-----\n", encoding="utf-8")
    monkeypatch.setattr(dsh, "SF_LOGIN_URL", "https://login.salesforce.com")
    monkeypatch.setattr(dsh, "SF_CONSUMER_KEY", "consumer")
    monkeypatch.setattr(dsh, "SF_USERNAME", "user@example.com")
    monkeypatch.setattr(dsh, "SF_PRIVATE_KEY", None)
    monkeypatch.setattr(dsh, "SF_PRIVATE_KEY_PATH", str(key_file))
    assert dsh._salesforce_configured() is True


@patch("src.pendo_aggregate.call_with_pendo_retry")
def test_check_pendo_retries_before_failure(mock_retry, monkeypatch: pytest.MonkeyPatch) -> None:
    mock_retry.side_effect = RuntimeError("Pendo down")
    monkeypatch.setattr(dsh, "PENDO_INTEGRATION_KEY", "test-key")
    ok, msg = dsh.check_pendo()
    assert ok is False
    assert msg is not None and msg.startswith("Pendo:")
    mock_retry.assert_called_once()
