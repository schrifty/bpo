"""Tests for Jira token secret sync helpers."""

from __future__ import annotations

from scripts.sync_jira_token import merge_jira_token_from_dotenv, token_fingerprint


def test_merge_updates_changed_jira_token_only() -> None:
    existing = {
        "EXECUTION_ENV": "Production",
        "JIRA_API_TOKEN": "old-token",
        "JIRA_EMAIL": "user@example.com",
    }
    dotenv = {
        "JIRA_API_TOKEN": "new-token",
        "JIRA_URL": "https://example.atlassian.net",
    }
    merged, updated = merge_jira_token_from_dotenv(existing, dotenv)
    assert updated == ["JIRA_API_TOKEN"]
    assert merged["JIRA_API_TOKEN"] == "new-token"
    assert merged["JIRA_EMAIL"] == "user@example.com"
    assert "JIRA_URL" not in merged


def test_merge_no_op_when_unchanged() -> None:
    existing = {"JIRA_API_TOKEN": "same"}
    dotenv = {"JIRA_API_TOKEN": "same"}
    merged, updated = merge_jira_token_from_dotenv(existing, dotenv)
    assert updated == []
    assert merged == existing


def test_token_fingerprint_never_empty_for_value() -> None:
    fp = token_fingerprint("abc123")
    assert fp.startswith("len=6")
    assert "sha256=" in fp
