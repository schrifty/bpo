"""Tests for bearer token secret sync helpers."""

from __future__ import annotations

from scripts.sync_bearer import merge_bearer_token_from_dotenv, token_fingerprint


def test_merge_updates_changed_bearer_only() -> None:
    existing = {
        "EXECUTION_ENV": "Production",
        "PR_LEANDNA_DATA_API_BEARER_TOKEN": "old-token",
        "PENDO_INTEGRATION_KEY": "pendo",
    }
    dotenv = {
        "PR_LEANDNA_DATA_API_BEARER_TOKEN": "new-token",
        "PR_LEANDNA_DATA_API_COOKIE": "should-not-apply",
    }
    merged, updated = merge_bearer_token_from_dotenv(existing, dotenv)
    assert updated == ["PR_LEANDNA_DATA_API_BEARER_TOKEN"]
    assert merged["PR_LEANDNA_DATA_API_BEARER_TOKEN"] == "new-token"
    assert merged["PENDO_INTEGRATION_KEY"] == "pendo"
    assert "PR_LEANDNA_DATA_API_COOKIE" not in merged


def test_merge_no_op_when_unchanged() -> None:
    existing = {"PR_LEANDNA_DATA_API_BEARER_TOKEN": "same"}
    dotenv = {"PR_LEANDNA_DATA_API_BEARER_TOKEN": "same"}
    merged, updated = merge_bearer_token_from_dotenv(existing, dotenv)
    assert updated == []
    assert merged == existing


def test_token_fingerprint_never_empty_for_value() -> None:
    fp = token_fingerprint("abc123")
    assert fp.startswith("len=6")
    assert "sha256=" in fp
