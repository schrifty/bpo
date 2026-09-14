"""Tests for credential redaction in job logs and failure tails."""

from __future__ import annotations

from src.log_redact import REDACTED, redact_secrets


def test_redact_secrets_strips_bearer_cookie_and_keys() -> None:
    blob = (
        "Authorization: Bearer super-secret-session\n"
        "Cookie: sid=abc; other=xyz\n"
        "OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyz\n"
        "PENDO_INTEGRATION_KEY=pendo-live-secret\n"
        "url=https://sheets.googleapis.com/?access_token=ya29.secretValue\n"
        "xoxb-1234567890-abcdefghij\n"
    )
    out = redact_secrets(blob)
    assert "super-secret-session" not in out
    assert "sid=abc" not in out
    assert "sk-abcdefghijklmnopqrstuvwxyz" not in out
    assert "pendo-live-secret" not in out
    assert "ya29.secretValue" not in out
    assert "xoxb-1234567890-abcdefghij" not in out
    assert REDACTED in out
    assert "sheets.googleapis.com" in out


def test_redact_secrets_keeps_http_errors_for_retry() -> None:
    line = (
        "Pendo detailed export failed for Ford Motor Company: "
        "<HttpError 503 when requesting https://sheets.googleapis.com/v4/spreadsheets>"
    )
    assert redact_secrets(line) == line


def test_redact_secrets_pem_block() -> None:
    pem = "-----BEGIN RSA PRIVATE KEY-----\nMIISECRET\n-----END RSA PRIVATE KEY-----"
    out = redact_secrets(pem)
    assert "MIISECRET" not in out
    assert REDACTED in out
