"""Tests for Fernet cache encryption helpers."""

from __future__ import annotations

from cryptography.fernet import Fernet

from src.cache_crypto import decrypt_jsonable, encrypt_jsonable, load_cache_fernet


def test_encrypt_round_trip(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_CACHE_FERNET_KEY", Fernet.generate_key().decode("ascii"))
    token = encrypt_jsonable({"email": "a@example.com"})
    assert token
    assert "a@example.com" not in token
    assert decrypt_jsonable(token) == {"email": "a@example.com"}


def test_encrypt_skipped_without_key(monkeypatch) -> None:
    monkeypatch.delenv("CORTEX_CACHE_FERNET_KEY", raising=False)
    assert load_cache_fernet() is None
    assert encrypt_jsonable({"ok": True}) is None
    assert decrypt_jsonable("gAAAAABnot-a-token") is None


def test_decrypt_wrong_key(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_CACHE_FERNET_KEY", Fernet.generate_key().decode("ascii"))
    token = encrypt_jsonable({"ok": True})
    monkeypatch.setenv("CORTEX_CACHE_FERNET_KEY", Fernet.generate_key().decode("ascii"))
    assert decrypt_jsonable(token) is None
