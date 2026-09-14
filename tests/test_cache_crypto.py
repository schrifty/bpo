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


def test_leandna_drive_cache_skips_write_without_key(monkeypatch) -> None:
    from src import leandna_drive_cache as ldc

    monkeypatch.delenv("CORTEX_CACHE_FERNET_KEY", raising=False)
    monkeypatch.setattr("src.config.GOOGLE_QBR_GENERATOR_FOLDER_ID", "folder-id")
    called: list[str] = []
    monkeypatch.setattr(ldc, "_get_service", lambda: called.append("drive") or (None, None, None), raising=False)

    ldc.save_json("item_master_x.json", [{"secret": "x"}])
    assert called == []


def test_leandna_drive_cache_ignores_plaintext(monkeypatch) -> None:
    from src import leandna_drive_cache as ldc

    class _Files:
        def list(self, **_k):
            class _Exec:
                def execute(self):
                    return {"files": [{"id": "1", "modifiedTime": "2099-01-01T00:00:00Z"}]}

            return _Exec()

        def get_media(self, **_k):
            class _Req:
                def execute(self):
                    return b'[{"itemCode": "PLAIN"}]'

            return _Req()

    class _Drive:
        def files(self):
            return _Files()

    monkeypatch.setattr("src.slides_api._get_service", lambda: (None, _Drive(), None))
    monkeypatch.setattr("src.network_utils.network_timeout", lambda *_a, **_k: __import__("contextlib").nullcontext())
    assert ldc.load_json("item_master_x.json", ttl_hours=24) is None
