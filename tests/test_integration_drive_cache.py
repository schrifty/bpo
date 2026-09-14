"""Unit tests for Drive integration JSON cache helpers (no network)."""

from __future__ import annotations

import pytest

from src import integration_drive_cache as idc
from src.cache_crypto import CACHE_ALG_FERNET, encrypt_jsonable


def _encrypted_envelope(
    kind: str,
    customer_key: str,
    payload: dict,
    *,
    saved_at: str = "2026-01-01T00:00:00+00:00",
) -> dict:
    ct = encrypt_jsonable(payload)
    assert ct is not None
    return {
        "schema_version": idc.INTEGRATION_CACHE_SCHEMA_VERSION,
        "kind": kind,
        "customer_key": customer_key,
        "saved_at": saved_at,
        "alg": CACHE_ALG_FERNET,
        "ciphertext": ct,
    }


def test_integration_customer_key_all_customers() -> None:
    assert idc.integration_customer_key(None) == "__all__"
    assert idc.integration_customer_key("") == "__all__"
    assert idc.integration_customer_key("   ") == "__all__"


def test_integration_customer_key_normalized() -> None:
    assert idc.integration_customer_key("  Acme Corp  ") == "acme corp"


def test_integration_cache_filename_stable() -> None:
    ck = idc.integration_customer_key("Acme")
    n = idc.integration_cache_filename(idc.KIND_JIRA_SUPPORT, ck)
    assert n == idc.integration_cache_filename(idc.KIND_JIRA_SUPPORT, ck)
    assert n.startswith("integration_jira_support_v2_")
    assert n.endswith(".json")
    assert idc.KIND_JIRA_SUPPORT in n


def test_validate_envelope_accepts_matching() -> None:
    raw = _encrypted_envelope(
        idc.KIND_SALESFORCE_COMPREHENSIVE,
        "acme",
        {"matched": True},
    )
    env = idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "acme")
    assert env is not None
    assert env["payload"] == {"matched": True}


def test_validate_envelope_rejects_mismatch() -> None:
    raw = _encrypted_envelope(idc.KIND_SALESFORCE_COMPREHENSIVE, "acme", {})
    assert idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "other") is None
    assert idc._validate_envelope(raw, idc.KIND_JIRA_SUPPORT, "acme") is None


def test_validate_envelope_rejects_plaintext_payload() -> None:
    raw = {
        "schema_version": idc.INTEGRATION_CACHE_SCHEMA_VERSION,
        "kind": idc.KIND_SALESFORCE_COMPREHENSIVE,
        "customer_key": "acme",
        "saved_at": "2026-01-01T00:00:00+00:00",
        "alg": CACHE_ALG_FERNET,
        "payload": {"matched": True},
        "ciphertext": "gAAAAABnot-used",
    }
    assert idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "acme") is None


def test_validate_envelope_rejects_legacy_v1() -> None:
    raw = {
        "schema_version": 1,
        "kind": idc.KIND_SALESFORCE_COMPREHENSIVE,
        "customer_key": "acme",
        "saved_at": "2026-01-01T00:00:00+00:00",
        "payload": {"matched": True},
    }
    assert idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "acme") is None


def test_save_skips_without_fernet_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CORTEX_CACHE_FERNET_KEY", raising=False)
    monkeypatch.setattr("src.config.CORTEX_INTEGRATION_DRIVE_CACHE_DISABLED", False)
    called: list[str] = []
    monkeypatch.setattr(
        idc,
        "resolve_portfolio_snapshot_folder_id",
        lambda: called.append("folder") or "folder-id",
    )
    idc.save_integration_payload(idc.KIND_JIRA_SUPPORT, "acme", {"tickets": [1]})
    assert called == []


def test_integration_drive_cache_reads_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import config

    monkeypatch.setattr(config, "CORTEX_INTEGRATION_DRIVE_CACHE_DISABLED", True)
    monkeypatch.setattr(config, "CORTEX_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH", False)
    assert idc.integration_drive_cache_reads_enabled() is False


def test_integration_drive_cache_reads_force_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import config

    monkeypatch.setattr(config, "CORTEX_INTEGRATION_DRIVE_CACHE_DISABLED", False)
    monkeypatch.setattr(config, "CORTEX_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH", True)
    assert idc.integration_drive_cache_reads_enabled() is False
