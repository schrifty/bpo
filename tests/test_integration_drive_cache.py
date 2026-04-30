"""Unit tests for Drive integration JSON cache helpers (no network)."""

from __future__ import annotations

import pytest

from src import integration_drive_cache as idc


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
    assert n.startswith("integration_jira_support_v1_")
    assert n.endswith(".json")
    assert idc.KIND_JIRA_SUPPORT in n


def test_validate_envelope_accepts_matching() -> None:
    raw = {
        "schema_version": idc.INTEGRATION_CACHE_SCHEMA_VERSION,
        "kind": idc.KIND_SALESFORCE_COMPREHENSIVE,
        "customer_key": "acme",
        "saved_at": "2026-01-01T00:00:00+00:00",
        "payload": {"matched": True},
    }
    assert idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "acme") == raw


def test_validate_envelope_rejects_mismatch() -> None:
    raw = {
        "schema_version": idc.INTEGRATION_CACHE_SCHEMA_VERSION,
        "kind": idc.KIND_SALESFORCE_COMPREHENSIVE,
        "customer_key": "acme",
        "saved_at": "2026-01-01T00:00:00+00:00",
        "payload": {},
    }
    assert idc._validate_envelope(raw, idc.KIND_SALESFORCE_COMPREHENSIVE, "other") is None
    assert idc._validate_envelope(raw, idc.KIND_JIRA_SUPPORT, "acme") is None


def test_integration_drive_cache_reads_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import config

    monkeypatch.setattr(config, "BPO_INTEGRATION_DRIVE_CACHE_DISABLED", True)
    monkeypatch.setattr(config, "BPO_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH", False)
    assert idc.integration_drive_cache_reads_enabled() is False


def test_integration_drive_cache_reads_force_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import config

    monkeypatch.setattr(config, "BPO_INTEGRATION_DRIVE_CACHE_DISABLED", False)
    monkeypatch.setattr(config, "BPO_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH", True)
    assert idc.integration_drive_cache_reads_enabled() is False
