"""Tests for optional Salesforce Account Id mapping (customer_identity_map.yaml)."""

import pytest

from src.customer_identity import (
    invalidate_customer_identity_cache,
    lookup_salesforce_identity,
)


@pytest.fixture(autouse=True)
def clear_identity_cache():
    invalidate_customer_identity_cache()
    yield
    invalidate_customer_identity_cache()


def test_lookup_empty_customer():
    assert lookup_salesforce_identity("") == ([], None)
    assert lookup_salesforce_identity("   ") == ([], None)


def test_lookup_unmapped(tmp_path, monkeypatch):
    import src.customer_identity as ci

    y = tmp_path / "customer_identity_map.yaml"
    y.write_text("other:\n  salesforce_account_id: '001000000000001'\n")  # 15-char Id
    monkeypatch.setattr(ci, "_IDENTITY_FILE", y)
    invalidate_customer_identity_cache()
    assert lookup_salesforce_identity("nobody") == ([], None)


def test_lookup_single_id_string(tmp_path, monkeypatch):
    import src.customer_identity as ci

    y = tmp_path / "customer_identity_map.yaml"
    y.write_text('Acme Corp:\n  salesforce_account_id: "001000000000001AAA"\n')
    monkeypatch.setattr(ci, "_IDENTITY_FILE", y)
    invalidate_customer_identity_cache()
    ids, prim = lookup_salesforce_identity("acme corp")
    assert ids == ["001000000000001AAA"]
    assert prim == "001000000000001AAA"


def test_lookup_dict_primary_and_list(tmp_path, monkeypatch):
    import src.customer_identity as ci

    y = tmp_path / "customer_identity_map.yaml"
    y.write_text(
        "BigCo:\n"
        "  salesforce_account_ids:\n"
        "    - '001000000000001AAA'\n"
        "    - '001000000000002BBB'\n"
        "  salesforce_primary_account_id: '001000000000002BBB'\n"
    )
    monkeypatch.setattr(ci, "_IDENTITY_FILE", y)
    invalidate_customer_identity_cache()
    ids, prim = lookup_salesforce_identity("BigCo")
    assert len(ids) == 2
    assert prim == "001000000000002BBB"


def test_lookup_invalid_id_skipped(tmp_path, monkeypatch):
    import src.customer_identity as ci

    y = tmp_path / "customer_identity_map.yaml"
    y.write_text("X:\n  salesforce_account_id: 'too-short'\n")
    monkeypatch.setattr(ci, "_IDENTITY_FILE", y)
    ci.invalidate_customer_identity_cache()
    assert lookup_salesforce_identity("x") == ([], None)
