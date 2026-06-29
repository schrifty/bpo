"""Tests for Drive-backed Salesforce comprehensive cache loader."""

from __future__ import annotations

from src.salesforce_comprehensive_cache import load_or_fetch_salesforce_comprehensive


def test_load_or_fetch_uses_drive_cache(monkeypatch):
    cached = {"customer": "Acme", "matched": True, "row_limit": 75, "categories": {}}

    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache._salesforce_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.integration_drive_cache_reads_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.try_load_integration_payload",
        lambda kind, customer: cached if customer == "Acme" else None,
    )

    def _should_not_call_sf(*_a, **_k):
        raise AssertionError("SalesforceClient should not be called on cache hit")

    monkeypatch.setattr(
        "src.salesforce_client.SalesforceClient",
        _should_not_call_sf,
    )

    payload, source = load_or_fetch_salesforce_comprehensive("Acme", row_limit=75)
    assert source == "drive_cache"
    assert payload["matched"] is True


def test_load_or_fetch_refetches_when_row_limit_differs(monkeypatch):
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache._salesforce_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.integration_drive_cache_reads_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.try_load_integration_payload",
        lambda _k, _c: {"customer": "Acme", "matched": True, "row_limit": 50, "categories": {}},
    )

    class FakeSf:
        def get_customer_salesforce_comprehensive(self, name: str, *, row_limit: int = 75, **_kw):
            return {"customer": name, "matched": True, "row_limit": row_limit, "categories": {}}

    saved: list[tuple] = []

    monkeypatch.setattr(
        "src.salesforce_client.SalesforceClient",
        FakeSf,
    )
    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.save_integration_payload",
        lambda kind, customer, payload: saved.append((kind, customer, payload)),
    )

    payload, source = load_or_fetch_salesforce_comprehensive("Acme", row_limit=75)
    assert source == "salesforce"
    assert payload["row_limit"] == 75
    assert len(saved) == 1


def test_llm_export_counts_drive_cache_hits(monkeypatch):
    from src.llm_export_salesforce_comprehensive import attach_salesforce_comprehensive_for_llm_export

    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive.llm_export_sf_comprehensive_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.llm_export_salesforce_comprehensive._salesforce_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.customer_identity.lookup_salesforce_identity",
        lambda _label: ([], None),
    )

    def fake_load(label: str, *, row_limit: int = 75, **_kw):
        if label == "Acme":
            return (
                {"customer": label, "matched": True, "row_limit": row_limit, "categories": {}},
                "drive_cache",
            )
        return (
            {"customer": label, "matched": True, "row_limit": row_limit, "categories": {}},
            "salesforce",
        )

    monkeypatch.setattr(
        "src.salesforce_comprehensive_cache.load_or_fetch_salesforce_comprehensive",
        fake_load,
    )

    class FakeSf:
        def get_entity_accounts(self):
            return []

    monkeypatch.setattr("src.salesforce_client.SalesforceClient", FakeSf)

    report = {
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "Acme", "active": True},
                {"customer": "Beta", "active": True},
            ],
        }
    }
    summary = attach_salesforce_comprehensive_for_llm_export(report)
    assert summary["customers_drive_cache_hit"] == 1
    assert summary["customers_salesforce_fetch"] == 1
