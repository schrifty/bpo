"""Unit tests for Pendo preload Drive cache filenames and envelope validation."""

from src import pendo_preload_cache_drive as ppc


def test_pendo_preload_cache_filename_days_and_catalog():
    assert "days90" in ppc.pendo_preload_cache_filename(ppc.PRELOAD_KIND_VISITORS, 90)
    assert "_days90.json" in ppc.pendo_preload_cache_filename(ppc.PRELOAD_KIND_VISITORS, 90)
    assert "days" not in ppc.pendo_preload_cache_filename(ppc.PRELOAD_KIND_PAGE_CATALOG, None)
    assert ppc.pendo_preload_cache_filename(ppc.PRELOAD_KIND_PAGE_CATALOG, None).endswith(
        f"_{ppc.PRELOAD_KIND_PAGE_CATALOG}.json"
    )


def test_validate_envelope_accepts_catalog_and_visitors():
    raw_cat = {
        "schema_version": ppc.PENDO_PRELOAD_CACHE_SCHEMA_VERSION,
        "kind": ppc.PRELOAD_KIND_PAGE_CATALOG,
        "days": None,
        "saved_at": "2026-01-01T00:00:00+00:00",
        "payload": {"p1": "Home"},
    }
    assert ppc._validate_envelope(raw_cat, ppc.PRELOAD_KIND_PAGE_CATALOG, None) == raw_cat

    raw_v = {
        "schema_version": ppc.PENDO_PRELOAD_CACHE_SCHEMA_VERSION,
        "kind": ppc.PRELOAD_KIND_VISITORS,
        "days": 30,
        "saved_at": "2026-01-01T00:00:00+00:00",
        "payload": {"days": 30, "now_ms": 1, "all_visitors": [], "all_customer_stats": {}},
    }
    assert ppc._validate_envelope(raw_v, ppc.PRELOAD_KIND_VISITORS, 30) == raw_v
    assert ppc._validate_envelope(raw_v, ppc.PRELOAD_KIND_VISITORS, 90) is None
