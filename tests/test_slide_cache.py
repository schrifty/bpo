"""Tests for slide analysis cache (hash, classification, adapt, versioning)."""
import base64
import json
import pytest

from src import evaluate
from src.hydrate_cache import adapt_cache_key


def _thumb_b64(contents: bytes) -> str:
    return base64.b64encode(contents).decode("ascii")


def test_slide_content_hash_thumbnail():
    """Hash is deterministic from thumbnail bytes."""
    raw = b"fake-png-bytes"
    b64 = _thumb_b64(raw)
    h1 = evaluate._slide_content_hash(b64)
    h2 = evaluate._slide_content_hash(b64)
    assert h1 == h2
    assert len(h1) == 64
    assert all(c in "0123456789abcdef" for c in h1)


def test_slide_content_hash_different_thumbnails():
    """Different thumbnails produce different hashes."""
    h1 = evaluate._slide_content_hash(_thumb_b64(b"aaa"))
    h2 = evaluate._slide_content_hash(_thumb_b64(b"bbb"))
    assert h1 != h2


def test_slide_content_hash_text_fallback():
    """When no thumbnail, hash from text snapshot."""
    h1 = evaluate._slide_content_hash(None, "Agenda\n• Item 1")
    h2 = evaluate._slide_content_hash(None, "Agenda\n• Item 1")
    assert h1 == h2
    assert len(h1) == 64
    h3 = evaluate._slide_content_hash(None, "Other text")
    assert h1 != h3


def test_slide_content_hash_thumbnail_overrides_text():
    """When thumbnail is present, text_snapshot is ignored (thumbnail-only)."""
    raw = b"same"
    b64 = _thumb_b64(raw)
    h_thumb = evaluate._slide_content_hash(b64, "different text A")
    h_thumb2 = evaluate._slide_content_hash(b64, "different text B")
    assert h_thumb == h_thumb2


def test_slide_content_hash_page_id_isolates_slides():
    """Same thumbnail on different slides (different page_id) must not share cache — avoids wrong speaker notes."""
    b64 = _thumb_b64(b"identical-template-thumb")
    h_s5 = evaluate._slide_content_hash(b64, page_id="slide_5_oid")
    h_s6 = evaluate._slide_content_hash(b64, page_id="slide_6_oid")
    assert h_s5 != h_s6
    assert evaluate._slide_content_hash(b64, page_id="slide_5_oid") == h_s5


def test_slide_content_hash_none_empty_returns_none():
    """No thumbnail and no text returns None."""
    assert evaluate._slide_content_hash(None, "") is None
    assert evaluate._slide_content_hash(None) is None


def test_classification_cache_roundtrip(monkeypatch, tmp_path):
    """Set and get classification cache returns the same result."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "abc123"
    result = {"slide_type": "custom", "title": "Test", "reasoning": "Because."}
    evaluate._set_cached_classification(key, result)
    got = evaluate._get_cached_classification(key)
    assert got == result


def test_classification_cache_missing_returns_none(monkeypatch, tmp_path):
    """Missing cache file returns None."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    assert evaluate._get_cached_classification("nonexistent") is None


def test_classification_cache_version_mismatch_returns_none(monkeypatch, tmp_path):
    """Cached result with wrong version is ignored."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "v2key"
    d = tmp_path / "classification"
    d.mkdir(parents=True)
    (d / f"{key}.json").write_text(
        json.dumps({"_version": 999, "slide_type": "custom", "title": "X"}),
        encoding="utf-8",
    )
    assert evaluate._get_cached_classification(key) is None


def test_classification_cache_strips_internal_keys(monkeypatch, tmp_path):
    """Cached result does not expose _version to caller."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "k"
    evaluate._set_cached_classification(key, {"slide_type": "agenda", "title": "T"})
    got = evaluate._get_cached_classification(key)
    assert "_version" not in got
    assert got["slide_type"] == "agenda"


def test_adapt_cache_key_uses_text_snapshot_when_no_thumbnail():
    """Adapt disk cache can key off slide text instead of thumbnail bytes."""
    ds = {"total_sites": 3}
    k1 = adapt_cache_key(None, "pageA", ds, text_snapshot="id1\tshape\t42")
    k2 = adapt_cache_key(None, "pageA", ds, text_snapshot="id1\tshape\t42")
    k3 = adapt_cache_key(None, "pageB", ds, text_snapshot="id1\tshape\t42")
    assert k1 is not None and k2 is not None and k3 is not None
    assert k1 == k2
    assert k1 != k3


def test_slide_text_snapshot_for_adapt_cache_truncates():
    els = [{"element_id": "x", "type": "shape", "text": "a" * 5000} for _ in range(4)]
    s = evaluate._slide_text_snapshot_for_adapt_cache(els, max_chars=100)
    assert len(s) == 100


def test_adapt_cache_roundtrip(monkeypatch, tmp_path):
    """Set and get adapt cache returns the same replacements."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "def456"
    replacements = [
        {"original": "31", "new_value": "14", "mapped": True, "field": "total_sites"},
        {"original": "Q1", "new_value": "[???]", "mapped": False, "field": "n/a"},
    ]
    evaluate._set_cached_adapt(key, replacements)
    got = evaluate._get_cached_adapt(key)
    assert got == replacements


def test_adapt_cache_missing_returns_none(monkeypatch, tmp_path):
    """Missing adapt cache returns None."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    assert evaluate._get_cached_adapt("nonexistent") is None


def test_adapt_cache_version_mismatch_returns_none(monkeypatch, tmp_path):
    """Cached adapt with wrong version is ignored."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "v2adapt"
    d = tmp_path / "adapt"
    d.mkdir(parents=True)
    (d / f"{key}.json").write_text(
        json.dumps({"_version": 999, "replacements": [{"original": "x", "new_value": "y"}]}),
        encoding="utf-8",
    )
    assert evaluate._get_cached_adapt(key) is None


def test_slide_analysis_cache_roundtrip(monkeypatch, tmp_path):
    """Set and get slide analysis cache (data_ask + purpose)."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    key = "abc789"
    analysis = {
        "data_ask": [{"key": "total_sites", "example_from_slide": "31 sites"}],
        "purpose": "Account overview with key metrics",
        "slide_type": "custom",
        "title": "Overview",
    }
    evaluate._set_cached_slide_analysis(key, analysis)
    got = evaluate._get_cached_slide_analysis(key)
    assert got is not None
    assert got["purpose"] == analysis["purpose"]
    assert got["slide_type"] == analysis["slide_type"]
    assert len(got["data_ask"]) == 1 and got["data_ask"][0]["key"] == "total_sites"
