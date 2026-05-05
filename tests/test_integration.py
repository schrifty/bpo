"""Integration tests: multi-component flows with mocked external services."""
from unittest.mock import MagicMock, patch

import pytest

from src import evaluate
from src.qbr_hydrate_mappings import REPORT_KEY_EXPLICIT_QBR_MAPPINGS


def _make_mock_slides_svc(pres_id: str, page_id: str = "page1"):
    """Build a mock Google Slides service that returns a one-slide deck with one text shape."""
    slide = {
        "objectId": page_id,
        "pageElements": [
            {
                "objectId": "el1",
                "shape": {
                    "text": {
                        "textElements": [{"textRun": {"content": "31 sites"}}]
                    }
                },
            }
        ],
    }
    pres = {"slides": [slide]}

    def get_execute(**kwargs):
        return pres

    def batch_update_execute(**kwargs):
        return {}

    mock_pres = MagicMock()
    mock_pres.get.return_value.execute.side_effect = get_execute
    mock_batch = MagicMock()
    mock_batch.execute.side_effect = batch_update_execute

    mock_slides = MagicMock()
    mock_slides.presentations.return_value.get.return_value.execute.side_effect = get_execute
    mock_slides.presentations.return_value.batchUpdate.return_value.execute.side_effect = (
        batch_update_execute
    )
    return mock_slides


def _make_mock_slides_svc_with_text(pres_id: str, page_id: str, text: str):
    """Like _make_mock_slides_svc but with arbitrary shape text (for QBR mapping-first)."""
    slide = {
        "objectId": page_id,
        "pageElements": [
            {
                "objectId": "el1",
                "shape": {
                    "text": {
                        "textElements": [{"textRun": {"content": text}}]
                    }
                },
            }
        ],
    }
    pres = {"slides": [slide]}

    def get_execute(**kwargs):
        return pres

    batch_update_calls: list[dict] = []

    def batch_update_fn(**kwargs):
        batch_update_calls.append(kwargs)
        m = MagicMock()
        m.execute.return_value = {}
        return m

    mock_slides = MagicMock()
    mock_slides.presentations.return_value.get.return_value.execute.side_effect = get_execute
    mock_slides.presentations.return_value.batchUpdate.side_effect = batch_update_fn
    mock_slides._test_batch_update_calls = batch_update_calls
    return mock_slides


def _iter_batch_requests(mock_slides):
    """Yield each sub-request dict from all captured batchUpdate bodies."""
    for kw in getattr(mock_slides, "_test_batch_update_calls", []):
        body = kw.get("body") or {}
        for req in body.get("requests") or []:
            if isinstance(req, dict):
                yield req


def test_adapt_custom_slides_qbr_mapping_first_no_llm(monkeypatch, tmp_path):
    """Explicit QBR: replacements come only from qbr_mappings.yaml; LLM adapt is not called.

    Proves the mapping-first path in ``adapt_custom_slides`` through Phase B replaceAllText.
    """
    monkeypatch.setenv("BPO_SLIDES_WRITE_INTERVAL_SEC", "0")
    monkeypatch.setattr("src.slides_api._throttle_before_slides_write", lambda: None)
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(evaluate, "bootstrap_qbr_mappings_from_slides", lambda *a, **k: 0)
    # Summary slide appends + extra batchUpdate; not needed to prove YAML → replaceAllText.
    monkeypatch.setattr(evaluate, "_append_hydrate_summary_slide", lambda *args, **kwargs: False)
    cfg = {
        "version": 1,
        "mappings": [
            {"slide_id": None, "source": "[USERS]", "target": "total_users"},
        ],
        "bracket_placeholder_sources": [],
    }
    monkeypatch.setattr("src.qbr_hydrate_mappings.load_qbr_mappings", lambda **_: cfg)

    pres_id = "pres_qbr"
    page_id = "page1"
    slides_svc = _make_mock_slides_svc_with_text(pres_id, page_id, "[USERS]")
    report = {
        "customer": "Acme",
        REPORT_KEY_EXPLICIT_QBR_MAPPINGS: True,
        "_slide_plan": [{"slide_type": "health"}],
        # Avoid ``get_slide_definition("qbr_agenda")`` → Drive YAML subset scan (~60s in CI).
        "_hydrate_slide_hints": {"qbr_agenda": {}},
        "account": {"total_visitors": 99, "total_sites": 1},
    }

    llm_calls: list[object] = []

    def no_llm(*args, **kwargs):
        llm_calls.append(1)
        raise AssertionError("mapping-first QBR must not call _get_data_replacements (LLM adapt)")

    with (
        patch.object(evaluate, "_get_slide_thumbnail_url", return_value=None),
        patch.object(evaluate, "_get_data_replacements", side_effect=no_llm),
    ):
        oai = MagicMock()
        stats = evaluate.adapt_custom_slides(
            slides_svc, pres_id, [page_id], report, oai
        )

    assert not llm_calls, "LLM adapt path should be skipped"
    assert stats["cache"].get("llm", 0) == 0
    assert stats["adapted"] == 1 or stats["clean"] == 1

    replace_hits = [
        r
        for r in _iter_batch_requests(slides_svc)
        if "replaceAllText" in r
        and r["replaceAllText"].get("containsText", {}).get("text") == "[USERS]"
    ]
    assert replace_hits, "expected at least one replaceAllText for YAML source [USERS]"
    new_texts = {h["replaceAllText"].get("replaceText", "") for h in replace_hits}
    assert any("99" in t for t in new_texts), f"expected total_users 99 in replaceText, got {new_texts}"


def test_adapt_custom_slides_integration(monkeypatch, tmp_path):
    """Adapt pipeline runs end-to-end: extract elements, get replacements, call batchUpdate."""
    monkeypatch.setattr(evaluate, "_slide_cache_dir", lambda: tmp_path)

    pres_id = "pres_123"
    page_id = "page1"
    slides_svc = _make_mock_slides_svc(pres_id, page_id)
    report = {
        "customer": "Acme",
        "account": {"total_sites": 14, "total_visitors": 50},
    }

    # Avoid real thumbnail fetch and LLM: inject fixed replacements
    def fake_thumbnail_url(*args, **kwargs):
        return None

    def fake_get_replacements(
        oai, text_elements, data_summary, thumb_b64=None, slide_label="?", extra_system_rules="",
    ):
        return [
            {"original": "31", "new_value": "14", "mapped": True, "field": "total_sites"},
        ]

    with (
        patch.object(evaluate, "_get_slide_thumbnail_url", side_effect=fake_thumbnail_url),
        patch.object(evaluate, "_get_data_replacements", side_effect=fake_get_replacements),
    ):
        oai = MagicMock()
        stats = evaluate.adapt_custom_slides(
            slides_svc, pres_id, [page_id], report, oai
        )

    assert stats["adapted"] == 1 or stats["clean"] == 1 or stats["incomplete"] == 1
    # Replacements + optional styling + clear notes + summary slide each may call batchUpdate.
    assert slides_svc.presentations.return_value.batchUpdate.return_value.execute.call_count >= 1


def test_hydrate_early_exit_when_no_intake_group():
    """hydrate_new_slides returns [] when GOOGLE_HYDRATE_INTAKE_GROUP is not set."""
    with patch.object(evaluate, "GOOGLE_HYDRATE_INTAKE_GROUP", None):
        result = evaluate.hydrate_new_slides(customer_override="TestCustomer")
    assert result == []


def test_hydrate_early_exit_when_no_presentations():
    """hydrate_new_slides returns [] when the group scan finds no presentations."""
    with (
        patch.object(evaluate, "GOOGLE_HYDRATE_INTAKE_GROUP", "intake@example.com"),
        patch.object(evaluate, "_list_presentations_shared_with_group", return_value=[]),
    ):
        result = evaluate.hydrate_new_slides(customer_override="TestCustomer")
    assert result == []
