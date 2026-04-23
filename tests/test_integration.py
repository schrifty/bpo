"""Integration tests: multi-component flows with mocked external services."""
from unittest.mock import MagicMock, patch

import pytest

from src import evaluate


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
