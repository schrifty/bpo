"""Tests for LLM management guidance appended to speaker notes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.deck_builder_utils import build_slide_jql_speaker_notes_for_entry
from src.speaker_notes_llm import (
    enrich_speaker_notes_with_management_guidance,
    generate_slide_management_guidance,
)


@pytest.fixture(autouse=True)
def _no_slide_yaml_drive_lookup():
    with patch("src.speaker_notes_llm._slide_yaml_prompt", return_value=""):
        yield

def _mock_llm_response(text: str) -> MagicMock:
    msg = MagicMock()
    msg.content = text
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def test_speaker_notes_llm_disabled_returns_unchanged(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "false")
    base = "2026-01-01 12:00:00\n\nSlide: Data Quality"
    out = enrich_speaker_notes_with_management_guidance(base, report={}, entry={"slide_type": "data_quality"})
    assert out == base


def test_generate_management_guidance_success(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _mock_llm_response(
        "Use backlog age to decide whether to re-prioritize sprint commitments and unblock Support escalations."
    )
    with patch("src.speaker_notes_llm.llm_client", return_value=mock_client):
        with patch("src.speaker_notes_llm._llm_create_with_retry", side_effect=lambda c, **kw: c.chat.completions.create(**kw)):
            out = generate_slide_management_guidance(
                slide_title="Backlog Health",
                slide_type="eng_backlog_health",
                report={"customer": "Acme", "_deck_id": "engineering-portfolio"},
            )
    assert "backlog age" in out.lower()
    assert mock_client.chat.completions.create.called


def test_generate_management_guidance_failure_no_fallback(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    monkeypatch.delenv("CORTEX_SPEAKER_NOTES_LLM_ALLOW_FALLBACK", raising=False)
    with patch("src.speaker_notes_llm.llm_client", side_effect=RuntimeError("no api key")):
        out = generate_slide_management_guidance(slide_title="Test", slide_type="data_quality")
    assert out == ""


def test_generate_management_guidance_failure_with_fallback(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM_ALLOW_FALLBACK", "true")
    with patch("src.speaker_notes_llm.llm_client", side_effect=RuntimeError("no api key")):
        out = generate_slide_management_guidance(slide_title="Cursor Usage", slide_type="cursor_usage")
    assert "Cursor Usage" in out
    assert "executive review" in out.lower()


def test_enrich_appends_how_to_use_block(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    paragraph = (
        "This slide shows sprint throughput so you can calibrate capacity planning with Support "
        "and Implementation leads before the next release train."
    )
    with patch(
        "src.speaker_notes_llm.generate_slide_management_guidance",
        return_value=paragraph,
    ):
        out = enrich_speaker_notes_with_management_guidance(
            "Base notes",
            report={"customer": "Acme"},
            entry={"slide_type": "eng_current_sprint", "title": "Current Sprint"},
        )
    assert out.startswith("Base notes")
    assert "How to use this slide" in out
    assert paragraph in out


def test_build_slide_jql_speaker_notes_includes_guidance(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    report = {"jira": {"jql_queries": ["project = LEAN"]}}
    entry = {"slide_type": "data_quality", "title": "Data Quality", "id": "data_quality"}
    paragraph = "LEAN project volume helps you spot Support-to-Engineering handoff bottlenecks."
    with patch(
        "src.speaker_notes_llm.generate_slide_management_guidance",
        return_value=paragraph,
    ):
        notes = build_slide_jql_speaker_notes_for_entry(report, entry)
    assert "project = LEAN" in notes
    assert "How to use this slide" in notes
    assert paragraph in notes


def test_build_hydrate_speaker_notes_includes_guidance(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    from src import evaluate

    paragraph = "Compare mapped KPIs to last quarter before committing headcount to Support backlog burn-down."
    with patch(
        "src.speaker_notes_llm.generate_slide_management_guidance",
        return_value=paragraph,
    ):
        out = evaluate._build_hydrate_speaker_notes(
            [{"field": "open_tickets", "new_value": "42", "mapped": True}],
            [{"type": "shape", "text": "Open tickets"}],
            report={"customer": "Acme"},
            analysis={"slide_type": "support_kpis_intake", "purpose": "Intake volume"},
        )
    assert "Mapped values:" in out
    assert "How to use this slide" in out
    assert paragraph in out


def test_user_prompt_includes_metrics_and_yaml(monkeypatch):
    monkeypatch.setenv("CORTEX_SPEAKER_NOTES_LLM", "true")
    captured: dict = {}

    def _capture_create(**kwargs):
        captured["user"] = kwargs["messages"][1]["content"]
        return _mock_llm_response("Paragraph about engineering capacity.")

    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = _capture_create
    with patch("src.speaker_notes_llm.llm_client", return_value=mock_client):
        with patch("src.speaker_notes_llm._llm_create_with_retry", side_effect=lambda c, **kw: c.chat.completions.create(**kw)):
            with patch(
                "src.speaker_notes_llm._slide_yaml_prompt",
                return_value="Engineering portfolio sprint snapshot for VP review.",
            ):
                generate_slide_management_guidance(
                    slide_title="Current Sprint",
                    slide_type="eng_current_sprint",
                    report={
                        "customer": "Acme",
                        "_deck_id": "engineering-portfolio",
                        "eng_portfolio": {"in_flight_count": 12, "closed_count": 34},
                    },
                    data_keys=["eng_portfolio"],
                )
    user = captured.get("user", "")
    assert "eng_current_sprint" in user
    assert "engineering-portfolio" in user
    assert "Acme" in user
    assert "in_flight_count" in user
    assert "Engineering portfolio sprint snapshot" in user
