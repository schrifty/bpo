"""Tests for optional LLM Notable Signals (Phases 2–3: facts envelope + editorial)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.signals_llm import (
    build_signals_llm_payload,
    build_signals_llm_user_envelope,
    extract_executive_signals_slide_prompt,
    maybe_rewrite_signals_with_llm,
    _normalize_item_text,
)


def test_build_signals_llm_payload_shape():
    report = {
        "customer": "Acme",
        "quarter": "2026 Q1",
        "days": 90,
        "signals": ["Line one", "Line two"],
        "engagement": {"active_7d": 10, "dormant": 2, "active_rate_7d": 12.5},
        "benchmarks": {"customer_active_rate": 12.5, "cohort_name": "Industrial"},
        "account": {"total_visitors": 40, "total_sites": 3},
        "jira": {"open_issues": 5, "error": None},
        "salesforce": {"matched": False},
        "champions": [],
        "at_risk_users": [{"x": 1}],
    }
    p = build_signals_llm_payload(report)
    assert p["customer"] == "Acme"
    assert p["heuristic_signals"] == ["Line one", "Line two"]
    assert p["people"]["at_risk_users_count"] == 1
    assert p["jira"]["open_issues"] == 5


def test_extract_executive_signals_slide_prompt_finds_signals_slide():
    fake = {
        "slides": [
            {"id": "health", "slide_type": "health", "prompt": ""},
            {"id": "signals", "slide_type": "signals", "prompt": "  Close with action items.  "},
        ]
    }
    with patch("src.deck_loader.resolve_deck", return_value=fake):
        out = extract_executive_signals_slide_prompt("AnyCustomer", max_chars=500)
    assert out == "Close with action items."


def test_normalize_item_text_strips_leading_number():
    assert _normalize_item_text("1. Something important") == "Something important"
    assert _normalize_item_text("12) Another") == "Another"


def test_build_signals_llm_user_envelope_includes_editorial():
    report = {"customer": "X", "days": 30, "signals": ["A"], "engagement": {}, "benchmarks": {}, "account": {}}
    with patch("src.signals_llm.BPO_SIGNALS_LLM_EDITORIAL", True):
        env = build_signals_llm_user_envelope(
            report,
            manifest_rules="Focus on renewal risk.",
            slide_prompt="Number each theme for the CSM.",
        )
    assert "facts" in env
    assert env["facts"]["heuristic_signals"] == ["A"]
    assert env["editorial"]["manifest_rules"] == "Focus on renewal risk."
    assert "slide_brief_from_yaml" in env["editorial"]


def test_maybe_rewrite_pops_editorial_keys_even_when_llm_disabled():
    report = {"signals": ["x"], "_signals_llm_manifest_rules": "secret", "_signals_llm_slide_prompt": "brief"}
    with patch("src.signals_llm.BPO_SIGNALS_LLM", False):
        maybe_rewrite_signals_with_llm(report)
    assert "_signals_llm_manifest_rules" not in report
    assert "_signals_llm_slide_prompt" not in report


def test_maybe_rewrite_skips_when_flag_off():
    report = {"signals": ["keep me"]}
    with patch("src.signals_llm.BPO_SIGNALS_LLM", False):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == ["keep me"]
    assert "_signals_llm_meta" not in report


def test_maybe_rewrite_skips_empty_signals():
    report = {"signals": []}
    with patch("src.signals_llm.BPO_SIGNALS_LLM", True):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == []
    assert report.get("_signals_llm_meta", {}).get("source") == "skipped"


def test_maybe_rewrite_applies_llm_output():
    report = {
        "customer": "Acme",
        "days": 30,
        "signals": ["Heuristic A", "Heuristic B"],
        "engagement": {"active_7d": 1, "dormant": 0, "active_rate_7d": 1.0},
        "benchmarks": {},
        "account": {},
    }
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(
            message=MagicMock(
                content=(
                    '{"items":[{"text":"Merged insight from data","theme":"engagement"},'
                    '{"text":"Second point","theme":"support"}],'
                    '"trend_summary_for_slide":"QoQ: rate +1pp","preferred_comparison_horizon":"QoQ"}'
                )
            )
        )
    ]
    mock_client = MagicMock()
    with patch("src.signals_llm.BPO_SIGNALS_LLM", True), patch(
        "src.signals_llm.llm_client", return_value=mock_client
    ), patch("src.signals_llm._llm_create_with_retry", return_value=mock_resp):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == ["Merged insight from data", "Second point"]
    assert report["_signals_llm_meta"]["source"] == "llm"
    assert report["_signals_llm_meta"]["count"] == 2
    assert report["_signals_llm_meta"]["editorial"] is False
    assert report["signals_trends_display"] == "QoQ: rate +1pp"
    assert report["_signals_llm_meta"]["comparison_horizon"] == "QoQ"


def test_maybe_rewrite_fallback_on_bad_json():
    report = {"customer": "X", "days": 30, "signals": ["Only heuristic"], "engagement": {}, "benchmarks": {}, "account": {}}
    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock(message=MagicMock(content="not json"))]
    with patch("src.signals_llm.BPO_SIGNALS_LLM", True), patch(
        "src.signals_llm.llm_client", return_value=MagicMock()
    ), patch("src.signals_llm._llm_create_with_retry", return_value=mock_resp):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == ["Only heuristic"]
    assert report["_signals_llm_meta"]["source"] == "heuristic"
