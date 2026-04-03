"""Tests for optional LLM Notable Signals pass (Phase 2)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.signals_llm import (
    build_signals_llm_payload,
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


def test_normalize_item_text_strips_leading_number():
    assert _normalize_item_text("1. Something important") == "Something important"
    assert _normalize_item_text("12) Another") == "Another"


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
                content='{"items":[{"text":"Merged insight from data","theme":"engagement"},{"text":"Second point","theme":"support"}]}'
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
