"""Tests for optional LLM Notable Signals (Phases 2–3: facts envelope + editorial)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.signals_llm import (
    build_portfolio_signals_llm_payload,
    build_signals_llm_payload,
    build_signals_llm_user_envelope,
    extract_executive_signals_slide_prompt,
    maybe_rewrite_portfolio_signals_with_llm,
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
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM_EDITORIAL", True):
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
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", False):
        maybe_rewrite_signals_with_llm(report)
    assert "_signals_llm_manifest_rules" not in report
    assert "_signals_llm_slide_prompt" not in report


def test_maybe_rewrite_skips_when_flag_off():
    report = {"signals": ["keep me"]}
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", False):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == ["keep me"]
    assert "_signals_llm_meta" not in report


def test_maybe_rewrite_skips_empty_signals():
    report = {"signals": []}
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", True):
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
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", True), patch(
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
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", True), patch(
        "src.signals_llm.llm_client", return_value=MagicMock()
    ), patch("src.signals_llm._llm_create_with_retry", return_value=mock_resp):
        maybe_rewrite_signals_with_llm(report)
    assert report["signals"] == ["Only heuristic"]
    assert report["_signals_llm_meta"]["source"] == "heuristic"


def test_build_portfolio_signals_llm_payload_multisource():
    report = {
        "type": "portfolio",
        "days": 30,
        "customer_count": 1,
        "customers": [
            {
                "customer": "Acme",
                "pendo_csm": "Pat",
                "login_pct": 40.0,
                "active_users": 10,
                "total_users": 100,
                "benchmarks": {"cohort_name": "Industrial", "customer_active_rate": 10.0},
                "engagement": {"active_7d": 10, "active_30d": 2, "dormant": 88, "active_rate_7d": 10.0},
                "depth": {"write_ratio": 22, "collab_events": 3},
                "kei": {"total_queries": 0, "unique_users": 0},
                "guides": {"dismiss_rate": 5, "guide_reach": 40},
                "exports": {"total_exports": 12, "exports_per_active_user": 1.2},
                "signals": ["No Kei AI usage detected — rollout opportunity"],
            },
        ],
        "portfolio_signals": [{"customer": "Acme", "signal": "No Kei AI usage", "severity": 2}],
        "portfolio_trends": {"trends": [{"trend": "Kei AI has zero usage at 5 customers", "type": "opportunity", "customers": "A,B"}]},
        "portfolio_leaders": {"login_rate": [{"rank": 1, "customer": "Acme", "login_pct": 90}]},
        "cohort_digest": {},
        "cohort_findings_bullets": ["Cohort finding A"],
        "portfolio_help_ticket_metrics": {"unresolved_count": 12, "by_type_open": {"Bug": 3}},
        "portfolio_revenue_book": {"total_arr": 1_000_000.0, "top_customers_by_arr": []},
    }
    p = build_portfolio_signals_llm_payload(report)
    assert p["heuristic_critical_signals"][0]["customer"] == "Acme"
    assert p["customers_fact_pack"][0]["per_customer_signal_lines"]
    assert p["jira_help_portfolio_rollup"]["unresolved_help"] == 12
    assert p["salesforce_revenue_book_compact"]["total_arr"] == 1_000_000.0


def test_maybe_rewrite_portfolio_signals_applies_llm():
    report = {
        "type": "portfolio",
        "days": 30,
        "customer_count": 1,
        "customers": [
            {
                "customer": "Acme",
                "pendo_csm": "Pat",
                "login_pct": 40.0,
                "active_users": 10,
                "total_users": 100,
                "benchmarks": {},
                "engagement": {"active_7d": 10},
                "signals": ["Heuristic line"],
            },
        ],
        "portfolio_signals": [{"customer": "Acme", "signal": "Heuristic line", "severity": 1}],
        "portfolio_trends": {"trends": []},
        "portfolio_leaders": {},
        "cohort_digest": {},
        "cohort_findings_bullets": [],
        "portfolio_help_ticket_metrics": {"unresolved_count": 5},
    }
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(
            message=MagicMock(
                content='{"items":[{"customer":"Acme","signal":"Jira backlog elevated with low Kei adoption on account."}]}'
            )
        )
    ]
    mock_client = MagicMock()
    with patch("src.signals_llm.CORTEX_SIGNALS_LLM", True), patch(
        "src.signals_llm.CORTEX_SIGNALS_LLM_EDITORIAL", False
    ), patch("src.signals_llm.extract_portfolio_signals_slide_prompt", return_value=None), patch(
        "src.signals_llm.llm_client", return_value=mock_client
    ), patch("src.signals_llm._llm_create_with_retry", return_value=mock_resp):
        maybe_rewrite_portfolio_signals_with_llm(report, deck_id="portfolio_review")
    assert report["portfolio_signals"][0]["customer"] == "Acme"
    assert "Jira" in report["portfolio_signals"][0]["signal"]
    assert report["_portfolio_signals_llm_meta"]["source"] == "llm"
