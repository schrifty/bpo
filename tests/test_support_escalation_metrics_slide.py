"""Escalation Metrics slide (HELP): renders from help_escalation_metrics payload."""

from src.slides_client import _support_help_escalation_metrics_slide
from src.support_notable_llm import _normalize_escalation_llm_text


def test_support_help_escalation_metrics_renders_kpis():
    report: dict = {
        "_current_slide": {
            "slide_type": "support_help_escalation_metrics",
            "title": "HELP — Escalation metrics",
        },
        "jira": {
            "base_url": "https://jira.example.com",
            "help_escalation_metrics": {
                "not_done_escalation_count": 4,
                "escalations_opened_90d": 12,
                "escalations_closed_90d": 9,
                "ttr_open_backlog_customer_escalation": {"median": "3.0h"},
                "ttr_open_backlog_not_customer_escalation": {"median": "1.5h"},
            },
        },
        "_charts": object(),
    }
    reqs: list = []
    _support_help_escalation_metrics_slide(reqs, "s_em", report, 0)
    assert not report.get("_missing_slide_data")
    assert any("createSlide" in r and r["createSlide"].get("objectId") == "s_em" for r in reqs)
    # Title includes project → shorter KPI line (no repeat "HELP" / awkward NOT DONE)
    _reqs_str = str(reqs)
    assert "HELP — Escalation metrics" in _reqs_str
    assert "Open w/ label (not done)" in _reqs_str
    assert "s_em_defs" not in _reqs_str


def test_support_help_escalation_metrics_renders_llm_nature_summary():
    report: dict = {
        "_current_slide": {
            "slide_type": "support_help_escalation_metrics",
            "title": "HELP — Escalation metrics",
        },
        "jira": {
            "help_escalation_metrics": {
                "not_done_escalation_count": 1,
                "escalations_opened_90d": 2,
                "escalations_closed_90d": 3,
                "ttr_open_backlog_customer_escalation": {"median": "1h"},
                "ttr_open_backlog_not_customer_escalation": {"median": "2h"},
                "llm_nature_summary": "These escalations center on data sync and access issues.",
            },
        },
        "_charts": object(),
    }
    reqs: list = []
    _support_help_escalation_metrics_slide(reqs, "s_em", report, 0)
    _reqs_str = str(reqs)
    assert "s_em_quote" in _reqs_str
    assert "These escalations center on data sync" in _reqs_str
    assert "s_em_defs" not in _reqs_str
    _quote_style = next(
        (r for r in reqs if r.get("updateTextStyle", {}).get("objectId") == "s_em_quote"),
        None,
    )
    assert _quote_style is not None
    assert _quote_style["updateTextStyle"]["style"]["fontSize"]["magnitude"] == 11


def test_escalation_llm_normalize_preserves_paragraphs():
    out = _normalize_escalation_llm_text("First theme.\n\nSecond theme with more.\n\nThird.")
    assert "\n\n" in out
    assert "First theme" in out
    assert "Second theme" in out
