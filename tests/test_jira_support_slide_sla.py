"""Tests for Jira support slide SLA header helper."""

from src.slide_jira_support import _jira_support_window_and_sla


def test_jira_support_sla_header_includes_breaches() -> None:
    header, sla_text, offset = _jira_support_window_and_sla(
        {
            "days": 90,
            "ttfr": {"measured": 10, "median": "2h", "avg": "3h", "breached": 2, "waiting": 1},
            "ttr": {"measured": 8, "median": "1d", "avg": "2d", "breached": 1},
        }
    )
    assert "90d" in header
    assert "First Response" in sla_text
    assert "2 breaches" in sla_text
    assert "Resolution" in sla_text
    assert offset > 28
