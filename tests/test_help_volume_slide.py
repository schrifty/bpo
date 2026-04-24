"""HELP ticket volume trends slide: missing-data vs successful payload."""

from unittest.mock import MagicMock

from src.slides_client import _eng_help_volume_trends_slide


def test_help_volume_jira_error_is_missing_data_not_no_usage():
    report: dict = {
        "_current_slide": {"slide_type": "eng_help_volume_trends", "title": "Volume Analysis"},
        "eng_portfolio": {
            "help_ticket_trends": {
                "error": "401 Unauthorized",
                "all": [],
                "escalated": [],
                "non_escalated": [],
            },
        },
        "_charts": object(),
    }
    reqs: list = []
    _eng_help_volume_trends_slide(reqs, "sid_h", report, 0)
    missing = report.get("_missing_slide_data") or []
    assert len(missing) == 1
    assert "Jira error" in missing[0]["missing"]
    assert report.get("eng_help_volume_jql_trace") == {"jql_queries": []}


def test_help_volume_all_months_zero_is_not_missing_data():
    """Twelve buckets of zeros means the Jira query succeeded; not the red missing banner."""
    months = [{"label": f"M{i}", "created": 0, "resolved": 0} for i in range(12)]
    trends = {"all": months, "escalated": months, "non_escalated": months}
    charts = MagicMock()
    charts.add_line_chart.return_value = ("spreadsheet_id", "chart_id")

    report: dict = {
        "_current_slide": {"slide_type": "eng_help_volume_trends", "title": "Volume Analysis"},
        "eng_portfolio": {"help_ticket_trends": trends},
        "_charts": charts,
    }
    reqs: list = []
    _eng_help_volume_trends_slide(reqs, "sid_h2", report, 0)
    assert not report.get("_missing_slide_data")
    assert charts.add_line_chart.call_count == 3
    assert report.get("eng_help_volume_jql_trace") == {"jql_queries": []}


def test_help_volume_speaker_jql_trace_passes_jira_queries():
    months = [{"label": f"M{i}", "created": 0, "resolved": 0} for i in range(12)]
    jql = [{"description": "HELP volume trends (12-month created vs resolved)", "jql": "project = HELP ORDER BY created DESC"}]
    trends = {"all": months, "escalated": months, "non_escalated": months, "jql_queries": jql}
    charts = MagicMock()
    charts.add_line_chart.return_value = ("spreadsheet_id", "chart_id")
    report: dict = {
        "_current_slide": {"slide_type": "eng_help_volume_trends", "title": "Volume Analysis"},
        "eng_portfolio": {"help_ticket_trends": trends},
        "_charts": charts,
    }
    reqs: list = []
    _eng_help_volume_trends_slide(reqs, "sid_jql", report, 0)
    assert report.get("eng_help_volume_jql_trace") == {"jql_queries": jql}
