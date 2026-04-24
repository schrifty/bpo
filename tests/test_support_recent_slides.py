"""Support deck: recently opened / closed HELP ticket slides."""

from src.slides_client import (
    _support_recent_closed_slide,
    _support_recent_opened_slide,
    _truncate_table_cell,
    slide_type_may_paginate,
)


def test_slide_types_may_paginate():
    assert slide_type_may_paginate("support_recent_opened") is True
    assert slide_type_may_paginate("support_recent_closed") is True


def test_support_recent_opened_renders_empty_window_without_missing_flag():
    report: dict = {
        "_current_slide": {"slide_type": "support_recent_opened", "title": "Recent opened"},
        "customer": "Acme",
        "jira": {
            "base_url": "https://example.atlassian.net",
            "customer_help_recent": {
                "customer": "Acme",
                "opened_within_days": None,
                "closed_within_days": None,
                "recently_opened": [],
                "recently_closed": [],
                "jql_queries": [],
            },
        },
    }
    reqs: list = []
    _support_recent_opened_slide(reqs, "s_ro", report, 0)
    assert not report.get("_missing_slide_data")
    assert sum(1 for r in reqs if "createSlide" in r) == 1


def test_truncate_table_cell_word_boundary():
    long_status = "Verify Engineering Change Request Status Pending"
    out = _truncate_table_cell(long_status, 26)
    assert out.endswith("...")
    assert len(out) <= 26
    pr = "Major: Workaround Available"
    assert _truncate_table_cell(pr, 40) == pr
    assert _truncate_table_cell(None, 20) == "—"


def test_support_help_customer_escalations_renders_table():
    report: dict = {
        "_current_slide": {
            "slide_type": "support_help_customer_escalations",
            "title": "Escalations",
        },
        "customer": "Acme",
        "jira": {
            "base_url": "https://example.atlassian.net",
            "help_customer_escalations": {
                "tickets": [
                    {
                        "key": "HELP-1",
                        "summary": "Test",
                        "status": "Open",
                        "priority": "P2",
                        "created_short": "2026-01-01",
                        "updated_short": "2026-01-10",
                    },
                ],
            },
        },
    }
    reqs: list = []
    from src.slides_client import _support_help_customer_escalations_slide

    _support_help_customer_escalations_slide(reqs, "s_esc", report, 0)
    assert not report.get("_missing_slide_data")
    assert any("createTable" in r for r in reqs)
    assert any("Updated" in str(r) for r in reqs)


def test_support_help_orgs_by_opened_renders_table():
    report: dict = {
        "_current_slide": {
            "slide_type": "support_help_orgs_by_opened",
            "title": "Orgs",
        },
        "customer": None,
        "jira": {
            "help_orgs_by_opened": {
                "days": 90,
                "total_issues": 3,
                "by_organization": [
                    {"organization": "Acme Corp", "count": 2},
                    {"organization": "Beta", "count": 1},
                ],
            },
        },
    }
    reqs: list = []
    from src.slides_client import _support_help_orgs_by_opened_slide

    _support_help_orgs_by_opened_slide(reqs, "s_org", report, 0)
    assert not report.get("_missing_slide_data")
    assert any("createTable" in r for r in reqs)
    assert any("Organization" in str(r) for r in reqs)


def test_support_recent_closed_error_uses_missing_slide():
    report: dict = {
        "_current_slide": {"slide_type": "support_recent_closed", "title": "Recent closed"},
        "jira": {
            "customer_help_recent": {
                "error": "401",
                "recently_closed": [],
            },
        },
    }
    reqs: list = []
    _support_recent_closed_slide(reqs, "s_rc", report, 0)
    assert report.get("_missing_slide_data")
