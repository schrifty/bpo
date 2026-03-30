"""Support deck: recently opened / closed HELP ticket slides."""

from src.slides_client import (
    _support_recent_closed_slide,
    _support_recent_opened_slide,
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
                "opened_within_days": 45,
                "closed_within_days": 45,
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
