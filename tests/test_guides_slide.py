"""Guide Engagement slide: distinguish missing data from zero usage."""

from src.slides_client import _guides_slide


def test_guides_zero_events_is_no_usage_not_missing_data():
    report: dict = {
        "_current_slide": {"slide_type": "guides", "title": "Guide Engagement"},
        "guides": {
            "customer": "Acme",
            "days": 30,
            "total_visitors": 12,
            "total_guide_events": 0,
            "seen": 0,
            "advanced": 0,
            "dismissed": 0,
            "guide_reach": 0.0,
            "dismiss_rate": 0,
            "advance_rate": 0,
            "top_guides": [],
        },
    }
    reqs: list = []
    _guides_slide(reqs, "sid_g", report, 0)
    assert not report.get("_missing_slide_data")


def test_guides_error_still_missing_data():
    report: dict = {
        "_current_slide": {"slide_type": "guides", "title": "Guide Engagement"},
        "guides": {"error": "Could not fetch guide events: timeout"},
    }
    reqs: list = []
    _guides_slide(reqs, "sid_g2", report, 0)
    missing = report.get("_missing_slide_data") or []
    assert len(missing) == 1
    assert "timeout" in missing[0]["missing"]


def test_guides_absent_is_missing_data():
    report: dict = {
        "_current_slide": {"slide_type": "guides", "title": "Guide Engagement"},
    }
    reqs: list = []
    _guides_slide(reqs, "sid_g3", report, 0)
    assert report.get("_missing_slide_data")
