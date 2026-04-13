"""Tests for QBR agenda post-hydrate visual refinement config."""

from src.qbr_agenda_visual_refine import (
    _qbr_agenda_visual_quality_ok,
    _qbr_agenda_visual_refinement_config,
)


def test_visual_refinement_config_from_hints():
    report = {
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "visual_refinement": {"enabled": True, "max_refinements": 2},
                }
            }
        }
    }
    c = _qbr_agenda_visual_refinement_config(report)
    assert c["enabled"] is True
    assert c["max_refinements"] == 2


def test_visual_refinement_disabled():
    report = {
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "visual_refinement": {"enabled": False},
                }
            }
        }
    }
    c = _qbr_agenda_visual_refinement_config(report)
    assert c["enabled"] is False


def test_visual_quality_missing_thumbnail_is_not_pass():
    ok, issues = _qbr_agenda_visual_quality_ok(None, None)
    assert ok is False
    assert "thumbnail" in issues.lower()
