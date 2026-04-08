"""Tests for collapsing Pendo time-series event rows into merged rows."""

from src.pendo_client import _merge_guide_event_rows, _merge_visitor_event_rows_by_dimension


def test_merge_visitor_event_rows_by_dimension_sums():
    raw = [
        {"visitorId": "a", "featureId": "f1", "numEvents": 3},
        {"visitorId": "a", "featureId": "f1", "numEvents": 2, "numMinutes": 1},
        {"visitorId": "b", "featureId": "f1", "numEvents": 1},
    ]
    out = _merge_visitor_event_rows_by_dimension(raw, "featureId")
    by_key = {(r["visitorId"], r["featureId"]): r for r in out}
    assert by_key[("a", "f1")]["numEvents"] == 5
    assert by_key[("a", "f1")]["numMinutes"] == 1
    assert by_key[("b", "f1")]["numEvents"] == 1
    assert "numMinutes" not in by_key[("b", "f1")]


def test_merge_guide_event_rows_counts_types():
    raw = [
        {"visitorId": "v", "guideId": "g", "type": "guideSeen"},
        {"visitorId": "v", "guideId": "g", "type": "guideSeen", "numEvents": 2},
    ]
    out = _merge_guide_event_rows(raw)
    assert len(out) == 1
    assert out[0]["numEvents"] == 3
