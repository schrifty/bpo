"""Unit tests for Pendo frustration row merge helper."""

from src.pendo_client import _FRUSTRATION_FIELDS, _merge_visitor_event_rows_with_frustration


def test_merge_sums_frustration_across_buckets():
    raw = [
        {
            "visitorId": "v1",
            "pageId": "p1",
            "numEvents": 2,
            "numMinutes": 1,
            "rageClickCount": 1,
            "deadClickCount": 0,
            "errorClickCount": 1,
            "uTurnCount": 0,
        },
        {
            "visitorId": "v1",
            "pageId": "p1",
            "numEvents": 1,
            "numMinutes": 0,
            "rageClickCount": 2,
            "deadClickCount": 1,
            "errorClickCount": 0,
            "uTurnCount": 1,
        },
    ]
    merged = _merge_visitor_event_rows_with_frustration(raw, "pageId")
    assert len(merged) == 1
    row = merged[0]
    assert row["numEvents"] == 3
    assert row["rageClickCount"] == 3
    assert row["deadClickCount"] == 1
    assert row["errorClickCount"] == 1
    assert row["uTurnCount"] == 1
    for k in _FRUSTRATION_FIELDS:
        assert k in row
