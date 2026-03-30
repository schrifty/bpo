"""Unit tests for Feature Adoption usage-pattern narrative (Pendo half-over-half)."""

from src.pendo_client import _feature_adoption_pattern_narrative


def test_narrative_mentions_total_shift():
    cat = {"a": "Alpha", "b": "Beta"}
    ff = {"a": 200, "b": 50}
    fr = {"a": 140, "b": 20}
    text = _feature_adoption_pattern_narrative(
        days=30,
        recent_days=15,
        prior_days=15,
        feat_full=ff,
        feat_recent=fr,
        feature_catalog=cat,
    )
    assert "%" in text
    assert "feature clicks" in text.lower()
    assert "notably up" in text.lower() or "softer" in text.lower() or "similar" in text.lower()


def test_narrative_empty_when_no_events():
    assert (
        _feature_adoption_pattern_narrative(
            days=30,
            recent_days=15,
            prior_days=15,
            feat_full={},
            feat_recent={},
            feature_catalog={},
        )
        == ""
    )
