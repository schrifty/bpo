"""Product idea submissions from Pendo feedback polls."""

from datetime import date, datetime, timezone

import pytest

from src.healthscore_web.product_ideas import (
    ProductIdeasGeneratorError,
    credited_entity_ids,
    get_product_ideas,
    product_idea_points,
    submission_tone,
)

AS_OF = date(2026, 9, 30)
GUIDE = "guide-feedback"
ENTITIES = [
    {"id": "E1", "name": "Johnson Controls - Wichita", "parent_name": "Johnson Controls Corp"},
    {"id": "E2", "name": "Johnson Controls - Durango", "parent_name": "Johnson Controls Corp"},
    {"id": "E3", "name": "AGI : Albion", "parent_name": "AGI"},
    {"id": "E4", "name": "Carrier UTEC US", "parent_name": "Carrier UTEC US (FSP)"},
    {"id": "E5", "name": "Carrier Chattanooga Entity Account", "parent_name": "Carrier Chattanooga"},
]


def _millis(day: str) -> int:
    moment = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def _event(event_id: str, *, account: str, day: str, response: int) -> dict:
    return {
        "eventId": event_id,
        "guideId": GUIDE,
        "accountId": account,
        "pollType": "PositiveNegative",
        "pollResponse": response,
        "browserTime": _millis(day),
    }


def test_points_are_two_for_any_submission_and_zero_for_none():
    assert product_idea_points(0) == 0
    assert product_idea_points(1) == 2
    assert product_idea_points(4) == 2
    with pytest.raises(ProductIdeasGeneratorError):
        product_idea_points(-1)


def test_positive_negative_tone():
    assert submission_tone("PositiveNegative", 1) == "positive"
    assert submission_tone("PositiveNegative", 0) == "negative"
    assert submission_tone("NPSReason", "ship faster") == "other"


def test_parent_match_credits_every_entity_and_ambiguous_carrier_does_not():
    assert credited_entity_ids("Johnson Controls", ENTITIES) == ["E1", "E2"]
    assert credited_entity_ids("AGI", ENTITIES) == ["E3"]
    assert credited_entity_ids("Carrier", ENTITIES) == []


def test_trailing_year_counts_tone_and_scores_sites_under_the_parent():
    result = get_product_ideas(
        entities=ENTITIES,
        guide_ids=[GUIDE],
        account_names={"120": "Johnson Controls", "9": "Carrier"},
        events=[
            _event("a", account="120", day="2026-05-20T00:00:00", response=1),
            _event("b", account="120", day="2026-05-21T00:00:00", response=0),
            _event("old", account="120", day="2025-01-01T00:00:00", response=1),
            _event("c", account="9", day="2026-06-01T00:00:00", response=1),
            {
                "guideId": GUIDE,
                "accountId": "120",
                "visitorId": "v1",
                "pollId": "poll",
                "pollType": "PositiveNegative",
                "pollResponse": 1,
                "browserTime": _millis("2026-05-22T00:00:00"),
            },
        ],
        as_of=AS_OF,
    )
    by_id = {row["entity_id"]: row for row in result["readings"]}
    assert by_id["E1"]["points"] == 2
    assert by_id["E1"]["value"] == 3
    assert by_id["E1"]["meta"]["positive"] == 2
    assert by_id["E1"]["meta"]["negative"] == 1
    assert by_id["E2"]["points"] == 2
    assert by_id["E3"]["points"] == 0
    assert by_id["E4"]["points"] == 0
    assert any("Carrier" in warning for warning in result["warnings"])
