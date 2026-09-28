"""Enhancement Engagement & Delivery scoring and Aha-to-entity attribution."""

from datetime import date

import pytest

from src.healthscore_web.enhancement_engagement import (
    EnhancementEngagementGeneratorError,
    enhancement_engagement_points,
    get_enhancement_engagement,
)

AS_OF = date(2026, 9, 28)
ENTITIES = [
    {"id": "E1", "name": "Site A"},
    {"id": "E2", "name": "Site B"},
    {"id": "E3", "name": "Other"},
]
PARENTS = {"E1": "PARENT1", "E2": "PARENT1", "E3": "PARENT2"}


def _org(domain: str, account_id: str) -> dict:
    return {
        "email_domains": domain,
        "integration_fields": [
            {"service_name": "salesforce", "name": "Id", "value": account_id}
        ],
    }


def _idea(
    idea_id: str,
    *,
    email: str | None,
    status: str,
    created_at: str = "2026-03-01T12:00:00.000Z",
) -> dict:
    idea = {
        "id": idea_id,
        "reference_num": idea_id,
        "created_at": created_at,
        "workflow_status": {"name": status, "complete": status == "Shipped"},
    }
    if email is not None:
        idea["created_by_portal_user"] = {"email": email}
    return idea


def _score(ideas: list[dict], organizations: list[dict] | None = None) -> dict[str, dict]:
    result = get_enhancement_engagement(
        entities=ENTITIES,
        ideas=ideas,
        organizations=organizations
        if organizations is not None
        else [_org("acme.com", "PARENT1")],
        parent_ids=PARENTS,
        as_of=AS_OF,
        persist=False,
    )
    return {row["entity_id"]: row for row in result["readings"]}


@pytest.mark.parametrize(
    ("requests", "delivered", "expected"),
    [
        (0, 0, None),
        (1, 0, 0.6),
        (1, 1, 7.6),
        (3, 1, 4.1),
        (5, 2, 5.8),
        (5, 4, 8.6),
        (5, 5, 10.0),
        (10, 3, 5.1),
        (10, 8, 8.6),
    ],
)
def test_enhancement_engagement_points_match_the_examples(requests, delivered, expected):
    assert enhancement_engagement_points(requests, delivered) == expected


def test_delivered_count_must_be_a_subset_of_requests():
    with pytest.raises(EnhancementEngagementGeneratorError, match="subset"):
        enhancement_engagement_points(1, 2)


def test_parent_account_scores_every_active_child_entity():
    rows = _score([_idea("I-1", email="buyer@acme.com", status="Shipped")])
    assert rows["E1"]["points"] == 7.6
    assert rows["E2"]["points"] == 7.6
    assert rows["E1"]["meta"]["request_count"] == 1
    assert rows["E1"]["meta"]["delivered_count"] == 1
    assert rows["E3"]["points"] is None


def test_direct_customer_entity_link_does_not_fan_out_to_siblings():
    rows = _score(
        [_idea("I-1", email="buyer@other.com", status="Needs review")],
        [_org("other.com", "E3")],
    )
    assert rows["E3"]["points"] == 0.6
    assert rows["E1"]["points"] is None
    assert rows["E2"]["points"] is None


def test_shipped_is_the_only_delivered_status():
    rows = _score(
        [
            _idea("I-1", email="buyer@acme.com", status="Shipped"),
            _idea("I-2", email="buyer@acme.com", status="Already exists"),
            _idea("I-3", email="buyer@acme.com", status="Will not implement"),
            _idea("I-4", email="buyer@acme.com", status="Needs review"),
        ]
    )
    assert rows["E1"]["meta"]["request_count"] == 4
    assert rows["E1"]["meta"]["delivered_count"] == 1
    assert rows["E1"]["points"] == 4.2


def test_same_idea_is_counted_once_when_two_organizations_match():
    rows = _score(
        [_idea("I-1", email="buyer@acme.com", status="Shipped")],
        [_org("acme.com", "PARENT1"), _org("acme.com", "PARENT1")],
    )
    assert rows["E1"]["meta"]["request_count"] == 1
    assert rows["E1"]["points"] == 7.6


def test_ideas_outside_the_trailing_year_and_internal_submitters_are_excluded():
    rows = _score(
        [
            _idea(
                "I-old",
                email="buyer@acme.com",
                status="Shipped",
                created_at="2025-09-27T00:00:00.000Z",
            ),
            _idea("I-staff", email="pm@leandna.com", status="Shipped"),
            _idea("I-blank", email=None, status="Shipped"),
        ]
    )
    assert rows["E1"]["points"] is None
    assert rows["E1"]["meta"]["request_count"] == 0


def test_unmatched_customer_domain_warns_and_stays_unscored():
    result = get_enhancement_engagement(
        entities=ENTITIES,
        ideas=[_idea("I-1", email="buyer@unknown.example", status="Shipped")],
        organizations=[_org("acme.com", "PARENT1")],
        parent_ids=PARENTS,
        as_of=AS_OF,
        persist=False,
    )
    assert all(row["points"] is None for row in result["readings"])
    assert any("unknown.example" in warning["warning"] for warning in result["warnings"])
