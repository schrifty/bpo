"""Meeting cadence adherence scoring from Chorus engagements."""

from datetime import date

import pytest

from src.healthscore_web.meeting_cadence import (
    MeetingCadenceGeneratorError,
    get_meeting_cadence,
    held_percent,
    meeting_cadence_points,
    window_start,
)

AS_OF = date(2026, 9, 28)
SITE_A = "001000000000001AAA"
SITE_B = "001000000000002AAA"
OTHER = "001000000000003AAA"
PARENT = "001000000000099AAA"
ENTITIES = [
    {"id": SITE_A, "name": "Site A"},
    {"id": SITE_B, "name": "Site B"},
    {"id": OTHER, "name": "Other"},
]
PARENTS = {SITE_A: PARENT, SITE_B: PARENT, OTHER: "001000000000088AAA"}
USERS = [
    {"id": 1, "role": "CSM"},
    {"id": 2, "role": "Rep (AE/Other)"},
    {"id": 3, "role": "CS Director"},
]


def _meeting(
    engagement_id: str,
    *,
    owner: int = 1,
    account: str = PARENT,
    kind: str = "meeting",
    no_show: bool = False,
    subject: str = "Weekly check-in",
    when: str = "2026-06-01",
) -> dict:
    return {
        "engagement_id": engagement_id,
        "user_id": owner,
        "account_id": account,
        "engagement_type": kind,
        "no_show": no_show,
        "subject": subject,
        "date_time": when,
    }


def _score(engagements: list[dict]) -> dict[str, dict]:
    result = get_meeting_cadence(
        entities=ENTITIES,
        engagements=engagements,
        users=USERS,
        parent_ids=PARENTS,
        as_of=AS_OF,
        persist=False,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_window_is_two_trailing_quarters():
    assert window_start(date(2026, 9, 28)) == date(2026, 3, 28)
    assert window_start(date(2024, 3, 31)) == date(2023, 9, 30)


@pytest.mark.parametrize(
    ("pct", "points"),
    [(None, None), (80, 2), (80.0, 2), (100, 2), (79.9, 0), (0, 0)],
)
def test_points_follow_the_eighty_percent_line(pct, points):
    assert meeting_cadence_points(pct) == points


def test_percent_is_unscored_when_nothing_was_scheduled():
    assert held_percent(0, 0) is None


def test_eighty_percent_held_scores_two_for_every_child_entity():
    rows = _score(
        [
            _meeting("h1"),
            _meeting("h2"),
            _meeting("h3"),
            _meeting("h4"),
            _meeting("miss", no_show=True),
            _meeting("sales", owner=2, subject="Pipeline review"),
            _meeting("mail", kind="email", no_show=True, subject="Follow up"),
        ]
    )
    for entity_id in (SITE_A, SITE_B):
        assert rows[entity_id]["value"] == 80.0
        assert rows[entity_id]["points"] == 2
        assert rows[entity_id]["meta"]["scheduled"] == 5
        assert rows[entity_id]["meta"]["held"] == 4
        assert rows[entity_id]["error"] is None
    assert rows[OTHER]["points"] is None
    assert rows[OTHER]["value"] is None


def test_below_eighty_percent_scores_zero():
    rows = _score(
        [
            _meeting("h1"),
            _meeting("h2"),
            _meeting("h3"),
            _meeting("m1", no_show=True),
            _meeting("m2", kind="unrecorded_meeting", no_show=True),
        ]
    )
    assert rows[SITE_A]["value"] == 60.0
    assert rows[SITE_A]["points"] == 0
    assert rows[SITE_A]["meta"]["scheduled"] == 5
    assert rows[SITE_A]["meta"]["held"] == 3


def test_qbr_owned_by_sales_counts_and_direct_account_stays_on_that_entity():
    rows = _score(
        [
            _meeting("qbr", owner=2, account=OTHER, subject="Q3 QBR"),
            _meeting("ae", owner=2, account=OTHER, subject="Discovery"),
        ]
    )
    assert rows[OTHER]["points"] == 2
    assert rows[OTHER]["meta"] == {
        "held": 1,
        "scheduled": 1,
        "held_pct": 100.0,
        "window_start": "2026-03-28",
        "window_end": "2026-09-28",
        "target_pct": 80.0,
        "owner": "lindsay.brown@leandna.com",
    }
    assert rows[SITE_A]["points"] is None


def test_cs_director_and_duplicate_ids_and_dates_outside_the_window():
    rows = _score(
        [
            _meeting("once", owner=3, account=OTHER),
            _meeting("once", owner=3, account=OTHER),
            _meeting("early", account=OTHER, when="2026-03-27"),
            _meeting("late", account=OTHER, when="2026-09-29"),
            _meeting("edge", account=OTHER, when="2026-03-28"),
        ]
    )
    assert rows[OTHER]["meta"]["scheduled"] == 2
    assert rows[OTHER]["meta"]["held"] == 2
    assert rows[OTHER]["points"] == 2


def test_eighteen_character_account_matches_the_fifteen_character_key():
    rows = _score([_meeting("h1", account=PARENT)])
    assert rows[SITE_A]["meta"]["scheduled"] == 1


def test_missing_no_show_fails_loud():
    broken = _meeting("h1")
    del broken["no_show"]
    with pytest.raises(MeetingCadenceGeneratorError, match="no_show"):
        _score([broken])


def test_no_csm_or_qbr_meetings_fails_loud():
    with pytest.raises(MeetingCadenceGeneratorError, match="no CSM or QBR"):
        _score([_meeting("sales", owner=2, subject="Pipeline review")])


def test_empty_entity_inventory_fails_loud():
    with pytest.raises(MeetingCadenceGeneratorError, match="inventory is empty"):
        get_meeting_cadence(
            entities=[],
            engagements=[_meeting("h1")],
            users=USERS,
            parent_ids={},
            as_of=AS_OF,
        )
