"""Multi-threading depth from Salesforce activity and Chorus calls."""

from datetime import date

import pytest

from src.healthscore_web.framework import load_framework
from src.healthscore_web.multithreading_depth import (
    MultithreadingDepthGeneratorError,
    get_multithreading_depth,
    is_director_title,
    multithreading_points,
)

AS_OF = date(2026, 9, 30)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
PARENT = "001000000000099AAA"
ENTITIES = [{"id": SITE, "name": "Site"}, {"id": OTHER, "name": "Other"}]


def _contact(contact_id: str, account: str, title: str, email: str = "") -> dict:
    return {"Id": contact_id, "AccountId": account, "Title": title, "Email": email}


def _task(who: str, day: str) -> dict:
    return {"Id": f"00T{day.replace('-', '')}", "WhoId": who, "ActivityDate": day}


def _call(engagement_id: str, *, account: str, when: str, participants: list[dict]) -> dict:
    return {
        "engagement_id": engagement_id,
        "account_id": account,
        "engagement_type": "meeting",
        "duration": 60,
        "processing_state": "done",
        "date_time": when,
        "participants": participants,
    }


def _score(**kwargs) -> dict[str, dict]:
    result = get_multithreading_depth(entities=ENTITIES, as_of=AS_OF, persist=False, **kwargs)
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_points_the_generator_at_salesforce_and_chorus() -> None:
    row = next(item for item in load_framework()["inputs"] if item["key"] == "multithreading_depth")
    assert row["metric-generator"] == "get_multithreading_depth"
    assert row["automation"] == "automated"
    assert row["status"] == "defined"
    assert row["max_points"] == 4
    assert row["data_source"] == ["Salesforce", "Chorus"]


@pytest.mark.parametrize(
    ("title", "director"),
    [
        ("Director of Operations", True),
        ("Senior Director of Operations", True),
        ("Sr. Director, Supply Chain", True),
        ("VP of Manufacturing", True),
        ("Chief Executive Officer", True),
        ("Manager of Planning", False),
        ("Executive Assistant to the CEO", False),
        ("Assistant to the Director", False),
        ("", False),
    ],
)
def test_director_titles(title, director):
    assert is_director_title(title) is director


@pytest.mark.parametrize(
    ("directors", "engaged", "prior", "points"),
    [
        (0, 0, 0, None),
        (2, 0, 1, 0),
        (1, 1, 1, 1),
        (1, 1, 0, 2),
        (2, 2, 2, 2),
        (2, 2, 1, 3),
        (3, 3, 4, 3),
        (3, 4, 3, 4),
    ],
)
def test_points(directors, engaged, prior, points):
    assert multithreading_points(
        director_count=directors, engaged=engaged, prior_engaged=prior
    ) == points


def test_one_new_director_scores_two():
    rows = _score(
        contacts=[_contact("003000000000001AAA", SITE, "Director of Operations", "dir@site.example")],
        tasks=[_task("003000000000001AAA", "2026-09-01")],
        events=[],
        engagements=[],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["value"] == 1
    assert rows[SITE]["meta"]["engaged"] == 1
    assert rows[SITE]["meta"]["prior_engaged"] == 0
    assert rows[SITE]["meta"]["growing"] is True
    assert rows[OTHER]["points"] is None


def test_same_person_on_a_task_and_a_call_counts_once():
    rows = _score(
        contacts=[_contact("003000000000001AAA", SITE, "Director", "dir@site.example")],
        tasks=[_task("003000000000001AAA", "2026-09-01")],
        events=[],
        engagements=[
            _call(
                "e1",
                account=SITE,
                when="2026-09-15",
                participants=[{"type": "prospect", "title": "Director", "email": "dir@site.example", "name": "Ada"}],
            )
        ],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["meta"]["engaged"] == 1


def test_two_directors_score_three_and_a_repeat_does_not_grow():
    fresh = _score(
        contacts=[
            _contact("003000000000001AAA", SITE, "Director", "a@site.example"),
            _contact("003000000000002AAA", SITE, "VP Operations", "b@site.example"),
        ],
        tasks=[
            _task("003000000000001AAA", "2026-09-01"),
            _task("003000000000002AAA", "2026-09-10"),
        ],
        events=[],
        engagements=[],
    )
    assert fresh[SITE]["points"] == 3
    repeat = _score(
        contacts=[
            _contact("003000000000001AAA", SITE, "Director", "a@site.example"),
            _contact("003000000000002AAA", SITE, "VP Operations", "b@site.example"),
        ],
        tasks=[
            _task("003000000000001AAA", "2026-05-01"),
            _task("003000000000002AAA", "2026-05-10"),
            _task("003000000000001AAA", "2026-09-01"),
            _task("003000000000002AAA", "2026-09-10"),
        ],
        events=[],
        engagements=[],
    )
    assert repeat[SITE]["points"] == 2
    assert repeat[SITE]["meta"]["growing"] is False


def test_manager_rep_and_parent_call_rules():
    rows = _score(
        contacts=[
            _contact("003000000000001AAA", SITE, "Manager", "mgr@site.example"),
            _contact("003000000000002AAA", SITE, "Director", "dir@site.example"),
        ],
        tasks=[_task("003000000000001AAA", "2026-09-01")],
        events=[],
        engagements=[
            _call(
                "rep",
                account=SITE,
                when="2026-09-02",
                participants=[{"type": "rep", "title": "Director", "email": "rep@leandna.com", "name": "Rep"}],
            ),
            _call(
                "parent",
                account=PARENT,
                when="2026-09-03",
                participants=[{"type": "prospect", "title": "Director", "email": "dir@site.example", "name": "Bea"}],
            ),
        ],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["meta"]["engaged"] == 1
    assert rows[SITE]["meta"]["director_contacts"] == 1
    assert rows[OTHER]["points"] is None


def test_blank_activity_date_fails_loud():
    with pytest.raises(MultithreadingDepthGeneratorError, match="ActivityDate"):
        _score(
            contacts=[_contact("003000000000001AAA", SITE, "Director")],
            tasks=[{"Id": "00T1", "WhoId": "003000000000001AAA", "ActivityDate": None}],
            events=[],
            engagements=[],
        )


def test_empty_inventory_fails_loud():
    with pytest.raises(MultithreadingDepthGeneratorError, match="inventory is empty"):
        get_multithreading_depth(entities=[], contacts=[], tasks=[], events=[], engagements=[])
