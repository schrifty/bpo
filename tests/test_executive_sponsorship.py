"""Executive sponsorship strength from Salesforce activity and Chorus calls."""

from datetime import date

import pytest

from src.healthscore_web.executive_sponsorship import (
    ExecutiveSponsorshipGeneratorError,
    get_executive_sponsorship,
    is_senior_title,
    sponsorship_points,
)
from src.healthscore_web.framework import load_framework

AS_OF = date(2026, 9, 30)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
PARENT = "001000000000099AAA"
ENTITIES = [{"id": SITE, "name": "Site"}, {"id": OTHER, "name": "Other"}]


def _contact(contact_id: str, account: str, title: str, email: str = "") -> dict:
    return {"Id": contact_id, "AccountId": account, "Title": title, "Email": email}


def _task(who: str, day: str) -> dict:
    return {"Id": f"00T{day.replace('-', '')}", "WhoId": who, "ActivityDate": day}


def _call(
    engagement_id: str,
    *,
    account: str,
    when: str,
    participants: list[dict],
) -> dict:
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
    result = get_executive_sponsorship(entities=ENTITIES, as_of=AS_OF, persist=False, **kwargs)
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_points_the_generator_at_salesforce_and_chorus() -> None:
    row = next(item for item in load_framework()["inputs"] if item["key"] == "executive_sponsorship")
    assert row["metric-generator"] == "get_executive_sponsorship"
    assert row["automation"] == "automated"
    assert row["status"] == "defined"
    assert row["max_points"] == 4
    assert row["data_source"] == ["Salesforce", "Chorus"]


@pytest.mark.parametrize(
    ("title", "senior"),
    [
        ("Senior Director of Operations", True),
        ("Sr. Director, Supply Chain", True),
        ("Director of Operations", False),
        ("VP of Manufacturing", True),
        ("Chief Executive Officer", True),
        ("CEO", True),
        ("Executive Assistant to the CEO", False),
        ("Assistant to the Vice President", False),
        ("", False),
    ],
)
def test_senior_titles(title, senior):
    assert is_senior_title(title) is senior


@pytest.mark.parametrize(
    ("senior_count", "touches", "days", "points"),
    [
        (0, 0, None, None),
        (1, 0, None, 0),
        (1, 1, 10, 2),
        (1, 2, 10, 4),
        (1, 3, 91, 1),
        (0, 1, 5, 2),
    ],
)
def test_points(senior_count, touches, days, points):
    assert sponsorship_points(senior_count=senior_count, touch_count=touches, days_since=days) == points


def test_one_recent_call_scores_two():
    rows = _score(
        contacts=[_contact("003000000000001AAA", SITE, "Senior Director", "vp@site.example")],
        tasks=[],
        events=[],
        engagements=[
            _call(
                "e1",
                account=SITE,
                when="2026-09-01",
                participants=[{"type": "prospect", "title": "Senior Director", "email": "vp@site.example", "name": "Ada"}],
            )
        ],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["value"] == 1
    assert rows[SITE]["meta"]["days_since_last"] == 29
    assert rows[OTHER]["points"] is None


def test_task_and_call_on_the_same_day_count_once():
    rows = _score(
        contacts=[_contact("003000000000001AAA", SITE, "Vice President", "vp@site.example")],
        tasks=[_task("003000000000001AAA", "2026-09-01")],
        events=[],
        engagements=[
            _call(
                "e1",
                account=SITE,
                when="2026-09-01",
                participants=[{"type": "prospect", "title": "Vice President", "email": "vp@site.example", "name": "Ada"}],
            )
        ],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["meta"]["touches"] == 1


def test_two_recent_touches_score_four_and_an_old_touch_scores_one():
    recent = _score(
        contacts=[_contact("003000000000001AAA", SITE, "CEO", "a@site.example")],
        tasks=[_task("003000000000001AAA", "2026-09-01"), _task("003000000000001AAA", "2026-09-15")],
        events=[],
        engagements=[],
    )
    assert recent[SITE]["points"] == 4
    older = _score(
        contacts=[_contact("003000000000001AAA", SITE, "CEO", "a@site.example")],
        tasks=[_task("003000000000001AAA", "2026-06-01")],
        events=[],
        engagements=[],
    )
    assert older[SITE]["points"] == 1


def test_director_rep_and_parent_call_rules():
    rows = _score(
        contacts=[
            _contact("003000000000001AAA", SITE, "Director", "dir@site.example"),
            _contact("003000000000002AAA", SITE, "SVP Operations", "svp@site.example"),
        ],
        tasks=[_task("003000000000001AAA", "2026-09-01")],
        events=[],
        engagements=[
            _call(
                "rep",
                account=SITE,
                when="2026-09-02",
                participants=[{"type": "rep", "title": "VP Customer Success", "email": "rep@leandna.com", "name": "Rep"}],
            ),
            _call(
                "parent",
                account=PARENT,
                when="2026-09-03",
                participants=[{"type": "prospect", "title": "SVP Operations", "email": "svp@site.example", "name": "Bea"}],
            ),
        ],
    )
    assert rows[SITE]["points"] == 2
    assert rows[SITE]["meta"]["touches"] == 1
    assert rows[SITE]["meta"]["senior_contacts"] == 1
    assert rows[OTHER]["points"] is None


def test_blank_activity_date_fails_loud():
    with pytest.raises(ExecutiveSponsorshipGeneratorError, match="ActivityDate"):
        _score(
            contacts=[_contact("003000000000001AAA", SITE, "CEO")],
            tasks=[{"Id": "00T1", "WhoId": "003000000000001AAA", "ActivityDate": None}],
            events=[],
            engagements=[],
        )


def test_empty_inventory_fails_loud():
    with pytest.raises(ExecutiveSponsorshipGeneratorError, match="inventory is empty"):
        get_executive_sponsorship(entities=[], contacts=[], tasks=[], events=[], engagements=[])
