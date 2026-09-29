"""Champion and economic-buyer turnover from Salesforce contact changes."""

from datetime import date
from pathlib import Path

import pytest

from src.healthscore_web.champion_turnover import (
    ChampionTurnoverGeneratorError,
    champion_turnover_points,
    get_champion_turnover,
    title_changed,
)
from src.healthscore_web.store import connect, observations_for_metric

AS_OF = date(2026, 9, 29)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
ENTITIES = [
    {"id": SITE, "name": "Site A"},
    {"id": OTHER, "name": "Other"},
]
SPONSORS = [
    {"Id": SITE, "Executive_Sponsor__c": "003000000000010AAA"},
    {"Id": OTHER},
]


def _contact(
    contact_id: str,
    account_id: str,
    *,
    buying: str = "",
    user_role: str = "",
    job_started: str = "",
    role_started: str = "",
) -> dict:
    return {
        "Id": contact_id,
        "AccountId": account_id,
        "Buyer_Role_HubSpot_Sync__c": buying,
        "User_Role__c": user_role,
        "UserGem__JobStartedDate__c": job_started,
        "UserGem__RoleStartedDate__c": role_started,
    }


def _history(
    contact_id: str,
    field: str,
    old: str | None,
    new: str | None,
    created: str = "2026-09-29T14:00:00.000+0000",
) -> dict:
    return {
        "ContactId": contact_id,
        "Field": field,
        "OldValue": old,
        "NewValue": new,
        "CreatedDate": created,
    }


def _score(
    contacts: list[dict],
    history: list[dict] | None = None,
    *,
    sponsors: list[dict] | None = None,
    entities: list[dict] | None = None,
    as_of: date = AS_OF,
    persist: bool = False,
) -> dict:
    result = get_champion_turnover(
        entities=entities or ENTITIES,
        sponsor_rows=sponsors if sponsors is not None else SPONSORS,
        contacts=contacts,
        history=history or [],
        as_of=as_of,
        persist=persist,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_points_follow_tracked_and_turnover_flags() -> None:
    assert champion_turnover_points(tracked=False, turned_over=False) is None
    assert champion_turnover_points(tracked=True, turned_over=False) == 4
    assert champion_turnover_points(tracked=True, turned_over=True) == 0


def test_title_normalization_ignores_punctuation_and_word_order() -> None:
    assert not title_changed("Director, Logistics", "Director of Logistics")
    assert not title_changed(
        "Vice President and General Manager - USA and Canada",
        "Vice President & General Manager - USA/Canada",
    )
    assert not title_changed("Manager, Procurement Operations", "Procurement Operations Manager")
    assert not title_changed(None, "IS Solution Architect")
    assert title_changed("Chief Operating Officer", "President")
    assert title_changed("Vice President, Operations", "Senior VP, Global Operations")


def test_stable_buyer_scores_full_points() -> None:
    by_id = _score([_contact("003-buyer", SITE, user_role="Buyer,Engineer")])
    assert by_id[SITE]["points"] == 4
    assert by_id[SITE]["value"] == 0
    assert by_id[SITE]["error"] is None
    assert by_id[SITE]["period_key"] == "2026-W40"
    assert by_id[SITE]["meta"]["week_start"] == "2026-09-28"
    assert by_id[SITE]["meta"]["week_end"] == "2026-09-29"
    assert by_id[SITE]["meta"]["tracked_contacts"] == 2


def test_cosmetic_title_edit_does_not_score_turnover() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        [_history("003-buyer", "Title", "Director, Logistics", "Director of Logistics")],
    )
    assert by_id[SITE]["points"] == 4
    assert by_id[SITE]["meta"]["events"] == []


def test_real_title_change_scores_zero_for_champion_and_economic_buyer() -> None:
    by_id = _score(
        [
            _contact("003-champion", SITE, buying="Technical Buyer"),
            _contact("003-budget", OTHER, buying="Budget Holder"),
        ],
        [
            _history("003-champion", "Title", "Chief Operating Officer", "President"),
            _history("003-budget", "Title", "Director of Finance", "Chief Financial Officer"),
        ],
    )
    assert by_id[SITE]["points"] == 0
    assert by_id[SITE]["value"] == 1
    assert by_id[SITE]["meta"]["events"][0]["kind"] == "title_change"
    assert by_id[SITE]["meta"]["events"][0]["roles"] == ["champion"]
    assert by_id[OTHER]["points"] == 0
    assert by_id[OTHER]["meta"]["events"][0]["roles"] == ["economic_buyer"]


def test_buyer_who_left_the_account_scores_zero_once() -> None:
    by_id = _score(
        [_contact("003-buyer", OTHER, user_role="Buyer")],
        [
            _history("003-buyer", "Account", SITE, OTHER),
            _history("003-buyer", "Account", "Site A", "Other"),
        ],
    )
    assert by_id[SITE]["points"] == 0
    assert by_id[SITE]["meta"]["events"] == [
        {
            "contact_id": "003-buyer",
            "kind": "left_account",
            "roles": ["champion"],
            "detail": "moved to a different Account",
        }
    ]
    assert by_id[OTHER]["points"] == 4


def test_move_onto_the_account_is_not_turnover() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        [_history("003-buyer", "Account", "Somewhere Else", "Site A")],
    )
    assert by_id[SITE]["points"] == 4


def test_job_and_role_start_dates_in_the_week_score_zero() -> None:
    by_id = _score(
        [
            _contact("003-decision", SITE, buying="Decision Maker", job_started="2026-09-28"),
            _contact("003-champion", OTHER, buying="Champion", role_started="2026-09-29"),
        ]
    )
    assert by_id[SITE]["points"] == 0
    assert by_id[SITE]["meta"]["events"][0]["kind"] == "job_started"
    assert by_id[OTHER]["points"] == 0
    assert by_id[OTHER]["meta"]["events"][0]["kind"] == "role_started"


def test_role_change_outside_the_week_is_ignored() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer", job_started="2026-09-21")],
        [
            _history(
                "003-buyer",
                "Title",
                "Chief Operating Officer",
                "President",
                created="2026-09-21T14:00:00.000+0000",
            )
        ],
    )
    assert by_id[SITE]["points"] == 4
    assert by_id[SITE]["meta"]["events"] == []


def test_sponsor_change_counts_without_a_buying_role() -> None:
    by_id = _score(
        [],
        [
            _history(
                "003000000000010AAA",
                "Account",
                "Parent Co",
                "Somewhere Else",
            )
        ],
    )
    assert by_id[SITE]["points"] == 0
    assert by_id[SITE]["meta"]["events"][0]["roles"] == ["champion"]
    assert by_id[OTHER]["points"] is None


def test_end_user_and_buyers_plural_stay_unscored() -> None:
    by_id = _score(
        [
            _contact("003-end", SITE, buying="End User", user_role="Planner"),
            _contact("003-plural", OTHER, user_role="Buyers"),
        ],
        sponsors=[{"Id": SITE}, {"Id": OTHER}],
    )
    assert by_id[SITE]["points"] is None
    assert "no primary champion or economic buyer" in by_id[SITE]["error"]
    assert by_id[OTHER]["points"] is None


def test_named_sponsor_without_a_contact_stays_unscored() -> None:
    by_id = _score(
        [],
        sponsors=[
            {"Id": SITE, "Executive_Sponsor_Email__c": "sponsor@example.com"},
            {"Id": OTHER},
        ],
    )
    assert by_id[SITE]["points"] is None
    assert by_id[SITE]["meta"]["tracked_contacts"] == 0


def test_blank_history_timestamp_fails_loud() -> None:
    with pytest.raises(ChampionTurnoverGeneratorError, match="CreatedDate is blank"):
        _score(
            [_contact("003-buyer", SITE, user_role="Buyer")],
            [_history("003-buyer", "Title", "Buyer", "Director", created="")],
        )


def test_empty_sponsor_rows_fail_loud() -> None:
    with pytest.raises(ChampionTurnoverGeneratorError, match="no Customer Entity"):
        get_champion_turnover(
            entities=ENTITIES,
            sponsor_rows=[],
            contacts=[],
            history=[],
            as_of=AS_OF,
        )


def test_empty_entity_inventory_fails_loud() -> None:
    with pytest.raises(ChampionTurnoverGeneratorError, match="inventory is empty"):
        get_champion_turnover(
            entities=[],
            sponsor_rows=SPONSORS,
            contacts=[],
            history=[],
            as_of=AS_OF,
        )


def test_persist_writes_the_weekly_reading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        entities=[ENTITIES[0]],
        sponsors=[SPONSORS[0]],
        persist=True,
    )
    assert by_id[SITE]["points"] == 4
    conn = connect()
    try:
        stored = observations_for_metric(conn, SITE, "champion_turnover")
    finally:
        conn.close()
    assert stored[-1]["points"] == 4
    assert stored[-1]["period_key"] == "2026-W40"
