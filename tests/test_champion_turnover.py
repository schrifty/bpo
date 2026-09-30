"""Champion and economic-buyer turnover from Salesforce contact changes."""

from datetime import date
from pathlib import Path

import pytest

from src.healthscore_web.champion_turnover import (
    ChampionTurnoverGeneratorError,
    departure_points,
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
    return {row["entity_id"]: row for row in _run(contacts, history, sponsors=sponsors, entities=entities, as_of=as_of, persist=persist)["readings"]}


def _run(
    contacts: list[dict],
    history: list[dict] | None = None,
    *,
    sponsors: list[dict] | None = None,
    entities: list[dict] | None = None,
    as_of: date = AS_OF,
    persist: bool = False,
) -> dict:
    return get_champion_turnover(
        entities=entities or ENTITIES,
        sponsor_rows=sponsors if sponsors is not None else SPONSORS,
        contacts=contacts,
        history=history or [],
        as_of=as_of,
        persist=persist,
    )


def _flags(contacts: list[dict], history: list[dict] | None = None, **kwargs) -> dict:
    return {row["entity_id"]: row for row in _run(contacts, history, **kwargs)["flag_readings"]}


def test_departure_points_are_minus_five_or_five() -> None:
    assert departure_points([]) == 5
    assert departure_points([{"kind": "title_change"}]) == 5
    assert departure_points([{"kind": "role_started"}]) == 5
    assert departure_points([{"kind": "left_account"}]) == -5
    assert departure_points([{"kind": "job_started"}]) == -5


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


def test_stable_buyer_scores_five() -> None:
    by_id = _score([_contact("003-buyer", SITE, user_role="Buyer,Engineer")])
    assert by_id[SITE]["points"] == 5
    assert by_id[SITE]["value"] == 0
    assert by_id[SITE]["error"] is None
    assert by_id[SITE]["period_key"] == "2026-W40"
    assert by_id[SITE]["meta"]["window_start"] == "2026-06-29"
    assert by_id[SITE]["meta"]["window_end"] == "2026-09-29"
    assert by_id[SITE]["meta"]["tracked_contacts"] == 2


def test_cosmetic_title_edit_stays_at_five() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        [_history("003-buyer", "Title", "Director, Logistics", "Director of Logistics")],
    )
    assert by_id[SITE]["points"] == 5
    assert by_id[SITE]["meta"]["events"] == []


def test_title_change_does_not_count_as_leaving() -> None:
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
    assert by_id[SITE]["points"] == 5
    assert by_id[SITE]["value"] == 0
    assert by_id[SITE]["meta"]["departures"] == []
    assert by_id[OTHER]["points"] == 5


def test_departure_records_the_contact_name() -> None:
    contact = _contact("003-buyer", OTHER, user_role="Buyer")
    contact["Name"] = "Alex Morgan"
    by_id = _score([contact], [_history("003-buyer", "Account", SITE, OTHER)])
    assert by_id[SITE]["meta"]["departures"][0]["name"] == "Alex Morgan"


def test_sponsor_first_name_fills_in_when_the_contact_has_no_name() -> None:
    contact = _contact("003000000000010AAA", OTHER, user_role="Buyer")
    left = _score(
        [contact],
        [_history("003000000000010AAA", "Account", SITE, OTHER)],
        sponsors=[{"Id": SITE, "Executive_Sponsor__c": "003000000000010AAA", "Executive_Sponsor_First_Name__c": "Riley"}],
    )
    assert left[SITE]["meta"]["departures"][0]["name"] == "Riley"


def test_buyer_who_left_the_account_scores_minus_five_once() -> None:
    by_id = _score(
        [_contact("003-buyer", OTHER, user_role="Buyer")],
        [
            _history("003-buyer", "Account", SITE, OTHER),
            _history("003-buyer", "Account", "Site A", "Other"),
        ],
    )
    assert by_id[SITE]["points"] == -5
    assert by_id[SITE]["value"] == 1
    assert by_id[SITE]["meta"]["departures"] == [
        {
            "contact_id": "003-buyer",
            "kind": "left_account",
            "roles": ["champion"],
            "detail": "moved to a different Account",
        }
    ]
    assert by_id[OTHER]["points"] == 5


def test_move_onto_the_account_is_not_a_departure() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        [_history("003-buyer", "Account", "Somewhere Else", "Site A")],
    )
    assert by_id[SITE]["points"] == 5


def test_job_start_scores_minus_five_and_role_start_does_not() -> None:
    by_id = _score(
        [
            _contact("003-decision", SITE, buying="Decision Maker", job_started="2026-09-28"),
            _contact("003-champion", OTHER, buying="Champion", role_started="2026-09-29"),
        ]
    )
    assert by_id[SITE]["points"] == -5
    assert by_id[SITE]["meta"]["departures"][0]["kind"] == "job_started"
    assert by_id[OTHER]["points"] == 5


def test_departure_before_the_three_month_window_scores_five() -> None:
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer", job_started="2026-06-28")],
        [
            _history(
                "003-buyer",
                "Account",
                SITE,
                OTHER,
                created="2026-06-28T14:00:00.000+0000",
            )
        ],
    )
    assert by_id[SITE]["points"] == 5
    assert by_id[SITE]["meta"]["departures"] == []


def test_sponsor_who_left_scores_minus_five_without_a_buying_role() -> None:
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
    assert by_id[SITE]["points"] == -5
    assert by_id[SITE]["meta"]["departures"][0]["roles"] == ["champion"]
    assert by_id[OTHER]["points"] == 5


def test_end_user_and_buyers_plural_score_five() -> None:
    by_id = _score(
        [
            _contact("003-end", SITE, buying="End User", user_role="Planner"),
            _contact("003-plural", OTHER, user_role="Buyers"),
        ],
        sponsors=[{"Id": SITE}, {"Id": OTHER}],
    )
    assert by_id[SITE]["points"] == 5
    assert by_id[SITE]["error"] is None
    assert by_id[OTHER]["points"] == 5


def test_named_sponsor_without_a_contact_scores_five() -> None:
    by_id = _score(
        [],
        sponsors=[
            {"Id": SITE, "Executive_Sponsor_Email__c": "sponsor@example.com"},
            {"Id": OTHER},
        ],
    )
    assert by_id[SITE]["points"] == 5
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


def test_departure_flag_follows_the_same_signal() -> None:
    left = _flags(
        [_contact("003-buyer", OTHER, user_role="Buyer")],
        [_history("003-buyer", "Account", SITE, OTHER)],
    )
    assert left[SITE]["metric_name"] == "champion_departure_external"
    assert left[SITE]["value"] == 1
    assert left[SITE]["points"] is None
    assert left[SITE]["tags"] == ["customer-success", "healthscore", "override"]
    assert left[SITE]["meta"]["departures"][0]["kind"] == "left_account"
    assert left[OTHER]["value"] == 0

    titled = _flags(
        [_contact("003-champion", SITE, buying="Technical Buyer")],
        [_history("003-champion", "Title", "Chief Operating Officer", "President")],
    )
    assert titled[SITE]["value"] == 0

    nobody = _flags([], sponsors=[{"Id": SITE}, {"Id": OTHER}])
    assert nobody[SITE]["value"] == 0

    missing = _flags([], sponsors=[{"Id": OTHER}])
    assert missing[SITE]["value"] is None


def test_persist_writes_the_weekly_reading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    by_id = _score(
        [_contact("003-buyer", SITE, user_role="Buyer")],
        entities=[ENTITIES[0]],
        sponsors=[SPONSORS[0]],
        persist=True,
    )
    assert by_id[SITE]["points"] == 5
    conn = connect()
    try:
        stored = observations_for_metric(conn, SITE, "champion_turnover")
        flag = observations_for_metric(conn, SITE, "champion_departure_external")
    finally:
        conn.close()
    assert stored[-1]["points"] == 5
    assert stored[-1]["period_key"] == "2026-W40"
    assert flag[-1]["value"] == 0
    assert flag[-1]["points"] is None
    assert flag[-1]["period_key"] == "2026-W40"
