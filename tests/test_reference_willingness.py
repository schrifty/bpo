"""Reference willingness from Salesforce logo and press approval."""

from datetime import date

import pytest

from src.healthscore_web.reference_willingness import (
    ReferenceWillingnessGeneratorError,
    get_reference_willingness,
    reference_willingness_points,
)

AS_OF = date(2026, 9, 30)


def test_yes_scores_two_and_no_scores_zero():
    assert reference_willingness_points(True) == 2
    assert reference_willingness_points(False) == 0
    with pytest.raises(ReferenceWillingnessGeneratorError):
        reference_willingness_points("yes")  # type: ignore[arg-type]


def test_parent_approval_rolls_down_and_a_blank_account_scores_zero():
    entities = [
        {"id": "E1", "name": "Site A"},
        {"id": "E2", "name": "Site B"},
        {"id": "E3", "name": "Plain"},
    ]
    accounts = {
        "E1": {
            "Id": "E1",
            "ParentId": "PARENT",
            "Logo_Use_Approved__c": False,
            "Press_Approved__c": False,
            "Last_Marketing_Response__c": None,
        },
        "E2": {
            "Id": "E2",
            "ParentId": "PARENT",
            "Logo_Use_Approved__c": True,
            "Press_Approved__c": False,
            "Last_Marketing_Response__c": "2026-04-01",
        },
        "E3": {
            "Id": "E3",
            "ParentId": None,
            "Logo_Use_Approved__c": False,
            "Press_Approved__c": False,
            "Last_Marketing_Response__c": None,
        },
        "PARENT": {
            "Id": "PARENT",
            "ParentId": None,
            "Logo_Use_Approved__c": False,
            "Press_Approved__c": True,
            "Last_Marketing_Response__c": "2026-03-01",
        },
    }
    result = get_reference_willingness(entities=entities, accounts=accounts, as_of=AS_OF)
    by_id = {row["entity_id"]: row for row in result["readings"]}
    assert by_id["E1"]["points"] == 2
    assert by_id["E1"]["meta"]["inherited_from_parent"] is True
    assert by_id["E1"]["meta"]["press_approved"] is True
    assert by_id["E2"]["points"] == 2
    assert by_id["E2"]["meta"]["inherited_from_parent"] is False
    assert by_id["E3"]["points"] == 0
    assert result["willing"] == 2
