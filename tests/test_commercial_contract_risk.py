"""Time to renewal and contract value trend from Salesforce."""

from datetime import date

import pytest

from src.healthscore_web.contract_value_trend import (
    ContractValueTrendGeneratorError,
    contract_value_points,
    get_contract_value_trend,
)
from src.healthscore_web.framework import load_framework
from src.healthscore_web.time_to_renewal import (
    TimeToRenewalGeneratorError,
    auto_renew_status,
    get_time_to_renewal,
    renewal_points,
)

AS_OF = date(2026, 9, 30)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
PARENT = "001000000000099AAA"
ENTITIES = [
    {"id": SITE, "name": "Site"},
    {"id": OTHER, "name": "Other"},
]


def _renewal_rows(**kwargs) -> dict[str, dict]:
    result = get_time_to_renewal(entities=ENTITIES, as_of=AS_OF, persist=False, **kwargs)
    return {row["entity_id"]: row for row in result["readings"]}


def _value_rows(**kwargs) -> dict[str, dict]:
    result = get_contract_value_trend(
        entities=kwargs.pop("entities", ENTITIES),
        as_of=AS_OF,
        persist=False,
        **kwargs,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def _opp(opp_id: str, account: str, arr: float, close_date: str, opp_type: str = "Renewal") -> dict:
    return {
        "Id": opp_id,
        "AccountId": account,
        "ARR__c": arr,
        "CloseDate": close_date,
        "Type": opp_type,
    }


def test_framework_wires_both_generators() -> None:
    inputs = {row["key"]: row for row in load_framework()["inputs"]}
    renewal = inputs["time_to_renewal"]
    assert renewal["metric-generator"] == "get_time_to_renewal"
    assert renewal["automation"] == "automated"
    assert renewal["status"] == "defined"
    assert renewal["grain"] == "weekly"
    assert renewal["max_points"] == 3
    assert renewal["data_source"] == ["Salesforce"]
    trend = inputs["contract_value_trend"]
    assert trend["metric-generator"] == "get_contract_value_trend"
    assert trend["automation"] == "automated"
    assert trend["grain"] == "monthly"
    assert trend["max_points"] == 5
    payment = inputs["payment_delinquency"]
    assert payment["metric-generator"] is None
    assert payment["automation"] == "blocked"


@pytest.mark.parametrize(
    ("terms", "auto"),
    [
        ("Auto-renewal unless cancelled within 90 days", True),
        ("Auto-renewal with uplift", True),
        ("Customer confirmation required for renewal", False),
        ("Custom", None),
        ("", None),
        (None, None),
    ],
)
def test_auto_renew_terms(terms, auto):
    assert auto_renew_status(terms) is auto


@pytest.mark.parametrize(
    ("days", "auto", "points"),
    [
        (90, True, 0),
        (180, True, 0),
        (89, True, 3),
        (181, True, 3),
        (0, False, 0),
        (120, False, 0),
        (121, False, 3),
        (-1, False, 3),
        (None, True, None),
        (30, None, None),
    ],
)
def test_renewal_window_points(days, auto, points):
    assert renewal_points(days, auto) == points


def test_auto_renew_inside_the_window_scores_zero():
    rows = _renewal_rows(
        contracts={
            SITE: {"end_date": "2026-12-29", "renewal_terms": "Auto-renewal unless cancelled within 90 days"},
            OTHER: {"end_date": "2027-06-30", "renewal_terms": "Customer confirmation required for renewal"},
        }
    )
    assert rows[SITE]["points"] == 0
    assert rows[SITE]["value"] == 90
    assert rows[OTHER]["points"] == 3


def test_custom_terms_stay_unscored():
    rows = _renewal_rows(
        contracts={
            SITE: {"end_date": "2027-01-01", "renewal_terms": "Custom"},
            OTHER: {"end_date": None, "renewal_terms": "Auto-renewal with uplift"},
        }
    )
    assert rows[SITE]["points"] is None
    assert "do not say" in rows[SITE]["error"]
    assert rows[OTHER]["points"] is None
    assert "end date" in rows[OTHER]["error"]


def test_empty_renewal_inventory_fails():
    with pytest.raises(TimeToRenewalGeneratorError, match="empty"):
        get_time_to_renewal(entities=[], contracts={SITE: {}})
    with pytest.raises(TimeToRenewalGeneratorError, match="no contract"):
        get_time_to_renewal(entities=ENTITIES, contracts={})


@pytest.mark.parametrize(
    ("change", "points"),
    [
        (5, 5),
        (4.9, 4),
        (4, 4),
        (3, 3),
        (2, 2),
        (1.9, 1),
        (0, 1),
        (-0.01, 0),
        (None, None),
    ],
)
def test_contract_value_bands(change, points):
    assert contract_value_points(change) == points


def test_five_percent_growth_scores_five():
    rows = _value_rows(
        parents={},
        opportunities=[
            _opp("006A", SITE, 100, "2025-09-30", "New Business"),
            _opp("006B", SITE, 105, "2026-09-15", "Renewal"),
        ],
    )
    assert rows[SITE]["value"] == 5
    assert rows[SITE]["points"] == 5
    assert rows[OTHER]["points"] is None


def test_contraction_scores_zero_and_flat_scores_one():
    rows = _value_rows(
        parents={},
        opportunities=[
            _opp("006A", SITE, 100, "2025-09-01", "New Business"),
            _opp("006B", SITE, 99, "2026-09-10", "Invoice Only Renewal"),
            _opp("006C", OTHER, 50, "2025-01-01", "New Business"),
            _opp("006D", OTHER, 50, "2026-09-20", "Renewal"),
        ],
    )
    assert rows[SITE]["value"] == -1
    assert rows[SITE]["points"] == 0
    assert rows[OTHER]["value"] == 0
    assert rows[OTHER]["points"] == 1


def test_a_renewal_in_another_month_is_ignored():
    rows = _value_rows(
        parents={},
        opportunities=[
            _opp("006A", SITE, 100, "2025-08-01", "New Business"),
            _opp("006B", SITE, 110, "2026-08-15", "Renewal"),
        ],
    )
    assert rows[SITE]["points"] is None
    assert "No Salesforce renewal" in rows[SITE]["error"]


def test_latest_renewal_in_the_month_is_used():
    rows = _value_rows(
        parents={},
        opportunities=[
            _opp("006A", SITE, 100, "2025-01-01", "New Business"),
            _opp("006B", SITE, 110, "2026-09-05", "Renewal"),
            _opp("006C", SITE, 100, "2026-09-20", "Renewal"),
        ],
    )
    assert rows[SITE]["value"] == -9.09
    assert rows[SITE]["points"] == 0
    assert rows[SITE]["meta"]["opportunity_id"] == "006C"


def test_single_child_parent_renewal_credits_that_entity():
    rows = _value_rows(
        entities=[{"id": SITE, "name": "Site"}],
        parents={SITE: PARENT},
        opportunities=[
            _opp("006A", PARENT, 80, "2025-06-01", "New Business"),
            _opp("006B", PARENT, 84, "2026-09-12", "Renewal"),
        ],
    )
    assert rows[SITE]["value"] == 5
    assert rows[SITE]["points"] == 5


def test_shared_parent_renewal_does_not_fan_out():
    rows = _value_rows(
        parents={SITE: PARENT, OTHER: PARENT},
        opportunities=[
            _opp("006A", PARENT, 80, "2025-06-01", "New Business"),
            _opp("006B", PARENT, 90, "2026-09-12", "Renewal"),
        ],
    )
    assert rows[SITE]["points"] is None
    assert rows[OTHER]["points"] is None


def test_renewal_without_a_prior_term_stays_unscored():
    rows = _value_rows(
        parents={},
        opportunities=[_opp("006B", SITE, 105, "2026-09-15", "Renewal")],
    )
    assert rows[SITE]["points"] is None
    assert "prior-term" in rows[SITE]["error"]


def test_empty_contract_value_inventory_fails():
    with pytest.raises(ContractValueTrendGeneratorError, match="empty"):
        get_contract_value_trend(entities=[], opportunities=[], parents={})
    with pytest.raises(ContractValueTrendGeneratorError, match="no won"):
        get_contract_value_trend(entities=ENTITIES, opportunities=[], parents={})
