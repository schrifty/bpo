"""Use case / anchor expansion from Pendo premium-module usage."""

from datetime import date

import pytest

from src.healthscore_web.framework import load_framework
from src.healthscore_web.premium_anchor_expansion import (
    PremiumAnchorGeneratorError,
    accounts_for_entity,
    get_premium_anchor_expansion,
    is_premium_name,
    premium_points,
)

AS_OF = date(2026, 4, 30)
SITE = "001000000000001AAA"
OTHER = "001000000000002AAA"
ENTITIES = [
    {"id": SITE, "name": "Acme Charlotte", "parent_name": "Acme"},
    {"id": OTHER, "name": "Other Plant", "parent_name": "Other"},
]
ACCOUNTS = [
    {"id": "acme", "name": "Acme"},
    {"id": "other", "name": "Other Plant"},
]


def _score(**kwargs) -> dict[str, dict]:
    result = get_premium_anchor_expansion(
        entities=ENTITIES,
        accounts=kwargs.pop("accounts", ACCOUNTS),
        events=kwargs.pop("events", {}),
        as_of=AS_OF,
        persist=False,
        **kwargs,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_points_the_generator_at_pendo() -> None:
    row = next(item for item in load_framework()["inputs"] if item["key"] == "premium_anchor_expansion")
    assert row["metric-generator"] == "get_premium_anchor_expansion"
    assert row["automation"] == "automated"
    assert row["status"] == "defined"
    assert row["max_points"] == 3
    assert row["data_source"] == ["Pendo"]
    assert row["grain"] == "monthly"


@pytest.mark.parametrize(
    ("name", "premium"),
    [
        ("Inventory Optimization (IOP) dashboard", True),
        ("AIOP forecast", True),
        ("DSM workbench", True),
        ("Kei AI nav", False),
        ("Supply Chain Analytics", False),
        ("", False),
    ],
)
def test_premium_module_names(name, premium):
    assert is_premium_name(name) is premium


@pytest.mark.parametrize(("events", "points"), [(0, 0), (1, 3), (40, 3)])
def test_any_use_scores_three(events, points):
    assert premium_points(events) == points


def test_parent_account_with_usage_scores_three():
    rows = _score(events={"acme": {(2026, 4): 12}})
    assert rows[SITE]["points"] == 3
    assert rows[SITE]["value"] == 12
    assert rows[OTHER]["points"] == 0
    assert rows[OTHER]["value"] == 0


def test_usage_in_another_month_scores_zero():
    rows = _score(events={"acme": {(2026, 3): 9}})
    assert rows[SITE]["points"] == 0
    assert rows[SITE]["value"] == 0


def test_matching_accounts_are_summed():
    entity = {"id": SITE, "name": "Acme Charlotte", "parent_name": "Acme"}
    accounts = [{"id": "parent", "name": "Acme"}, {"id": "site", "name": "Acme Charlotte"}]
    assert accounts_for_entity(entity, accounts) == ["parent", "site"]
    rows = _score(
        accounts=accounts,
        events={"parent": {(2026, 4): 2}, "site": {(2026, 4): 5}},
    )
    assert rows[SITE]["value"] == 7
    assert rows[SITE]["points"] == 3


def test_entity_without_a_pendo_account_stays_unscored():
    rows = _score(accounts=[{"id": "acme", "name": "Acme"}], events={})
    assert rows[SITE]["points"] == 0
    assert rows[SITE]["value"] == 0
    assert rows[OTHER]["points"] is None
    assert rows[OTHER]["value"] is None
    assert "No Pendo account" in rows[OTHER]["error"]


def test_empty_inventory_fails():
    with pytest.raises(PremiumAnchorGeneratorError, match="empty"):
        get_premium_anchor_expansion(entities=[], accounts=ACCOUNTS, events={})


def test_pendo_with_no_accounts_fails():
    with pytest.raises(PremiumAnchorGeneratorError, match="no accounts"):
        get_premium_anchor_expansion(entities=ENTITIES, accounts=[], events={})
