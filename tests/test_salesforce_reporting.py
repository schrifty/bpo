"""Tests for Salesforce-first corporate reporting groups."""

from src.salesforce_reporting import (
    aggregate_accounts_by_corporate_group,
    aggregate_accounts_by_ultimate_parent,
    entity_account_corporate_group,
    entity_account_ultimate_parent_group,
    invalidate_salesforce_reporting_cache,
    resolve_corporate_label,
)


def setup_function() -> None:
    invalidate_salesforce_reporting_cache()


def _acct(*, name: str, parent: str = "", arr: float = 0, ult: str = "") -> dict:
    return {
        "Id": f"id-{name[:8]}",
        "Name": name,
        "parent_name": parent,
        "ultimate_parent_name": ult,
        "ARR__c": arr,
        "Contract_Status__c": "Active",
    }


def test_safran_divisions_roll_up_to_corporate_safran():
    accounts = [
        _acct(name="Site A", parent="Safran Cabin", arr=100_000),
        _acct(name="Site B", parent="Safran Electrical & Power", arr=200_000),
    ]
    groups = aggregate_accounts_by_corporate_group(accounts)
    assert set(groups) == {"Safran"}
    assert sum(float(a["ARR__c"]) for a in groups["Safran"]) == 300_000


def test_johnson_controls_divisions_roll_up():
    accounts = [
        _acct(name="JCI Plant", parent="Johnson Controls - Chillers", arr=50_000),
        _acct(name="JCI Plant 2", parent="Johnson Controls Fire Suppression", arr=60_000),
    ]
    groups = aggregate_accounts_by_corporate_group(accounts)
    assert set(groups) == {"Johnson Controls"}


def test_standalone_entity_uses_division_name():
    a = _acct(name="Acme Division East", arr=10_000)
    assert entity_account_corporate_group(a) == "Acme Division East"


def test_resolve_corporate_label_maps_jci():
    assert resolve_corporate_label("JCI") == "Johnson Controls"


def test_ultimate_parent_from_name_parenthetical_when_lookup_blank():
    a = _acct(name="Commercial HVAC (Carrier)", arr=100_000)
    assert entity_account_ultimate_parent_group(a) == "Carrier"


def test_ultimate_parent_collapses_divisions_sharing_parenthetical_parent():
    accounts = [
        _acct(name="Commercial HVAC (Carrier)", arr=100_000),
        _acct(name="Residential HVAC (Carrier)", arr=250_000),
        _acct(name="Safran", arr=50_000),
    ]
    groups = aggregate_accounts_by_ultimate_parent(accounts)
    assert set(groups) == {"Carrier", "Safran"}
    assert sum(float(a["ARR__c"]) for a in groups["Carrier"]) == 350_000


def test_ultimate_parent_prefers_explicit_lookup_value():
    a = _acct(name="Commercial HVAC (Carrier)", ult="Carrier Global", arr=1.0)
    assert entity_account_ultimate_parent_group(a) == "Carrier Global"


def test_ultimate_parent_parenthetical_resolves_alias():
    a = _acct(name="Fire Suppression (JCI)", arr=1.0)
    assert entity_account_ultimate_parent_group(a) == "Johnson Controls"


def test_ultimate_parent_falls_back_to_corporate_group():
    a = _acct(name="Standalone Plant", parent="Safran Cabin", arr=1.0)
    assert entity_account_ultimate_parent_group(a) == "Safran"
