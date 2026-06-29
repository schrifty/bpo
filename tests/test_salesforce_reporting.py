"""Tests for Salesforce-first corporate reporting groups."""

from src.salesforce_reporting import (
    aggregate_accounts_by_corporate_group,
    entity_account_corporate_group,
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
