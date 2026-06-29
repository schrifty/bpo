"""SF portfolio label matching for Ford / Ford Motor Company."""

from __future__ import annotations

from src.portfolio_salesforce_allowlist import (
    matching_entity_accounts_for_customer_label,
    _entity_rows_for_portfolio_label,
)


def test_ford_pendo_label_resolves_to_ford_motor_company_entities():
    accounts = [
        {
            "Id": "1",
            "Name": "Ford Motor Co/Windsor",
            "parent_name": "Ford Motor Company",
            "ultimate_parent_name": "",
            "Contract_Status__c": "Expired",
        },
        {
            "Id": "2",
            "Name": "HarcoSemco - Branford",
            "parent_name": "HarcoSemco",
            "ultimate_parent_name": "",
            "Contract_Status__c": "Activated",
        },
    ]
    rows = matching_entity_accounts_for_customer_label("Ford", accounts)
    assert len(rows) == 1
    assert rows[0]["Name"] == "Ford Motor Co/Windsor"
    assert _entity_rows_for_portfolio_label("Ford Motor Company", accounts) == rows
