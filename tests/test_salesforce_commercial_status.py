"""Tests for Salesforce commercial_status and ARR rollup fields."""

from __future__ import annotations

import datetime

from src.salesforce_commercial_status import (
    COMMERCIAL_STATUS_ACTIVE,
    COMMERCIAL_STATUS_CHURNED,
    COMMERCIAL_STATUS_FUTURE,
    COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
    derive_commercial_status,
    entity_has_future_contract,
    is_current_book_commercial_status,
    rollup_arr_fields,
)


def test_derive_commercial_status_active_when_any_entity_active():
    matching = [
        {"Contract_Status__c": "Activated", "ARR__c": 100},
        {"Contract_Status__c": "Expired", "ARR__c": 50},
    ]
    assert (
        derive_commercial_status(matching, renewal_in_flight=False)
        == COMMERCIAL_STATUS_ACTIVE
    )


def test_derive_commercial_status_renewal_before_churn():
    matching = [{"Contract_Status__c": "Expired", "ARR__c": 100}]
    assert (
        derive_commercial_status(matching, renewal_in_flight=True)
        == COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING
    )


def test_derive_commercial_status_future_from_start_date():
    future = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    matching = [{"Contract_Status__c": "Expired", "Contract_Contract_Start_Date__c": future, "ARR__c": 80}]
    assert (
        derive_commercial_status(matching, renewal_in_flight=False)
        == COMMERCIAL_STATUS_FUTURE
    )
    assert entity_has_future_contract(matching[0])


def test_derive_commercial_status_active_when_expired_status_but_contract_in_term():
    """SF often marks Contract_Status__c Expired while end date is still in the future."""
    future_end = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    matching = [{"Contract_Status__c": "Expired", "Contract_Contract_End_Date__c": future_end, "ARR__c": 50}]
    assert (
        derive_commercial_status(matching, renewal_in_flight=False)
        == COMMERCIAL_STATUS_ACTIVE
    )
    fields = rollup_arr_fields(matching, commercial_status=COMMERCIAL_STATUS_ACTIVE)
    assert fields["active_arr"] == 50.0
    assert fields["current_arr"] == 50.0


def test_derive_commercial_status_churned_default():
    matching = [{"Contract_Status__c": "Churned", "ARR__c": 25}]
    assert (
        derive_commercial_status(matching, renewal_in_flight=False)
        == COMMERCIAL_STATUS_CHURNED
    )


def test_derive_commercial_status_active_when_signed_renewal_closed_won():
    matching = [
        {
            "Contract_Status__c": "Expired",
            "Contract_Contract_End_Date__c": "2026-04-10",
            "ARR__c": 105_000,
        }
    ]
    assert (
        derive_commercial_status(
            matching,
            renewal_in_flight=False,
            signed_renewal_closed_won=True,
        )
        == COMMERCIAL_STATUS_ACTIVE
    )


def test_rollup_arr_fields_current_includes_renewal():
    matching = [{"Contract_Status__c": "Expired", "ARR__c": 100_000}]
    fields = rollup_arr_fields(
        matching, commercial_status=COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING
    )
    assert fields["active_arr"] == 0.0
    assert fields["renewal_arr"] == 100_000.0
    assert fields["current_arr"] == 100_000.0
    assert fields["historical_arr"] == 100_000.0


def test_is_current_book_commercial_status():
    assert is_current_book_commercial_status(COMMERCIAL_STATUS_ACTIVE)
    assert is_current_book_commercial_status(COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING)
    assert not is_current_book_commercial_status(COMMERCIAL_STATUS_CHURNED)
    assert not is_current_book_commercial_status(COMMERCIAL_STATUS_FUTURE)
