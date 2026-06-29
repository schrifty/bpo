"""Tests for customer reporting group rollups (SF corporate labels)."""

from src.customer_reporting import (
    build_reporting_group_index,
    invalidate_customer_reporting_cache,
    reporting_group,
)


def setup_function() -> None:
    invalidate_customer_reporting_cache()


def test_safran_cs_report_business_units_roll_up():
    assert reporting_group("Safran Cabin and Seats") == "Safran"
    assert reporting_group("Safran Electrical and Power") == "Safran"


def test_jci_cs_report_names_roll_up():
    assert reporting_group("Johnson Controls") == "Johnson Controls"
    assert reporting_group("JCI Sandbox") == "Johnson Controls"


def test_unmapped_customer_unchanged():
    assert reporting_group("Bombardier") == "Bombardier"


def test_build_reporting_group_index_includes_safran():
    idx = build_reporting_group_index()
    assert "Safran" in idx
    assert "safran cabin and seats" in {n.lower() for n in idx["Safran"]}
