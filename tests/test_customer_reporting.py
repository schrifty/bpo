"""Tests for customer reporting group rollups (SF corporate labels)."""

import logging

from src import customer_reporting
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


def test_alias_conflict_warns_and_keeps_first(caplog):
    out: dict[str, str] = {}
    customer_reporting._add_mapping(out, "Shared Alias", "Corp A")
    with caplog.at_level(logging.WARNING, logger=customer_reporting.logger.name):
        customer_reporting._add_mapping(out, "Shared Alias", "Corp B")
    assert out["shared alias"] == "Corp A"
    assert any("maps to both" in rec.getMessage() for rec in caplog.records)


def test_unreadable_alias_yaml_warns_and_returns_empty_map(monkeypatch, tmp_path, caplog):
    bad = tmp_path / "aliases.yaml"
    bad.write_text("corp: [unclosed")
    monkeypatch.setattr(customer_reporting, "_CSR_ALIASES_FILE", bad)
    invalidate_customer_reporting_cache()
    with caplog.at_level(logging.WARNING, logger=customer_reporting.logger.name):
        mapping = customer_reporting._load_cs_to_corporate_map()
    assert mapping == {}
    assert any("could not load" in rec.getMessage() for rec in caplog.records)
