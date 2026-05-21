"""Unit tests for cross-system company name resolution."""

from __future__ import annotations

from src.match_companies import resolve_pendo_name


def test_resolve_pendo_uses_sf_alias_file(monkeypatch):
    prefixes = frozenset({"JCI", "Spirit"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {"johnson": ["JCI"]},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: "JCI" if sf_label == "Johnson" else None,
    )

    name, notes = resolve_pendo_name("Johnson", prefixes)
    assert name == "JCI"
    assert notes and "sf_portfolio_pendo_aliases" in notes[0]


def test_resolve_pendo_direct_prefix(monkeypatch):
    prefixes = frozenset({"Safran"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: None,
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist.resolve_sf_label_to_pendo_prefix",
        lambda sf_label, pendo_prefixes: None,
    )

    name, notes = resolve_pendo_name("Safran", prefixes)
    assert name == "Safran"
    assert notes == []
