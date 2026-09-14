"""Tests for LeanDNA customer → site ID mapping."""

from __future__ import annotations

from src.leandna_site_map import resolve_customer_sites


def test_resolve_customer_sites_from_env(monkeypatch) -> None:
    monkeypatch.setenv("LEANDNA_SITES_ACME_CORP", "101,202")
    monkeypatch.setattr("src.leandna_site_map.load_teams", lambda: {})
    assert resolve_customer_sites("Acme Corp") == "101,202"


def test_resolve_customer_sites_from_teams_yaml(monkeypatch) -> None:
    monkeypatch.delenv("LEANDNA_SITES_ACME", raising=False)
    monkeypatch.setattr(
        "src.leandna_site_map.load_teams",
        lambda: {"Acme": {"leandna_site_ids": ["9", "8"]}},
    )
    assert resolve_customer_sites("acme") == "9,8"


def test_resolve_customer_sites_warns_when_unmapped(monkeypatch, caplog) -> None:
    monkeypatch.setattr("src.leandna_site_map.load_teams", lambda: {})
    import logging

    caplog.set_level(logging.WARNING)
    assert resolve_customer_sites("Unknown Customer") is None
    assert "using all authorized sites" in caplog.text
