"""Unit tests for Pendo portfolio Drive snapshot helpers (no network)."""

from __future__ import annotations

import datetime

import pytest

import src.pendo_portfolio_snapshot_drive as pendo_portfolio_snapshot_drive

from src.pendo_portfolio_snapshot_drive import (
    parse_portfolio_snapshot_envelope,
    portfolio_snapshot_filename,
    saved_at_to_calendar_date,
)


def test_portfolio_snapshot_filename() -> None:
    assert portfolio_snapshot_filename(30, None) == "portfolio_snapshot_v1_days30_all.json"
    assert portfolio_snapshot_filename(90, 5) == "portfolio_snapshot_v1_days90_max5.json"


def test_parse_envelope_ok() -> None:
    report = {"type": "portfolio", "days": 30, "customer_count": 2, "customers": []}
    env = {
        "schema_version": 1,
        "saved_at": "2026-03-31T12:00:00+00:00",
        "days": 30,
        "max_customers": None,
        "report": report,
    }
    out = parse_portfolio_snapshot_envelope(env, expect_days=30, expect_max_customers=None)
    assert out == report


def test_parse_envelope_wrong_days() -> None:
    report = {"type": "portfolio", "days": 30, "customers": []}
    env = {
        "schema_version": 1,
        "saved_at": "2026-03-31T12:00:00+00:00",
        "days": 30,
        "max_customers": None,
        "report": report,
    }
    assert parse_portfolio_snapshot_envelope(env, expect_days=60, expect_max_customers=None) is None


def test_parse_envelope_max_customers_mismatch() -> None:
    report = {"type": "portfolio", "days": 30, "customers": []}
    env = {
        "schema_version": 1,
        "saved_at": "2026-03-31T12:00:00+00:00",
        "days": 30,
        "max_customers": 10,
        "report": report,
    }
    assert parse_portfolio_snapshot_envelope(env, expect_days=30, expect_max_customers=None) is None
    assert parse_portfolio_snapshot_envelope(env, expect_days=30, expect_max_customers=10) == report


def test_resolve_portfolio_snapshot_folder_id_explicit_override(monkeypatch: pytest.MonkeyPatch) -> None:
    pendo_portfolio_snapshot_drive._resolved_generator_cache_folder_id = pendo_portfolio_snapshot_drive._UNRESOLVED
    monkeypatch.setattr(pendo_portfolio_snapshot_drive, "BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID", "folder_explicit")
    monkeypatch.setattr(pendo_portfolio_snapshot_drive, "GOOGLE_QBR_GENERATOR_FOLDER_ID", "folder_gen")
    assert pendo_portfolio_snapshot_drive.resolve_portfolio_snapshot_folder_id() == "folder_explicit"


def test_saved_at_to_calendar_date_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pendo_portfolio_snapshot_drive, "BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ", "UTC")
    d = saved_at_to_calendar_date("2026-03-31T18:00:00+00:00")
    assert d == datetime.date(2026, 3, 31)


def test_resolve_portfolio_snapshot_folder_id_none_without_config(monkeypatch: pytest.MonkeyPatch) -> None:
    pendo_portfolio_snapshot_drive._resolved_generator_cache_folder_id = pendo_portfolio_snapshot_drive._UNRESOLVED
    monkeypatch.setattr(pendo_portfolio_snapshot_drive, "BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID", None)
    monkeypatch.setattr(pendo_portfolio_snapshot_drive, "GOOGLE_QBR_GENERATOR_FOLDER_ID", None)
    assert pendo_portfolio_snapshot_drive.resolve_portfolio_snapshot_folder_id() is None


def test_parse_envelope_report_days_mismatch() -> None:
    report = {"type": "portfolio", "days": 60, "customers": []}
    env = {
        "schema_version": 1,
        "saved_at": "2026-03-31T12:00:00+00:00",
        "days": 30,
        "max_customers": None,
        "report": report,
    }
    assert parse_portfolio_snapshot_envelope(env, expect_days=30, expect_max_customers=None) is None
