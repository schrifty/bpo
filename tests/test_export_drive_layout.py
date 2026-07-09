"""Tests for persistent/historical Drive export naming (no network)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.export_drive_layout import (
    historical_day_folder_label,
    historical_snapshot_filename,
    historical_snapshot_spreadsheet_title,
    is_legacy_dated_output_folder,
    is_legacy_export_container_folder,
    parse_historical_flat_dated_name,
    persistent_filename,
    persistent_spreadsheet_title,
    target_historical_snapshot_name,
    target_persistent_name,
)


def test_persistent_filename_appends_suffix_before_extension() -> None:
    assert persistent_filename("Pendo Export  (Ford, 30d)", ext=".md") == (
        "Pendo Export  (Ford, 30d)-persistent.md"
    )


def test_historical_snapshot_filename_has_no_date_or_persistent_suffix() -> None:
    assert historical_snapshot_filename("Pendo Export  (Ford, 30d)", ext=".md") == (
        "Pendo Export  (Ford, 30d).md"
    )


def test_historical_day_folder_label() -> None:
    assert historical_day_folder_label(dt.date(2026, 7, 7)) == "2026-07-07"


def test_spreadsheet_titles_match_markdown_pattern() -> None:
    stem = "Pendo Export  (Carrier, 30d)"
    assert persistent_spreadsheet_title(stem) == f"{stem}-persistent"
    assert historical_snapshot_spreadsheet_title(stem) == stem


def test_target_persistent_name_from_legacy_pendo_markdown() -> None:
    assert target_persistent_name("Pendo Export  (Ford, 30d).md", mime_type="text/markdown") == (
        "Pendo Export  (Ford, 30d)-persistent.md"
    )


def test_target_historical_snapshot_name_strips_persistent_and_date() -> None:
    assert target_historical_snapshot_name(
        "Pendo Export  (Ford, 30d)-persistent.md",
        mime_type="text/markdown",
    ) == "Pendo Export  (Ford, 30d).md"
    assert target_historical_snapshot_name(
        "Pendo Export  (Ford, 30d) 2026-06-15.md",
        mime_type="text/markdown",
    ) == "Pendo Export  (Ford, 30d).md"


def test_parse_historical_flat_dated_name() -> None:
    parsed = parse_historical_flat_dated_name("Pendo Export  (Ford, 30d) 2026-06-15.md")
    assert parsed == ("Pendo Export  (Ford, 30d)", dt.date(2026, 6, 15), ".md")


def test_is_legacy_dated_output_folder_skips_today() -> None:
    today = dt.date(2026, 7, 7)
    assert not is_legacy_dated_output_folder("2026-07-07 - Output", today=today)
    assert is_legacy_dated_output_folder("2026-07-06 - Output", today=today)


def test_is_legacy_export_container_folder_at_customer_includes_today() -> None:
    assert is_legacy_export_container_folder("2026-07-07 - Output", include_todays_dated=True)


def test_is_allowed_export_base_subfolder_allows_todays_dated_output_folder() -> None:
    from datetime import date

    from src.export_drive_layout import is_allowed_export_base_subfolder

    today_name = f"{date.today().isoformat()} - Output"
    assert is_allowed_export_base_subfolder(today_name, portfolio_root=True)
    assert not is_allowed_export_base_subfolder("2020-01-01 - Output", portfolio_root=True)


def test_is_allowed_export_base_subfolder_rejects_monthly_bucket_at_base() -> None:
    from src.export_drive_layout import is_allowed_export_base_subfolder

    assert not is_allowed_export_base_subfolder("2026-06", portfolio_root=True)
    assert not is_allowed_export_base_subfolder("2026-06", portfolio_root=False)
    assert is_allowed_export_base_subfolder("Historical Data", portfolio_root=True)


def test_portfolio_deck_persistent_title_matches_export_pattern() -> None:
    from src.export_drive_layout import portfolio_deck_persistent_title

    assert portfolio_deck_persistent_title("engineering-portfolio") == (
        "Engineering-Review-Portfolio-persistent"
    )


def test_ensure_customer_exports_parent_folder_returns_existing(monkeypatch) -> None:
    from src.export_drive_layout import CUSTOMER_EXPORTS_FOLDER, ensure_customer_exports_parent_folder

    monkeypatch.setattr(
        "src.drive_config.find_file_in_folder",
        lambda name, pid, **kwargs: "new-id" if name == CUSTOMER_EXPORTS_FOLDER else None,
    )
    created: list[str] = []
    monkeypatch.setattr(
        "src.drive_config._find_or_create_folder",
        lambda name, pid: created.append(name) or "created-id",
    )
    monkeypatch.setattr("src.drive_config.rename_drive_file", lambda *_a, **_k: pytest.fail("unexpected"))

    assert ensure_customer_exports_parent_folder("output-root") == "new-id"
    assert created == []


def test_ensure_customer_exports_parent_folder_renames_legacy(monkeypatch) -> None:
    from src.export_drive_layout import (
        CUSTOMER_EXPORTS_FOLDER,
        _LEGACY_CUSTOMER_EXPORTS_FOLDER,
        ensure_customer_exports_parent_folder,
    )

    def fake_find(name, pid, **kwargs):
        if name == CUSTOMER_EXPORTS_FOLDER:
            return None
        if name == _LEGACY_CUSTOMER_EXPORTS_FOLDER:
            return "legacy-id"
        return None

    monkeypatch.setattr("src.drive_config.find_file_in_folder", fake_find)
    renames: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "src.drive_config.rename_drive_file",
        lambda fid, new_name: renames.append((fid, new_name)),
    )
    monkeypatch.setattr(
        "src.drive_config._find_or_create_folder",
        lambda *_a, **_k: pytest.fail("unexpected"),
    )

    assert ensure_customer_exports_parent_folder("output-root") == "legacy-id"
    assert renames == [("legacy-id", CUSTOMER_EXPORTS_FOLDER)]


def test_ensure_customer_exports_parent_folder_creates_when_missing(monkeypatch) -> None:
    from src.export_drive_layout import CUSTOMER_EXPORTS_FOLDER, ensure_customer_exports_parent_folder

    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.drive_config.rename_drive_file", lambda *_a, **_k: pytest.fail("unexpected"))
    monkeypatch.setattr(
        "src.drive_config._find_or_create_folder",
        lambda name, pid: "created-id" if name == CUSTOMER_EXPORTS_FOLDER and pid == "output-root" else pytest.fail("unexpected"),
    )

    assert ensure_customer_exports_parent_folder("output-root") == "created-id"
