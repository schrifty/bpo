"""Tests for persistent/historical Drive export naming (no network)."""

from __future__ import annotations

import datetime as dt

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


def test_is_allowed_export_base_subfolder_rejects_monthly_bucket_at_base() -> None:
    from src.export_drive_layout import is_allowed_export_base_subfolder

    assert not is_allowed_export_base_subfolder("2026-06", portfolio_root=True)
    assert not is_allowed_export_base_subfolder("2026-06", portfolio_root=False)
    assert is_allowed_export_base_subfolder("Historical Data", portfolio_root=True)


def test_portfolio_deck_persistent_title_matches_export_pattern() -> None:
    from src.export_drive_layout import portfolio_deck_persistent_title

    assert portfolio_deck_persistent_title("engineering-portfolio") == (
        "Portfolio - Engineering Review-persistent"
    )
