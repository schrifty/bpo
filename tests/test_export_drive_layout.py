"""Tests for persistent/historical Drive export naming (no network)."""

from __future__ import annotations

import datetime as dt

from src.export_drive_layout import (
    historical_filename,
    historical_spreadsheet_title,
    persistent_filename,
    persistent_spreadsheet_title,
)


def test_persistent_filename_appends_suffix_before_extension() -> None:
    assert persistent_filename("Pendo Export  (Ford, 30d)", ext=".md") == (
        "Pendo Export  (Ford, 30d)-persistent.md"
    )


def test_historical_filename_includes_iso_date() -> None:
    day = dt.date(2026, 7, 7)
    assert historical_filename("LLM-Context-All_Customers", ext=".md", export_date=day) == (
        "LLM-Context-All_Customers 2026-07-07.md"
    )


def test_spreadsheet_titles_match_markdown_pattern() -> None:
    day = dt.date(2026, 7, 7)
    stem = "Pendo Export  (Carrier, 30d)"
    assert persistent_spreadsheet_title(stem) == f"{stem}-persistent"
    assert historical_spreadsheet_title(stem, export_date=day) == f"{stem} 2026-07-07"
