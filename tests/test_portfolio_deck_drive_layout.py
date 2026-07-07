"""Tests for engineering portfolio deck Drive placement (persistent + historical)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.export_drive_layout import (
    portfolio_deck_export_stem,
    portfolio_deck_persistent_title,
    portfolio_deck_snapshot_title,
    resolve_portfolio_deck_output,
    snapshot_presentation_to_historical_day,
    uses_portfolio_deck_export_layout,
)


def test_portfolio_deck_export_stem_engineering() -> None:
    assert portfolio_deck_export_stem("engineering-portfolio") == "Portfolio - Engineering Review"
    assert portfolio_deck_export_stem("engineering-portfolio", cursor_suffix=True) == (
        "Portfolio - Engineering Review — Cursor"
    )
    assert portfolio_deck_export_stem("portfolio_review") is None


def test_portfolio_deck_persistent_and_snapshot_titles() -> None:
    assert portfolio_deck_persistent_title("engineering-portfolio") == (
        "Portfolio - Engineering Review-persistent"
    )
    assert portfolio_deck_snapshot_title("engineering-portfolio") == "Portfolio - Engineering Review"
    assert uses_portfolio_deck_export_layout("engineering-portfolio")
    assert not uses_portfolio_deck_export_layout("implementations_review")


def test_resolve_portfolio_deck_output(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.export_drive_layout.ensure_portfolio_output_folders",
        lambda: {
            "persistent_folder_id": "output-root",
            "historical_folder_id": "historical-root",
            "base_label": "Output",
        },
    )
    out = resolve_portfolio_deck_output("engineering-portfolio")
    assert out == {
        "persistent_folder_id": "output-root",
        "historical_folder_id": "historical-root",
        "persistent_title": "Portfolio - Engineering Review-persistent",
        "snapshot_title": "Portfolio - Engineering Review",
        "base_label": "Output",
    }


def test_snapshot_presentation_to_historical_day(monkeypatch) -> None:
    copy_calls: list[dict] = []
    trashed: list[str] = []

    class _Files:
        def copy(self, **kwargs):
            copy_calls.append(kwargs)
            return self

        def execute(self):
            return {"id": "historical-pres-id"}

    class _Drive:
        def files(self):
            return _Files()

    monkeypatch.setattr(
        "src.export_drive_layout.ensure_portfolio_output_folders",
        lambda: {
            "persistent_folder_id": "output-root",
            "historical_folder_id": "historical-root",
            "base_label": "Output",
        },
    )
    monkeypatch.setattr(
        "src.export_drive_layout.ensure_historical_day_folder",
        lambda _hid, _day=None: "historical-day-folder",
    )
    monkeypatch.setattr(
        "src.drive_config.list_files_by_name_in_folder",
        lambda *_a, **_k: [{"id": "old-snapshot"}],
    )
    monkeypatch.setattr(
        "src.drive_config.trash_drive_file",
        lambda fid: trashed.append(fid),
    )
    monkeypatch.setattr(
        "src.drive_config.dedupe_duplicate_names_in_folder",
        lambda *_a, **_k: None,
    )

    out = snapshot_presentation_to_historical_day(
        presentation_id="pres-123",
        deck_id="engineering-portfolio",
        drive_service=_Drive(),
        export_date=dt.date(2026, 7, 7),
    )

    assert trashed == ["old-snapshot"]
    assert copy_calls[0]["fileId"] == "pres-123"
    assert copy_calls[0]["body"] == {
        "name": "Portfolio - Engineering Review",
        "parents": ["historical-day-folder"],
    }
    assert out["historical_file_id"] == "historical-pres-id"
    assert out["historical_day_folder"] == "2026-07-07"
    assert out["historical_filename"] == "Portfolio - Engineering Review"


def test_snapshot_presentation_requires_layout_deck() -> None:
    class _Drive:
        def files(self):
            raise AssertionError("should not call Drive")

    with pytest.raises(ValueError, match="does not use portfolio deck export layout"):
        snapshot_presentation_to_historical_day(
            presentation_id="pres-123",
            deck_id="portfolio_review",
            drive_service=_Drive(),
        )
