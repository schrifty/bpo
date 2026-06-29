"""Single-deck loads must not walk the full Drive decks folder."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.deck_loader import load_deck


def test_load_deck_does_not_call_load_yaml_from_drive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Drive-enabled runs fetch one deck file, not the entire decks/ catalog."""
    decks_dir = tmp_path / "decks"
    decks_dir.mkdir()
    (decks_dir / "support-kpis.yaml").write_text(
        "id: support-kpis\nname: Support KPIs\nslides: []\n",
        encoding="utf-8",
    )
    full_drive_load = MagicMock(return_value=[])
    single_drive_load = MagicMock(
        return_value={"id": "support-kpis", "name": "Support KPIs", "slides": [], "_source": "drive"}
    )
    monkeypatch.setattr("src.deck_loader._USE_DRIVE", True)
    monkeypatch.setattr("src.drive_config.load_yaml_from_drive", full_drive_load)
    monkeypatch.setattr("src.drive_config.load_deck_yaml_from_drive", single_drive_load)

    got = load_deck("support-kpis", decks_dir=decks_dir)

    assert got is not None
    assert got["id"] == "support-kpis"
    assert got["_source"] == "local"
    full_drive_load.assert_not_called()
    single_drive_load.assert_not_called()


def test_load_deck_from_drive_only_uses_single_file_helper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    decks_dir = tmp_path / "decks"
    decks_dir.mkdir()
    full_drive_load = MagicMock(return_value=[{"id": "other-deck"}])
    single_drive_load = MagicMock(
        return_value={"id": "support-kpis", "name": "Support KPIs", "slides": [], "_source": "drive"}
    )
    monkeypatch.setattr("src.deck_loader._USE_DRIVE", True)
    monkeypatch.setattr("src.deck_loader.DEFAULT_DECKS_DIR", decks_dir)
    monkeypatch.setattr("src.drive_config.load_yaml_from_drive", full_drive_load)
    monkeypatch.setattr("src.drive_config.load_deck_yaml_from_drive", single_drive_load)

    got = load_deck("support-kpis")

    assert got is not None
    assert got["_source"] == "drive"
    full_drive_load.assert_not_called()
    single_drive_load.assert_called_once_with("support-kpis", decks_dir)


def test_load_deck_yaml_from_drive_uses_find_file_in_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src import drive_config as dc

    decks_dir = tmp_path / "decks"
    decks_dir.mkdir()
    list_all = MagicMock(return_value=[{"name": "cohort-review.yaml", "id": "x"}])
    find_one = MagicMock(return_value="file-id-support-kpis")
    read_file = MagicMock(return_value="id: support-kpis\nname: KPIs\nslides: []\n")
    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", "folder-root")
    monkeypatch.setattr(dc, "ensure_drive_config_matches_repo", lambda: None)
    monkeypatch.setattr(dc, "_get_config_folder_ids", lambda: ("", "decks-folder", ""))
    monkeypatch.setattr(dc, "_list_drive_files", list_all)
    monkeypatch.setattr(dc, "find_file_in_folder", find_one)
    monkeypatch.setattr(dc, "_read_drive_file", read_file)

    got = dc.load_deck_yaml_from_drive("support-kpis", decks_dir)

    assert got is not None
    assert got["id"] == "support-kpis"
    list_all.assert_not_called()
    find_one.assert_called_once_with("support-kpis.yaml", "decks-folder")
    read_file.assert_called_once_with("file-id-support-kpis")
