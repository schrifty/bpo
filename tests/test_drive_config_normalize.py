"""Tests for Drive config text normalization and repo sync guard (no network)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.drive_config import _normalize_config_text, clear_yaml_config_cache, config_text_matches_local


def test_normalize_line_endings_and_trailing_space() -> None:
    a = "id: foo  \r\nbar: 1\n"
    b = "id: foo\nbar: 1\n"
    assert _normalize_config_text(a) == _normalize_config_text(b)


def test_config_text_matches_local_equivalent_yaml_spacing() -> None:
    assert config_text_matches_local("a: 1\n", "a: 1\n\n")
    assert config_text_matches_local("x: 1\r\n", "x: 1\n")


def test_config_text_matches_local_different_content() -> None:
    assert not config_text_matches_local("a: 1\n", "a: 2\n")


def test_ensure_drive_config_matches_repo_skips_without_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.drive_config as dc

    dc._drive_repo_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_DRIVE_FOLDER_ID", "")
    calls: list[int] = []

    def sync(**_kw: object) -> dict:
        calls.append(1)
        return {}

    monkeypatch.setattr(dc, "sync_obsolete_drive_config", sync)
    dc.ensure_drive_config_matches_repo()
    assert calls == []


def test_ensure_drive_config_matches_repo_runs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.drive_config as dc

    dc._drive_repo_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_DRIVE_FOLDER_ID", "x")
    n = 0

    def sync(**_kw: object) -> dict:
        nonlocal n
        n += 1
        return {}

    monkeypatch.setattr(dc, "sync_obsolete_drive_config", sync)
    dc.ensure_drive_config_matches_repo()
    dc.ensure_drive_config_matches_repo()
    assert n == 1


def test_load_yaml_from_drive_skips_drive_file_without_top_level_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Artifacts like qbr-template-authoring-cues.yaml have no ``id``; do not warn or qa-flag."""
    import src.drive_config as dc

    clear_yaml_config_cache()
    dc._drive_repo_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_DRIVE_FOLDER_ID", "fake-folder")
    monkeypatch.setattr(dc, "ensure_drive_config_matches_repo", lambda: None)
    monkeypatch.setattr(dc, "_get_config_folder_ids", lambda: ("root", "decks", "slides"))
    reads = {
        "fid-slide": "id: slide_a\ntype: std\n",
        "fid-artifact": "generated_at: '2026-01-01'\nslides: []\n",
    }
    monkeypatch.setattr(
        dc,
        "_list_drive_files",
        lambda _folder_id: [
            {"name": "real-slide.yaml", "id": "fid-slide"},
            {"name": "qbr-template-authoring-cues.yaml", "id": "fid-artifact"},
        ],
    )
    monkeypatch.setattr(dc, "_read_drive_file", lambda fid: reads[fid])

    result = dc._load_yaml_from_drive_uncached("slides", tmp_path)
    assert len(result) == 1
    assert result[0]["id"] == "slide_a"
    assert result[0]["_source"] == "drive"
