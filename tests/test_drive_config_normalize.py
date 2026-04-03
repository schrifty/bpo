"""Tests for Drive config text normalization and repo sync guard (no network)."""

from __future__ import annotations

import pytest

from src.drive_config import _normalize_config_text, config_text_matches_local


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
