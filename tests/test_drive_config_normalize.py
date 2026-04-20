"""Tests for Drive config text normalization and repo sync guard (no network)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.drive_config import (
    _dedupe_drive_yaml_files_by_name,
    _drive_transport_retryable,
    _normalize_config_text,
    clear_yaml_config_cache,
    config_text_matches_local,
)


def test_normalize_line_endings_and_trailing_space() -> None:
    a = "id: foo  \r\nbar: 1\n"
    b = "id: foo\nbar: 1\n"
    assert _normalize_config_text(a) == _normalize_config_text(b)


def test_config_text_matches_local_equivalent_yaml_spacing() -> None:
    assert config_text_matches_local("a: 1\n", "a: 1\n\n")
    assert config_text_matches_local("x: 1\r\n", "x: 1\n")


def test_config_text_matches_local_different_content() -> None:
    assert not config_text_matches_local("a: 1\n", "a: 2\n")


def test_dedupe_drive_yaml_files_by_name_keeps_newest() -> None:
    files = [
        {"id": "older", "name": "dup.yaml", "modifiedTime": "2024-01-01T00:00:00.000Z"},
        {"id": "newer", "name": "dup.yaml", "modifiedTime": "2025-01-01T00:00:00.000Z"},
        {"id": "only", "name": "solo.yaml", "modifiedTime": "2024-06-01T00:00:00.000Z"},
    ]
    out = _dedupe_drive_yaml_files_by_name(files)
    assert len(out) == 2
    assert {f["id"] for f in out} == {"newer", "only"}


def test_drive_transport_retryable_recognizes_pipe_and_connection() -> None:
    assert _drive_transport_retryable(BrokenPipeError())
    assert _drive_transport_retryable(ConnectionResetError())
    import errno as errno_mod

    assert _drive_transport_retryable(OSError(errno_mod.ECONNRESET, "reset"))
    assert not _drive_transport_retryable(ValueError("nope"))


def test_ensure_drive_config_matches_repo_skips_without_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.drive_config as dc

    dc._drive_repo_sync_ran = False
    dc._qbr_adapt_prompt_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", None)
    calls: list[int] = []

    def sync(**_kw: object) -> dict:
        calls.append(1)
        return {}

    monkeypatch.setattr(dc, "sync_obsolete_drive_config", sync)
    dc.ensure_drive_config_matches_repo()
    assert calls == []


def test_ensure_qbr_adapt_prompt_sync_runs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Idempotent guard: second call does not hit Drive APIs."""
    import src.drive_config as dc

    dc._qbr_adapt_prompt_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", "gen")
    monkeypatch.setattr(dc, "get_qbr_generator_folder_id_for_drive_config", lambda: "gen")
    n = 0

    def fake_find_or_create(*_a: object, **_k: object) -> str:
        nonlocal n
        n += 1
        return "prompts_folder_id"

    monkeypatch.setattr(dc, "_find_or_create_folder", fake_find_or_create)
    monkeypatch.setattr(dc, "_list_drive_files", lambda _fid: [])
    monkeypatch.setattr(dc, "_upload_file", lambda *_a, **_k: "new_id")
    dc.ensure_qbr_adapt_prompt_yaml_synced_from_repo()
    dc.ensure_qbr_adapt_prompt_yaml_synced_from_repo()
    assert n == 1


def test_assert_qbr_prompts_ready_raises_without_generator_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hydrate requires GOOGLE_QBR_GENERATOR_FOLDER_ID."""
    import src.drive_config as dc

    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", None)
    with pytest.raises(RuntimeError, match="GOOGLE_QBR_GENERATOR_FOLDER_ID"):
        dc.assert_qbr_prompts_ready_or_raise()


def test_ensure_drive_config_matches_repo_runs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.drive_config as dc

    dc._drive_repo_sync_ran = False
    dc._qbr_adapt_prompt_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", "gen")
    monkeypatch.setattr(dc, "get_qbr_generator_folder_id_for_drive_config", lambda: "gen")
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
    dc._qbr_adapt_prompt_sync_ran = False
    monkeypatch.setattr(dc, "GOOGLE_QBR_GENERATOR_FOLDER_ID", "gen")
    monkeypatch.setattr(dc, "get_qbr_generator_folder_id_for_drive_config", lambda: "root")
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
