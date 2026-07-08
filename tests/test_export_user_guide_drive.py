"""Tests for Drive export user guide startup sync (no network)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.export_user_guide_drive import (
    _local_user_guide_is_newer,
    clear_user_guide_sync_guard,
    maybe_sync_export_user_guide_on_startup,
)


def test_local_user_guide_is_newer_when_drive_missing() -> None:
    assert _local_user_guide_is_newer(local_mtime=1000.0, drive_modified_time=None)


def test_local_user_guide_is_newer_when_local_mtime_ahead() -> None:
    # 2026-02-06 UTC vs 2020-01-01 on Drive
    assert _local_user_guide_is_newer(
        local_mtime=1770336000.0,
        drive_modified_time="2020-01-01T00:00:00.000Z",
    )


def test_local_user_guide_not_newer_when_drive_is_current() -> None:
    assert not _local_user_guide_is_newer(
        local_mtime=1000.0,
        drive_modified_time="2030-01-01T00:00:00.000Z",
    )


def test_maybe_sync_uploads_when_drive_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_user_guide_sync_guard()
    guide = tmp_path / "docs" / "Cortex Export - User Guide.md"
    guide.parent.mkdir(parents=True)
    guide.write_text("# Guide\n", encoding="utf-8")

    uploads: list[tuple[str, str, str]] = []

    monkeypatch.setattr("src.export_user_guide_drive._USER_GUIDE_REPO_PATH", guide)
    monkeypatch.setattr("src.drive_config.get_qbr_output_root_folder_id", lambda: "out-root")
    monkeypatch.setattr(
        "src.drive_config.list_files_by_name_in_folder",
        lambda *_a, **_k: [],
    )

    def fake_upload(name: str, content: str, folder_id: str, **kwargs: object) -> str:
        uploads.append((name, content, folder_id))
        return "file-new"

    monkeypatch.setattr("src.drive_config.upload_text_file_to_drive_folder", fake_upload)

    result = maybe_sync_export_user_guide_on_startup(force=True)
    assert result["action"] == "created"
    assert result["file_id"] == "file-new"
    assert uploads == [("Cortex Export - User Guide.md", "# Guide\n", "out-root")]


def test_maybe_sync_skips_when_drive_is_current(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_user_guide_sync_guard()
    guide = tmp_path / "Cortex Export - User Guide.md"
    guide.write_text("# Guide\n", encoding="utf-8")

    monkeypatch.setattr("src.export_user_guide_drive._USER_GUIDE_REPO_PATH", guide)
    monkeypatch.setattr("src.drive_config.get_qbr_output_root_folder_id", lambda: "out-root")
    monkeypatch.setattr(
        "src.drive_config.list_files_by_name_in_folder",
        lambda *_a, **_k: [{"id": "drive-1", "modifiedTime": "2099-01-01T00:00:00.000Z"}],
    )
    monkeypatch.setattr(
        "src.drive_config.upload_text_file_to_drive_folder",
        lambda *_a, **_k: pytest.fail("should not upload"),
    )

    result = maybe_sync_export_user_guide_on_startup()
    assert result["skipped"] == "drive_current"
    assert result["file_id"] == "drive-1"


def test_maybe_sync_updates_when_local_is_newer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_user_guide_sync_guard()
    guide = tmp_path / "Cortex Export - User Guide.md"
    guide.write_text("# Guide v2\n", encoding="utf-8")

    monkeypatch.setattr("src.export_user_guide_drive._USER_GUIDE_REPO_PATH", guide)
    monkeypatch.setattr("src.drive_config.get_qbr_output_root_folder_id", lambda: "out-root")
    monkeypatch.setattr(
        "src.drive_config.list_files_by_name_in_folder",
        lambda *_a, **_k: [{"id": "drive-1", "modifiedTime": "2020-01-01T00:00:00.000Z"}],
    )
    monkeypatch.setattr(
        "src.drive_config.upload_text_file_to_drive_folder",
        lambda *_a, **_k: "file-updated",
    )

    result = maybe_sync_export_user_guide_on_startup(force=True)
    assert result["action"] == "updated"
    assert result["file_id"] == "file-updated"
