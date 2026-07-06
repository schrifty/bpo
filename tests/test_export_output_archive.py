"""Tests for prior-month Drive export archiving (no network)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.export_output_archive import (
    _MIME_FOLDER,
    archive_previous_month_in_folder,
    clear_output_archive_guard,
    item_month_key,
    maybe_archive_previous_month_exports,
    previous_month_key,
    should_archive_item,
)


def test_previous_month_key_july_2026() -> None:
    assert previous_month_key(today=dt.date(2026, 7, 6)) == "2026-06"


def test_item_month_key_from_dated_output_folder() -> None:
    assert item_month_key("2026-06-15 - Output", "", mime_type=_MIME_FOLDER) == "2026-06"


def test_item_month_key_from_modified_time() -> None:
    assert item_month_key("LLM-Context-All_Customers.md", "2026-06-30T10:00:00.000Z", mime_type="text/markdown") == "2026-06"


def test_should_archive_skips_customer_exports_and_archive_folders() -> None:
    assert not should_archive_item(
        "customer-exports",
        "2026-06-01T00:00:00.000Z",
        mime_type=_MIME_FOLDER,
        archive_month="2026-06",
        skip_names=frozenset({"customer-exports"}),
    )
    assert not should_archive_item(
        "2026-06",
        "2026-06-01T00:00:00.000Z",
        mime_type=_MIME_FOLDER,
        archive_month="2026-06",
    )


def test_should_archive_current_month_dated_folder_stays_put() -> None:
    assert not should_archive_item(
        "2026-07-01 - Output",
        "2026-07-01T00:00:00.000Z",
        mime_type=_MIME_FOLDER,
        archive_month="2026-06",
    )


def test_should_archive_previous_month_file() -> None:
    assert should_archive_item(
        "Pendo Export  (Ford, 30d).md",
        "2026-06-20T12:00:00.000Z",
        mime_type="text/markdown",
        archive_month="2026-06",
    )


def test_archive_previous_month_in_folder_moves_qualifying_children(monkeypatch) -> None:
    parent_id = "output-root"
    archive_id = "archive-2026-06"
    calls: list[tuple[str, str, str]] = []

    def fake_list(_pid: str):
        assert _pid == parent_id
        return [
            {
                "id": "f1",
                "name": "2026-06-10 - Output",
                "mimeType": _MIME_FOLDER,
                "modifiedTime": "2026-06-10T00:00:00.000Z",
            },
            {
                "id": "f2",
                "name": "LLM-Context-All_Customers.md",
                "mimeType": "text/markdown",
                "modifiedTime": "2026-07-01T00:00:00.000Z",
            },
            {
                "id": "f3",
                "name": "customer-exports",
                "mimeType": _MIME_FOLDER,
                "modifiedTime": "2026-06-01T00:00:00.000Z",
            },
        ]

    monkeypatch.setattr("src.export_output_archive._list_folder_children", fake_list)
    monkeypatch.setattr(
        "src.export_output_archive._find_or_create_folder",
        lambda name, pid: archive_id if name == "2026-06" and pid == parent_id else pytest.fail("unexpected"),
    )

    def fake_move(file_id: str, from_parent: str, to_parent: str) -> None:
        calls.append((file_id, from_parent, to_parent))

    monkeypatch.setattr("src.export_output_archive._move_drive_item", fake_move)

    result = archive_previous_month_in_folder(
        parent_id,
        "2026-06",
        skip_names=frozenset({"customer-exports"}),
    )
    assert [m["id"] for m in result["moved"]] == ["f1"]
    assert calls == [("f1", parent_id, archive_id)]


def test_maybe_archive_runs_once_and_honors_skip_env(monkeypatch) -> None:
    clear_output_archive_guard()
    monkeypatch.setenv("CORTEX_SKIP_OUTPUT_ARCHIVE", "1")
    first = maybe_archive_previous_month_exports()
    second = maybe_archive_previous_month_exports()
    assert first == {"skipped": "env"}
    assert second == {"skipped": "already_ran"}


def test_maybe_archive_walks_customer_exports(monkeypatch) -> None:
    clear_output_archive_guard()
    monkeypatch.delenv("CORTEX_SKIP_OUTPUT_ARCHIVE", raising=False)
    monkeypatch.setattr(
        "src.export_output_archive.previous_month_key",
        lambda **kwargs: "2026-06",
    )
    monkeypatch.setattr(
        "src.drive_config.get_qbr_output_root_folder_id",
        lambda: "output-root",
    )

    def fake_archive(parent_id: str, archive_month: str, **kwargs):
        if parent_id == "output-root":
            return {"parent_id": parent_id, "archive_month": archive_month, "moved": [{"id": "a", "name": "june.md"}]}
        if parent_id == "ford-folder":
            return {
                "parent_id": parent_id,
                "archive_month": archive_month,
                "moved": [{"id": "b", "name": "2026-06-01 - Output"}],
            }
        raise AssertionError(parent_id)

    monkeypatch.setattr("src.export_output_archive.archive_previous_month_in_folder", fake_archive)
    monkeypatch.setattr(
        "src.export_output_archive._find_folder_in_parent",
        lambda name, pid: "customer-exports-id" if name == "customer-exports" and pid == "output-root" else None,
    )
    monkeypatch.setattr(
        "src.export_output_archive._list_folder_children",
        lambda pid: (
            [{"id": "ford-folder", "name": "Ford", "mimeType": _MIME_FOLDER, "modifiedTime": "2026-01-01T00:00:00.000Z"}]
            if pid == "customer-exports-id"
            else []
        ),
    )

    summary = maybe_archive_previous_month_exports(force=True)
    assert summary["moved_count"] == 2
    assert summary["output_root"]["moved"][0]["name"] == "june.md"
    assert summary["customer_exports"][0]["customer"] == "Ford"
