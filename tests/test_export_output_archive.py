"""Tests for Drive export layout migration (no network)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.export_drive_layout import CUSTOMER_EXPORTS_FOLDER, _LEGACY_CUSTOMER_EXPORTS_FOLDER
from src.export_output_archive import (
    _MIME_FOLDER,
    archive_past_month_day_folders_in_historical_data,
    archive_previous_month_in_folder,
    clear_output_archive_guard,
    item_month_key,
    maybe_archive_previous_month_exports,
    maybe_migrate_export_layout_on_startup,
    migrate_export_folder_to_historical_data,
    previous_month_key,
    restore_misplaced_output_root_metrics_decks,
    should_archive_item,
)


def test_previous_month_key_july_2026() -> None:
    assert previous_month_key(today=dt.date(2026, 7, 6)) == "2026-06"


def test_item_month_key_from_dated_output_folder() -> None:
    assert item_month_key("2026-06-15 - Output", "", mime_type=_MIME_FOLDER) == "2026-06"


def test_item_month_key_from_modified_time() -> None:
    assert item_month_key("LLM-Context-Portfolio.md", "2026-06-30T10:00:00.000Z", mime_type="text/markdown") == "2026-06"


def test_should_archive_skips_customer_exports_and_archive_folders() -> None:
    skip = frozenset({CUSTOMER_EXPORTS_FOLDER, _LEGACY_CUSTOMER_EXPORTS_FOLDER})
    assert not should_archive_item(
        CUSTOMER_EXPORTS_FOLDER,
        "2026-06-01T00:00:00.000Z",
        mime_type=_MIME_FOLDER,
        archive_month="2026-06",
        skip_names=skip,
    )
    assert not should_archive_item(
        _LEGACY_CUSTOMER_EXPORTS_FOLDER,
        "2026-06-01T00:00:00.000Z",
        mime_type=_MIME_FOLDER,
        archive_month="2026-06",
        skip_names=skip,
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


def test_should_archive_skips_persistent_exports_and_metrics_decks() -> None:
    assert not should_archive_item(
        "LLM-Context-Portfolio-persistent.md",
        "2026-06-20T12:00:00.000Z",
        mime_type="text/markdown",
        archive_month="2026-06",
    )
    assert not should_archive_item(
        "AKKR Metrics",
        "2026-06-20T12:00:00.000Z",
        mime_type="application/vnd.google-apps.presentation",
        archive_month="2026-06",
    )
    assert should_archive_item(
        "AKKR Metrics - June",
        "2026-06-20T12:00:00.000Z",
        mime_type="application/vnd.google-apps.presentation",
        archive_month="2026-06",
    )


def test_archive_previous_month_in_folder_moves_qualifying_children(monkeypatch) -> None:
    parent_id = "output-root"
    historical_id = "historical-root"
    archive_id = "archive-2026-06"
    calls: list[tuple[str, str, str]] = []

    def fake_list(_pid: str):
        if _pid == parent_id:
            return [
                {
                    "id": "f1",
                    "name": "2026-06-10 - Output",
                    "mimeType": _MIME_FOLDER,
                    "modifiedTime": "2026-06-10T00:00:00.000Z",
                },
                {
                    "id": "f2",
                    "name": "LLM-Context-Portfolio.md",
                    "mimeType": "text/markdown",
                    "modifiedTime": "2026-07-01T00:00:00.000Z",
                },
                {
                    "id": "f3",
                    "name": CUSTOMER_EXPORTS_FOLDER,
                    "mimeType": _MIME_FOLDER,
                    "modifiedTime": "2026-06-01T00:00:00.000Z",
                },
            ]
        return []

    monkeypatch.setattr("src.export_output_archive._list_folder_children", fake_list)
    monkeypatch.setattr(
        "src.export_output_archive.ensure_historical_data_folder",
        lambda _pid: historical_id,
    )
    monkeypatch.setattr(
        "src.export_output_archive._ensure_month_archive_folder",
        lambda hid, month: archive_id if hid == historical_id and month == "2026-06" else pytest.fail("unexpected"),
    )

    def fake_move(file_id: str, from_parent: str, to_parent: str) -> None:
        calls.append((file_id, from_parent, to_parent))

    monkeypatch.setattr("src.export_output_archive._move_drive_item", fake_move)

    result = archive_previous_month_in_folder(
        parent_id,
        "2026-06",
        skip_names=frozenset({CUSTOMER_EXPORTS_FOLDER, _LEGACY_CUSTOMER_EXPORTS_FOLDER}),
    )
    assert [m["id"] for m in result["moved"]] == ["f1"]
    assert calls == [("f1", parent_id, archive_id)]


def test_archive_past_month_day_folders_moves_june_days(monkeypatch) -> None:
    """Current-month day folders stay at the root; past-month ones get nested."""
    historical_id = "historical-root"
    month_folder_id = "month-2026-06"
    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        "src.export_output_archive._list_folder_children",
        lambda pid: (
            [
                {"id": "d1", "name": "2026-06-23", "mimeType": _MIME_FOLDER},
                {"id": "d2", "name": "2026-06-30", "mimeType": _MIME_FOLDER},
                {"id": "d3", "name": "2026-07-01", "mimeType": _MIME_FOLDER},
                {"id": "m1", "name": "2026-06", "mimeType": _MIME_FOLDER},
            ]
            if pid == historical_id
            else []
        ),
    )
    monkeypatch.setattr(
        "src.export_output_archive._ensure_month_archive_folder",
        lambda hid, month: month_folder_id if hid == historical_id and month == "2026-06" else pytest.fail("unexpected"),
    )

    def fake_move(file_id: str, from_parent: str, to_parent: str) -> None:
        calls.append((file_id, from_parent, to_parent))

    monkeypatch.setattr("src.export_output_archive._move_drive_item", fake_move)

    result = archive_past_month_day_folders_in_historical_data(
        historical_id, today=dt.date(2026, 7, 6)
    )
    assert [m["name"] for m in result["moved"]] == ["2026-06-23", "2026-06-30"]
    assert calls == [
        ("d1", historical_id, month_folder_id),
        ("d2", historical_id, month_folder_id),
    ]


def test_archive_past_month_day_folders_sweeps_older_stranded_months(monkeypatch) -> None:
    """Day folders older than the previous month are nested too, not stranded at root."""
    historical_id = "historical-root"
    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        "src.export_output_archive._list_folder_children",
        lambda pid: (
            [
                {"id": "d1", "name": "2026-06-01", "mimeType": _MIME_FOLDER},
                {"id": "d2", "name": "2026-07-01", "mimeType": _MIME_FOLDER},
                {"id": "d3", "name": "2026-08-11", "mimeType": _MIME_FOLDER},
            ]
            if pid == historical_id
            else []
        ),
    )
    monkeypatch.setattr(
        "src.export_output_archive._ensure_month_archive_folder",
        lambda hid, month: f"month-{month}",
    )
    monkeypatch.setattr(
        "src.export_output_archive.dedupe_child_folders_by_name", lambda pid: []
    )
    monkeypatch.setattr(
        "src.export_output_archive._move_drive_item",
        lambda fid, src, dst: calls.append((fid, src, dst)),
    )

    result = archive_past_month_day_folders_in_historical_data(
        historical_id, today=dt.date(2026, 8, 11)
    )
    # 2026-08-11 is the current month, so it stays at the Historical Data root.
    assert [m["name"] for m in result["moved"]] == ["2026-06-01", "2026-07-01"]
    assert result["archive_months"] == ["2026-06", "2026-07"]
    assert calls == [
        ("d1", historical_id, "month-2026-06"),
        ("d2", historical_id, "month-2026-07"),
    ]


def test_maybe_archive_runs_once_and_honors_skip_env(monkeypatch) -> None:
    clear_output_archive_guard()
    monkeypatch.setenv("CORTEX_SKIP_OUTPUT_ARCHIVE", "1")
    first = maybe_archive_previous_month_exports()
    second = maybe_archive_previous_month_exports()
    assert first == {"skipped": "env"}
    assert second == {"skipped": "already_ran"}


def test_maybe_migrate_walks_customer_exports(monkeypatch) -> None:
    clear_output_archive_guard()
    monkeypatch.delenv("CORTEX_SKIP_OUTPUT_ARCHIVE", raising=False)
    monkeypatch.setattr(
        "src.drive_config.get_qbr_output_root_folder_id",
        lambda: "output-root",
    )

    def fake_archive(parent_id: str, **kwargs):
        if parent_id == "output-root":
            return {
                "parent_id": parent_id,
                "moved": [{"id": "a", "name": "june.md"}],
                "trashed_folders": ["2026-06-01 - Output"],
            }
        if parent_id == "ford-folder":
            return {
                "parent_id": parent_id,
                "moved": [{"id": "b", "name": "Pendo Export.md"}],
                "trashed_folders": [],
            }
        raise AssertionError(parent_id)

    monkeypatch.setattr(
        "src.export_output_archive._archive_export_base_on_startup",
        fake_archive,
    )
    monkeypatch.setattr(
        "src.export_output_archive.ensure_customer_exports_parent_folder",
        lambda pid: "customer-exports-id" if pid == "output-root" else pytest.fail("unexpected"),
    )
    monkeypatch.setattr(
        "src.export_output_archive._list_folder_children",
        lambda pid: (
            [{"id": "ford-folder", "name": "Ford", "mimeType": _MIME_FOLDER, "modifiedTime": "2026-01-01T00:00:00.000Z"}]
            if pid == "customer-exports-id"
            else []
        ),
    )

    summary = maybe_migrate_export_layout_on_startup(force=True)
    assert summary["moved_count"] == 2
    assert summary["output_root"]["moved"][0]["name"] == "june.md"
    assert summary["customer_exports"][0]["customer"] == "Ford"


def test_maybe_archive_alias_delegates_to_migration(monkeypatch) -> None:
    clear_output_archive_guard()
    monkeypatch.setenv("CORTEX_SKIP_OUTPUT_ARCHIVE", "1")
    assert maybe_archive_previous_month_exports() == {"skipped": "env"}


def test_migrate_legacy_dated_folder_moves_children_and_trashes_container(monkeypatch) -> None:
    parent_id = "account-folder"
    historical_id = "historical-folder"
    month_folder_id = "month-folder"
    moves: list[tuple[str, str, str]] = []
    trashed: list[str] = []

    def fake_list(pid: str):
        if pid == parent_id:
            return [
                {
                    "id": "dated",
                    "name": "2026-06-15 - Output",
                    "mimeType": _MIME_FOLDER,
                    "modifiedTime": "2026-06-15T00:00:00.000Z",
                },
                {
                    "id": "keep",
                    "name": "Pendo Export  (Ford, 30d)-persistent.md",
                    "mimeType": "text/markdown",
                    "modifiedTime": "2026-07-01T00:00:00.000Z",
                },
            ]
        if pid == "dated":
            return [
                {
                    "id": "inner",
                    "name": "Pendo Export  (Ford, 30d).md",
                    "mimeType": "text/markdown",
                    "modifiedTime": "2026-06-15T00:00:00.000Z",
                }
            ]
        if pid in {historical_id, month_folder_id}:
            return []
        raise AssertionError(pid)

    monkeypatch.setattr("src.export_output_archive._list_folder_children", fake_list)
    monkeypatch.setattr(
        "src.export_output_archive.ensure_historical_data_folder",
        lambda _pid: historical_id,
    )
    monkeypatch.setattr(
        "src.export_output_archive._ensure_month_archive_folder",
        lambda hid, month: month_folder_id if month == "2026-06" else pytest.fail("unexpected"),
    )
    monkeypatch.setattr("src.export_output_archive.dedupe_duplicate_names_in_folder", lambda *_a, **_k: None)

    def fake_move(file_id: str, from_parent: str, to_parent: str) -> None:
        moves.append((file_id, from_parent, to_parent))

    monkeypatch.setattr("src.export_output_archive._move_drive_item", fake_move)
    monkeypatch.setattr(
        "src.export_output_archive.trash_drive_file",
        lambda fid: trashed.append(fid),
    )
    monkeypatch.setattr("src.export_output_archive.rename_drive_file", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "src.export_output_archive.archive_previous_month_in_folder",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.consolidate_historical_data_to_monthly_archives",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.normalize_loose_historical_data",
        lambda *_a, **_k: {"reorganized": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.archive_past_month_day_folders_in_historical_data",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.ensure_persistent_exports_in_base",
        lambda *_a, **_k: [],
    )

    result = migrate_export_folder_to_historical_data(parent_id)
    assert moves == [("inner", "dated", month_folder_id)]
    assert trashed == ["dated"]
    assert result["moved"][0]["target"] == "Pendo Export  (Ford, 30d).md"
    assert result["moved"][0]["month"] == "2026-06"


def test_relocate_non_persistent_skips_user_guide_at_output_root() -> None:
    from src.export_output_archive import _relocate_non_persistent_base_file

    child = {
        "id": "guide-1",
        "name": "Cortex Export - User Guide.md",
        "mimeType": "text/markdown",
        "modifiedTime": "2026-07-01T00:00:00.000Z",
    }
    assert _relocate_non_persistent_base_file(child, parent_id="out", historical_id="hist") is None


def test_relocate_non_persistent_skips_akkr_metrics_deck() -> None:
    from src.export_output_archive import _relocate_non_persistent_base_file

    child = {
        "id": "akkr-1",
        "name": "AKKR Metrics",
        "mimeType": "application/vnd.google-apps.presentation",
        "modifiedTime": "2026-08-18T14:56:50.000Z",
    }
    assert _relocate_non_persistent_base_file(child, parent_id="out", historical_id="hist") is None


def test_relocate_non_persistent_skips_csr_dump_persistent() -> None:
    from src.export_output_archive import _relocate_non_persistent_base_file

    child = {
        "id": "csr-1",
        "name": "Ford CSR Dump-persistent.md",
        "mimeType": "text/markdown",
        "modifiedTime": "2026-08-21T11:00:00.000Z",
    }
    assert _relocate_non_persistent_base_file(child, parent_id="out", historical_id="hist") is None


def test_restore_misplaced_akkr_metrics_moves_newest_and_trashes_dupes(monkeypatch) -> None:
    children = {
        "out": [],
        "hist": [
            {
                "id": "aug-folder",
                "name": "2026-08",
                "mimeType": _MIME_FOLDER,
            }
        ],
        "aug-folder": [
            {
                "id": "older",
                "name": "AKKR Metrics",
                "mimeType": "application/vnd.google-apps.presentation",
                "modifiedTime": "2026-08-11T23:49:42.000Z",
            },
            {
                "id": "newer",
                "name": "AKKR Metrics",
                "mimeType": "application/vnd.google-apps.presentation",
                "modifiedTime": "2026-08-18T14:56:50.000Z",
            },
            {
                "id": "july-copy",
                "name": "AKKR Metrics - July",
                "mimeType": "application/vnd.google-apps.presentation",
                "modifiedTime": "2026-08-18T14:56:51.000Z",
            },
        ],
    }

    def fake_list(pid: str):
        return list(children.get(pid) or [])

    moves: list[tuple[str, str, str]] = []
    trashed: list[str] = []
    monkeypatch.setattr("src.export_output_archive._list_folder_children", fake_list)
    monkeypatch.setattr(
        "src.export_output_archive.move_drive_file",
        lambda fid, *, from_parent_id, to_parent_id, new_name=None: moves.append(
            (fid, from_parent_id, to_parent_id)
        ),
    )
    monkeypatch.setattr("src.export_output_archive.trash_drive_file", lambda fid: trashed.append(fid))

    result = restore_misplaced_output_root_metrics_decks("out", historical_id="hist")
    assert moves == [("newer", "aug-folder", "out")]
    assert trashed == ["older"]
    assert result["restored"] == [{"id": "newer", "name": "AKKR Metrics"}]
    assert result["trashed"] == [{"id": "older", "name": "AKKR Metrics"}]


def test_normalize_loose_skips_run_slot_folder_at_historical_root(monkeypatch) -> None:
    from src.export_output_archive import normalize_loose_historical_data

    historical_id = "hist"
    monkeypatch.setattr(
        "src.export_output_archive._list_folder_children",
        lambda pid: (
            [
                {"id": "slot", "name": "0600", "mimeType": _MIME_FOLDER},
                {"id": "day", "name": "2026-08-21", "mimeType": _MIME_FOLDER},
            ]
            if pid == historical_id
            else []
        ),
    )
    monkeypatch.setattr(
        "src.export_output_archive.ensure_historical_data_folder",
        lambda _pid: historical_id,
    )
    moves: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        "src.export_output_archive._move_drive_item",
        lambda fid, from_parent, to_parent: moves.append((fid, from_parent, to_parent)),
    )
    result = normalize_loose_historical_data("parent", historical_id=historical_id)
    assert result["reorganized"] == []
    assert moves == []


def test_migrate_promotes_legacy_base_pendo_to_persistent(monkeypatch) -> None:
    parent_id = "account-folder"
    historical_id = "historical-folder"

    def fake_list(pid: str):
        if pid == parent_id:
            return [
                {
                    "id": "legacy",
                    "name": "Pendo Export  (Ford, 30d).md",
                    "mimeType": "text/markdown",
                    "modifiedTime": "2026-07-01T00:00:00.000Z",
                }
            ]
        if pid == historical_id:
            return []
        raise AssertionError(pid)

    monkeypatch.setattr("src.export_output_archive._list_folder_children", fake_list)
    monkeypatch.setattr(
        "src.export_output_archive.ensure_historical_data_folder",
        lambda _pid: historical_id,
    )
    monkeypatch.setattr("src.export_output_archive.dedupe_duplicate_names_in_folder", lambda *_a, **_k: None)
    renames: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "src.export_output_archive.rename_drive_file",
        lambda fid, name: renames.append((fid, name)),
    )
    monkeypatch.setattr(
        "src.export_output_archive.archive_previous_month_in_folder",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.consolidate_historical_data_to_monthly_archives",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.normalize_loose_historical_data",
        lambda *_a, **_k: {"reorganized": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.archive_past_month_day_folders_in_historical_data",
        lambda *_a, **_k: {"moved": []},
    )
    monkeypatch.setattr(
        "src.export_output_archive.ensure_persistent_exports_in_base",
        lambda *_a, **_k: [],
    )

    result = migrate_export_folder_to_historical_data(parent_id)
    assert renames == [("legacy", "Pendo Export  (Ford, 30d)-persistent.md")]
    assert result["promoted"][0]["target"] == "Pendo Export  (Ford, 30d)-persistent.md"
