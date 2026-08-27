"""Tests for CSR dump Google Sheet create/update and Drive copy."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.export_csr_spreadsheet import (
    _tab_add_requests,
    copy_csr_spreadsheet_into_folder,
    upload_csr_dump_spreadsheet,
)


def test_tab_add_requests_skips_existing() -> None:
    assert _tab_add_requests({"meta"}, ["meta", "factories"]) == [
        {"addSheet": {"properties": {"title": "factories"}}}
    ]
    assert _tab_add_requests({"meta", "factories"}, ["meta", "factories"]) == []


def test_upload_csr_dump_spreadsheet_updates_existing_in_place(monkeypatch) -> None:
    creates: list[object] = []
    clears: list[str] = []
    updates: list[str] = []

    monkeypatch.setattr("src.drive_config.dedupe_duplicate_names_in_folder", lambda *_a, **_k: "ss-existing")
    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: "ss-existing")

    sheets_svc = MagicMock()
    sheets_svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "meta"}},
            {"properties": {"title": "factories"}},
        ]
    }
    monkeypatch.setattr("src.charts._build_sheets_service", lambda: sheets_svc)
    monkeypatch.setattr("src.slides_api._get_service", lambda: (None, MagicMock(), None))
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_create",
        lambda *_a, **_k: creates.append("created") or {"spreadsheetId": "new"},
    )
    monkeypatch.setattr("src.slides_api.sheets_spreadsheet_batch_update", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_values_clear",
        lambda *_a, **kwargs: clears.append(str(kwargs.get("range_str") or "")),
    )
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_values_update",
        lambda *_a, **kwargs: updates.append(str(kwargs.get("spreadsheet_id") or "")),
    )

    tables = {"meta": [["field", "value"]], "factories": [["Factory"], ["Plant A"]]}
    assert upload_csr_dump_spreadsheet(tables, "CustomerSuccessReport-21-Aug-2026", "folder") == "ss-existing"
    assert creates == []
    assert clears == ["'meta'", "'factories'"]
    assert updates == ["ss-existing", "ss-existing"]


def test_upload_csr_dump_spreadsheet_creates_when_missing(monkeypatch) -> None:
    monkeypatch.setattr("src.drive_config.dedupe_duplicate_names_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.charts._build_sheets_service", lambda: MagicMock())
    drive = MagicMock()
    monkeypatch.setattr("src.slides_api._get_service", lambda: (None, drive, None))
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_create",
        lambda *_a, **_k: {"spreadsheetId": "ss-new"},
    )
    updates: list[str] = []
    monkeypatch.setattr(
        "src.slides_api.sheets_spreadsheet_values_update",
        lambda *_a, **kwargs: updates.append(str(kwargs.get("spreadsheet_id") or "")),
    )

    tables = {"meta": [["a"]], "factories": [["b"]]}
    assert upload_csr_dump_spreadsheet(tables, "Title", "folder") == "ss-new"
    drive.files.return_value.update.assert_called()
    assert updates == ["ss-new", "ss-new"]


def test_copy_csr_spreadsheet_trashes_existing_then_copies(monkeypatch) -> None:
    trashed: list[str] = []
    copied: list[tuple[str, str, str]] = []
    monkeypatch.setattr("src.drive_config.dedupe_duplicate_names_in_folder", lambda *_a, **_k: None)
    monkeypatch.setattr("src.drive_config.find_file_in_folder", lambda *_a, **_k: "old-hist")
    monkeypatch.setattr("src.drive_config.trash_drive_file", lambda fid: trashed.append(fid))
    monkeypatch.setattr(
        "src.drive_config.copy_drive_file_to_folder",
        lambda file_id, *, name, parent_id: copied.append((file_id, name, parent_id)) or "new-hist",
    )
    assert copy_csr_spreadsheet_into_folder("ss-p", "Title", "slot") == "new-hist"
    assert trashed == ["old-hist"]
    assert copied == [("ss-p", "Title", "slot")]
