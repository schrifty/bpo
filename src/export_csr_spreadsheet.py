"""Google Sheet upload for CSR customer dumps."""

from __future__ import annotations

import re
from typing import Any

from .config import logger
from .export_pendo_spreadsheet import _cell_value, spreadsheet_url

_SHEET_TITLE_BAD = re.compile(r"[:\\/?*\[\]]")
_MIME_SPREADSHEET = "application/vnd.google-apps.spreadsheet"


def _safe_sheet_title(name: str) -> str:
    title = _SHEET_TITLE_BAD.sub("_", (name or "sheet").strip())[:100]
    return title or "sheet"


def _tab_add_requests(existing_titles: set[str], wanted: list[str]) -> list[dict[str, Any]]:
    return [
        {"addSheet": {"properties": {"title": title}}}
        for title in wanted
        if title not in existing_titles
    ]


def _grid_values(grid: list[list[Any]]) -> list[list[Any]]:
    return [[_cell_value(c) for c in row] for row in grid]


def _write_tables_to_spreadsheet(sheets_svc: Any, ss_id: str, tables: dict[str, list[list[Any]]]) -> None:
    from .slides_api import (
        sheets_spreadsheet_batch_update,
        sheets_spreadsheet_get,
        sheets_spreadsheet_values_clear,
        sheets_spreadsheet_values_update,
    )

    wanted = [_safe_sheet_title(tab) for tab in tables]
    meta = sheets_spreadsheet_get(
        sheets_svc,
        spreadsheet_id=ss_id,
        fields="spreadsheetId,sheets.properties.title",
    )
    existing = {
        str(s.get("properties", {}).get("title") or "")
        for s in meta.get("sheets") or []
    }
    sheets_spreadsheet_batch_update(
        sheets_svc,
        spreadsheet_id=ss_id,
        requests=_tab_add_requests(existing, wanted),
    )
    for tab_title, grid in tables.items():
        safe = _safe_sheet_title(tab_title)
        range_str = f"'{safe}'"
        sheets_spreadsheet_values_clear(
            sheets_svc,
            spreadsheet_id=ss_id,
            range_str=range_str,
        )
        sheets_spreadsheet_values_update(
            sheets_svc,
            spreadsheet_id=ss_id,
            range_str=f"{range_str}!A1",
            values=_grid_values(grid),
        )


def _create_spreadsheet(
    sheets_svc: Any,
    drive_svc: Any,
    *,
    title: str,
    folder_id: str,
    tables: dict[str, list[list[Any]]],
) -> str:
    from .drive_config import drive_api_lock
    from .slides_api import sheets_spreadsheet_create, sheets_spreadsheet_values_update

    sheet_defs = [{"properties": {"title": _safe_sheet_title(tab)}} for tab in tables]
    ss = sheets_spreadsheet_create(
        sheets_svc,
        body={"properties": {"title": title}, "sheets": sheet_defs},
        fields="spreadsheetId,sheets.properties.title",
    )
    ss_id = ss["spreadsheetId"]
    with drive_api_lock:
        drive_svc.files().update(
            fileId=ss_id, addParents=folder_id, fields="id,parents", supportsAllDrives=True
        ).execute()
    for tab_title, grid in tables.items():
        sheets_spreadsheet_values_update(
            sheets_svc,
            spreadsheet_id=ss_id,
            range_str=f"'{_safe_sheet_title(tab_title)}'!A1",
            values=_grid_values(grid),
        )
    return ss_id


def copy_csr_spreadsheet_into_folder(spreadsheet_id: str, title: str, folder_id: str) -> str:
    """Replace any same-named Sheet in ``folder_id`` with a Drive copy of ``spreadsheet_id``."""
    from .drive_config import (
        copy_drive_file_to_folder,
        dedupe_duplicate_names_in_folder,
        find_file_in_folder,
        trash_drive_file,
    )

    dedupe_duplicate_names_in_folder(folder_id, title)
    existing = find_file_in_folder(title, folder_id, mime_type=_MIME_SPREADSHEET)
    if existing and existing != spreadsheet_id:
        trash_drive_file(existing)
    copied_id = copy_drive_file_to_folder(spreadsheet_id, name=title, parent_id=folder_id)
    logger.info("Copied CSR dump spreadsheet %s → %s (%s)", spreadsheet_id, copied_id, title)
    return copied_id


def upload_csr_dump_spreadsheet(
    tables: dict[str, list[list[Any]]],
    title: str,
    folder_id: str,
) -> str:
    """Create or update a Google Sheet in ``folder_id``. Returns spreadsheet file id."""
    from .charts import _build_sheets_service
    from .drive_config import dedupe_duplicate_names_in_folder, find_file_in_folder
    from .slides_api import _get_service

    dedupe_duplicate_names_in_folder(folder_id, title)
    existing = find_file_in_folder(title, folder_id, mime_type=_MIME_SPREADSHEET)
    sheets_svc = _build_sheets_service()
    _, drive_svc, _ = _get_service()
    if existing:
        _write_tables_to_spreadsheet(sheets_svc, existing, tables)
        logger.info("Updated CSR dump spreadsheet %s (%s)", existing, title)
        return existing
    ss_id = _create_spreadsheet(
        sheets_svc,
        drive_svc,
        title=title,
        folder_id=folder_id,
        tables=tables,
    )
    logger.info("Created CSR dump spreadsheet %s (%s)", ss_id, title)
    return ss_id


__all__ = [
    "copy_csr_spreadsheet_into_folder",
    "spreadsheet_url",
    "upload_csr_dump_spreadsheet",
]
