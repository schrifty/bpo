"""Google Sheet upload for CSR customer dumps."""

from __future__ import annotations

import re
from typing import Any

from .config import logger
from .export_pendo_spreadsheet import _cell_value, spreadsheet_url

_SHEET_TITLE_BAD = re.compile(r"[:\\/?*\[\]]")


def _safe_sheet_title(name: str) -> str:
    title = _SHEET_TITLE_BAD.sub("_", (name or "sheet").strip())[:100]
    return title or "sheet"


def upload_csr_dump_spreadsheet(
    tables: dict[str, list[list[Any]]],
    title: str,
    folder_id: str,
) -> str:
    """Create or replace a Google Sheet in ``folder_id``. Returns spreadsheet file id."""
    from googleapiclient.errors import HttpError

    from .charts import _build_sheets_service
    from .drive_config import dedupe_duplicate_names_in_folder, drive_api_lock, find_file_in_folder
    from .slides_api import _get_service, sheets_spreadsheet_create, sheets_spreadsheet_values_update

    mime = "application/vnd.google-apps.spreadsheet"
    dedupe_duplicate_names_in_folder(folder_id, title)
    existing = find_file_in_folder(title, folder_id, mime_type=mime)
    if existing:
        with drive_api_lock:
            _, drive, _ = _get_service()
            try:
                drive.files().delete(fileId=existing).execute()
            except HttpError as exc:
                logger.warning("Could not remove prior CSR dump spreadsheet %s: %s", existing, exc)

    sheets_svc = _build_sheets_service()
    _, drive_svc, _ = _get_service()
    sheet_defs = [{"properties": {"title": _safe_sheet_title(tab)}} for tab in tables]
    ss = sheets_spreadsheet_create(
        sheets_svc,
        body={"properties": {"title": title}, "sheets": sheet_defs},
        fields="spreadsheetId,sheets.properties.title",
    )
    ss_id = ss["spreadsheetId"]

    with drive_api_lock:
        drive_svc.files().update(fileId=ss_id, addParents=folder_id, fields="id,parents").execute()

    for tab_title, grid in tables.items():
        sheets_spreadsheet_values_update(
            sheets_svc,
            spreadsheet_id=ss_id,
            range_str=f"'{_safe_sheet_title(tab_title)}'!A1",
            values=[[_cell_value(c) for c in row] for row in grid],
        )

    logger.info("Created CSR dump spreadsheet %s (%s)", ss_id, title)
    return ss_id


__all__ = ["spreadsheet_url", "upload_csr_dump_spreadsheet"]
