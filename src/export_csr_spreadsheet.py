"""Google Sheet upload for CSR customer dumps."""

from __future__ import annotations

from typing import Any

from .config import logger
from .export_pendo_spreadsheet import (
    _create_spreadsheet,
    _tab_add_requests,
    _write_tables_to_spreadsheet,
    spreadsheet_url,
)

_MIME_SPREADSHEET = "application/vnd.google-apps.spreadsheet"


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
