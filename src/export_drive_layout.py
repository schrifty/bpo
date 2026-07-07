"""Drive folder layout for portfolio and customer exports.

Persistent artifacts live in the export base folder with a ``-persistent`` suffix
(always replaced in place). Historical snapshots go under ``Historical Data/``.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from .config import logger

HISTORICAL_DATA_FOLDER = "Historical Data"
PERSISTENT_SUFFIX = "-persistent"
_CUSTOMER_EXPORTS_FOLDER = "customer-exports"
_DATED_OUTPUT_FOLDER_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}) - Output$")
_ARCHIVE_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_MIME_FOLDER = "application/vnd.google-apps.folder"


def persistent_filename(stem: str, *, ext: str) -> str:
    """e.g. ``Pendo Export  (Ford, 30d)-persistent.md``."""
    return f"{stem}{PERSISTENT_SUFFIX}{ext}"


def historical_filename(stem: str, *, ext: str, export_date: dt.date | None = None) -> str:
    """e.g. ``Pendo Export  (Ford, 30d) 2026-07-07.md``."""
    day = (export_date or dt.date.today()).isoformat()
    return f"{stem} {day}{ext}"


def persistent_spreadsheet_title(stem: str) -> str:
    return f"{stem}{PERSISTENT_SUFFIX}"


def historical_spreadsheet_title(stem: str, export_date: dt.date | None = None) -> str:
    day = (export_date or dt.date.today()).isoformat()
    return f"{stem} {day}"


def ensure_historical_data_folder(parent_id: str) -> str:
    from .drive_config import _find_or_create_folder

    return _find_or_create_folder(HISTORICAL_DATA_FOLDER, parent_id)


def ensure_customer_export_folders(customer: str) -> dict[str, str]:
    """Return persistent (account) and historical folder ids under customer-exports."""
    from .drive_config import _find_or_create_folder, get_qbr_output_root_folder_id

    root = get_qbr_output_root_folder_id()
    if not root:
        raise RuntimeError(
            "Could not resolve Drive Output folder (set GOOGLE_QBR_GENERATOR_FOLDER_ID)."
        )
    customer_exports = _find_or_create_folder(_CUSTOMER_EXPORTS_FOLDER, root)
    account_folder = _find_or_create_folder(customer, customer_exports)
    historical_id = ensure_historical_data_folder(account_folder)
    return {
        "persistent_folder_id": account_folder,
        "historical_folder_id": historical_id,
        "base_label": f"customer-exports/{customer}",
    }


def ensure_portfolio_output_folders() -> dict[str, str]:
    """Return persistent (Output root) and historical folder ids."""
    from .drive_config import get_qbr_output_root_folder_id

    root = get_qbr_output_root_folder_id()
    if not root:
        raise RuntimeError(
            "Could not resolve Drive Output folder (set GOOGLE_QBR_GENERATOR_FOLDER_ID)."
        )
    historical_id = ensure_historical_data_folder(root)
    return {
        "persistent_folder_id": root,
        "historical_folder_id": historical_id,
        "base_label": "Output",
    }


def upload_pendo_markdown_and_spreadsheet(
    *,
    stem: str,
    md: str,
    report: dict[str, Any],
    persistent_folder_id: str,
    historical_folder_id: str,
    base_label: str,
    export_date: dt.date | None = None,
) -> dict[str, str]:
    """Upload markdown + workbook to persistent and historical locations."""
    from .drive_config import upload_text_file_to_drive_folder
    from .export_pendo_spreadsheet import spreadsheet_url, upload_pendo_export_spreadsheet

    day = export_date or dt.date.today()
    p_md = persistent_filename(stem, ext=".md")
    h_md = historical_filename(stem, ext=".md", export_date=day)
    p_ss = persistent_spreadsheet_title(stem)
    h_ss = historical_spreadsheet_title(stem, export_date=day)

    fid_p = upload_text_file_to_drive_folder(p_md, md, persistent_folder_id, mime_type="text/markdown")
    fid_h = upload_text_file_to_drive_folder(h_md, md, historical_folder_id, mime_type="text/markdown")
    ss_p = upload_pendo_export_spreadsheet(report, p_ss, persistent_folder_id)
    ss_h = upload_pendo_export_spreadsheet(report, h_ss, historical_folder_id)

    logger.info(
        "Uploaded %s → %s/%s and Historical Data/%s",
        stem,
        base_label,
        p_md,
        h_md,
    )
    return {
        "persistent_md_id": fid_p,
        "historical_md_id": fid_h,
        "persistent_md_name": p_md,
        "historical_md_name": h_md,
        "persistent_spreadsheet_id": ss_p,
        "historical_spreadsheet_id": ss_h,
        "persistent_spreadsheet_url": spreadsheet_url(ss_p),
        "historical_spreadsheet_url": spreadsheet_url(ss_h),
    }


def upload_text_persistent_and_historical(
    *,
    stem: str,
    content: str,
    ext: str,
    persistent_folder_id: str,
    historical_folder_id: str,
    base_label: str,
    mime_type: str = "text/markdown",
    export_date: dt.date | None = None,
) -> dict[str, str]:
    from .drive_config import upload_text_file_to_drive_folder

    day = export_date or dt.date.today()
    p_name = persistent_filename(stem, ext=ext)
    h_name = historical_filename(stem, ext=ext, export_date=day)
    fid_p = upload_text_file_to_drive_folder(p_name, content, persistent_folder_id, mime_type=mime_type)
    fid_h = upload_text_file_to_drive_folder(h_name, content, historical_folder_id, mime_type=mime_type)
    logger.info(
        "Uploaded %s → %s/%s and Historical Data/%s",
        stem,
        base_label,
        p_name,
        h_name,
    )
    return {
        "persistent_file_id": fid_p,
        "historical_file_id": fid_h,
        "persistent_filename": p_name,
        "historical_filename": h_name,
    }
