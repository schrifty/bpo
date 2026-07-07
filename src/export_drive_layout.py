"""Drive folder layout for portfolio and customer exports.

Persistent artifacts live in the export base folder with a ``-persistent`` suffix
(always replaced in place). Each export also writes a same-day snapshot under
``Historical Data/{YYYY-MM-DD}/`` using the plain export stem (no ``-persistent``).
Prior-month base-folder exports are bucketed into ``Historical Data/{YYYY-MM}/`` at startup via
:func:`src.export_output_archive.archive_previous_month_in_folder`. Prior-month day subfolders
under ``Historical Data/`` are nested under that same monthly bucket.
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
_HISTORICAL_DAY_FOLDER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HISTORICAL_FLAT_DATED_NAME_RE = re.compile(r"^(.+) (\d{4}-\d{2}-\d{2})(\..+)?$")
_MANAGED_EXPORT_PREFIXES = ("Pendo Export  ", "LLM-Context-Portfolio", "match-customer-names")
_MIME_SPREADSHEET = "application/vnd.google-apps.spreadsheet"
_MIME_FOLDER = "application/vnd.google-apps.folder"


def split_filename_stem_ext(name: str, *, mime_type: str = "") -> tuple[str, str]:
    """Return ``(stem, ext)``; spreadsheets use ``ext=''``."""
    if mime_type == _MIME_SPREADSHEET or (
        not name.endswith(".md")
        and not name.endswith(".txt")
        and not name.endswith(".json")
        and "." not in name
    ):
        return name, ""
    if "." in name:
        base, ext = name.rsplit(".", 1)
        return base, f".{ext}"
    return name, ""


def parse_historical_flat_dated_name(name: str) -> tuple[str, dt.date, str] | None:
    """Parse legacy flat ``{stem} {YYYY-MM-DD}{ext}`` under ``Historical Data/``."""
    m = _HISTORICAL_FLAT_DATED_NAME_RE.match(name or "")
    if not m:
        return None
    try:
        day = dt.date.fromisoformat(m.group(2))
    except ValueError:
        return None
    ext = m.group(3) or ""
    return m.group(1), day, ext


def is_historical_day_subfolder(name: str) -> bool:
    return bool(_HISTORICAL_DAY_FOLDER_RE.match(name or ""))


def is_managed_export_filename(name: str) -> bool:
    """True for portfolio/customer export artifacts governed by persistent/historical layout."""
    if not name or name.startswith("."):
        return False
    if PERSISTENT_SUFFIX in name:
        return True
    if parse_historical_flat_dated_name(name):
        return True
    return any(name.startswith(p) for p in _MANAGED_EXPORT_PREFIXES)


def export_stem_from_filename(name: str, *, mime_type: str = "") -> str | None:
    """Extract logical export stem (without ``-persistent`` or legacy flat date suffix)."""
    stem, _ext = split_filename_stem_ext(name, mime_type=mime_type)
    if PERSISTENT_SUFFIX in stem:
        stem = stem[: stem.index(PERSISTENT_SUFFIX)]
    dated = parse_historical_flat_dated_name(stem)
    if dated:
        stem = dated[0]
    if is_managed_export_filename(name):
        return stem
    return None


def modified_time_to_date(modified_time: str | None) -> dt.date | None:
    if not modified_time or len(modified_time) < 10:
        return None
    try:
        return dt.date.fromisoformat(modified_time[:10])
    except ValueError:
        return None


def persistent_filename(stem: str, *, ext: str) -> str:
    """e.g. ``Pendo Export  (Ford, 30d)-persistent.md``."""
    return f"{stem}{PERSISTENT_SUFFIX}{ext}"


def persistent_spreadsheet_title(stem: str) -> str:
    return f"{stem}{PERSISTENT_SUFFIX}"


def historical_snapshot_filename(stem: str, *, ext: str) -> str:
    """Plain snapshot name inside ``Historical Data/{date}/`` (no ``-persistent``)."""
    return f"{stem}{ext}"


def historical_snapshot_spreadsheet_title(stem: str) -> str:
    return stem


def historical_day_folder_label(export_date: dt.date | None = None) -> str:
    return (export_date or dt.date.today()).isoformat()


def ensure_historical_data_folder(parent_id: str) -> str:
    from .drive_config import _find_or_create_folder

    return _find_or_create_folder(HISTORICAL_DATA_FOLDER, parent_id)


def is_historical_month_subfolder(name: str) -> bool:
    return bool(_ARCHIVE_MONTH_RE.match(name or ""))


def ensure_historical_month_folder(historical_root_id: str, month_key: str) -> str:
    from .drive_config import _find_or_create_folder

    return _find_or_create_folder(month_key, historical_root_id)


def ensure_historical_day_folder(historical_root_id: str, export_date: dt.date | None = None) -> str:
    from .drive_config import _find_or_create_folder

    return _find_or_create_folder(historical_day_folder_label(export_date), historical_root_id)


def target_persistent_name(name: str, *, mime_type: str = "") -> str | None:
    stem = export_stem_from_filename(name, mime_type=mime_type)
    if stem is None:
        return None
    _stem, ext = split_filename_stem_ext(name, mime_type=mime_type)
    if mime_type == _MIME_SPREADSHEET:
        return persistent_spreadsheet_title(stem)
    if ext:
        return persistent_filename(stem, ext=ext)
    return persistent_spreadsheet_title(stem)


def target_historical_snapshot_name(name: str, *, mime_type: str = "") -> str | None:
    """Plain historical filename (no date, no ``-persistent``)."""
    stem = export_stem_from_filename(name, mime_type=mime_type)
    if stem is None:
        return None
    _stem, ext = split_filename_stem_ext(name, mime_type=mime_type)
    if mime_type == _MIME_SPREADSHEET:
        return historical_snapshot_spreadsheet_title(stem)
    if ext:
        return historical_snapshot_filename(stem, ext=ext)
    return historical_snapshot_spreadsheet_title(stem)


def historical_snapshot_day(
    name: str,
    *,
    export_date: dt.date | None = None,
    modified_time: str | None = None,
) -> dt.date:
    flat = parse_historical_flat_dated_name(name)
    if flat:
        return flat[1]
    return export_date or modified_time_to_date(modified_time) or dt.date.today()


def dated_output_folder_date(name: str) -> dt.date | None:
    m = _DATED_OUTPUT_FOLDER_RE.match(name or "")
    if not m:
        return None
    try:
        return dt.date.fromisoformat(m.group(1))
    except ValueError:
        return None


PORTFOLIO_EXPORT_BASE_ALLOWED_SUBFOLDERS = frozenset({_CUSTOMER_EXPORTS_FOLDER, HISTORICAL_DATA_FOLDER})
CUSTOMER_EXPORT_BASE_ALLOWED_SUBFOLDERS = frozenset({HISTORICAL_DATA_FOLDER})


def is_persistent_export_name(name: str) -> bool:
    """True when a base-folder export artifact uses the bookmarkable ``-persistent`` suffix."""
    return PERSISTENT_SUFFIX in (name or "")


def is_allowed_export_base_subfolder(name: str, *, portfolio_root: bool) -> bool:
    allowed = (
        PORTFOLIO_EXPORT_BASE_ALLOWED_SUBFOLDERS
        if portfolio_root
        else CUSTOMER_EXPORT_BASE_ALLOWED_SUBFOLDERS
    )
    return name in allowed


def is_legacy_export_container_folder(name: str, *, include_todays_dated: bool = False) -> bool:
    if _ARCHIVE_MONTH_RE.match(name or ""):
        return True
    folder_day = dated_output_folder_date(name)
    if folder_day is None:
        return False
    if include_todays_dated:
        return True
    return folder_day < dt.date.today()


def is_legacy_dated_output_folder(name: str, *, today: dt.date | None = None) -> bool:
    """Portfolio Output root: migrate dated folders before today (today reserved for QBR/deck copies)."""
    ref = today or dt.date.today()
    folder_day = dated_output_folder_date(name)
    if folder_day is not None:
        return folder_day < ref
    return False


def ensure_customer_export_folders(customer: str) -> dict[str, str]:
    """Return persistent (account) and historical root folder ids under customer-exports."""
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
    """Return persistent (Output root) and historical root folder ids."""
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
    """Upload markdown + workbook to persistent base and today's historical day folder."""
    from .drive_config import dedupe_duplicate_names_in_folder, upload_text_file_to_drive_folder
    from .export_pendo_spreadsheet import spreadsheet_url, upload_pendo_export_spreadsheet

    day = export_date or dt.date.today()
    day_label = historical_day_folder_label(day)
    historical_day_id = ensure_historical_day_folder(historical_folder_id, day)

    p_md = persistent_filename(stem, ext=".md")
    h_md = historical_snapshot_filename(stem, ext=".md")
    p_ss = persistent_spreadsheet_title(stem)
    h_ss = historical_snapshot_spreadsheet_title(stem)

    dedupe_duplicate_names_in_folder(persistent_folder_id, p_md)
    dedupe_duplicate_names_in_folder(persistent_folder_id, p_ss)
    dedupe_duplicate_names_in_folder(historical_day_id, h_md)
    dedupe_duplicate_names_in_folder(historical_day_id, h_ss)

    fid_p = upload_text_file_to_drive_folder(p_md, md, persistent_folder_id, mime_type="text/markdown")
    fid_h = upload_text_file_to_drive_folder(h_md, md, historical_day_id, mime_type="text/markdown")
    ss_p = upload_pendo_export_spreadsheet(report, p_ss, persistent_folder_id)
    ss_h = upload_pendo_export_spreadsheet(report, h_ss, historical_day_id)

    logger.info(
        "Uploaded %s → %s/%s and Historical Data/%s/%s",
        stem,
        base_label,
        p_md,
        day_label,
        h_md,
    )
    return {
        "persistent_md_id": fid_p,
        "historical_md_id": fid_h,
        "persistent_md_name": p_md,
        "historical_md_name": h_md,
        "historical_day_folder": day_label,
        "historical_folder_id": historical_folder_id,
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
    """Upload persistent base copy and same-day historical snapshot (plain stem)."""
    from .drive_config import dedupe_duplicate_names_in_folder, upload_text_file_to_drive_folder

    day = export_date or dt.date.today()
    day_label = historical_day_folder_label(day)
    historical_day_id = ensure_historical_day_folder(historical_folder_id, day)

    p_name = persistent_filename(stem, ext=ext)
    h_name = historical_snapshot_filename(stem, ext=ext)
    dedupe_duplicate_names_in_folder(persistent_folder_id, p_name)
    dedupe_duplicate_names_in_folder(historical_day_id, h_name)
    fid_p = upload_text_file_to_drive_folder(p_name, content, persistent_folder_id, mime_type=mime_type)
    fid_h = upload_text_file_to_drive_folder(h_name, content, historical_day_id, mime_type=mime_type)
    logger.info(
        "Uploaded %s → %s/%s and Historical Data/%s/%s",
        stem,
        base_label,
        p_name,
        day_label,
        h_name,
    )
    return {
        "persistent_file_id": fid_p,
        "historical_file_id": fid_h,
        "persistent_filename": p_name,
        "historical_filename": h_name,
        "historical_day_folder": day_label,
        "historical_folder_id": historical_folder_id,
    }


# Backward-compatible aliases for tests/docs that referenced flat dated filenames.
def historical_filename(stem: str, *, ext: str, export_date: dt.date | None = None) -> str:
    return historical_snapshot_filename(stem, ext=ext)


def historical_spreadsheet_title(stem: str, export_date: dt.date | None = None) -> str:
    return historical_snapshot_spreadsheet_title(stem)


def parse_historical_dated_name(name: str) -> tuple[str, dt.date, str] | None:
    return parse_historical_flat_dated_name(name)
