"""Prepare Drive export folders: migrate legacy layout into ``Historical Data``.

On startup (once per process), moves dated ``{ISO-date} - Output`` folders, monthly
``YYYY-MM`` archive folders, and non-persistent files from:

- ``<QBR Generator>/Output/``
- ``<QBR Generator>/Output/customer-exports/{customer}/``

into ``Historical Data/`` under each export base. Set ``CORTEX_SKIP_OUTPUT_ARCHIVE=1`` to disable.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any

from .config import logger
from .export_drive_layout import (
    HISTORICAL_DATA_FOLDER,
    PERSISTENT_SUFFIX,
    _ARCHIVE_MONTH_RE,
    _CUSTOMER_EXPORTS_FOLDER,
    _DATED_OUTPUT_FOLDER_RE,
    _MIME_FOLDER,
    ensure_historical_data_folder,
)
from .drive_config import QBR_OUTPUT_SUBFOLDER, drive_api_lock

_archive_ran = False


def clear_output_archive_guard() -> None:
    """Reset once-per-process guard (tests only)."""
    global _archive_ran
    _archive_ran = False


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def previous_month_key(*, today: dt.date | None = None) -> str:
    """Return ``YYYY-MM`` for the calendar month before ``today``."""
    ref = today or dt.date.today()
    first_of_month = ref.replace(day=1)
    last_prev = first_of_month - dt.timedelta(days=1)
    return last_prev.strftime("%Y-%m")


def item_month_key(name: str, modified_time: str, *, mime_type: str) -> str | None:
    """Infer ``YYYY-MM`` for a Drive child; ``None`` when not parseable."""
    if mime_type == _MIME_FOLDER:
        dated = _DATED_OUTPUT_FOLDER_RE.match(name or "")
        if dated:
            return dated.group(1)[:7]
        if _ARCHIVE_MONTH_RE.match(name or ""):
            return None
    if modified_time and len(modified_time) >= 7:
        return modified_time[:7]
    return None


def should_archive_item(
    name: str,
    modified_time: str,
    *,
    mime_type: str,
    archive_month: str,
    skip_names: frozenset[str] | None = None,
) -> bool:
    if skip_names and name in skip_names:
        return False
    if _ARCHIVE_MONTH_RE.match(name or ""):
        return False
    month = item_month_key(name, modified_time, mime_type=mime_type)
    return month == archive_month


def _get_drive():
    from .drive_config import _get_drive as get_drive

    return get_drive()


def _find_folder_in_parent(name: str, parent_id: str) -> str | None:
    from .drive_config import _drive_q_escape

    esc = _drive_q_escape(name)
    q = (
        f"name = '{esc}' and mimeType = '{_MIME_FOLDER}' and '{parent_id}' in parents "
        "and trashed = false"
    )
    with drive_api_lock:
        drive = _get_drive()
        results = drive.files().list(q=q, fields="files(id)", pageSize=5).execute()
        files = results.get("files") or []
        return files[0]["id"] if files else None


def _list_folder_children(parent_id: str) -> list[dict[str, Any]]:
    q = f"'{parent_id}' in parents and trashed = false"
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    with drive_api_lock:
        drive = _get_drive()
        while True:
            results = (
                drive.files()
                .list(
                    q=q,
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                    pageSize=200,
                    pageToken=page_token,
                )
                .execute()
            )
            out.extend(results.get("files") or [])
            page_token = results.get("nextPageToken")
            if not page_token:
                break
    return out


def _move_drive_item(file_id: str, from_parent_id: str, to_parent_id: str) -> None:
    with drive_api_lock:
        drive = _get_drive()
        drive.files().update(
            fileId=file_id,
            addParents=to_parent_id,
            removeParents=from_parent_id,
            fields="id",
        ).execute()


def _trash_drive_item(file_id: str) -> None:
    with drive_api_lock:
        drive = _get_drive()
        drive.files().update(fileId=file_id, body={"trashed": True}).execute()


def _is_legacy_container_folder(name: str) -> bool:
    return bool(_DATED_OUTPUT_FOLDER_RE.match(name or "") or _ARCHIVE_MONTH_RE.match(name or ""))


def _should_migrate_base_file(name: str) -> bool:
    if not name or name.startswith("."):
        return False
    if PERSISTENT_SUFFIX in name:
        return False
    if name == HISTORICAL_DATA_FOLDER:
        return False
    return True


def migrate_export_folder_to_historical_data(
    parent_id: str,
    *,
    skip_folder_names: frozenset[str] | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Move legacy export artifacts under ``parent_id`` into ``Historical Data/``."""
    historical_id = ensure_historical_data_folder(parent_id)
    moved: list[dict[str, str]] = []
    trashed: list[str] = []

    for child in _list_folder_children(parent_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid:
            continue
        if name == HISTORICAL_DATA_FOLDER:
            continue
        if skip_folder_names and name in skip_folder_names:
            continue

        if mime == _MIME_FOLDER:
            if not _is_legacy_container_folder(name):
                continue
            for inner in _list_folder_children(cid):
                inner_id = str(inner.get("id") or "")
                inner_name = str(inner.get("name") or "")
                if not inner_id:
                    continue
                _move_drive_item(inner_id, cid, historical_id)
                moved.append({"id": inner_id, "name": inner_name, "from": name})
            _trash_drive_item(cid)
            trashed.append(name)
            logger.info(
                "Migrated legacy folder %s → %s (%s)",
                name,
                HISTORICAL_DATA_FOLDER,
                context or parent_id[:12],
            )
            continue

        if _should_migrate_base_file(name):
            _move_drive_item(cid, parent_id, historical_id)
            moved.append({"id": cid, "name": name, "from": "(base)"})
            logger.info(
                "Migrated legacy export %s → %s (%s)",
                name,
                HISTORICAL_DATA_FOLDER,
                context or parent_id[:12],
            )

    return {
        "parent_id": parent_id,
        "historical_folder_id": historical_id,
        "moved": moved,
        "trashed_folders": trashed,
    }


def archive_previous_month_in_folder(
    parent_id: str,
    archive_month: str,
    *,
    skip_names: frozenset[str] | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Legacy monthly archive helper (tests only; production uses Historical Data migration)."""
    from .drive_config import _find_or_create_folder

    moved: list[dict[str, str]] = []
    for child in _list_folder_children(parent_id):
        name = str(child.get("name") or "")
        if not should_archive_item(
            name,
            str(child.get("modifiedTime") or ""),
            mime_type=str(child.get("mimeType") or ""),
            archive_month=archive_month,
            skip_names=skip_names,
        ):
            continue
        archive_folder_id = _find_or_create_folder(archive_month, parent_id)
        _move_drive_item(str(child["id"]), parent_id, archive_folder_id)
        moved.append({"id": str(child["id"]), "name": name})
        logger.info(
            "Archived Drive export %s → %s/%s (%s)",
            name,
            archive_month,
            name,
            context or parent_id[:12],
        )
    return {"parent_id": parent_id, "archive_month": archive_month, "moved": moved}


def maybe_migrate_export_layout_on_startup(*, force: bool = False) -> dict[str, Any]:
    """Migrate legacy dated/monthly export layout into Historical Data (once per process)."""
    global _archive_ran
    if _archive_ran and not force:
        return {"skipped": "already_ran"}
    _archive_ran = True

    if _truthy_env("CORTEX_SKIP_OUTPUT_ARCHIVE"):
        return {"skipped": "env"}

    from .drive_config import get_qbr_output_root_folder_id

    root_id = get_qbr_output_root_folder_id()
    if not root_id:
        logger.debug("Export layout migration: no Drive Output folder configured")
        return {"skipped": "no_output_folder"}

    summary: dict[str, Any] = {
        "output_root": None,
        "customer_exports": [],
        "moved_count": 0,
        "trashed_folder_count": 0,
    }

    try:
        root_result = migrate_export_folder_to_historical_data(
            root_id,
            skip_folder_names=frozenset({_CUSTOMER_EXPORTS_FOLDER}),
            context=QBR_OUTPUT_SUBFOLDER,
        )
        summary["output_root"] = root_result
        summary["moved_count"] += len(root_result.get("moved") or [])
        summary["trashed_folder_count"] += len(root_result.get("trashed_folders") or [])

        customer_exports_id = _find_folder_in_parent(_CUSTOMER_EXPORTS_FOLDER, root_id)
        if customer_exports_id:
            for customer_folder in _list_folder_children(customer_exports_id):
                if str(customer_folder.get("mimeType") or "") != _MIME_FOLDER:
                    continue
                customer_name = str(customer_folder.get("name") or "")
                if not customer_name:
                    continue
                cust_result = migrate_export_folder_to_historical_data(
                    str(customer_folder["id"]),
                    context=f"{_CUSTOMER_EXPORTS_FOLDER}/{customer_name}",
                )
                summary["customer_exports"].append({"customer": customer_name, **cust_result})
                summary["moved_count"] += len(cust_result.get("moved") or [])
                summary["trashed_folder_count"] += len(cust_result.get("trashed_folders") or [])

        if summary["moved_count"] or summary["trashed_folder_count"]:
            logger.info(
                "Export layout migration: moved %d file(s), removed %d legacy folder(s) under Drive %s",
                summary["moved_count"],
                summary["trashed_folder_count"],
                QBR_OUTPUT_SUBFOLDER,
            )
        else:
            logger.debug("Export layout migration: nothing to move under Drive %s", QBR_OUTPUT_SUBFOLDER)
        return summary
    except Exception as e:
        logger.warning("Export layout migration failed (continuing): %s", e)
        return {"skipped": "error", "error": str(e)}


def maybe_archive_previous_month_exports(*, force: bool = False) -> dict[str, Any]:
    """Backward-compatible startup hook (delegates to Historical Data migration)."""
    return maybe_migrate_export_layout_on_startup(force=force)
