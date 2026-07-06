"""Archive prior-month export artifacts on Drive into ``YYYY-MM`` subfolders.

On startup (once per process), moves export files and dated ``{ISO-date} - Output``
folders from the previous calendar month out of:

- ``<QBR Generator>/Output/``
- ``<QBR Generator>/Output/customer-exports/{customer}/``

Current-month exports stay at the top level. Already-archived ``YYYY-MM`` folders are
left in place. Set ``CORTEX_SKIP_OUTPUT_ARCHIVE=1`` to disable.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from typing import Any

from .config import logger
from .drive_config import QBR_OUTPUT_SUBFOLDER, _find_or_create_folder, drive_api_lock

_CUSTOMER_EXPORTS_FOLDER = "customer-exports"
_MIME_FOLDER = "application/vnd.google-apps.folder"
_ARCHIVE_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_DATED_OUTPUT_FOLDER_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}) - Output$")

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


def archive_previous_month_in_folder(
    parent_id: str,
    archive_month: str,
    *,
    skip_names: frozenset[str] | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Move prior-month children of ``parent_id`` into ``parent_id/{archive_month}/``."""
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


def maybe_archive_previous_month_exports(*, force: bool = False) -> dict[str, Any]:
    """Archive last month's exports on Drive (at most once per process)."""
    global _archive_ran
    if _archive_ran and not force:
        return {"skipped": "already_ran"}
    _archive_ran = True

    if _truthy_env("CORTEX_SKIP_OUTPUT_ARCHIVE"):
        return {"skipped": "env"}

    from .drive_config import get_qbr_output_root_folder_id

    root_id = get_qbr_output_root_folder_id()
    if not root_id:
        logger.debug("Output archive: no Drive Output folder configured")
        return {"skipped": "no_output_folder"}

    archive_month = previous_month_key()
    summary: dict[str, Any] = {
        "archive_month": archive_month,
        "output_root": None,
        "customer_exports": [],
        "moved_count": 0,
    }

    try:
        root_result = archive_previous_month_in_folder(
            root_id,
            archive_month,
            skip_names=frozenset({_CUSTOMER_EXPORTS_FOLDER}),
            context=QBR_OUTPUT_SUBFOLDER,
        )
        summary["output_root"] = root_result
        summary["moved_count"] += len(root_result.get("moved") or [])

        customer_exports_id = _find_folder_in_parent(_CUSTOMER_EXPORTS_FOLDER, root_id)
        if customer_exports_id:
            for customer_folder in _list_folder_children(customer_exports_id):
                if str(customer_folder.get("mimeType") or "") != _MIME_FOLDER:
                    continue
                customer_name = str(customer_folder.get("name") or "")
                if not customer_name or _ARCHIVE_MONTH_RE.match(customer_name):
                    continue
                cust_result = archive_previous_month_in_folder(
                    str(customer_folder["id"]),
                    archive_month,
                    context=f"{_CUSTOMER_EXPORTS_FOLDER}/{customer_name}",
                )
                summary["customer_exports"].append(
                    {"customer": customer_name, **cust_result},
                )
                summary["moved_count"] += len(cust_result.get("moved") or [])

        if summary["moved_count"]:
            logger.info(
                "Output archive: moved %d item(s) into %s folders under Drive %s",
                summary["moved_count"],
                archive_month,
                QBR_OUTPUT_SUBFOLDER,
            )
        else:
            logger.debug(
                "Output archive: nothing to move for %s under Drive %s",
                archive_month,
                QBR_OUTPUT_SUBFOLDER,
            )
        return summary
    except Exception as e:
        logger.warning("Output archive failed (continuing): %s", e)
        return {"skipped": "error", "error": str(e), "archive_month": archive_month}
