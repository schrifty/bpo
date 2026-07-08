"""Publish the Cortex Export user guide to Drive ``Output/`` on startup when stale or missing."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import _PROJECT_ROOT, logger
from .export_drive_layout import EXPORT_USER_GUIDE_DRIVE_FILENAME

_USER_GUIDE_REPO_PATH = _PROJECT_ROOT / "docs" / EXPORT_USER_GUIDE_DRIVE_FILENAME
_sync_ran = False


def clear_user_guide_sync_guard() -> None:
    """Reset once-per-process guard (tests)."""
    global _sync_ran
    _sync_ran = False


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _drive_modified_epoch(modified_time: str | None) -> float | None:
    if not modified_time:
        return None
    try:
        s = modified_time.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError):
        return None


def _local_user_guide_is_newer(*, local_mtime: float, drive_modified_time: str | None) -> bool:
    drive_epoch = _drive_modified_epoch(drive_modified_time)
    if drive_epoch is None:
        return True
    return local_mtime > drive_epoch


def maybe_sync_export_user_guide_on_startup(*, force: bool = False) -> dict[str, Any]:
    """Upload ``docs/Cortex Export - User Guide.md`` to ``Output/`` when missing or locally newer."""
    global _sync_ran
    if _sync_ran and not force:
        return {"skipped": "already_ran"}
    _sync_ran = True

    if _truthy_env("CORTEX_SKIP_USER_GUIDE_SYNC"):
        return {"skipped": "env"}

    if not _USER_GUIDE_REPO_PATH.is_file():
        logger.warning(
            "Export user guide sync: local file missing (%s)",
            _USER_GUIDE_REPO_PATH,
        )
        return {"skipped": "missing_local", "path": str(_USER_GUIDE_REPO_PATH)}

    from .drive_config import get_qbr_output_root_folder_id, list_files_by_name_in_folder, upload_text_file_to_drive_folder

    output_root_id = get_qbr_output_root_folder_id()
    if not output_root_id:
        logger.debug("Export user guide sync: no Drive Output folder configured")
        return {"skipped": "no_output_folder"}

    local_mtime = _USER_GUIDE_REPO_PATH.stat().st_mtime
    content = _USER_GUIDE_REPO_PATH.read_text(encoding="utf-8")
    drive_files = list_files_by_name_in_folder(
        EXPORT_USER_GUIDE_DRIVE_FILENAME,
        output_root_id,
        mime_type="text/markdown",
    )
    drive_file = drive_files[0] if drive_files else None

    if drive_file and not force:
        if not _local_user_guide_is_newer(
            local_mtime=local_mtime,
            drive_modified_time=str(drive_file.get("modifiedTime") or ""),
        ):
            logger.debug(
                "Export user guide sync: Drive copy is current (%s)",
                EXPORT_USER_GUIDE_DRIVE_FILENAME,
            )
            return {
                "skipped": "drive_current",
                "file_id": str(drive_file.get("id") or ""),
            }

    try:
        file_id = upload_text_file_to_drive_folder(
            EXPORT_USER_GUIDE_DRIVE_FILENAME,
            content,
            output_root_id,
            mime_type="text/markdown",
            replace_existing=True,
        )
    except Exception as exc:
        logger.warning("Export user guide sync failed (continuing): %s", exc)
        return {"skipped": "error", "error": str(exc)}

    action = "updated" if drive_file else "created"
    logger.info(
        "Export user guide sync: %s %s in Drive Output/",
        action,
        EXPORT_USER_GUIDE_DRIVE_FILENAME,
    )
    return {
        "action": action,
        "file_id": file_id,
        "filename": EXPORT_USER_GUIDE_DRIVE_FILENAME,
        "output_folder_id": output_root_id,
    }
