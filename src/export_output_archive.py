"""Prepare Drive export folders: enforce persistent-only export bases + monthly archives.

Export base folders (``Output/`` and ``Output/customer-exports/{customer}/``) may contain
only ``-persistent`` export files and allowed subfolders (``customer-exports``, ``Historical Data``).
Each export also writes a same-day snapshot under ``Historical Data/{YYYY-MM-DD}/`` (plain stem).
Prior-month base-folder exports are moved into ``Historical Data/{YYYY-MM}/`` via
:func:`archive_previous_month_in_folder` at startup. Prior-month day subfolders
(``Historical Data/{YYYY-MM-DD}/``) are nested under that same monthly bucket.

Set ``CORTEX_SKIP_OUTPUT_ARCHIVE=1`` to disable startup enforcement.
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
    dated_output_folder_date,
    ensure_customer_export_folders,
    ensure_historical_data_folder,
    ensure_historical_day_folder,
    ensure_historical_month_folder,
    historical_snapshot_day,
    is_allowed_export_base_subfolder,
    is_historical_day_subfolder,
    is_historical_month_subfolder,
    is_legacy_dated_output_folder,
    is_legacy_export_container_folder,
    is_persistent_export_name,
    modified_time_to_date,
    parse_historical_flat_dated_name,
    target_historical_snapshot_name,
    target_persistent_name,
)
from .drive_config import (
    QBR_OUTPUT_SUBFOLDER,
    dedupe_duplicate_names_in_folder,
    drive_api_lock,
    move_drive_file,
    rename_drive_file,
    trash_drive_file,
)

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


def _is_legacy_container_folder(
    name: str,
    *,
    today: dt.date | None = None,
    include_todays_dated: bool = False,
) -> bool:
    if include_todays_dated:
        return is_legacy_export_container_folder(name, include_todays_dated=True)
    return is_legacy_dated_output_folder(name, today=today)


def _plain_snapshot_name(name: str, *, mime_type: str = "") -> str:
    snapshot = target_historical_snapshot_name(name, mime_type=mime_type)
    if snapshot:
        return snapshot
    plain = name
    if PERSISTENT_SUFFIX in plain:
        plain = plain.replace(PERSISTENT_SUFFIX, "")
    flat = parse_historical_flat_dated_name(plain)
    if flat:
        stem, _day, ext = flat
        return f"{stem}{ext}" if ext else stem
    return plain


def _move_to_historical_day_folder(
    file_id: str,
    *,
    from_parent_id: str,
    historical_root_id: str,
    name: str,
    mime_type: str = "",
    export_date: dt.date | None = None,
    modified_time: str = "",
) -> dict[str, str]:
    day = historical_snapshot_day(
        name,
        export_date=export_date,
        modified_time=modified_time,
    )
    day_folder_id = ensure_historical_day_folder(historical_root_id, day)
    snapshot_name = _plain_snapshot_name(name, mime_type=mime_type)
    dedupe_duplicate_names_in_folder(day_folder_id, snapshot_name)
    move_drive_file(
        file_id,
        from_parent_id=from_parent_id,
        to_parent_id=day_folder_id,
        new_name=snapshot_name,
    )
    return {
        "target": snapshot_name,
        "day": day.isoformat(),
        "path": f"{HISTORICAL_DATA_FOLDER}/{day.isoformat()}/{snapshot_name}",
    }


def _month_key_for_item(
    name: str,
    *,
    export_date: dt.date | None = None,
    modified_time: str = "",
    mime_type: str = "",
) -> str:
    if export_date:
        return export_date.strftime("%Y-%m")
    month = item_month_key(name, modified_time, mime_type=mime_type)
    if month:
        return month
    flat = parse_historical_flat_dated_name(name)
    if flat:
        return flat[1].strftime("%Y-%m")
    return previous_month_key()


def _ensure_month_archive_folder(historical_id: str, month_key: str) -> str:
    return ensure_historical_month_folder(historical_id, month_key)


def _move_to_month_archive_folder(
    file_id: str,
    *,
    historical_id: str,
    from_parent_id: str,
    name: str,
    mime_type: str = "",
    export_date: dt.date | None = None,
    modified_time: str = "",
) -> dict[str, str]:
    month_key = _month_key_for_item(
        name,
        export_date=export_date,
        modified_time=modified_time,
        mime_type=mime_type,
    )
    archive_folder_id = _ensure_month_archive_folder(historical_id, month_key)
    _move_drive_item(file_id, from_parent_id, archive_folder_id)
    return {
        "target": name,
        "month": month_key,
        "path": f"{HISTORICAL_DATA_FOLDER}/{month_key}/{name}",
    }


def _relocate_non_persistent_base_file(
    child: dict[str, Any],
    *,
    parent_id: str,
    historical_id: str,
) -> dict[str, str] | None:
    """Move or promote any non-``-persistent`` file out of an export base folder."""
    cid = str(child.get("id") or "")
    name = str(child.get("name") or "")
    mime = str(child.get("mimeType") or "")
    modified = str(child.get("modifiedTime") or "")
    if not cid or not name or is_persistent_export_name(name):
        return None
    from .export_drive_layout import is_output_root_static_filename

    if is_output_root_static_filename(name):
        return None

    persistent_name = target_persistent_name(name, mime_type=mime)
    if persistent_name and persistent_name != name:
        existing = dedupe_duplicate_names_in_folder(parent_id, persistent_name)
        if existing:
            archived = _move_to_month_archive_folder(
                cid,
                historical_id=historical_id,
                from_parent_id=parent_id,
                name=name,
                mime_type=mime,
                modified_time=modified,
            )
            return {
                "id": cid,
                "name": name,
                "action": "archived_duplicate",
                **archived,
            }
        rename_drive_file(cid, persistent_name)
        return {"id": cid, "name": name, "action": "promoted_persistent", "target": persistent_name}

    archived = _move_to_month_archive_folder(
        cid,
        historical_id=historical_id,
        from_parent_id=parent_id,
        name=name,
        mime_type=mime,
        modified_time=modified,
    )
    return {"id": cid, "name": name, "action": "archived", **archived}


def _flatten_legacy_container(
    *,
    container_id: str,
    container_name: str,
    historical_id: str,
    include_todays_dated: bool,
) -> tuple[list[dict[str, str]], bool]:
    moved: list[dict[str, str]] = []
    folder_day = dated_output_folder_date(container_name)
    for inner in _list_folder_children(container_id):
        inner_id = str(inner.get("id") or "")
        inner_name = str(inner.get("name") or "")
        inner_mime = str(inner.get("mimeType") or "")
        if not inner_id:
            continue
        archived = _move_to_month_archive_folder(
            inner_id,
            historical_id=historical_id,
            from_parent_id=container_id,
            name=inner_name,
            mime_type=inner_mime,
            export_date=folder_day,
            modified_time=str(inner.get("modifiedTime") or ""),
        )
        moved.append(
            {
                "id": inner_id,
                "name": inner_name,
                "from": container_name,
                **archived,
            }
        )
    trash_drive_file(container_id)
    return moved, True

def _migrate_legacy_containers_under_folder(
    folder_id: str,
    *,
    historical_id: str,
    context: str = "",
    today: dt.date | None = None,
    include_todays_dated: bool = False,
) -> tuple[list[dict[str, str]], list[str]]:
    """Flatten nested legacy ``{date} - Output`` folders into ``Historical Data/{YYYY-MM}/``."""
    moved: list[dict[str, str]] = []
    trashed: list[str] = []
    for child in _list_folder_children(folder_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid or mime != _MIME_FOLDER:
            continue
        if not _is_legacy_container_folder(
            name,
            today=today,
            include_todays_dated=include_todays_dated,
        ):
            continue
        inner_moved, _ = _flatten_legacy_container(
            container_id=cid,
            container_name=name,
            historical_id=historical_id,
            include_todays_dated=include_todays_dated,
        )
        moved.extend(inner_moved)
        trashed.append(name)
        logger.info(
            "Migrated nested legacy folder %s → %s/%s (%s)",
            name,
            HISTORICAL_DATA_FOLDER,
            inner_moved[0]["month"] if inner_moved else previous_month_key(today=today),
            context or folder_id[:12],
        )
    return moved, trashed


def _relocate_stray_base_month_folder(
    *,
    month_folder_id: str,
    month_name: str,
    parent_id: str,
    historical_id: str,
) -> list[dict[str, str]]:
    """Move a stray ``YYYY-MM`` bucket from the export base into ``Historical Data/``."""
    target_month_id = _ensure_month_archive_folder(historical_id, month_name)
    moved: list[dict[str, str]] = []
    for inner in _list_folder_children(month_folder_id):
        inner_id = str(inner.get("id") or "")
        inner_name = str(inner.get("name") or "")
        if not inner_id:
            continue
        _move_drive_item(inner_id, month_folder_id, target_month_id)
        moved.append(
            {
                "id": inner_id,
                "name": inner_name,
                "month": month_name,
                "path": f"{HISTORICAL_DATA_FOLDER}/{month_name}/{inner_name}",
            }
        )
    trash_drive_file(month_folder_id)
    return moved


def _flatten_legacy_container_into_historical(
    *,
    container_id: str,
    container_name: str,
    historical_id: str,
    include_todays_dated: bool,
) -> tuple[list[dict[str, str]], bool]:
    moved: list[dict[str, str]] = []
    folder_day = dated_output_folder_date(container_name)
    for inner in _list_folder_children(container_id):
        inner_id = str(inner.get("id") or "")
        inner_name = str(inner.get("name") or "")
        inner_mime = str(inner.get("mimeType") or "")
        if not inner_id:
            continue
        archived = _move_to_historical_day_folder(
            inner_id,
            from_parent_id=container_id,
            historical_root_id=historical_id,
            name=inner_name,
            mime_type=inner_mime,
            export_date=folder_day,
            modified_time=str(inner.get("modifiedTime") or ""),
        )
        moved.append(
            {
                "id": inner_id,
                "name": inner_name,
                "from": container_name,
                **archived,
            }
        )
    trash_drive_file(container_id)
    return moved, True


def normalize_loose_historical_data(
    parent_id: str,
    *,
    historical_id: str | None = None,
    context: str = "",
    today: dt.date | None = None,
    include_todays_dated: bool = True,
) -> dict[str, Any]:
    """Move legacy loose files at ``Historical Data/`` root into day subfolders (leave day folders)."""
    historical_id = historical_id or ensure_historical_data_folder(parent_id)
    reorganized: list[dict[str, str]] = []
    trashed_containers: list[str] = []

    for child in _list_folder_children(historical_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid:
            continue
        if mime == _MIME_FOLDER and is_historical_day_subfolder(name):
            continue
        if mime == _MIME_FOLDER and is_historical_month_subfolder(name):
            continue
        if mime == _MIME_FOLDER and _is_legacy_container_folder(
            name,
            today=today,
            include_todays_dated=include_todays_dated,
        ):
            inner_moved, _ = _flatten_legacy_container_into_historical(
                container_id=cid,
                container_name=name,
                historical_id=historical_id,
                include_todays_dated=include_todays_dated,
            )
            reorganized.extend(inner_moved)
            trashed_containers.append(name)
            continue
        if mime == _MIME_FOLDER:
            continue
        archived = _move_to_historical_day_folder(
            cid,
            from_parent_id=historical_id,
            historical_root_id=historical_id,
            name=name,
            mime_type=mime,
            modified_time=str(child.get("modifiedTime") or ""),
        )
        reorganized.append({"id": cid, "name": name, **archived})

    if reorganized:
        logger.info(
            "Normalized %d loose Historical Data item(s) into day subfolders (%s)",
            len(reorganized),
            context or parent_id[:12],
        )
    return {
        "historical_folder_id": historical_id,
        "reorganized": reorganized,
        "trashed_containers": trashed_containers,
    }


def consolidate_historical_data_to_monthly_archives(
    parent_id: str,
    *,
    historical_id: str | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Backward-compatible alias; day folders are preserved, only loose files are normalized."""
    return normalize_loose_historical_data(
        parent_id,
        historical_id=historical_id,
        context=context,
    )


def promote_legacy_exports_in_base(
    parent_id: str,
    *,
    historical_id: str | None = None,
    skip_folder_names: frozenset[str] | None = None,
    context: str = "",
    today: dt.date | None = None,
    portfolio_root: bool = False,
) -> dict[str, Any]:
    """Promote legacy base exports to ``-persistent`` and archive other non-persistent files."""
    historical_id = historical_id or ensure_historical_data_folder(parent_id)
    include_todays_dated = not portfolio_root
    moved: list[dict[str, str]] = []
    trashed: list[str] = []
    promoted: list[dict[str, str]] = []

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
            if is_allowed_export_base_subfolder(name, portfolio_root=portfolio_root):
                continue
            if is_historical_month_subfolder(name):
                stray = _relocate_stray_base_month_folder(
                    month_folder_id=cid,
                    month_name=name,
                    parent_id=parent_id,
                    historical_id=historical_id,
                )
                moved.extend(stray)
                trashed.append(name)
                logger.info(
                    "Relocated stray base monthly folder %s → %s/%s (%s)",
                    name,
                    HISTORICAL_DATA_FOLDER,
                    name,
                    context or parent_id[:12],
                )
                continue
            if not _is_legacy_container_folder(
                name,
                today=today,
                include_todays_dated=include_todays_dated,
            ):
                logger.warning(
                    "Unexpected subfolder %r under export base (%s); leaving in place",
                    name,
                    context or parent_id[:12],
                )
                continue
            inner_moved, _ = _flatten_legacy_container(
                container_id=cid,
                container_name=name,
                historical_id=historical_id,
                include_todays_dated=include_todays_dated,
            )
            moved.extend(inner_moved)
            trashed.append(name)
            logger.info(
                "Migrated legacy folder %s → %s/%s (%s)",
                name,
                HISTORICAL_DATA_FOLDER,
                inner_moved[0]["month"] if inner_moved else previous_month_key(today=today),
                context or parent_id[:12],
            )
            continue

        action = _relocate_non_persistent_base_file(
            child,
            parent_id=parent_id,
            historical_id=historical_id,
        )
        if not action:
            continue
        if action.get("action") == "promoted_persistent":
            promoted.append(action)
            logger.info(
                "Promoted legacy export %s → %s (%s)",
                name,
                action.get("target"),
                context or parent_id[:12],
            )
        else:
            moved.append(action)
            logger.info(
                "Archived legacy export %s → %s (%s)",
                name,
                action.get("path"),
                context or parent_id[:12],
            )

    return {
        "parent_id": parent_id,
        "moved": moved,
        "promoted": promoted,
        "trashed_folders": trashed,
    }


def _copy_drive_file_to_folder(file_id: str, *, name: str, parent_id: str) -> str:
    with drive_api_lock:
        drive = _get_drive()
        copied = (
            drive.files()
            .copy(fileId=file_id, body={"name": name, "parents": [parent_id]}, fields="id")
            .execute()
        )
        return str(copied["id"])


def ensure_persistent_exports_in_base(parent_id: str, historical_id: str) -> list[dict[str, str]]:
    """Create missing ``-persistent`` exports in base from newest historical/archive snapshots."""
    from .export_drive_layout import (
        _MIME_SPREADSHEET,
        export_stem_from_filename,
        persistent_filename,
        persistent_spreadsheet_title,
    )

    created: list[dict[str, str]] = []
    newest_md: dict[str, tuple[str, dict[str, Any]]] = {}
    newest_ss: dict[str, tuple[str, dict[str, Any]]] = {}

    def _consider(stem: str, month_key: str, child: dict[str, Any], *, spreadsheet: bool) -> None:
        bucket = newest_ss if spreadsheet else newest_md
        prev = bucket.get(stem)
        if not prev or month_key > prev[0]:
            bucket[stem] = (month_key, child)

    def _scan_archive_children(folder_id: str, month_key: str) -> None:
        for inner in _list_folder_children(folder_id):
            inner_name = str(inner.get("name") or "")
            inner_mime = str(inner.get("mimeType") or "")
            stem = export_stem_from_filename(inner_name, mime_type=inner_mime)
            if not stem:
                continue
            _consider(
                stem,
                month_key,
                inner,
                spreadsheet=inner_mime == _MIME_SPREADSHEET,
            )

    for child in _list_folder_children(parent_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if mime == _MIME_FOLDER and name == HISTORICAL_DATA_FOLDER:
            for hist_child in _list_folder_children(cid):
                hist_name = str(hist_child.get("name") or "")
                hist_mime = str(hist_child.get("mimeType") or "")
                hist_id = str(hist_child.get("id") or "")
                if hist_mime == _MIME_FOLDER and is_historical_day_subfolder(hist_name) and hist_id:
                    _scan_archive_children(hist_id, hist_name)
                elif hist_mime == _MIME_FOLDER and is_historical_month_subfolder(hist_name) and hist_id:
                    _scan_archive_children(hist_id, hist_name)
                elif hist_mime != _MIME_FOLDER:
                    stem = export_stem_from_filename(hist_name, mime_type=hist_mime)
                    if not stem:
                        continue
                    flat = parse_historical_flat_dated_name(hist_name)
                    month_key = (
                        flat[1].strftime("%Y-%m")
                        if flat
                        else modified_time_to_date(str(hist_child.get("modifiedTime") or ""))
                    )
                    if month_key:
                        if isinstance(month_key, dt.date):
                            month_key = month_key.strftime("%Y-%m")
                        _consider(stem, month_key, hist_child, spreadsheet=hist_mime == _MIME_SPREADSHEET)
            continue
        if mime == _MIME_FOLDER:
            continue
        stem = export_stem_from_filename(name, mime_type=mime)
        if not stem:
            continue
        flat = parse_historical_flat_dated_name(name)
        month_key = (
            flat[1].strftime("%Y-%m")
            if flat
            else (modified_time_to_date(str(child.get("modifiedTime") or "")) or dt.date.today()).strftime("%Y-%m")
        )
        _consider(stem, month_key, child, spreadsheet=mime == _MIME_SPREADSHEET)

    for stem, (_month, child) in newest_md.items():
        persistent_name = persistent_filename(stem, ext=".md")
        if dedupe_duplicate_names_in_folder(parent_id, persistent_name):
            continue
        new_id = _copy_drive_file_to_folder(
            str(child["id"]),
            name=persistent_name,
            parent_id=parent_id,
        )
        created.append({"stem": stem, "kind": "markdown", "id": new_id, "name": persistent_name})

    for stem, (_month, child) in newest_ss.items():
        persistent_name = persistent_spreadsheet_title(stem)
        if dedupe_duplicate_names_in_folder(parent_id, persistent_name):
            continue
        new_id = _copy_drive_file_to_folder(
            str(child["id"]),
            name=persistent_name,
            parent_id=parent_id,
        )
        created.append({"stem": stem, "kind": "spreadsheet", "id": new_id, "name": persistent_name})

    return created


def normalize_historical_data_folder(
    historical_id: str,
    *,
    parent_id: str,
    today: dt.date | None = None,
    include_todays_dated: bool = True,
) -> dict[str, Any]:
    """Backward-compatible alias for normalizing loose files under ``Historical Data/``."""
    return normalize_loose_historical_data(
        parent_id,
        historical_id=historical_id,
        context=HISTORICAL_DATA_FOLDER,
        today=today,
        include_todays_dated=include_todays_dated,
    )


def migrate_export_folder_to_historical_data(
    parent_id: str,
    *,
    skip_folder_names: frozenset[str] | None = None,
    context: str = "",
    today: dt.date | None = None,
    portfolio_root: bool = False,
) -> dict[str, Any]:
    """Repair one export base: promote persistent, consolidate legacy day buckets, archive prior month."""
    historical_id = ensure_historical_data_folder(parent_id)
    archive_month = previous_month_key(today=today)
    promoted_result = promote_legacy_exports_in_base(
        parent_id,
        historical_id=historical_id,
        skip_folder_names=skip_folder_names,
        context=context,
        today=today,
        portfolio_root=portfolio_root,
    )
    consolidated = normalize_loose_historical_data(
        parent_id,
        historical_id=historical_id,
        context=context,
    )
    archived = archive_previous_month_in_folder(
        parent_id,
        archive_month,
        historical_id=historical_id,
        skip_names=skip_folder_names,
        context=context,
    )
    archived_days = archive_previous_month_day_folders_in_historical_data(
        historical_id,
        archive_month,
        context=context,
    )
    persistent_created = ensure_persistent_exports_in_base(parent_id, historical_id)

    return {
        "parent_id": parent_id,
        "historical_folder_id": historical_id,
        "archive_month": archive_month,
        "moved": (
            (promoted_result.get("moved") or [])
            + (archived.get("moved") or [])
            + (archived_days.get("moved") or [])
        ),
        "promoted": promoted_result.get("promoted") or [],
        "trashed_folders": promoted_result.get("trashed_folders") or [],
        "historical_consolidated": consolidated,
        "archived_previous_month": archived,
        "archived_previous_month_day_folders": archived_days,
        "persistent_created": persistent_created,
    }


def repair_customer_export_drive_layout(customer: str) -> dict[str, Any]:
    """Repair one ``customer-exports/{customer}/`` folder (promote persistent, dedupe historical)."""
    folders = ensure_customer_export_folders(customer)
    parent_id = folders["persistent_folder_id"]
    result = migrate_export_folder_to_historical_data(
        parent_id,
        context=f"{_CUSTOMER_EXPORTS_FOLDER}/{customer}",
        portfolio_root=False,
    )
    return {"customer": customer, **result}


def archive_previous_month_in_folder(
    parent_id: str,
    archive_month: str,
    *,
    historical_id: str | None = None,
    skip_names: frozenset[str] | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Move prior-month export artifacts into ``Historical Data/{archive_month}/``."""
    historical_id = historical_id or ensure_historical_data_folder(parent_id)
    archive_folder_id = _ensure_month_archive_folder(historical_id, archive_month)
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
        _move_drive_item(str(child["id"]), parent_id, archive_folder_id)
        moved.append({"id": str(child["id"]), "name": name})
        logger.info(
            "Archived Drive export %s → %s/%s/%s (%s)",
            name,
            HISTORICAL_DATA_FOLDER,
            archive_month,
            name,
            context or parent_id[:12],
        )
    return {
        "parent_id": parent_id,
        "historical_folder_id": historical_id,
        "archive_month": archive_month,
        "moved": moved,
    }


def _historical_day_folder_belongs_to_month(day_folder_name: str, archive_month: str) -> bool:
    if not is_historical_day_subfolder(day_folder_name):
        return False
    return day_folder_name.startswith(f"{archive_month}-")


def archive_previous_month_day_folders_in_historical_data(
    historical_id: str,
    archive_month: str,
    *,
    context: str = "",
) -> dict[str, Any]:
    """Nest prior-month ``Historical Data/{YYYY-MM-DD}/`` folders under ``Historical Data/{YYYY-MM}/``."""
    archive_folder_id = _ensure_month_archive_folder(historical_id, archive_month)
    moved: list[dict[str, str]] = []
    for child in _list_folder_children(historical_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid or mime != _MIME_FOLDER:
            continue
        if not _historical_day_folder_belongs_to_month(name, archive_month):
            continue
        _move_drive_item(cid, historical_id, archive_folder_id)
        moved.append({"id": cid, "name": name})
        logger.info(
            "Archived Drive day folder %s → %s/%s/%s (%s)",
            name,
            HISTORICAL_DATA_FOLDER,
            archive_month,
            name,
            context or historical_id[:12],
        )
    return {
        "historical_folder_id": historical_id,
        "archive_month": archive_month,
        "moved": moved,
    }


def _archive_export_base_on_startup(
    parent_id: str,
    *,
    skip_folder_names: frozenset[str] | None = None,
    context: str = "",
    portfolio_root: bool = False,
    today: dt.date | None = None,
) -> dict[str, Any]:
    historical_id = ensure_historical_data_folder(parent_id)
    archive_month = previous_month_key(today=today)
    promoted_result = promote_legacy_exports_in_base(
        parent_id,
        historical_id=historical_id,
        skip_folder_names=skip_folder_names,
        context=context,
        today=today,
        portfolio_root=portfolio_root,
    )
    consolidated = normalize_loose_historical_data(
        parent_id,
        historical_id=historical_id,
        context=context,
    )
    archived = archive_previous_month_in_folder(
        parent_id,
        archive_month,
        historical_id=historical_id,
        skip_names=skip_folder_names,
        context=context,
    )
    archived_days = archive_previous_month_day_folders_in_historical_data(
        historical_id,
        archive_month,
        context=context,
    )
    return {
        "parent_id": parent_id,
        "archive_month": archive_month,
        "promoted": promoted_result.get("promoted") or [],
        "moved": (
            (promoted_result.get("moved") or [])
            + (archived.get("moved") or [])
            + (archived_days.get("moved") or [])
        ),
        "trashed_folders": promoted_result.get("trashed_folders") or [],
        "historical_consolidated": consolidated,
        "archived_previous_month": archived,
        "archived_previous_month_day_folders": archived_days,
    }


def maybe_migrate_export_layout_on_startup(*, force: bool = False) -> dict[str, Any]:
    """Promote persistent exports, normalize loose Historical Data files, archive prior month."""
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
        root_result = _archive_export_base_on_startup(
            root_id,
            skip_folder_names=frozenset({_CUSTOMER_EXPORTS_FOLDER, HISTORICAL_DATA_FOLDER}),
            context=QBR_OUTPUT_SUBFOLDER,
            portfolio_root=True,
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
                cust_result = _archive_export_base_on_startup(
                    str(customer_folder["id"]),
                    skip_folder_names=frozenset({HISTORICAL_DATA_FOLDER}),
                    context=f"{_CUSTOMER_EXPORTS_FOLDER}/{customer_name}",
                    portfolio_root=False,
                )
                summary["customer_exports"].append({"customer": customer_name, **cust_result})
                summary["moved_count"] += len(cust_result.get("moved") or [])
                summary["trashed_folder_count"] += len(cust_result.get("trashed_folders") or [])

        if summary["moved_count"] or summary["trashed_folder_count"]:
            logger.info(
                "Export monthly archive: moved %d file(s), removed %d legacy folder(s) under Drive %s",
                summary["moved_count"],
                summary["trashed_folder_count"],
                QBR_OUTPUT_SUBFOLDER,
            )
        else:
            logger.debug("Export monthly archive: nothing to move under Drive %s", QBR_OUTPUT_SUBFOLDER)
        return summary
    except Exception as e:
        logger.warning("Export layout migration failed (continuing): %s", e)
        return {"skipped": "error", "error": str(e)}


def maybe_archive_previous_month_exports(*, force: bool = False) -> dict[str, Any]:
    """Startup hook: promote persistent exports and archive prior month under Historical Data."""
    return maybe_migrate_export_layout_on_startup(force=force)
