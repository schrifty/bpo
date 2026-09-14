"""Prepare Drive export folders: enforce persistent-only export bases + monthly archives.

Export base folders (``Output/`` and ``Output/Customer Exports/{customer}/``) may contain
only ``-persistent`` export files and allowed subfolders (``Customer Exports``, ``Historical Data``).
Each export also writes a same-day snapshot under ``Historical Data/{YYYY-MM-DD}/`` (plain stem).
Prior-month base-folder exports are moved into ``Historical Data/{YYYY-MM}/`` via
:func:`archive_previous_month_in_folder` at startup. Prior-month day subfolders
under ``Historical Data/`` are nested under that same monthly bucket.
Months that are **two or more calendar months** behind ``today`` are then pruned:
keep exports generated on the **1st** of that month, permanently delete the 2nd through month-end
(e.g. on 1 Sep, delete 2 Jul–31 Jul and keep 1 Jul). Current and previous calendar
months are left intact.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any

from .config import logger
from .export_drive_layout import (
    CSR_DUMP_SOURCE_MARKER_FILENAME,
    HISTORICAL_DATA_FOLDER,
    PERSISTENT_SUFFIX,
    CUSTOMER_EXPORTS_FOLDER,
    _LEGACY_CUSTOMER_EXPORTS_FOLDER,
    _ARCHIVE_MONTH_RE,
    _DATED_OUTPUT_FOLDER_RE,
    _MIME_FOLDER,
    PORTFOLIO_DRIVE_FILENAME_RENAMES,
    dated_output_folder_date,
    ensure_customer_export_folders,
    ensure_customer_exports_parent_folder,
    ensure_historical_data_folder,
    ensure_historical_day_folder,
    ensure_historical_month_folder,
    historical_snapshot_day,
    is_allowed_export_base_subfolder,
    is_historical_day_subfolder,
    is_historical_month_subfolder,
    is_csr_customer_success_report_filename,
    is_historical_run_slot_subfolder,
    is_legacy_dated_output_folder,
    is_legacy_export_container_folder,
    is_output_root_metrics_deck_filename,
    is_output_root_resident_filename,
    is_persistent_export_name,
    modified_time_to_date,
    parse_csr_report_title_date,
    parse_historical_flat_dated_name,
    target_historical_snapshot_name,
    target_persistent_name,
)
from .drive_config import (
    CORTEX_DECKS_FOLDER,
    CORTEX_EXPORTS_CUSTOMER_FOLDER,
    CORTEX_EXPORTS_FOLDER,
    CORTEX_EXPORTS_HISTORY_FOLDER,
    CORTEX_SHARED_DRIVE_ID,
    QBR_OUTPUT_SUBFOLDER,
    _MIME_PRESENTATION,
    copy_drive_file_to_folder,
    dedupe_duplicate_names_in_folder,
    drive_api_lock,
    find_file_in_folder,
    move_drive_file,
    rename_drive_file,
    delete_drive_file,
)

_archive_ran = False


def clear_output_archive_guard() -> None:
    """Reset once-per-process guard (tests only)."""
    global _archive_ran
    _archive_ran = False


def previous_month_key(*, today: dt.date | None = None) -> str:
    """Return ``YYYY-MM`` for the calendar month before ``today``."""
    ref = today or dt.date.today()
    first_of_month = ref.replace(day=1)
    last_prev = first_of_month - dt.timedelta(days=1)
    return last_prev.strftime("%Y-%m")


# Keep the current month and the previous calendar month fully. Months older than
# that keep only the 1st-of-month snapshot (see ``should_trash_stale_non_first_export``).
STALE_EXPORT_FULL_RETENTION_MONTHS = 2


def calendar_months_between(later: dt.date, earlier: dt.date) -> int:
    """Whole calendar months from *earlier*'s month to *later*'s month (same month → 0)."""
    return (later.year - earlier.year) * 12 + (later.month - earlier.month)


def export_item_date(name: str, modified_time: str, *, mime_type: str) -> dt.date | None:
    """Best-effort generation date for an export artifact (folder or file)."""
    if mime_type == _MIME_FOLDER:
        dated = dated_output_folder_date(name)
        if dated is not None:
            return dated
        if is_historical_day_subfolder(name):
            try:
                return dt.date.fromisoformat(name)
            except ValueError:
                return None
        return None
    csr_day = parse_csr_report_title_date(name)
    if csr_day is not None:
        return csr_day
    flat = parse_historical_flat_dated_name(name)
    if flat is not None:
        return flat[1]
    return modified_time_to_date(modified_time)


def should_trash_stale_non_first_export(
    name: str,
    modified_time: str,
    *,
    mime_type: str,
    today: dt.date,
    skip_names: frozenset[str] | None = None,
) -> bool:
    """True when this artifact is from day 2+ of a month two or more months before *today*."""
    if skip_names and name in skip_names:
        return False
    if is_output_root_resident_filename(name):
        return False
    if is_historical_month_subfolder(name):
        return False
    when = export_item_date(name, modified_time, mime_type=mime_type)
    if when is None:
        return False
    if calendar_months_between(today, when) < STALE_EXPORT_FULL_RETENTION_MONTHS:
        return False
    return when.day != 1


def item_month_key(name: str, modified_time: str, *, mime_type: str) -> str | None:
    """Infer ``YYYY-MM`` for a Drive child; ``None`` when not parseable."""
    if mime_type == _MIME_FOLDER:
        dated = _DATED_OUTPUT_FOLDER_RE.match(name or "")
        if dated:
            return dated.group(1)[:7]
        if _ARCHIVE_MONTH_RE.match(name or ""):
            return None
    csr_day = parse_csr_report_title_date(name)
    if csr_day is not None:
        return csr_day.strftime("%Y-%m")
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
    if is_output_root_resident_filename(name):
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
        results = drive.files().list(
            q=q,
            fields="files(id)",
            pageSize=5,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
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
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    pageSize=200,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
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
            supportsAllDrives=True,
        ).execute()


def _delete_drive_item(file_id: str) -> None:
    delete_drive_file(file_id)


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
    if not cid or not name or is_output_root_resident_filename(name):
        return None
    if is_csr_customer_success_report_filename(name):
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
    delete_drive_file(container_id)
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
    delete_drive_file(month_folder_id)
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
    delete_drive_file(container_id)
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
        if mime == _MIME_FOLDER and is_historical_run_slot_subfolder(name):
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
        new_id = copy_drive_file_to_folder(
            str(child["id"]),
            name=persistent_name,
            parent_id=parent_id,
        )
        created.append({"stem": stem, "kind": "markdown", "id": new_id, "name": persistent_name})

    for stem, (_month, child) in newest_ss.items():
        persistent_name = persistent_spreadsheet_title(stem)
        if dedupe_duplicate_names_in_folder(parent_id, persistent_name):
            continue
        new_id = copy_drive_file_to_folder(
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


def restore_misplaced_output_root_metrics_decks(
    parent_id: str,
    *,
    historical_id: str,
    context: str = "",
) -> dict[str, Any]:
    """Move ``{TAG} Metrics`` decks back to Output if startup archive misplaced them.

    Historical copies must keep month titles (``AKKR Metrics - July``). Exact
    persistent names under ``Historical Data/{YYYY-MM}/`` are restored (newest
    wins) and extra duplicates are trashed.
    """
    restored: list[dict[str, str]] = []
    trashed: list[dict[str, str]] = []

    present_at_output = {
        str(child.get("name") or "")
        for child in _list_folder_children(parent_id)
        if is_output_root_metrics_deck_filename(str(child.get("name") or ""))
    }

    misplaced: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for child in _list_folder_children(historical_id):
        cid = str(child.get("id") or "")
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        if not cid:
            continue
        if mime == _MIME_FOLDER:
            if not is_historical_month_subfolder(name):
                continue
            for inner in _list_folder_children(cid):
                inner_name = str(inner.get("name") or "")
                if is_output_root_metrics_deck_filename(inner_name):
                    misplaced[inner_name].append({**inner, "_from_parent": cid})
            continue
        if is_output_root_metrics_deck_filename(name):
            misplaced[name].append({**child, "_from_parent": historical_id})

    for name, copies in misplaced.items():
        ordered = sorted(
            copies,
            key=lambda item: str(item.get("modifiedTime") or ""),
            reverse=True,
        )
        if name not in present_at_output:
            winner = ordered[0]
            move_drive_file(
                str(winner["id"]),
                from_parent_id=str(winner["_from_parent"]),
                to_parent_id=parent_id,
            )
            restored.append({"id": str(winner["id"]), "name": name})
            logger.info(
                "Restored metrics deck %s → Output/ (%s)",
                name,
                context or parent_id[:12],
            )
            leftovers = ordered[1:]
        else:
            leftovers = ordered
        for extra in leftovers:
            extra_id = str(extra.get("id") or "")
            if extra_id:
                delete_drive_file(extra_id)
                trashed.append({"id": extra_id, "name": name})
                logger.info(
                    "Removed duplicate misplaced metrics deck %s (%s)",
                    name,
                    context or parent_id[:12],
                )

    return {"restored": restored, "trashed": trashed}


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
    archived_days = archive_past_month_day_folders_in_historical_data(
        historical_id,
        today=today,
        context=context,
    )
    persistent_created = ensure_persistent_exports_in_base(parent_id, historical_id)
    metrics_restored: dict[str, Any] = {"restored": [], "trashed": []}
    if portfolio_root:
        metrics_restored = restore_misplaced_output_root_metrics_decks(
            parent_id,
            historical_id=historical_id,
            context=context,
        )

    return {
        "parent_id": parent_id,
        "historical_folder_id": historical_id,
        "archive_month": archive_month,
        "moved": (
            (promoted_result.get("moved") or [])
            + (archived.get("moved") or [])
            + (archived_days.get("moved") or [])
            + (metrics_restored.get("restored") or [])
        ),
        "promoted": promoted_result.get("promoted") or [],
        "trashed_folders": promoted_result.get("trashed_folders") or [],
        "historical_consolidated": consolidated,
        "archived_previous_month": archived,
        "archived_previous_month_day_folders": archived_days,
        "persistent_created": persistent_created,
        "metrics_decks": metrics_restored,
    }


def repair_customer_export_drive_layout(customer: str) -> dict[str, Any]:
    """Repair one ``Customer Exports/{customer}/`` folder (promote persistent, dedupe historical)."""
    folders = ensure_customer_export_folders(customer)
    parent_id = folders["persistent_folder_id"]
    result = migrate_export_folder_to_historical_data(
        parent_id,
        context=f"{CUSTOMER_EXPORTS_FOLDER}/{customer}",
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


def _folder_recency(folder: dict[str, Any]) -> str:
    """Recency key for a folder: prefer createdTime, fall back to modifiedTime."""
    return str(folder.get("createdTime") or folder.get("modifiedTime") or "")


def _folder_has_children(folder_id: str) -> bool:
    return bool(_list_folder_children(folder_id))


def _dedupe_keeper_rank(folder: dict[str, Any]) -> tuple[int, str]:
    """Rank same-name folders: keep the one with content, breaking ties by recency.

    Empty duplicate folders (accidental stubs) never win over a sibling that holds
    files, so cleanup can never trash the only copy that has data. Among folders
    that all have (or all lack) content, the most recently created wins.
    """
    fid = str(folder.get("id") or "")
    has_content = 1 if (fid and _folder_has_children(fid)) else 0
    return (has_content, _folder_recency(folder))


def find_child_folders_by_name(parent_id: str, name: str) -> list[dict[str, Any]]:
    """Return non-trashed subfolders of *parent_id* named exactly *name*, best keeper first."""
    matches = [
        child
        for child in _list_folder_children(parent_id)
        if str(child.get("mimeType") or "") == _MIME_FOLDER
        and str(child.get("name") or "") == name
    ]
    matches.sort(key=_dedupe_keeper_rank, reverse=True)
    return matches


def dedupe_child_folders_by_name(parent_id: str) -> list[dict[str, str]]:
    """Trash duplicate same-name subfolders under *parent_id*, keeping the best copy.

    Drive permits sibling folders to share a name; this enforces the invariant that
    a folder name is unique within its parent. The kept folder is the one with
    content (if any), and otherwise the most recently created — so a newer empty
    stub never replaces an older folder that still holds files.
    """
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for child in _list_folder_children(parent_id):
        if str(child.get("mimeType") or "") != _MIME_FOLDER:
            continue
        groups[str(child.get("name") or "")].append(child)

    trashed: list[dict[str, str]] = []
    for name, items in groups.items():
        if len(items) < 2:
            continue
        items.sort(key=_dedupe_keeper_rank, reverse=True)
        for stale in items[1:]:
            sid = str(stale.get("id") or "")
            if not sid:
                continue
            _delete_drive_item(sid)
            trashed.append({"id": sid, "name": name})
            logger.info("Deleted duplicate folder %s (%s) under %s", name, sid, parent_id[:12])
    return trashed


def archive_past_month_day_folders_in_historical_data(
    historical_id: str,
    *,
    today: dt.date | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Nest every past-month ``Historical Data/{YYYY-MM-DD}/`` folder under ``Historical Data/{YYYY-MM}/``.

    Only day folders for the **current** month stay at the ``Historical Data`` root;
    anything older is bucketed into its own month folder. Sweeping all past months
    (not just the previous one) keeps older day folders from being stranded at the
    root when the archive did not run during their month.
    """
    current_month = (today or dt.date.today()).strftime("%Y-%m")
    month_folders: dict[str, str] = {}
    moved: list[dict[str, str]] = []
    replaced: list[dict[str, str]] = []
    for child in _list_folder_children(historical_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid or mime != _MIME_FOLDER:
            continue
        if not is_historical_day_subfolder(name):
            continue
        month_key = name[:7]
        if month_key >= current_month:
            continue
        archive_folder_id = month_folders.get(month_key)
        if archive_folder_id is None:
            archive_folder_id = _ensure_month_archive_folder(historical_id, month_key)
            month_folders[month_key] = archive_folder_id
        _move_drive_item(cid, historical_id, archive_folder_id)
        moved.append({"id": cid, "name": name, "month": month_key})
        logger.info(
            "Archived Drive day folder %s → %s/%s/%s (%s)",
            name,
            HISTORICAL_DATA_FOLDER,
            month_key,
            name,
            context or historical_id[:12],
        )
    # Collapse same-name day folders now sharing an archive (content-aware keeper).
    for archive_folder_id in month_folders.values():
        replaced.extend(dedupe_child_folders_by_name(archive_folder_id))
    return {
        "historical_folder_id": historical_id,
        "archive_months": sorted(month_folders),
        "moved": moved,
        "replaced": replaced,
    }


def prune_stale_non_first_of_month_exports(
    parent_id: str,
    *,
    historical_id: str | None = None,
    today: dt.date | None = None,
    skip_names: frozenset[str] | None = None,
    context: str = "",
) -> dict[str, Any]:
    """Trash day-2+ exports from months two or more calendar months before *today*.

    On 1 Sep 2026 this keeps 1 Jul (and earlier firsts) and removes 2 Jul–31 Jul
    (and the same pattern for June, May, …). August and September stay complete.
    """
    ref = today or dt.date.today()
    hist_id = historical_id or ensure_historical_data_folder(parent_id)
    skip = set(skip_names or ())
    skip.update({HISTORICAL_DATA_FOLDER, CUSTOMER_EXPORTS_FOLDER, _LEGACY_CUSTOMER_EXPORTS_FOLDER})
    skip_frozen = frozenset(skip)
    trashed: list[dict[str, str]] = []

    def _maybe_trash(child: dict[str, Any], *, folder_id: str) -> None:
        cid = str(child.get("id") or "")
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        modified = str(child.get("modifiedTime") or "")
        if not cid or not name:
            return
        if not should_trash_stale_non_first_export(
            name,
            modified,
            mime_type=mime,
            today=ref,
            skip_names=skip_frozen,
        ):
            return
        _delete_drive_item(cid)
        trashed.append({"id": cid, "name": name, "from": folder_id})
        logger.info(
            "Deleted stale export %s (kept 1st-of-month only for months ≥%d old) (%s)",
            name,
            STALE_EXPORT_FULL_RETENTION_MONTHS,
            context or folder_id[:12],
        )

    for child in _list_folder_children(parent_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        if name in skip_frozen:
            continue
        _maybe_trash(child, folder_id=parent_id)

    for child in _list_folder_children(hist_id):
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        cid = str(child.get("id") or "")
        if not cid:
            continue
        if mime == _MIME_FOLDER and is_historical_month_subfolder(name):
            try:
                month_first = dt.date.fromisoformat(f"{name}-01")
            except ValueError:
                continue
            if calendar_months_between(ref, month_first) < STALE_EXPORT_FULL_RETENTION_MONTHS:
                continue
            for nested in _list_folder_children(cid):
                _maybe_trash(nested, folder_id=cid)
            continue
        _maybe_trash(child, folder_id=hist_id)

    return {
        "parent_id": parent_id,
        "historical_folder_id": hist_id,
        "trashed": trashed,
    }


def _archive_export_base_on_startup(
    parent_id: str,
    *,
    skip_folder_names: frozenset[str] | None = None,
    context: str = "",
    portfolio_root: bool = False,
    today: dt.date | None = None,
    historical_id: str | None = None,
) -> dict[str, Any]:
    historical_id = historical_id or ensure_historical_data_folder(parent_id)
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
    archived_days = archive_past_month_day_folders_in_historical_data(
        historical_id,
        today=today,
        context=context,
    )
    pruned = prune_stale_non_first_of_month_exports(
        parent_id,
        historical_id=historical_id,
        today=today,
        skip_names=skip_folder_names,
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
        "trashed_stale": pruned.get("trashed") or [],
        "historical_consolidated": consolidated,
        "archived_previous_month": archived,
        "archived_previous_month_day_folders": archived_days,
        "pruned_stale_non_first": pruned,
    }


def _summarize_archive_walk(
    *,
    root_id: str,
    customer_parent_id: str,
    skip_folder_names: frozenset[str],
    context: str,
    historical_id: str | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "output_root": None,
        "customer_exports": [],
        "moved_count": 0,
        "trashed_folder_count": 0,
        "trashed_stale_count": 0,
    }
    root_result = _archive_export_base_on_startup(
        root_id,
        skip_folder_names=skip_folder_names,
        context=context,
        portfolio_root=True,
        historical_id=historical_id,
    )
    summary["output_root"] = root_result
    summary["moved_count"] += len(root_result.get("moved") or [])
    summary["trashed_folder_count"] += len(root_result.get("trashed_folders") or [])
    summary["trashed_stale_count"] += len(root_result.get("trashed_stale") or [])

    if customer_parent_id:
        for customer_folder in _list_folder_children(customer_parent_id):
            if str(customer_folder.get("mimeType") or "") != _MIME_FOLDER:
                continue
            customer_name = str(customer_folder.get("name") or "")
            if not customer_name:
                continue
            cust_result = _archive_export_base_on_startup(
                str(customer_folder["id"]),
                skip_folder_names=frozenset({HISTORICAL_DATA_FOLDER}),
                context=f"{context}/{customer_name}",
                portfolio_root=False,
            )
            summary["customer_exports"].append({"customer": customer_name, **cust_result})
            summary["moved_count"] += len(cust_result.get("moved") or [])
            summary["trashed_folder_count"] += len(cust_result.get("trashed_folders") or [])
            summary["trashed_stale_count"] += len(cust_result.get("trashed_stale") or [])
    return summary


def _move_folder_children(src_id: str, dest_id: str) -> list[dict[str, str]]:
    moved: list[dict[str, str]] = []
    if src_id == dest_id:
        return moved
    for child in _list_folder_children(src_id):
        cid = str(child.get("id") or "")
        if not cid:
            continue
        _move_drive_item(cid, src_id, dest_id)
        moved.append({"id": cid, "name": str(child.get("name") or "")})
    return moved


def _relocate_named_folder(
    *,
    src_parent_id: str,
    src_names: tuple[str, ...],
    dest_parent_id: str,
    dest_name: str,
) -> dict[str, Any]:
    """Move or merge a named folder into dest_parent/dest_name."""
    src_id = None
    src_name = None
    for name in src_names:
        found = find_file_in_folder(name, src_parent_id, mime_type=_MIME_FOLDER)
        if found:
            src_id = found
            src_name = name
            break
    dest_id = find_file_in_folder(dest_name, dest_parent_id, mime_type=_MIME_FOLDER)
    if not src_id:
        return {"moved": [], "src": src_name, "dest_id": dest_id, "action": "missing"}
    if src_id == dest_id:
        return {"moved": [], "src": src_name, "dest_id": dest_id, "action": "already"}
    if dest_id:
        moved = _move_folder_children(src_id, dest_id)
        if not _list_folder_children(src_id):
            delete_drive_file(src_id)
        return {"moved": moved, "src": src_name, "dest_id": dest_id, "action": "merged"}
    _move_drive_item(src_id, src_parent_id, dest_parent_id)
    if src_name != dest_name:
        rename_drive_file(src_id, dest_name)
    return {"moved": [{"id": src_id, "name": dest_name}], "src": src_name, "dest_id": src_id, "action": "relocated"}


def _rename_drive_titles_in_folder(folder_id: str) -> list[dict[str, str]]:
    """Apply :data:`PORTFOLIO_DRIVE_FILENAME_RENAMES` in one folder."""
    changed: list[dict[str, str]] = []
    for old_name, new_name in PORTFOLIO_DRIVE_FILENAME_RENAMES:
        old_id = find_file_in_folder(old_name, folder_id)
        if not old_id:
            continue
        new_id = find_file_in_folder(new_name, folder_id)
        if new_id and new_id != old_id:
            delete_drive_file(old_id)
            changed.append({"action": "dropped_legacy", "name": old_name, "kept": new_name})
            continue
        rename_drive_file(old_id, new_name)
        changed.append({"action": "renamed", "from": old_name, "to": new_name, "id": old_id})
        logger.info("Renamed Drive file %r → %r", old_name, new_name)
    return changed


def rename_portfolio_export_drive_titles(
    *,
    root_id: str,
    historical_id: str | None = None,
) -> list[dict[str, str]]:
    """Rename persistent/historical portfolio files (LLM context + user guide) in place."""
    changed = _rename_drive_titles_in_folder(root_id)
    if not historical_id:
        return changed
    changed.extend(_rename_drive_titles_in_folder(historical_id))
    for child in _list_folder_children(historical_id):
        if str(child.get("mimeType") or "") != _MIME_FOLDER:
            continue
        cid = str(child.get("id") or "")
        if not cid:
            continue
        changed.extend(_rename_drive_titles_in_folder(cid))
        for nested in _list_folder_children(cid):
            if str(nested.get("mimeType") or "") != _MIME_FOLDER:
                continue
            nid = str(nested.get("id") or "")
            if nid:
                changed.extend(_rename_drive_titles_in_folder(nid))
    return changed


def _relocate_named_file(
    *,
    src_parent_id: str,
    dest_parent_id: str,
    name: str,
) -> dict[str, Any]:
    """Move a file into dest_parent, replacing a same-name dest if needed."""
    src_id = find_file_in_folder(name, src_parent_id)
    if not src_id:
        return {"action": "missing", "name": name}
    if src_parent_id == dest_parent_id:
        return {"action": "already", "id": src_id, "name": name}
    dest_id = find_file_in_folder(name, dest_parent_id)
    if dest_id and dest_id != src_id:
        delete_drive_file(dest_id)
    _move_drive_item(src_id, src_parent_id, dest_parent_id)
    return {"action": "relocated", "id": src_id, "name": name}


def migrate_cortex_shared_drive_layout() -> dict[str, Any]:
    """Move Cortex shared-drive ``Output/`` artifacts into ``exports/`` and ``decks/``.

    Idempotent. QBR Generator ``Output/`` is not touched.
    """
    from .drive_config import (
        CORTEX_SYS_CACHE_FOLDER,
        CORTEX_SYS_CHART_DATA_FOLDER,
        _cortex_output_dual_write_enabled,
        get_cortex_decks_folder_id,
        get_cortex_exports_customer_folder_id,
        get_cortex_exports_history_folder_id,
        get_cortex_exports_root_folder_id,
        get_cortex_sys_folder_id,
    )

    if not _cortex_output_dual_write_enabled():
        return {"skipped": "dual_write_off"}
    exports_id = get_cortex_exports_root_folder_id()
    customer_id = get_cortex_exports_customer_folder_id()
    history_id = get_cortex_exports_history_folder_id()
    decks_id = get_cortex_decks_folder_id()
    sys_id = get_cortex_sys_folder_id()
    if not exports_id or not customer_id or not history_id or not decks_id:
        return {"skipped": "cortex_folders_unresolved"}

    result: dict[str, Any] = {
        "exports_id": exports_id,
        "customer_id": customer_id,
        "history_id": history_id,
        "decks_id": decks_id,
        "presentations": [],
        "export_files": [],
        "customer": None,
        "history": None,
        "cache": None,
        "chart_data": None,
        "output_deleted": False,
    }
    if sys_id:
        result["cache"] = _relocate_named_folder(
            src_parent_id=CORTEX_SHARED_DRIVE_ID,
            src_names=("Cache",),
            dest_parent_id=sys_id,
            dest_name=CORTEX_SYS_CACHE_FOLDER,
        )
        result["chart_data"] = _relocate_named_folder(
            src_parent_id=CORTEX_SHARED_DRIVE_ID,
            src_names=("chart-data",),
            dest_parent_id=sys_id,
            dest_name=CORTEX_SYS_CHART_DATA_FOLDER,
        )
        result["csr_dump_source"] = _relocate_named_file(
            src_parent_id=exports_id,
            dest_parent_id=sys_id,
            name=CSR_DUMP_SOURCE_MARKER_FILENAME,
        )

    output_id = find_file_in_folder(
        QBR_OUTPUT_SUBFOLDER, CORTEX_SHARED_DRIVE_ID, mime_type=_MIME_FOLDER
    )
    if not output_id:
        return {**result, "skipped": "no_legacy_output"}

    result["customer"] = _relocate_named_folder(
        src_parent_id=output_id,
        src_names=(CUSTOMER_EXPORTS_FOLDER, _LEGACY_CUSTOMER_EXPORTS_FOLDER),
        dest_parent_id=exports_id,
        dest_name=CORTEX_EXPORTS_CUSTOMER_FOLDER,
    )
    result["history"] = _relocate_named_folder(
        src_parent_id=output_id,
        src_names=(HISTORICAL_DATA_FOLDER,),
        dest_parent_id=exports_id,
        dest_name=CORTEX_EXPORTS_HISTORY_FOLDER,
    )

    dated_empty: list[str] = []
    for child in _list_folder_children(output_id):
        cid = str(child.get("id") or "")
        name = str(child.get("name") or "")
        mime = str(child.get("mimeType") or "")
        if not cid:
            continue
        if mime == _MIME_PRESENTATION:
            _move_drive_item(cid, output_id, decks_id)
            result["presentations"].append({"id": cid, "name": name, "from": "Output"})
            continue
        if mime == _MIME_FOLDER and dated_output_folder_date(name) is not None:
            leftover = False
            for nested in _list_folder_children(cid):
                nid = str(nested.get("id") or "")
                nname = str(nested.get("name") or "")
                nmime = str(nested.get("mimeType") or "")
                if not nid:
                    continue
                if nmime == _MIME_PRESENTATION:
                    _move_drive_item(nid, cid, decks_id)
                    result["presentations"].append(
                        {"id": nid, "name": nname, "from": name}
                    )
                else:
                    _move_drive_item(nid, cid, exports_id)
                    result["export_files"].append(
                        {"id": nid, "name": nname, "from": name}
                    )
                    leftover = True
            if not _list_folder_children(cid):
                delete_drive_file(cid)
                dated_empty.append(cid)
            elif leftover:
                dated_empty.append(cid)
            continue
        if mime == _MIME_FOLDER:
            continue
        _move_drive_item(cid, output_id, exports_id)
        result["export_files"].append({"id": cid, "name": name, "from": "Output"})

    result["dated_folders_removed"] = dated_empty
    if not _list_folder_children(output_id):
        delete_drive_file(output_id)
        result["output_deleted"] = True

    logger.info(
        "Cortex Drive layout: moved %d presentation(s) to decks/, %d export file(s) to exports/",
        len(result["presentations"]),
        len(result["export_files"]),
    )
    return result


def maybe_migrate_export_layout_on_startup(*, force: bool = False) -> dict[str, Any]:
    """Promote persistent exports, normalize loose Historical Data files, archive prior month."""
    global _archive_ran
    if _archive_ran and not force:
        return {"skipped": "already_ran"}
    _archive_ran = True

    from .drive_config import (
        get_cortex_exports_customer_folder_id,
        get_cortex_exports_history_folder_id,
        get_cortex_exports_root_folder_id,
        get_qbr_output_root_folder_id,
    )

    try:
        migrate_cortex_shared_drive_layout()
    except Exception as e:
        logger.error("Cortex shared-drive layout migration failed: %s", e)

    root_id = get_qbr_output_root_folder_id()
    if not root_id:
        logger.debug("Export layout migration: no Drive Output folder configured")
        return {"skipped": "no_output_folder"}

    try:
        qbr_hist = ensure_historical_data_folder(root_id)
        rename_portfolio_export_drive_titles(root_id=root_id, historical_id=qbr_hist)
    except Exception as e:
        logger.warning("Portfolio Drive title rename failed (continuing): %s", e)

    try:
        customer_exports_id = ensure_customer_exports_parent_folder(root_id)
        combined = _summarize_archive_walk(
            root_id=root_id,
            customer_parent_id=customer_exports_id,
            skip_folder_names=frozenset({
                CUSTOMER_EXPORTS_FOLDER,
                _LEGACY_CUSTOMER_EXPORTS_FOLDER,
                HISTORICAL_DATA_FOLDER,
            }),
            context=QBR_OUTPUT_SUBFOLDER,
        )
        if combined["moved_count"] or combined["trashed_folder_count"] or combined["trashed_stale_count"]:
            logger.info(
                "Export monthly archive: moved %d file(s), removed %d legacy folder(s), "
                "deleted %d stale day-2+ snapshot(s) under Drive %s",
                combined["moved_count"],
                combined["trashed_folder_count"],
                combined["trashed_stale_count"],
                QBR_OUTPUT_SUBFOLDER,
            )
        else:
            logger.debug(
                "Export monthly archive: nothing to move under Drive %s",
                QBR_OUTPUT_SUBFOLDER,
            )
    except Exception as e:
        logger.warning("Export layout migration failed (continuing): %s", e)
        return {"skipped": "error", "error": str(e)}

    try:
        cortex_exports = get_cortex_exports_root_folder_id()
        cortex_customer = get_cortex_exports_customer_folder_id()
        cortex_history = get_cortex_exports_history_folder_id()
        if cortex_exports and cortex_customer and cortex_history:
            rename_portfolio_export_drive_titles(
                root_id=cortex_exports,
                historical_id=cortex_history,
            )
            _summarize_archive_walk(
                root_id=cortex_exports,
                customer_parent_id=cortex_customer,
                skip_folder_names=frozenset({
                    CORTEX_EXPORTS_CUSTOMER_FOLDER,
                    CORTEX_EXPORTS_HISTORY_FOLDER,
                    CORTEX_DECKS_FOLDER,
                    CUSTOMER_EXPORTS_FOLDER,
                    HISTORICAL_DATA_FOLDER,
                }),
                context=f"{CORTEX_EXPORTS_FOLDER}",
                historical_id=cortex_history,
            )
    except Exception as e:
        logger.error("Cortex dual-write export layout migration failed: %s", e)
    return combined


def maybe_archive_previous_month_exports(*, force: bool = False) -> dict[str, Any]:
    """Startup hook: promote persistent exports and archive prior month under Historical Data."""
    return maybe_migrate_export_layout_on_startup(force=force)
