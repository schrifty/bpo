"""Drive intake helpers for evaluate/hydrate flows."""

from __future__ import annotations

import io
from typing import Any, Callable

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from .config import GOOGLE_HYDRATE_INTAKE_GROUP, GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .slides_api import _get_service

PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
GSLIDES_MIME = "application/vnd.google-apps.presentation"

PrintFunc = Callable[..., None]


def _default_print(*args, **kwargs) -> None:
    print(*args, **{**kwargs, "flush": kwargs.pop("flush", True)})


def drive_query_escape(value: str) -> str:
    """Escape a string for use inside single quotes in Drive API `q` queries."""
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def convert_pptx_to_slides(
    drive,
    file_id: str,
    name: str,
    folder_id: str,
    *,
    print_func: PrintFunc = _default_print,
) -> tuple[str, str]:
    """Copy a .pptx file into the same folder as a native Google Slides presentation."""
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    base_name = name.rsplit(".", 1)[0]
    media = MediaIoBaseUpload(fh, mimetype=PPTX_MIME, resumable=True)
    converted = drive.files().create(
        body={
            "name": base_name,
            "mimeType": GSLIDES_MIME,
            "parents": [folder_id],
        },
        media_body=media,
        fields="id,name",
    ).execute()
    print_func(f"Converted '{name}' → Google Slides '{base_name}' (id: {converted['id']})")
    return converted["id"], base_name


def parent_folder_for_file(drive, file_id: str) -> str | None:
    """First parent folder id for a Drive file."""
    try:
        meta = drive.files().get(fileId=file_id, fields="parents").execute()
        parents = meta.get("parents") or []
        return parents[0] if parents else None
    except Exception as e:
        logger.warning("Could not read parents for file %s: %s", file_id, e)
        return None


def file_has_group_permission(drive, file_id: str, group_email_lower: str) -> bool:
    """True if ``permissions.list`` includes the intake group."""
    page_token: str | None = None
    try:
        while True:
            resp = drive.permissions().list(
                fileId=file_id,
                fields="nextPageToken, permissions(emailAddress,deleted)",
                pageSize=100,
                pageToken=page_token,
            ).execute()
            for perm in resp.get("permissions", []):
                if perm.get("deleted"):
                    continue
                addr = (perm.get("emailAddress") or "").strip().lower()
                if addr == group_email_lower:
                    return True
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.debug("permissions.list failed for file %s: %s", file_id, e)
    return False


def intake_entries_from_drive_file(
    drive,
    file_row: dict[str, Any],
    *,
    print_func: PrintFunc = _default_print,
) -> list[dict[str, str]]:
    """Turn a Drive ``files.list`` row into zero or one intake presentation dict(s)."""
    mime = file_row.get("mimeType", "")
    out: list[dict[str, str]] = []
    if mime == GSLIDES_MIME:
        out.append({"id": file_row["id"], "name": file_row["name"]})
    elif mime == PPTX_MIME:
        parent = parent_folder_for_file(drive, file_row["id"])
        if not parent and GOOGLE_QBR_GENERATOR_FOLDER_ID:
            parent = GOOGLE_QBR_GENERATOR_FOLDER_ID
        if not parent:
            print_func(
                f"Skipping PPTX '{file_row['name']}' (no parent folder; share as Google Slides or set GOOGLE_QBR_GENERATOR_FOLDER_ID)."
            )
            return []
        try:
            new_id, new_name = convert_pptx_to_slides(
                drive, file_row["id"], file_row["name"], parent, print_func=print_func
            )
            out.append({"id": new_id, "name": new_name})
        except Exception as e:
            print_func(f"Could not convert '{file_row['name']}' to Google Slides: {e}")
    elif mime == "application/vnd.google-apps.shortcut":
        target = file_row.get("shortcutDetails", {})
        if target.get("targetMimeType") == GSLIDES_MIME:
            out.append({"id": target["targetId"], "name": file_row["name"]})
    return out


def list_presentations_shared_with_group(
    group_email: str,
    *,
    print_func: PrintFunc = _default_print,
) -> list[dict[str, str]]:
    """List Slides/PPTX/shortcuts where the intake group has access."""
    group = (group_email or "").strip()
    if not group:
        return []

    _x, drive, _sh = _get_service()
    esc = drive_query_escape(group)
    q_search = (
        f"(mimeType = '{GSLIDES_MIME}' or mimeType = '{PPTX_MIME}' "
        "or mimeType = 'application/vnd.google-apps.shortcut') "
        f"and ('{esc}' in readers or '{esc}' in writers) and trashed = false"
    )
    list_kw: dict[str, Any] = {
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }

    presentations: list[dict[str, str]] = []
    page_token: str | None = None
    try:
        while True:
            req = drive.files().list(
                q=q_search,
                fields="nextPageToken, files(id, name, mimeType, shortcutDetails)",
                pageSize=100,
                pageToken=page_token,
                **list_kw,
            )
            results = req.execute()
            for file_row in results.get("files", []):
                presentations.extend(intake_entries_from_drive_file(drive, file_row, print_func=print_func))
            page_token = results.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.warning("Drive query for group-shared presentations failed: %s", e)
        print_func(
            f"Could not list files shared with group '{group}': {e}\n"
            "Check GOOGLE_HYDRATE_INTAKE_GROUP (must match the group address exactly), Drive API access, "
            "and that the runner can see files shared with that group (Viewer or Editor)."
        )
        return []

    if not presentations:
        presentations = fallback_intake_presentations_by_group_permission(drive, group, list_kw, print_func=print_func)

    if not presentations:
        logger.info("intake group scan: no presentations shared with group %s", group)
    else:
        logger.info(
            "intake group scan: %d presentation(s) shared with group %s",
            len(presentations),
            group,
        )
    return presentations


def fallback_intake_presentations_by_group_permission(
    drive,
    group_email: str,
    list_kw: dict[str, Any],
    *,
    print_func: PrintFunc = _default_print,
) -> list[dict[str, str]]:
    """List recent presentation files and keep files whose ACL includes the intake group."""
    group_lower = group_email.strip().lower()
    q_broad = (
        f"(mimeType = '{GSLIDES_MIME}' or mimeType = '{PPTX_MIME}' "
        "or mimeType = 'application/vnd.google-apps.shortcut') "
        "and trashed = false"
    )
    out: list[dict[str, str]] = []
    page_token: str | None = None
    checked = 0
    max_files_to_scan = 500

    try:
        while checked < max_files_to_scan:
            results = drive.files().list(
                q=q_broad,
                fields="nextPageToken, files(id, name, mimeType, shortcutDetails)",
                pageSize=100,
                pageToken=page_token,
                orderBy="modifiedTime desc",
                **list_kw,
            ).execute()
            files = results.get("files", [])
            if not files:
                break
            for file_row in files:
                if checked >= max_files_to_scan:
                    break
                checked += 1
                fid = file_row.get("id")
                if not fid or not file_has_group_permission(drive, fid, group_lower):
                    continue
                out.extend(intake_entries_from_drive_file(drive, file_row, print_func=print_func))
            page_token = results.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.warning("intake permission fallback failed: %s", e)

    logger.info(
        "intake group scan: checked %d recent file(s), %d presentation(s) shared with group %s",
        min(checked, max_files_to_scan),
        len(out),
        group_email,
    )
    return out


def log_intake_decks_for_run(queue: list[dict[str, Any]], *, log_prefix: str) -> None:
    """Log each presentation that will be processed."""
    for item in queue:
        group = item.get("group_email") or GOOGLE_HYDRATE_INTAKE_GROUP or ""
        logger.debug(
            "%s: deck %r id=%s — shared with group %s",
            log_prefix,
            item["name"],
            item["id"],
            group,
        )


def collect_hydrate_intake_presentations(
    *,
    log_prefix: str = "intake",
    print_func: PrintFunc = _default_print,
) -> tuple[list[dict[str, Any]], str | None]:
    """List presentations shared with ``GOOGLE_HYDRATE_INTAKE_GROUP``."""
    if not GOOGLE_HYDRATE_INTAKE_GROUP:
        return [], (
            "Set GOOGLE_HYDRATE_INTAKE_GROUP in .env to your intake Google Group email "
            "(decks shared with that group as Reader are processed)."
        )

    raw = list_presentations_shared_with_group(GOOGLE_HYDRATE_INTAKE_GROUP, print_func=print_func)
    if not raw:
        return [], f"No presentations found shared with group {GOOGLE_HYDRATE_INTAKE_GROUP}."

    group = GOOGLE_HYDRATE_INTAKE_GROUP
    merged: list[dict[str, Any]] = [
        {"id": item["id"], "name": item["name"], "intake": "group", "group_email": group}
        for item in raw
    ]
    log_intake_decks_for_run(merged, log_prefix=log_prefix)
    return merged, None


def remove_intake_group_permission_from_file(drive_svc, file_id: str, group_email: str) -> int:
    """Remove Drive ACL entries for ``group_email`` on ``file_id``."""
    group = (group_email or "").strip().lower()
    if not group:
        return 0
    removed = 0
    try:
        page_token: str | None = None
        while True:
            req = drive_svc.permissions().list(
                fileId=file_id,
                fields="nextPageToken, permissions(id,emailAddress,type,role)",
                supportsAllDrives=True,
                pageSize=100,
                pageToken=page_token,
            )
            resp = req.execute()
            for permission in resp.get("permissions", []):
                addr = (permission.get("emailAddress") or "").strip().lower()
                if addr != group:
                    continue
                permission_id = permission.get("id")
                if not permission_id:
                    continue
                drive_svc.permissions().delete(
                    fileId=file_id,
                    permissionId=permission_id,
                    supportsAllDrives=True,
                ).execute()
                removed += 1
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.warning(
            "hydrate: failed removing intake group %s from file %s: %s",
            group_email,
            file_id,
            e,
        )
    return removed
