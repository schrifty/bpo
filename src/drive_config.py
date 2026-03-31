"""Sync decks and slides between the local repo and Google Drive.

Strategy:
  1. On first run (or when --sync-config is passed), copy local YAML files
     to a "bpo-config/" subfolder in the configured Drive folder.
  2. On subsequent runs, read from Drive first.  If a Drive file fails to
     parse, fall back to the local version and log a QA warning.
  3. New local files that don't exist on Drive are uploaded automatically.

The Drive folder structure mirrors the local layout:
    <GOOGLE_DRIVE_FOLDER_ID>/
        bpo-config/
            decks/
                cs-health-review.yaml
                ...
            slides/
                std-01-title.yaml
                ...
"""

from __future__ import annotations

import io
import threading
from pathlib import Path
from typing import Any

import yaml
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from .config import GOOGLE_DRIVE_FOLDER_ID, logger
from .qa import qa

_CONFIG_ROOT_NAME = "bpo-config"

_drive_service = None
_drive_lock = threading.Lock()

_yaml_cache: dict[str, list[dict[str, Any]]] = {}
_yaml_cache_lock = threading.Lock()


def _get_drive():
    global _drive_service
    with _drive_lock:
        if _drive_service is None:
            from .slides_client import _get_service
            _x2, _drive_service, _sh2 = _get_service()
        return _drive_service


def _find_or_create_folder(name: str, parent_id: str | None = None) -> str:
    """Find a subfolder by name, or create it. Returns the folder ID."""
    drive = _get_drive()
    q = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"

    results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    meta: dict[str, Any] = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        meta["parents"] = [parent_id]
    folder = drive.files().create(body=meta, fields="id").execute()
    logger.info("Created Drive folder: %s (%s)", name, folder["id"])
    return folder["id"]


def find_file_in_folder(
    name: str,
    parent_id: str,
    mime_type: str | None = None,
) -> str | None:
    """Return the file id of the first non-trashed file with exact ``name`` under ``parent_id``."""
    drive = _get_drive()
    esc = name.replace("\\", "\\\\").replace("'", "\\'")
    q = f"name = '{esc}' and '{parent_id}' in parents and trashed = false"
    if mime_type:
        mt = mime_type.replace("'", "\\'")
        q += f" and mimeType = '{mt}'"
    results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def export_google_doc_as_plain_text(file_id: str, *, _max_retries: int = 5) -> str:
    """Export a Google Doc to UTF-8 plain text (retries on rate-limit errors)."""
    import random, time

    drive = _get_drive()
    last_err: HttpError | None = None
    for attempt in range(_max_retries):
        try:
            request = drive.files().export(fileId=file_id, mimeType="text/plain")
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return buf.getvalue().decode("utf-8", errors="replace")
        except HttpError as e:
            last_err = e
            status = getattr(e.resp, "status", 0)
            if status not in (403, 429) or attempt >= _max_retries - 1:
                raise
            delay = min(60.0, (2 ** attempt) + random.random())
            logger.warning("Drive export rate-limited (%s); retry %d/%d in %.1fs",
                           status, attempt + 1, _max_retries, delay)
            time.sleep(delay)
    raise last_err  # unreachable, but keeps type-checker happy


def _get_config_folder_ids() -> tuple[str, str, str]:
    """Return (root_id, decks_id, slides_id) for the config tree on Drive."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID not set — cannot sync config to Drive")
    root = _find_or_create_folder(_CONFIG_ROOT_NAME, GOOGLE_DRIVE_FOLDER_ID)
    decks = _find_or_create_folder("decks", root)
    slides = _find_or_create_folder("slides", root)
    return root, decks, slides


def _list_drive_files(folder_id: str) -> list[dict[str, str]]:
    """List YAML files in a Drive folder. Returns [{id, name}]."""
    drive = _get_drive()
    q = f"'{folder_id}' in parents and trashed = false and (name contains '.yaml' or name contains '.yml')"
    results = drive.files().list(q=q, fields="files(id, name, modifiedTime)", pageSize=200).execute()
    return results.get("files", [])


def _read_drive_file(file_id: str) -> str:
    """Download a Drive file as UTF-8 text."""
    drive = _get_drive()
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8")


def _upload_file(name: str, content: str, folder_id: str, file_id: str | None = None) -> str:
    """Upload or update a YAML file on Drive. Returns the file ID."""
    drive = _get_drive()
    media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/yaml")
    if file_id:
        f = drive.files().update(fileId=file_id, media_body=media).execute()
        return f["id"]
    meta: dict[str, Any] = {"name": name, "parents": [folder_id]}
    f = drive.files().create(body=meta, media_body=media, fields="id").execute()
    return f["id"]


# ── Public API ──

def sync_config_to_drive(
    decks_dir: str | Path | None = None,
    slides_dir: str | Path | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Push local decks and slides to Drive.

    By default only uploads files that don't already exist on Drive.
    Set overwrite=True to replace existing Drive files with local versions.
    """
    from .deck_loader import DEFAULT_DECKS_DIR
    from .slide_loader import DEFAULT_SLIDES_DIR

    d_dir = Path(decks_dir) if decks_dir else DEFAULT_DECKS_DIR
    s_dir = Path(slides_dir) if slides_dir else DEFAULT_SLIDES_DIR
    _, d_folder, s_folder = _get_config_folder_ids()

    stats = {"decks_uploaded": 0, "slides_uploaded": 0, "skipped": 0}

    for local_dir, drive_folder, label in [
        (d_dir, d_folder, "decks"),
        (s_dir, s_folder, "slides"),
    ]:
        existing = {f["name"]: f["id"] for f in _list_drive_files(drive_folder)}
        for f in sorted(local_dir.glob("*.yaml")):
            if f.name in existing and not overwrite:
                stats["skipped"] += 1
                continue
            content = f.read_text()
            fid = existing.get(f.name) if overwrite else None
            _upload_file(f.name, content, drive_folder, file_id=fid)
            stats[f"{label}_uploaded"] += 1
            logger.info("Uploaded %s/%s to Drive", label, f.name)

    return stats


def load_yaml_from_drive(
    kind: str,
    local_dir: Path,
) -> list[dict[str, Any]]:
    """Load YAML configs from Drive with fallback to local files.

    Results are cached after the first successful load — deck/slide definitions
    don't change between customers, so there's no reason to refetch them 150+
    times during a batch run.

    Args:
        kind: "decks" or "slides"
        local_dir: local directory to fall back to

    Returns:
        List of parsed dicts with a "_source" key indicating "drive" or "local".
    """
    if kind in _yaml_cache:
        return _yaml_cache[kind]

    with _yaml_cache_lock:
        if kind in _yaml_cache:
            return _yaml_cache[kind]
        result = _load_yaml_from_drive_uncached(kind, local_dir)
        _yaml_cache[kind] = result
        return result


def _load_yaml_from_drive_uncached(
    kind: str,
    local_dir: Path,
) -> list[dict[str, Any]]:
    """Actual Drive fetch logic (called once, then cached)."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        return _load_all_local(local_dir, kind)

    try:
        _, d_folder, s_folder = _get_config_folder_ids()
        folder_id = d_folder if kind == "decks" else s_folder
        drive_files = _list_drive_files(folder_id)
    except Exception as e:
        logger.warning("Could not reach Drive config folder for %s: %s — using local", kind, e)
        qa.flag(
            f"Drive {kind} unavailable, using local defaults",
            sources=("Google Drive", f"local {kind}/"),
            severity="warning",
            internal=True,
        )
        return _load_all_local(local_dir, kind)

    if not drive_files:
        logger.info("No %s on Drive yet — using local defaults", kind)
        return _load_all_local(local_dir, kind)

    results: list[dict[str, Any]] = []
    drive_names = set()

    for df in drive_files:
        drive_names.add(df["name"])
        try:
            text = _read_drive_file(df["id"])
            parsed = yaml.safe_load(text)
            if not isinstance(parsed, dict) or "id" not in parsed:
                raise ValueError(f"Missing 'id' field in {df['name']}")
            parsed["_source"] = "drive"
            parsed["_file"] = df["name"]
            results.append(parsed)
            qa.check()
        except Exception as e:
            logger.warning("Drive %s/%s failed to parse: %s — falling back to local", kind, df["name"], e)
            qa.flag(
                f"Drive {kind}/{df['name']} parse error — using local version",
                expected="valid YAML",
                actual=str(e)[:120],
                sources=(f"Drive {kind}/{df['name']}", f"local {kind}/{df['name']}"),
                internal=True,
                severity="error",
            )
            local = _load_single_local(local_dir / df["name"])
            if local:
                results.append(local)

    for f in sorted(local_dir.glob("*.yaml")):
        if f.name not in drive_names:
            local = _load_single_local(f)
            if local:
                qa.flag(
                    f"New local {kind}/{f.name} not yet on Drive",
                    sources=(f"local {kind}/{f.name}",),
                    severity="info",
                    auto_corrected=False,
                    internal=True,
                )
                results.append(local)

    return results


def _load_all_local(local_dir: Path, kind: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for f in sorted(local_dir.glob("*.yaml")):
        d = _load_single_local(f)
        if d:
            results.append(d)
    return results


def _load_single_local(path: Path) -> dict[str, Any] | None:
    try:
        raw = yaml.safe_load(path.read_text())
        if isinstance(raw, dict) and "id" in raw:
            raw["_source"] = "local"
            raw["_file"] = path.name
            return raw
    except Exception:
        pass
    return None
