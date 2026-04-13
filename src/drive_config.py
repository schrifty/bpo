"""Sync decks and slides between the local repo and Google Drive.

Strategy:
  1. On first run (or when --sync-config is passed), copy local YAML files
     to a "bpo-config/" subfolder in the configured Drive folder.
  2. On subsequent runs, read from Drive first.  If a Drive file fails to
     parse, fall back to the local version and log a QA warning.
  3. New local files that don't exist on Drive are uploaded automatically.
  4. Before the first load of ``bpo-config`` from Drive in a process, the repo
     is pushed to Drive for any YAML that differs or is missing (see
     ``ensure_drive_config_matches_repo``) so git and Drive stay aligned.

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
# Serialize all googleapiclient Drive HTTP — the shared Resource/httplib2 stack is not thread-safe.
drive_api_lock = threading.RLock()

_yaml_cache: dict[str, list[dict[str, Any]]] = {}
_yaml_cache_lock = threading.Lock()

# Set by ensure_drive_config_matches_repo (at most once per process).
_drive_repo_sync_ran = False


def _get_drive():
    global _drive_service
    with _drive_lock:
        if _drive_service is None:
            from .slides_api import _get_service
            _x2, _drive_service, _sh2 = _get_service()
        return _drive_service


def _drive_q_escape(value: str) -> str:
    """Escape a value for use in a single-quoted Drive ``files.list`` query string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find_or_create_folder(name: str, parent_id: str | None = None) -> str:
    """Find a subfolder by name, or create it. Returns the folder ID."""
    with drive_api_lock:
        drive = _get_drive()
        esc = _drive_q_escape(name)
        q = f"name = '{esc}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
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
    with drive_api_lock:
        drive = _get_drive()
        esc = _drive_q_escape(name)
        q = f"name = '{esc}' and '{parent_id}' in parents and trashed = false"
        if mime_type:
            q += f" and mimeType = '{_drive_q_escape(mime_type)}'"
        results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
        files = results.get("files", [])
        return files[0]["id"] if files else None


def export_google_doc_as_plain_text(file_id: str, *, _max_retries: int = 5) -> str:
    """Export a Google Doc to UTF-8 plain text (retries on rate-limit errors)."""
    import random, time

    last_err: HttpError | None = None
    for attempt in range(_max_retries):
        try:
            with drive_api_lock:
                drive = _get_drive()
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
    with drive_api_lock:
        drive = _get_drive()
        q = f"'{folder_id}' in parents and trashed = false and (name contains '.yaml' or name contains '.yml')"
        results = drive.files().list(q=q, fields="files(id, name, modifiedTime)", pageSize=200).execute()
        return results.get("files", [])


def _read_drive_file(file_id: str) -> str:
    """Download a Drive file as UTF-8 text."""
    with drive_api_lock:
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
    with drive_api_lock:
        drive = _get_drive()
        media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/yaml")
        if file_id:
            f = drive.files().update(fileId=file_id, media_body=media).execute()
            return f["id"]
        meta: dict[str, Any] = {"name": name, "parents": [folder_id]}
        f = drive.files().create(body=meta, media_body=media, fields="id").execute()
        return f["id"]


def _normalize_config_text(text: str) -> str:
    """Normalize YAML text for equality checks (line endings, trailing whitespace)."""
    s = text.replace("\r\n", "\n").replace("\r", "\n")
    body = "\n".join(line.rstrip() for line in s.split("\n")).rstrip("\n")
    return body + "\n" if body else ""


def config_text_matches_local(local_text: str, drive_text: str) -> bool:
    """Return True if Drive content is equivalent to the local file for sync purposes."""
    return _normalize_config_text(local_text) == _normalize_config_text(drive_text)


def clear_yaml_config_cache() -> None:
    """Drop cached deck/slide YAML from Drive so the next load refetches."""
    with _yaml_cache_lock:
        _yaml_cache.clear()


def list_obsolete_drive_config(
    decks_dir: str | Path | None = None,
    slides_dir: str | Path | None = None,
    *,
    slides_only: bool = False,
    decks_only: bool = False,
) -> dict[str, Any]:
    """Compare local ``*.yaml`` to Drive; return files whose Drive copy differs from repo.

    Does not upload. Requires ``GOOGLE_DRIVE_FOLDER_ID`` and Drive API access.

    Returns:
        dict with keys ``stale_decks``, ``stale_slides`` (list of filenames),
        ``missing_on_drive_decks``, ``missing_on_drive_slides`` (local files with no Drive object),
        ``error`` (str, if setup failed).
    """
    from .deck_loader import DEFAULT_DECKS_DIR
    from .slide_loader import DEFAULT_SLIDES_DIR

    if slides_only and decks_only:
        raise ValueError("slides_only and decks_only cannot both be true")

    empty: dict[str, Any] = {
        "stale_decks": [],
        "stale_slides": [],
        "missing_on_drive_decks": [],
        "missing_on_drive_slides": [],
    }

    if not GOOGLE_DRIVE_FOLDER_ID:
        return {**empty, "error": "GOOGLE_DRIVE_FOLDER_ID not set"}

    d_dir = Path(decks_dir) if decks_dir else DEFAULT_DECKS_DIR
    s_dir = Path(slides_dir) if slides_dir else DEFAULT_SLIDES_DIR

    try:
        _, d_folder, s_folder = _get_config_folder_ids()
    except Exception as e:
        return {**empty, "error": str(e)}

    include_decks = not slides_only
    include_slides = not decks_only

    out = dict(empty)
    if include_decks:
        d = _list_stale_in_folder(d_dir, d_folder, "decks")
        out["stale_decks"] = d["stale_decks"]
        out["missing_on_drive_decks"] = d["missing_on_drive_decks"]
    if include_slides:
        s = _list_stale_in_folder(s_dir, s_folder, "slides")
        out["stale_slides"] = s["stale_slides"]
        out["missing_on_drive_slides"] = s["missing_on_drive_slides"]
    return out


def _list_stale_in_folder(
    local_dir: Path,
    drive_folder_id: str,
    kind: str,
) -> dict[str, list[str]]:
    """Return stale_* and missing_on_drive_* for one kind (decks or slides)."""
    drive_files = _list_drive_files(drive_folder_id)
    by_name = {f["name"]: f["id"] for f in drive_files}

    stale_key = f"stale_{kind}"
    missing_key = f"missing_on_drive_{kind}"
    result: dict[str, list[str]] = {stale_key: [], missing_key: []}

    for f in sorted(local_dir.glob("*.yaml")):
        local_text = f.read_text(encoding="utf-8")
        fid = by_name.get(f.name)
        if not fid:
            result[missing_key].append(f.name)
            continue
        try:
            drive_text = _read_drive_file(fid)
        except Exception as e:
            logger.warning("Drive %s/%s unreadable (%s) — treating as stale", kind, f.name, e)
            result[stale_key].append(f.name)
            continue
        if not config_text_matches_local(local_text, drive_text):
            result[stale_key].append(f.name)

    return result


def sync_obsolete_drive_config(
    decks_dir: str | Path | None = None,
    slides_dir: str | Path | None = None,
    *,
    dry_run: bool = False,
    slides_only: bool = False,
    decks_only: bool = False,
    upload_missing: bool = True,
) -> dict[str, Any]:
    """Overwrite Drive YAML that differs from the repo (and optionally upload missing files).

    After any successful upload or update, clears the in-process YAML cache so the
    next ``load_yaml_from_drive`` refetches.

    Args:
        dry_run: If True, only report what would change; no Drive writes.
        slides_only: Only check/update ``slides/`` on Drive.
        decks_only: Only check/update ``decks/`` on Drive.
        upload_missing: If True, upload local files that have no Drive object yet
            (same as a first-time sync for those names).

    Returns:
        Stats including ``decks_updated``, ``slides_updated``, ``decks_uploaded_new``,
        ``slides_uploaded_new``, lists of filenames, and ``dry_run``.
    """
    from .deck_loader import DEFAULT_DECKS_DIR
    from .slide_loader import DEFAULT_SLIDES_DIR

    report = list_obsolete_drive_config(
        decks_dir=decks_dir,
        slides_dir=slides_dir,
        slides_only=slides_only,
        decks_only=decks_only,
    )
    if report.get("error"):
        return {
            "error": report["error"],
            "dry_run": dry_run,
            "decks_updated": 0,
            "slides_updated": 0,
            "decks_uploaded_new": 0,
            "slides_uploaded_new": 0,
        }

    d_dir = Path(decks_dir) if decks_dir else DEFAULT_DECKS_DIR
    s_dir = Path(slides_dir) if slides_dir else DEFAULT_SLIDES_DIR
    _, d_folder, s_folder = _get_config_folder_ids()

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "decks_updated": 0,
        "slides_updated": 0,
        "decks_uploaded_new": 0,
        "slides_uploaded_new": 0,
        "updated_deck_files": [],
        "updated_slide_files": [],
        "new_deck_files": [],
        "new_slide_files": [],
    }

    include_decks = not slides_only
    include_slides = not decks_only

    def process_kind(
        local_dir: Path,
        drive_folder: str,
        label: str,
        stale_names: list[str],
        missing_names: list[str],
    ) -> None:
        existing = {f["name"]: f["id"] for f in _list_drive_files(drive_folder)}
        for name in stale_names:
            path = local_dir / name
            if not path.is_file():
                continue
            content = path.read_text(encoding="utf-8")
            fid = existing.get(name)
            if dry_run:
                if label == "decks":
                    stats["decks_updated"] += 1
                    stats["updated_deck_files"].append(name)
                else:
                    stats["slides_updated"] += 1
                    stats["updated_slide_files"].append(name)
                continue
            if fid:
                _upload_file(name, content, drive_folder, file_id=fid)
                logger.info("Drive %s/%s overwritten from repo (was obsolete)", label, name)
            if label == "decks":
                stats["decks_updated"] += 1
                stats["updated_deck_files"].append(name)
            else:
                stats["slides_updated"] += 1
                stats["updated_slide_files"].append(name)

        if not upload_missing:
            return
        for name in missing_names:
            path = local_dir / name
            if not path.is_file():
                continue
            content = path.read_text(encoding="utf-8")
            if dry_run:
                if label == "decks":
                    stats["decks_uploaded_new"] += 1
                    stats["new_deck_files"].append(name)
                else:
                    stats["slides_uploaded_new"] += 1
                    stats["new_slide_files"].append(name)
                continue
            _upload_file(name, content, drive_folder, file_id=None)
            logger.info("Drive %s/%s created from repo (was missing)", label, name)
            if label == "decks":
                stats["decks_uploaded_new"] += 1
                stats["new_deck_files"].append(name)
            else:
                stats["slides_uploaded_new"] += 1
                stats["new_slide_files"].append(name)

    if include_decks:
        process_kind(
            d_dir,
            d_folder,
            "decks",
            report["stale_decks"],
            report["missing_on_drive_decks"],
        )
    if include_slides:
        process_kind(
            s_dir,
            s_folder,
            "slides",
            report["stale_slides"],
            report["missing_on_drive_slides"],
        )

    changed = (
        stats["decks_updated"]
        + stats["slides_updated"]
        + stats["decks_uploaded_new"]
        + stats["slides_uploaded_new"]
    )
    if changed and not dry_run:
        clear_yaml_config_cache()

    return stats


def ensure_drive_config_matches_repo() -> None:
    """Once per process: overwrite stale or missing Drive YAML from the local repo.

    Invoked automatically before reading ``bpo-config`` from Drive so QBR and deck
    runs match the checked-in definitions. Failures are logged; loading continues
    with whatever is on Drive (or local fallback).
    """
    global _drive_repo_sync_ran
    if _drive_repo_sync_ran:
        return
    _drive_repo_sync_ran = True
    if not GOOGLE_DRIVE_FOLDER_ID:
        return
    try:
        stats = sync_obsolete_drive_config(dry_run=False, upload_missing=True)
        if stats.get("error"):
            logger.warning("Drive bpo-config repo sync skipped: %s", stats["error"])
            return
        total = (
            stats["decks_updated"]
            + stats["slides_updated"]
            + stats["decks_uploaded_new"]
            + stats["slides_uploaded_new"]
        )
        if total:
            logger.info(
                "Synced %d bpo-config file(s) from repo to Drive "
                "(decks replaced=%d, slides replaced=%d, new decks=%d, new slides=%d)",
                total,
                stats["decks_updated"],
                stats["slides_updated"],
                stats["decks_uploaded_new"],
                stats["slides_uploaded_new"],
            )
    except Exception as e:
        logger.warning("Drive bpo-config repo sync failed (continuing): %s", e)


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
            logger.debug("Uploaded %s/%s to Drive", label, f.name)

    if stats["decks_uploaded"] + stats["slides_uploaded"]:
        clear_yaml_config_cache()

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

    ensure_drive_config_matches_repo()

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
            if not isinstance(parsed, dict):
                raise ValueError(f"Expected mapping in {df['name']}")
            if "id" not in parsed:
                # Non-slide YAML in slides/ (e.g. ``qbr-template-authoring-cues.yaml`` — no ``id`` by design).
                logger.debug(
                    "Drive %s/%s has no top-level id — skipping (not a deck/slide definition)",
                    kind,
                    df["name"],
                )
                continue
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
