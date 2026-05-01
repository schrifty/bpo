"""Sync decks and slides between the local repo and Google Drive.

Strategy:
  1. On first run (or when --sync-config is passed), copy local YAML files
     into the QBR Generator area on Drive.
  2. On subsequent runs, read from Drive first.  If a Drive file fails to
     parse, fall back to the local version and log a QA warning.
  3. New local files that don't exist on Drive are uploaded automatically.
  4. Before the first load of deck/slide YAML from Drive in a process, the repo
     is pushed to Drive for any YAML that differs or is missing (see
     ``ensure_drive_config_matches_repo``) so git and Drive stay aligned.

The QBR Generator folder (``GOOGLE_QBR_GENERATOR_FOLDER_ID``) typically contains:
    chart-data/
    Decks-{ISO-date}/
    decks/
    Output/
        {ISO-date} - Output/
    slides/
    Prompts/
        (qbr_slide_list Google Doc, adapt_system_prompt.yaml, …)

:func:`get_qbr_generator_folder_id_for_drive_config` returns the folder id from
``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (required).
"""

from __future__ import annotations

import errno
import io
import threading
import time
from pathlib import Path
from typing import Any

import yaml
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .qa import qa

_drive_service = None
_drive_lock = threading.Lock()
# Serialize all googleapiclient Drive HTTP — the shared Resource/httplib2 stack is not thread-safe.
drive_api_lock = threading.RLock()

_yaml_cache: dict[str, list[dict[str, Any]]] = {}
_yaml_cache_lock = threading.Lock()
# Merged from full loads and per-deck subset loads so a multi-deck run does not
# re-walk the entire slides/ folder for every resolve_deck call.
_slide_def_id_cache: dict[str, dict[str, Any]] = {}
_drive_yaml_duplicate_log_lock = threading.Lock()
_drive_yaml_duplicate_signatures_warned: set[tuple[str, tuple[tuple[str, str, tuple[str, ...]], ...]]] = set()
_deck_output_folder_cache: str | None = None

# Set by ensure_drive_config_matches_repo (at most once per process).
_drive_repo_sync_ran = False

# QBR Generator → Prompts/adapt_system_prompt.yaml sync (at most once per process).
_qbr_adapt_prompt_sync_ran = False

# Same folder name as ``qbr_template.QBR_PROMPTS_SUBFOLDER`` (qbr_slide_list doc lives here).
QBR_PROMPTS_FOLDER_NAME = "Prompts"
ADAPT_SYSTEM_PROMPT_FILENAME = "adapt_system_prompt.yaml"
_MIME_FOLDER = "application/vnd.google-apps.folder"


def _get_drive():
    global _drive_service
    with _drive_lock:
        if _drive_service is None:
            from .slides_api import _get_service
            _x2, _drive_service, _sh2 = _get_service()
        return _drive_service


def _invalidate_drive_service() -> None:
    """Drop cached Drive API client so the next call builds a fresh HTTP connection."""
    global _drive_service
    with _drive_lock:
        _drive_service = None


def _drive_transport_retryable(exc: BaseException) -> bool:
    """True for dead sockets / broken pipes — common after long idle with one shared httplib2 pool."""
    if isinstance(exc, HttpError):
        return False
    if isinstance(exc, (BrokenPipeError, ConnectionError)):
        return True
    if isinstance(exc, OSError):
        n = getattr(exc, "errno", None)
        if n in (errno.EPIPE, errno.ECONNRESET, errno.ETIMEDOUT, errno.ENOTCONN):
            return True
    return False


def _drive_q_escape(value: str) -> str:
    """Escape a value for use in a single-quoted Drive ``files.list`` query string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find_or_create_folder(name: str, parent_id: str | None = None) -> str:
    """Find a subfolder by name, or create it. Returns the folder ID."""
    esc = _drive_q_escape(name)
    q = f"name = '{esc}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"

    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            with drive_api_lock:
                drive = _get_drive()
                results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
                files = results.get("files", [])
                if files:
                    return files[0]["id"]

                meta: dict[str, Any] = {
                    "name": name,
                    "mimeType": "application/vnd.google-apps.folder",
                }
                if parent_id:
                    meta["parents"] = [parent_id]
                folder = drive.files().create(body=meta, fields="id").execute()
                logger.info("Created Drive folder: %s (%s)", name, folder["id"])
                return folder["id"]
        except Exception as e:
            if not _drive_transport_retryable(e) or attempt >= max_attempts - 1:
                raise
            logger.warning(
                "Drive files.list/create failed (%s: %s) — recycling HTTP client, retry %d/%d",
                type(e).__name__,
                e,
                attempt + 1,
                max_attempts - 1,
            )
            _invalidate_drive_service()
            time.sleep(0.35 * (attempt + 1))
    raise RuntimeError("_find_or_create_folder: unreachable")  # pragma: no cover


def find_file_in_folder(
    name: str,
    parent_id: str,
    mime_type: str | None = None,
) -> str | None:
    """Return the file id of the first non-trashed file with exact ``name`` under ``parent_id``."""
    esc = _drive_q_escape(name)
    q = f"name = '{esc}' and '{parent_id}' in parents and trashed = false"
    if mime_type:
        q += f" and mimeType = '{_drive_q_escape(mime_type)}'"

    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            with drive_api_lock:
                drive = _get_drive()
                results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
                files = results.get("files", [])
                return files[0]["id"] if files else None
        except Exception as e:
            if not _drive_transport_retryable(e) or attempt >= max_attempts - 1:
                raise
            logger.warning(
                "Drive files.list failed (%s: %s) — recycling HTTP client, retry %d/%d",
                type(e).__name__,
                e,
                attempt + 1,
                max_attempts - 1,
            )
            _invalidate_drive_service()
            time.sleep(0.35 * (attempt + 1))
    raise RuntimeError("find_file_in_folder: unreachable")  # pragma: no cover


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


def get_qbr_generator_folder_id_for_drive_config() -> str:
    """Return the canonical QBR Generator folder id (YAML, Prompts, QBR template).

    Raises:
        RuntimeError: if ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` is unset or empty.
    """
    explicit = (GOOGLE_QBR_GENERATOR_FOLDER_ID or "").strip() or None
    if not explicit:
        raise RuntimeError(
            "GOOGLE_QBR_GENERATOR_FOLDER_ID must be set to the folder id for QBR template, "
            "Prompts, decks/, and slides/."
        )
    return explicit


def get_deck_output_folder_id() -> str | None:
    """Return the base QBR Generator folder id for generated deck outputs."""
    global _deck_output_folder_cache
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return None
    if _deck_output_folder_cache:
        return _deck_output_folder_cache
    _deck_output_folder_cache = get_qbr_generator_folder_id_for_drive_config()
    return _deck_output_folder_cache


def _get_config_folder_ids() -> tuple[str, str, str]:
    """Return (qbr_generator_root_id, decks_id, slides_id) on Drive."""
    qbr_gen = get_qbr_generator_folder_id_for_drive_config()
    decks = _find_or_create_folder("decks", qbr_gen)
    slides = _find_or_create_folder("slides", qbr_gen)
    return qbr_gen, decks, slides


def _dedupe_drive_yaml_files_by_name(
    files: list[dict[str, Any]],
    *,
    folder_id: str = "",
) -> list[dict[str, Any]]:
    """Drive allows multiple files with the same name in one folder; keep one per name (newest ``modifiedTime``)."""
    from collections import defaultdict

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in files:
        groups[f["name"]].append(f)
    out: list[dict[str, Any]] = []
    duplicate_details: list[tuple[str, str, str, tuple[str, ...], int]] = []
    for name in sorted(groups.keys()):
        g = groups[name]
        if len(g) == 1:
            out.append(g[0])
            continue
        g.sort(key=lambda x: x.get("modifiedTime") or "", reverse=True)
        keeper = g[0]
        dup_ids = [x["id"] for x in g[1:]]
        duplicate_details.append(
            (name, keeper["id"], keeper.get("modifiedTime", ""), tuple(dup_ids), len(g))
        )
        out.append(keeper)
    if duplicate_details:
        signature = (
            folder_id,
            tuple((name, keeper_id, dup_ids) for name, keeper_id, _mt, dup_ids, _count in duplicate_details),
        )
        with _drive_yaml_duplicate_log_lock:
            first_seen = signature not in _drive_yaml_duplicate_signatures_warned
            if first_seen:
                _drive_yaml_duplicate_signatures_warned.add(signature)
        names_preview = ", ".join(name for name, *_rest in duplicate_details[:8])
        more = "" if len(duplicate_details) <= 8 else f", +{len(duplicate_details) - 8} more"
        log = logger.warning if first_seen else logger.debug
        log(
            "Drive YAML folder has %d duplicate filename(s); keeping newest per name. "
            "Clean extra Drive files if desired. Names: %s%s",
            len(duplicate_details),
            names_preview,
            more,
        )
        for name, keeper_id, modified, dup_ids, count in duplicate_details:
            logger.debug(
                "Drive YAML duplicate name %r (%d copies): keeping %s… (newest modifiedTime=%s); "
                "extra file id(s): %s",
                name,
                count,
                keeper_id[:12],
                modified,
                ", ".join(d[:12] + "…" for d in dup_ids),
            )
    return out


def _list_drive_files(folder_id: str) -> list[dict[str, Any]]:
    """List YAML files in a Drive folder. Returns one file per basename (newest if duplicates exist)."""
    from .network_utils import network_timeout
    
    with network_timeout(30.0, "Drive folder listing"):
        with drive_api_lock:
            drive = _get_drive()
            q = f"'{folder_id}' in parents and trashed = false and (name contains '.yaml' or name contains '.yml')"
            results = drive.files().list(q=q, fields="files(id, name, modifiedTime)", pageSize=200).execute()
            raw = results.get("files", [])
        return _dedupe_drive_yaml_files_by_name(raw, folder_id=folder_id)


def _read_drive_file(file_id: str) -> str:
    """Download a Drive file as UTF-8 text."""
    from .network_utils import network_timeout
    
    with drive_api_lock:
        drive = _get_drive()
        # Set socket timeout for Drive API calls
        with network_timeout(30.0, "Drive file download"):
            request = drive.files().get_media(fileId=file_id)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            chunk_count = 0
            while not done:
                _, done = downloader.next_chunk()
                chunk_count += 1
                if chunk_count > 100:  # Safety limit: max 100 chunks per file
                    raise TimeoutError(f"Drive file {file_id[:12]}… exceeded max chunks (100)")
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


def upload_text_file_to_drive_folder(
    name: str,
    content: str,
    folder_id: str,
    *,
    mime_type: str = "text/markdown",
    replace_existing: bool = True,
) -> str:
    """Create or replace a UTF-8 text file on Drive under ``folder_id``. Returns file id.

    When ``replace_existing`` is True (default), updates the first non-trashed file with the
    same ``name`` in that folder (any mime); otherwise creates a new file (duplicates allowed).
    """
    with drive_api_lock:
        drive = _get_drive()
        media = MediaIoBaseUpload(
            io.BytesIO(content.encode("utf-8")),
            mimetype=mime_type,
            resumable=False,
        )
        fid: str | None = None
        if replace_existing:
            fid = find_file_in_folder(name, folder_id, mime_type=None)
        if fid:
            f = drive.files().update(fileId=fid, media_body=media).execute()
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


def ensure_qbr_adapt_prompt_yaml_synced_from_repo() -> None:
    """Push local ``prompts/adapt_system_prompt.yaml`` to QBR Generator's Prompts folder if stale or missing.

    Idempotent: runs at most once per process unless :func:`clear_yaml_config_cache` resets the guard.
    """
    global _qbr_adapt_prompt_sync_ran
    if _qbr_adapt_prompt_sync_ran:
        return
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return
    _qbr_adapt_prompt_sync_ran = True
    qbr_gen = get_qbr_generator_folder_id_for_drive_config()
    local_path = Path(__file__).resolve().parent.parent / "prompts" / ADAPT_SYSTEM_PROMPT_FILENAME
    if not local_path.is_file():
        logger.debug("No local %s — skip QBR Prompts sync", ADAPT_SYSTEM_PROMPT_FILENAME)
        return
    try:
        prompts_id = _find_or_create_folder(QBR_PROMPTS_FOLDER_NAME, qbr_gen)
        local_text = local_path.read_text(encoding="utf-8")
        existing = {f["name"]: f["id"] for f in _list_drive_files(prompts_id)}
        fid = existing.get(ADAPT_SYSTEM_PROMPT_FILENAME)
        if fid:
            try:
                drive_text = _read_drive_file(fid)
            except Exception as e:
                logger.warning(
                    "Could not read Drive QBR Prompts/%s (%s) — replacing from repo",
                    ADAPT_SYSTEM_PROMPT_FILENAME,
                    e,
                )
                _upload_file(ADAPT_SYSTEM_PROMPT_FILENAME, local_text, prompts_id, file_id=fid)
                logger.info("Replaced QBR Prompts/%s on Drive (read failed)", ADAPT_SYSTEM_PROMPT_FILENAME)
                return
            if config_text_matches_local(local_text, drive_text):
                return
            _upload_file(ADAPT_SYSTEM_PROMPT_FILENAME, local_text, prompts_id, file_id=fid)
            logger.info("Synced QBR Prompts/%s from repo to Drive", ADAPT_SYSTEM_PROMPT_FILENAME)
        else:
            _upload_file(ADAPT_SYSTEM_PROMPT_FILENAME, local_text, prompts_id)
            logger.info("Uploaded QBR Prompts/%s to Drive (new file)", ADAPT_SYSTEM_PROMPT_FILENAME)
    except Exception as e:
        logger.warning("QBR Prompts %s sync failed: %s", ADAPT_SYSTEM_PROMPT_FILENAME, e)


def read_adapt_system_prompt_yaml_text_from_drive() -> str | None:
    """Return raw YAML text for ``adapt_system_prompt.yaml`` from QBR Prompts folder, or None."""
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return None
    qbr_gen = get_qbr_generator_folder_id_for_drive_config()
    try:
        prompts_id = _find_or_create_folder(QBR_PROMPTS_FOLDER_NAME, qbr_gen)
        existing = {f["name"]: f["id"] for f in _list_drive_files(prompts_id)}
        fid = existing.get(ADAPT_SYSTEM_PROMPT_FILENAME)
        if not fid:
            return None
        return _read_drive_file(fid)
    except Exception as e:
        logger.warning(
            "Could not read QBR Prompts/%s from Drive: %s",
            ADAPT_SYSTEM_PROMPT_FILENAME,
            e,
        )
        return None


def _adapt_system_prompt_body_from_yaml_raw(raw: str) -> str | None:
    """Return usable adapt system prompt text, or None if YAML is invalid or key missing."""
    try:
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            return None
        s = data.get("adapt_system_prompt")
        if not isinstance(s, str) or not s.strip():
            return None
        return s.rstrip("\n") + "\n"
    except Exception:
        return None


def assert_qbr_prompts_ready_or_raise() -> None:
    """Validate local and (when Drive is configured) Drive ``Prompts/`` before heavy data work.

    Call from hydrate (and similar entry points) **before** Pendo / health-report loads so the
    run fails fast when ``adapt_system_prompt.yaml`` is missing or unusable.

    Raises:
        FileNotFoundError: local file missing, Prompts folder missing on Drive, or Drive YAML missing.
        ValueError: YAML present but ``adapt_system_prompt`` key missing/empty.
        RuntimeError: ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` is unset or empty.
    """
    root = Path(__file__).resolve().parent.parent
    local_path = root / "prompts" / ADAPT_SYSTEM_PROMPT_FILENAME
    if not local_path.is_file():
        raise FileNotFoundError(
            f"Missing local {ADAPT_SYSTEM_PROMPT_FILENAME} under {local_path.parent}"
        )
    local_raw = local_path.read_text(encoding="utf-8")
    if _adapt_system_prompt_body_from_yaml_raw(local_raw) is None:
        raise ValueError(
            f"{local_path} must contain a non-empty string key 'adapt_system_prompt'"
        )

    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        raise RuntimeError(
            "GOOGLE_QBR_GENERATOR_FOLDER_ID must be set (folder id for QBR template, Prompts, decks/, slides/)."
        )

    ensure_drive_config_matches_repo()
    qbr_gen = get_qbr_generator_folder_id_for_drive_config()
    prompts_id = find_file_in_folder(QBR_PROMPTS_FOLDER_NAME, qbr_gen, _MIME_FOLDER)
    if not prompts_id:
        raise FileNotFoundError(
            f"QBR Prompts folder {QBR_PROMPTS_FOLDER_NAME!r} not found under QBR Generator "
            f"(id={qbr_gen})"
        )
    ensure_qbr_adapt_prompt_yaml_synced_from_repo()
    drive_raw = read_adapt_system_prompt_yaml_text_from_drive()
    if drive_raw is None:
        raise FileNotFoundError(
            f"{ADAPT_SYSTEM_PROMPT_FILENAME} missing or unreadable in Drive Prompts "
            f"(folder id={prompts_id}) after sync"
        )
    if _adapt_system_prompt_body_from_yaml_raw(drive_raw) is None:
        raise ValueError(
            f"Drive {ADAPT_SYSTEM_PROMPT_FILENAME} must contain a non-empty string key 'adapt_system_prompt'"
        )


def clear_yaml_config_cache() -> None:
    """Drop cached deck/slide YAML from Drive so the next load refetches."""
    global _qbr_adapt_prompt_sync_ran
    with _yaml_cache_lock:
        _yaml_cache.clear()
        _slide_def_id_cache.clear()
    _qbr_adapt_prompt_sync_ran = False
    try:
        from .evaluate import _load_adapt_system_prompt_template

        _load_adapt_system_prompt_template.cache_clear()
    except Exception:
        pass


def reset_for_tests() -> None:
    """Reset Drive-backed module caches and one-shot sync guards for test isolation."""
    global _drive_repo_sync_ran, _qbr_adapt_prompt_sync_ran, _deck_output_folder_cache
    clear_yaml_config_cache()
    with _drive_yaml_duplicate_log_lock:
        _drive_yaml_duplicate_signatures_warned.clear()
    _drive_repo_sync_ran = False
    _qbr_adapt_prompt_sync_ran = False
    _deck_output_folder_cache = None


def list_obsolete_drive_config(
    decks_dir: str | Path | None = None,
    slides_dir: str | Path | None = None,
    *,
    slides_only: bool = False,
    decks_only: bool = False,
) -> dict[str, Any]:
    """Compare local ``*.yaml`` to Drive; return files whose Drive copy differs from repo.

    Does not upload. Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` and Drive API access.

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

    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return {
            **empty,
            "error": "GOOGLE_QBR_GENERATOR_FOLDER_ID must be set for Drive YAML",
        }

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

    Invoked automatically before reading deck/slide YAML from Drive so QBR and deck
    runs match the checked-in definitions. Failures are logged; loading continues
    with whatever is on Drive (or local fallback).

    Deck/slide files live under ``<QBR Generator>/decks|slides/`` (see
    :func:`get_qbr_generator_folder_id_for_drive_config`). Also syncs
    ``prompts/adapt_system_prompt.yaml`` to the same folder’s ``Prompts/`` subfolder.
    """
    global _drive_repo_sync_ran
    if _drive_repo_sync_ran:
        return
    _drive_repo_sync_ran = True
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return
    try:
        stats = sync_obsolete_drive_config(dry_run=False, upload_missing=True)
        if stats.get("error"):
            logger.warning("Drive QBR Generator deck|slide sync skipped: %s", stats["error"])
        else:
            total = (
                stats["decks_updated"]
                + stats["slides_updated"]
                + stats["decks_uploaded_new"]
                + stats["slides_uploaded_new"]
            )
            if total:
                logger.info(
                    "Synced %d deck/slide YAML file(s) from repo to Drive (QBR Generator) "
                    "(decks replaced=%d, slides replaced=%d, new decks=%d, new slides=%d)",
                    total,
                    stats["decks_updated"],
                    stats["slides_updated"],
                    stats["decks_uploaded_new"],
                    stats["slides_uploaded_new"],
                )
            else:
                try:
                    _, d_f, s_f = _get_config_folder_ids()
                    logger.info(
                        "Drive deck/slide YAML already matches repo (QBR Generator); "
                        "no uploads. decks_folder_id=%s… slides_folder_id=%s…",
                        (d_f or "")[:12],
                        (s_f or "")[:12],
                    )
                except Exception:
                    logger.info(
                        "Drive deck/slide YAML already matches repo (QBR Generator); "
                        "no uploads needed.",
                    )
    except Exception as e:
        logger.warning("Drive QBR Generator deck|slide sync failed (continuing): %s", e)
    try:
        ensure_qbr_adapt_prompt_yaml_synced_from_repo()
    except Exception as e:
        logger.warning("QBR Prompts adapt_system_prompt sync failed (continuing): %s", e)


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


def load_deck_yaml_from_drive(deck_id: str, local_dir: Path) -> dict[str, Any] | None:
    """Load a single ``decks/{deck_id}.yaml`` from Drive (one get_media) if the folder is configured.

    Skips the full multi-deck fetch used by :func:`load_yaml_from_drive` when only one deck
    is needed (e.g. support review).
    """
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return None
    basename = f"{deck_id}.yaml"
    p = local_dir / basename
    if p.is_file():
        d = _load_single_local(p)
        if d and d.get("id") == deck_id:
            return d
    try:
        ensure_drive_config_matches_repo()
        _, d_folder, _s = _get_config_folder_ids()
        for df in _list_drive_files(d_folder):
            if df.get("name") == basename:
                t_read = time.perf_counter()
                try:
                    text = _read_drive_file(df["id"])
                    raw = yaml.safe_load(text)
                    if isinstance(raw, dict) and raw.get("id") == deck_id:
                        raw["_source"] = "drive"
                        raw["_file"] = basename
                        dt = time.perf_counter() - t_read
                        logger.info(
                            "Drive decks: single-file load %s in %.2fs (folder_id=%s…)",
                            basename,
                            dt,
                            (d_folder or "")[:12],
                        )
                        return raw
                except Exception as e:
                    logger.debug("load_deck_yaml_from_drive: %s: %s", basename, e)
                break
    except Exception as e:
        logger.debug("load_deck_yaml_from_drive: %s", e)
    return None


def _register_slides_in_id_cache(items: list[dict[str, Any]]) -> None:
    for s in items:
        lid = s.get("id")
        if lid is not None:
            _slide_def_id_cache[str(lid)] = s


def _ordered_slides_from_id_cache(only_slide_ids: set[str]) -> list[dict[str, Any]]:
    return [_slide_def_id_cache[i] for i in sorted(only_slide_ids)]


def _filter_slides_to_ids(
    full: list[dict[str, Any]],
    only_ids: set[str],
    local_dir: Path,
    *,
    kind: str = "slides",
) -> list[dict[str, Any]]:
    """Sublist of ``full`` in original list order, then local files for any id still missing."""
    need = set(only_ids)
    results = [s for s in full if s.get("id") is not None and str(s["id"]) in need]
    have = {str(s.get("id")) for s in results if s.get("id")}
    missing = need - have
    if not missing:
        return results
    for f in sorted(local_dir.glob("*.yaml")):
        if not missing:
            break
        local = _load_single_local(f)
        lid = str(local.get("id")) if local and local.get("id") else ""
        if local and lid in missing:
            qa.flag(
                f"Local {kind}/{f.name} for missing id {lid}",
                sources=(f"local {kind}/{f.name}",),
                severity="info",
                auto_corrected=False,
                internal=True,
            )
            results.append(local)
            missing.discard(lid)
    return results


def load_yaml_from_drive(
    kind: str,
    local_dir: Path,
    *,
    only_slide_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Load YAML configs from Drive with fallback to local files.

    Results are cached after the first successful load — deck/slide definitions
    don't change between customers, so there's no reason to refetch them 150+
    times during a batch run.

    When ``only_slide_ids`` is set (``kind == "slides"`` only), loads only those
    slide definitions. The first such load walks Drive like before; later calls
    reuse a process-wide per-id cache (or a prior full ``slides`` list) so
    multi-deck runs (main + companions) do not repeat a full folder walk for
    every deck.

    Args:
        kind: "decks" or "slides"
        local_dir: local directory to fall back to
        only_slide_ids: If set, only load slide YAMLs whose top-level ``id`` is
            in this set (reduces Drive get_media for single-deck runs like support).

    Returns:
        List of parsed dicts with a "_source" key indicating "drive" or "local".
    """
    if only_slide_ids is not None and kind == "slides":
        if not only_slide_ids:
            return []
        with _yaml_cache_lock:
            if "slides" in _yaml_cache:
                return _filter_slides_to_ids(
                    _yaml_cache["slides"], only_slide_ids, local_dir, kind=kind
                )
            if only_slide_ids <= _slide_def_id_cache.keys():
                return _ordered_slides_from_id_cache(only_slide_ids)
            to_fetch = set(only_slide_ids) - set(_slide_def_id_cache.keys())
        if not to_fetch:
            with _yaml_cache_lock:
                if "slides" in _yaml_cache:
                    return _filter_slides_to_ids(
                        _yaml_cache["slides"], only_slide_ids, local_dir, kind=kind
                    )
                if only_slide_ids <= _slide_def_id_cache.keys():
                    return _ordered_slides_from_id_cache(only_slide_ids)
        else:
            new_items = _load_yaml_from_drive_uncached(
                kind, local_dir, only_slide_ids=set(to_fetch)
            )
            with _yaml_cache_lock:
                _register_slides_in_id_cache(new_items)
                if "slides" in _yaml_cache:
                    return _filter_slides_to_ids(
                        _yaml_cache["slides"], only_slide_ids, local_dir, kind=kind
                    )
                if only_slide_ids <= _slide_def_id_cache.keys():
                    return _ordered_slides_from_id_cache(only_slide_ids)
        with _yaml_cache_lock:
            if only_slide_ids <= _slide_def_id_cache.keys():
                return _ordered_slides_from_id_cache(only_slide_ids)
            return [
                _slide_def_id_cache[i]
                for i in sorted(only_slide_ids)
                if i in _slide_def_id_cache
            ]

    if kind in _yaml_cache:
        return _yaml_cache[kind]

    with _yaml_cache_lock:
        if kind in _yaml_cache:
            return _yaml_cache[kind]
        result = _load_yaml_from_drive_uncached(kind, local_dir)
        _yaml_cache[kind] = result
        if kind == "slides":
            _register_slides_in_id_cache(result)
        return result


def _load_yaml_from_drive_uncached(
    kind: str,
    local_dir: Path,
    *,
    only_slide_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Actual Drive fetch logic (called once, then cached, unless only_slide_ids is set)."""
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        if only_slide_ids is not None and kind == "slides":
            if not only_slide_ids:
                return []
            out = load_local_slide_definitions_for_ids(local_dir, only_slide_ids)
        else:
            out = _load_all_local(local_dir, kind)
        logger.info(
            "Loaded %d %s YAML file(s) from local repo only (set GOOGLE_QBR_GENERATOR_FOLDER_ID "
            "for Drive-backed YAML)",
            len(out),
            kind,
        )
        return out

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
        if only_slide_ids is not None and kind == "slides":
            if not only_slide_ids:
                return []
            return load_local_slide_definitions_for_ids(local_dir, only_slide_ids)
        return _load_all_local(local_dir, kind)

    if not drive_files:
        logger.info("No %s on Drive yet — using local defaults", kind)
        if only_slide_ids is not None and kind == "slides":
            if not only_slide_ids:
                return []
            return load_local_slide_definitions_for_ids(local_dir, only_slide_ids)
        return _load_all_local(local_dir, kind)

    results: list[dict[str, Any]] = []
    drive_names: set[str] = set()
    n_drive_files = len(drive_files)
    target_mode = only_slide_ids is not None and kind == "slides" and bool(only_slide_ids)
    found_ids: set[str] = set()
    if target_mode:
        logger.info(
            "Drive %s: need %d slide def(s) by id (subset load); up to %d file(s) in folder_id=%s…",
            kind,
            len(only_slide_ids or ()),
            n_drive_files,
            (folder_id or "")[:12],
        )
    else:
        logger.info(
            "Drive %s: reading %d YAML file(s) from folder_id=%s… (per-file progress follows)",
            kind,
            n_drive_files,
            (folder_id or "")[:12],
        )
    for i, df in enumerate(drive_files, 1):
        if target_mode and only_slide_ids and found_ids >= only_slide_ids:
            logger.info(
                "Drive %s: subset load complete — have all %d id(s) after %d get_media (skipped %d remaining in folder)",
                kind,
                len(only_slide_ids),
                i - 1,
                n_drive_files - (i - 1),
            )
            break
        drive_names.add(df["name"])
        logger.info("Drive %s: %d/%d %s — get_media starting …", kind, i, n_drive_files, df["name"])
        t_read = time.perf_counter()
        try:
            text = _read_drive_file(df["id"])
            dt = time.perf_counter() - t_read
            if dt >= 1.0:
                logger.info(
                    "Drive %s: %d/%d %s — get_media done in %.2fs",
                    kind,
                    i,
                    n_drive_files,
                    df["name"],
                    dt,
                )
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
            sid = str(parsed["id"])
            if target_mode and only_slide_ids and sid not in only_slide_ids:
                continue
            parsed["_source"] = "drive"
            parsed["_file"] = df["name"]
            results.append(parsed)
            if target_mode and only_slide_ids:
                found_ids.add(sid)
            qa.check()
        except Exception as e:
            dt_err = time.perf_counter() - t_read
            logger.warning(
                "Drive %s/%s failed after %.2fs: %s — falling back to local",
                kind,
                df["name"],
                dt_err,
                e,
            )
            qa.flag(
                f"Drive {kind}/{df['name']} parse error — using local version",
                expected="valid YAML",
                actual=str(e)[:120],
                sources=(f"Drive {kind}/{df['name']}", f"local {kind}/{df['name']}"),
                internal=True,
                severity="error",
            )
            local = _load_single_local(local_dir / df["name"])
            if local and local.get("id") and (not target_mode or str(local.get("id")) in (only_slide_ids or set())):
                results.append(local)
                if target_mode and only_slide_ids and local.get("id"):
                    found_ids.add(str(local["id"]))

    if target_mode and only_slide_ids:
        missing = (only_slide_ids or set()) - {str(r.get("id")) for r in results if r.get("id")}
        for f in sorted(local_dir.glob("*.yaml")):
            if not missing:
                break
            local = _load_single_local(f)
            lid = str(local.get("id")) if local and local.get("id") else ""
            if local and lid in missing:
                qa.flag(
                    f"Local {kind}/{f.name} for missing id {lid}",
                    sources=(f"local {kind}/{f.name}",),
                    severity="info",
                    auto_corrected=False,
                    internal=True,
                )
                results.append(local)
                missing.discard(lid)
    else:
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

    n_drive = sum(1 for r in results if r.get("_source") == "drive")
    n_local = sum(1 for r in results if r.get("_source") == "local")
    logger.info(
        "Loaded %s YAML: %d definition(s) (%d from Drive, %d local-only); folder_id=%s…",
        kind,
        len(results),
        n_drive,
        n_local,
        (folder_id or "")[:12],
    )
    return results


def load_local_slide_definitions_for_ids(
    local_dir: Path, only_slide_ids: set[str]
) -> list[dict[str, Any]]:
    """Read only ``slides/*.yaml`` that define an ``id`` in ``only_slide_ids`` (early exit when complete)."""
    if not only_slide_ids:
        return []
    have: set[str] = set()
    results: list[dict[str, Any]] = []
    for f in sorted(local_dir.glob("*.yaml")):
        d = _load_single_local(f)
        if d and d.get("id") in only_slide_ids:
            results.append(d)
            have.add(str(d["id"]))
            if have >= only_slide_ids:
                break
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
