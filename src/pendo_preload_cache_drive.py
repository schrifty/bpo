"""Google Drive JSON cache for heavy Pendo ``PendoClient.preload`` slices (same folder as portfolio).

Files live under ``resolve_portfolio_snapshot_folder_id()`` (QBR generator ``Cache`` subfolder or
``BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID``). Jira/JSM support and Salesforce comprehensive caches use the
same folder via ``integration_drive_cache``. Age policy matches
``pendo_portfolio_snapshot_drive.classify_drive_cache_age`` (7d fresh, weekday stale reuse, weekend
refresh). Drive writes for existing files run on weekends only.

Cached payloads are point-in-time snapshots (like the portfolio JSON): very fresh numbers may
differ slightly from a live Pendo pull until the file ages out and is refetched.
"""

from __future__ import annotations

import io
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from googleapiclient.http import MediaIoBaseUpload

from .config import (
    BPO_PENDO_CACHE_TTL_SECONDS,
    logger,
)
from .drive_config import _get_drive, drive_api_lock, find_file_in_folder
from .pendo_portfolio_snapshot_drive import (
    _drive_io_transient,
    _read_drive_file_text_retrying,
    classify_drive_cache_age,
    is_weekend_in_snapshot_tz,
    resolve_portfolio_snapshot_folder_id,
)

PENDO_PRELOAD_CACHE_SCHEMA_VERSION = 1
PENDO_PRELOAD_PREFIX = f"pendo_preload_v{PENDO_PRELOAD_CACHE_SCHEMA_VERSION}"

PRELOAD_KIND_VISITORS = "visitors"
PRELOAD_KIND_FEATURE_EVENTS = "feature_events"
PRELOAD_KIND_PAGE_EVENTS = "page_events"
PRELOAD_KIND_TRACK_EVENTS = "track_events"
PRELOAD_KIND_GUIDE_EVENTS = "guide_events"
PRELOAD_KIND_PAGE_CATALOG = "page_catalog"
PRELOAD_KIND_FEATURE_CATALOG = "feature_catalog"
PRELOAD_KIND_GUIDE_CATALOG = "guide_catalog"
PRELOAD_KIND_USAGE_BY_SITE = "usage_by_site"

_SAVE_LOCK = threading.Lock()

_CATALOG_KINDS = frozenset(
    {
        PRELOAD_KIND_PAGE_CATALOG,
        PRELOAD_KIND_FEATURE_CATALOG,
        PRELOAD_KIND_GUIDE_CATALOG,
    }
)


def pendo_preload_cache_filename(kind: str, days: int | None) -> str:
    if kind in _CATALOG_KINDS or days is None:
        return f"{PENDO_PRELOAD_PREFIX}_{kind}.json"
    return f"{PENDO_PRELOAD_PREFIX}_{kind}_days{int(days)}.json"


def _envelope_age_hours(saved_at: str | None, modified_time_rfc3339: str | None) -> float | None:
    if saved_at:
        try:
            s = saved_at.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
        except (TypeError, ValueError):
            pass
    if modified_time_rfc3339:
        try:
            s = modified_time_rfc3339.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
        except (TypeError, ValueError):
            pass
    return None


def _validate_envelope(raw: Any, kind: str, days: int | None) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if int(raw.get("schema_version") or 0) != PENDO_PRELOAD_CACHE_SCHEMA_VERSION:
        return None
    if raw.get("kind") != kind:
        return None
    if kind in _CATALOG_KINDS:
        if raw.get("days") is not None:
            return None
    else:
        if int(raw.get("days") or -1) != int(days or -1):
            return None
    payload = raw.get("payload")
    if payload is None:
        return None
    return raw


def try_load_pendo_preload_payload(kind: str, days: int | None) -> Any | None:
    """Return cached *payload* if a fresh JSON exists on Drive; else None."""
    from .drive_cache_stats import record_pendo_preload_load_attempt

    name = pendo_preload_cache_filename(kind, days)
    if BPO_PENDO_CACHE_TTL_SECONDS <= 0:
        logger.info("Pendo preload cache: bypass read for %r (cache disabled)", name)
        return None

    hit = False
    try:
        folder_id = resolve_portfolio_snapshot_folder_id()
        if not folder_id:
            logger.info(
                "Pendo preload cache: skip %r — no Drive cache folder (GOOGLE_QBR_GENERATOR_FOLDER_ID / "
                "BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID unset, or folder resolve failed)",
                name,
            )
            return None
        try:
            fid = find_file_in_folder(name, folder_id, mime_type=None)
            if not fid:
                logger.info(
                    "Pendo preload cache: skip %r — file not in Drive cache folder",
                    name,
                )
                return None
            with drive_api_lock:
                drive = _get_drive()
                from .network_utils import network_timeout
                with network_timeout(30.0, "Drive file metadata get"):
                    meta = drive.files().get(fileId=fid, fields="modifiedTime").execute()
            text = _read_drive_file_text_retrying(fid)
            raw = json.loads(text)
            env = _validate_envelope(raw, kind, days)
            if env is None:
                logger.info(
                    "Pendo preload cache: skip %r — JSON envelope invalid (schema/kind/days/payload)",
                    name,
                )
                return None
            age_h = _envelope_age_hours(env.get("saved_at"), meta.get("modifiedTime"))
            if age_h is None:
                logger.info(
                    "Pendo preload cache: skip %r — could not determine age",
                    name,
                )
                return None
            decision = classify_drive_cache_age(
                age_h,
                cache_name=name,
                log_label="Pendo preload cache",
            )
            if decision == "reject":
                return None
            if decision == "fresh":
                logger.info(
                    "Pendo preload cache: loaded %r from Drive (%.1fh old)",
                    name,
                    age_h,
                )
            else:
                logger.info(
                    "Pendo preload cache: loaded %r from Drive (stale weekday, %.1fh)",
                    name,
                    age_h,
                )
            hit = True
            return env["payload"]
        except Exception as e:
            logger.warning(
                "Pendo preload cache: read %r failed — %s: %s",
                name,
                type(e).__name__,
                e,
            )
            return None
    finally:
        record_pendo_preload_load_attempt(hit=hit)


def save_pendo_preload_payload(kind: str, days: int | None, payload: Any) -> None:
    """Write or replace a cache JSON on Drive (best-effort; logs failures)."""
    if BPO_PENDO_CACHE_TTL_SECONDS <= 0:
        logger.info(
            "Pendo preload cache: skip write for %r (cache disabled)",
            pendo_preload_cache_filename(kind, days),
        )
        return
    folder_id = resolve_portfolio_snapshot_folder_id()
    if not folder_id:
        return
    name = pendo_preload_cache_filename(kind, days)
    if (
        find_file_in_folder(name, folder_id, mime_type=None)
        and not is_weekend_in_snapshot_tz()
    ):
        logger.info(
            "Pendo preload cache: skip write %r — weekday (weekend-only Drive updates)",
            name,
        )
        return
    envelope: dict[str, Any] = {
        "schema_version": PENDO_PRELOAD_CACHE_SCHEMA_VERSION,
        "kind": kind,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    if kind in _CATALOG_KINDS:
        envelope["days"] = None
    else:
        envelope["days"] = int(days) if days is not None else None
    body = json.dumps(
        envelope,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    with _SAVE_LOCK:
        last_err: BaseException | None = None
        for attempt in range(4):
            try:
                from .network_utils import network_timeout
                with drive_api_lock:
                    drive = _get_drive()
                    media = MediaIoBaseUpload(io.BytesIO(body), mimetype="application/json")
                    fid = find_file_in_folder(name, folder_id, mime_type=None)
                    if fid:
                        with network_timeout(30.0, "Drive file update"):
                            drive.files().update(fileId=fid, media_body=media, fields="id").execute()
                    else:
                        with network_timeout(30.0, "Drive file creation"):
                            drive.files().create(
                                body={"name": name, "parents": [folder_id]},
                                media_body=media,
                                fields="id",
                            ).execute()
                logger.debug("Pendo preload cache: wrote %r (%d bytes)", name, len(body))
                last_err = None
                break
            except Exception as e:
                last_err = e
                if not _drive_io_transient(e) or attempt >= 3:
                    break
                time.sleep(0.35 * (attempt + 1))
        if last_err is not None:
            logger.warning("Pendo preload cache: failed to write %r — %s", name, last_err)
