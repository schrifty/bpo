"""Drive JSON cache for heavy Jira/JSM and Salesforce deck payloads (same folder as Pendo portfolio).

Files live under ``resolve_portfolio_snapshot_folder_id()`` (QBR ``Cache`` or
``BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID``). Read age policy matches
``pendo_portfolio_snapshot_drive.classify_drive_cache_age`` (7d fresh, 14d weekday stale cap,
weekend refresh in the stale band). Writes match ``pendo_preload_cache_drive``: create any day;
replace existing files on weekends only in the snapshot calendar zone.
"""

from __future__ import annotations

import hashlib
import io
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from googleapiclient.http import MediaIoBaseUpload

from . import config as _config_mod
from .config import logger
from .drive_config import _get_drive, drive_api_lock, find_file_in_folder
from .pendo_portfolio_snapshot_drive import (
    _drive_io_transient,
    _read_drive_file_text_retrying,
    classify_drive_cache_age,
    is_weekend_in_snapshot_tz,
    resolve_portfolio_snapshot_folder_id,
)

INTEGRATION_CACHE_SCHEMA_VERSION = 1
KIND_JIRA_SUPPORT = "jira_support"
KIND_SALESFORCE_COMPREHENSIVE = "salesforce_comprehensive"

_SAVE_LOCK = threading.Lock()


def integration_customer_key(customer: str | None) -> str:
    """Stable key for filenames and envelope validation (all-customers vs scoped)."""
    if customer is None or not str(customer).strip():
        return "__all__"
    return str(customer).strip().lower()


def integration_cache_filename(kind: str, customer_key: str) -> str:
    h = hashlib.sha256(customer_key.encode("utf-8")).hexdigest()[:16]
    return f"integration_{kind}_v{INTEGRATION_CACHE_SCHEMA_VERSION}_{h}.json"


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


def _validate_envelope(raw: Any, kind: str, customer_key: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if int(raw.get("schema_version") or 0) != INTEGRATION_CACHE_SCHEMA_VERSION:
        return None
    if raw.get("kind") != kind:
        return None
    if (raw.get("customer_key") or "") != customer_key:
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    return raw


def integration_drive_cache_reads_enabled() -> bool:
    return (
        not _config_mod.BPO_INTEGRATION_DRIVE_CACHE_DISABLED
        and not _config_mod.BPO_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH
    )


def try_load_integration_payload(kind: str, customer: str | None) -> dict[str, Any] | None:
    """Return cached *payload* dict if a valid JSON exists on Drive and passes age policy."""
    from .drive_cache_stats import record_integration_load_attempt

    if not integration_drive_cache_reads_enabled():
        return None

    hit = False
    try:
        customer_key = integration_customer_key(customer)
        name = integration_cache_filename(kind, customer_key)
        folder_id = resolve_portfolio_snapshot_folder_id()
        if not folder_id:
            logger.debug(
                "Integration Drive cache: skip %r — no snapshot folder",
                name,
            )
            return None
        try:
            fid = find_file_in_folder(name, folder_id, mime_type=None)
            if not fid:
                logger.debug(
                    "Integration Drive cache: skip %r — file not in Drive cache folder",
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
            env = _validate_envelope(raw, kind, customer_key)
            if env is None:
                logger.info(
                    "Integration Drive cache: skip %r — envelope invalid (schema/kind/customer/payload)",
                    name,
                )
                return None
            age_h = _envelope_age_hours(env.get("saved_at"), meta.get("modifiedTime"))
            if age_h is None:
                logger.info(
                    "Integration Drive cache: skip %r — could not determine age",
                    name,
                )
                return None
            decision = classify_drive_cache_age(
                age_h,
                cache_name=name,
                log_label="Integration Drive cache",
            )
            if decision == "reject":
                return None
            if decision == "fresh":
                logger.info(
                    "Integration Drive cache: loaded %r (%.1fh old)",
                    name,
                    age_h,
                )
            else:
                logger.info(
                    "Integration Drive cache: loaded %r (stale weekday, %.1fh)",
                    name,
                    age_h,
                )
            hit = True
            return dict(env["payload"])
        except Exception as e:
            logger.warning(
                "Integration Drive cache: read %r failed — %s: %s",
                name,
                type(e).__name__,
                e,
            )
            return None
    finally:
        record_integration_load_attempt(hit=hit)


def save_integration_payload(kind: str, customer: str | None, payload: dict[str, Any]) -> None:
    """Write or replace a cache JSON on Drive (best-effort; same weekend write rule as Pendo preload)."""
    if _config_mod.BPO_INTEGRATION_DRIVE_CACHE_DISABLED:
        return
    folder_id = resolve_portfolio_snapshot_folder_id()
    if not folder_id:
        return
    customer_key = integration_customer_key(customer)
    name = integration_cache_filename(kind, customer_key)
    if find_file_in_folder(name, folder_id, mime_type=None) and not is_weekend_in_snapshot_tz():
        logger.info(
            "Integration Drive cache: skip write %r — weekday (weekend-only Drive updates)",
            name,
        )
        return
    envelope: dict[str, Any] = {
        "schema_version": INTEGRATION_CACHE_SCHEMA_VERSION,
        "kind": kind,
        "customer_key": customer_key,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
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
                        with network_timeout(120.0, "Drive file update"):
                            drive.files().update(fileId=fid, media_body=media, fields="id").execute()
                    else:
                        with network_timeout(120.0, "Drive file creation"):
                            drive.files().create(
                                body={"name": name, "parents": [folder_id]},
                                media_body=media,
                                fields="id",
                            ).execute()
                logger.info(
                    "Integration Drive cache: wrote %r (%d bytes)",
                    name,
                    len(body),
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                if not _drive_io_transient(e) or attempt >= 3:
                    break
                time.sleep(0.35 * (attempt + 1))
        if last_err is not None:
            logger.warning("Integration Drive cache: failed to write %r — %s", name, last_err)
