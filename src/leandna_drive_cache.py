"""Encrypted Drive JSON cache for LeanDNA payloads.

Writes go under ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` with a Fernet envelope.
Missing folder or ``CORTEX_CACHE_FERNET_KEY`` skips the write (no plaintext).
Legacy plaintext JSON is ignored on read.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any

from googleapiclient.http import MediaIoBaseUpload

from .cache_crypto import (
    CACHE_ALG_FERNET,
    CACHE_FERNET_ENV,
    DISK_CACHE_FORMAT_VERSION,
    decrypt_jsonable,
    encrypt_jsonable,
)
from .config import logger


def save_json(filename: str, data: Any) -> None:
    from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID
    from .network_utils import network_timeout
    from .slides_api import _get_service

    folder = (GOOGLE_QBR_GENERATOR_FOLDER_ID or "").strip()
    if not folder:
        logger.warning(
            "LeanDNA Drive cache write skipped %s: GOOGLE_QBR_GENERATOR_FOLDER_ID unset",
            filename,
        )
        return
    ct = encrypt_jsonable(data)
    if ct is None:
        logger.warning(
            "LeanDNA Drive cache write skipped %s: %s unset or invalid",
            filename,
            CACHE_FERNET_ENV,
        )
        return
    envelope = {"v": DISK_CACHE_FORMAT_VERSION, "alg": CACHE_ALG_FERNET, "ct": ct}
    body = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    try:
        _, drive, _ = _get_service()
        media = MediaIoBaseUpload(io.BytesIO(body), mimetype="application/json")
        meta = {
            "name": filename,
            "parents": [folder],
            "mimeType": "application/json",
        }
        with network_timeout(30.0, "Drive file creation"):
            drive.files().create(
                body=meta,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
        logger.debug("LeanDNA Drive cache: wrote %s", filename)
    except Exception as e:
        logger.warning("LeanDNA Drive cache save failed for %s: %s", filename, e)


def load_json(filename: str, *, ttl_hours: float) -> Any | None:
    from .network_utils import network_timeout
    from .slides_api import _get_service

    ttl = max(0.0, float(ttl_hours))
    try:
        _, drive, _ = _get_service()
        q = f"name = '{filename}' and trashed = false"
        with network_timeout(30.0, "Drive file listing"):
            results = drive.files().list(
                q=q,
                pageSize=5,
                fields="files(id, name, modifiedTime)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
        files = results.get("files") or []
        if not files:
            return None
        file_info = files[0]
        modified = file_info.get("modifiedTime") or ""
        if modified:
            s = str(modified).replace("Z", "+00:00")
            mod_dt = datetime.fromisoformat(s)
            if mod_dt.tzinfo is None:
                mod_dt = mod_dt.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - mod_dt).total_seconds() / 3600.0
            if age_hours >= ttl:
                logger.debug("LeanDNA Drive cache %s stale (%.1fh)", filename, age_hours)
                return None
        request = drive.files().get_media(fileId=file_info["id"])
        with network_timeout(30.0, "Drive file download"):
            content = request.execute()
        raw = json.loads(content.decode("utf-8") if isinstance(content, bytes) else content)
        if not isinstance(raw, dict):
            return None
        if int(raw.get("v") or 0) != DISK_CACHE_FORMAT_VERSION or raw.get("alg") != CACHE_ALG_FERNET:
            return None
        return decrypt_jsonable(raw.get("ct") or "")
    except Exception as e:
        logger.debug("LeanDNA Drive cache load failed for %s: %s", filename, e)
        return None
