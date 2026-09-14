"""Shared on-disk TTL cache under ``CORTEX_CACHE_DIR`` (default: repo ``.cache/``).

Entries are Fernet-encrypted JSON. Plaintext ``{"ts","data"}`` files are ignored.
Writes are skipped when ``CORTEX_CACHE_FERNET_KEY`` is unset.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

from . import config as _config
from .cache_crypto import (
    CACHE_ALG_FERNET,
    CACHE_FERNET_ENV,
    DISK_CACHE_FORMAT_VERSION,
    decrypt_jsonable,
    encrypt_jsonable,
)
from .config import logger

_LOCK = threading.Lock()


def cache_key(name: str, params: dict[str, Any]) -> str:
    blob = json.dumps({"name": name, "params": params}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def cache_path(namespace: str, key: str) -> Path:
    return _config.CORTEX_CACHE_ROOT / namespace / f"{key}.json"


def cache_get(namespace: str, key: str, ttl_seconds: int | None) -> Any | None:
    ttl = max(0, int(ttl_seconds or 0))
    if ttl <= 0:
        return None
    path = cache_path(namespace, key)
    with _LOCK:
        try:
            raw = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None
    try:
        outer = json.loads(raw)
        if int(outer.get("v") or 0) != DISK_CACHE_FORMAT_VERSION:
            return None
        if outer.get("alg") != CACHE_ALG_FERNET:
            return None
        inner = decrypt_jsonable(outer.get("ct") or "")
        if not isinstance(inner, dict):
            return None
        if time.time() - float(inner.get("ts") or 0) > ttl:
            return None
        return inner.get("data")
    except (ValueError, TypeError):
        return None


def cache_set(namespace: str, key: str, data: Any, ttl_seconds: int | None) -> None:
    ttl = max(0, int(ttl_seconds or 0))
    if ttl <= 0:
        return
    ct = encrypt_jsonable({"ts": time.time(), "data": data})
    if ct is None:
        logger.warning(
            "Disk cache write skipped (%s/%s): %s unset or invalid",
            namespace,
            key[:8],
            CACHE_FERNET_ENV,
        )
        return
    path = cache_path(namespace, key)
    blob = json.dumps(
        {"v": DISK_CACHE_FORMAT_VERSION, "alg": CACHE_ALG_FERNET, "ct": ct},
        separators=(",", ":"),
    )
    with _LOCK:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(blob, encoding="utf-8")
            tmp.chmod(0o600)
            tmp.replace(path)
            path.chmod(0o600)
        except OSError as exc:
            logger.debug("Disk cache write failed (%s/%s): %s", namespace, key[:8], exc)


def cache_delete(namespace: str, key: str) -> bool:
    """Remove a cache entry if present. Returns True when a file was deleted."""
    path = cache_path(namespace, key)
    with _LOCK:
        removed = False
        for candidate in (path, path.with_suffix(".json.tmp")):
            try:
                candidate.unlink()
                removed = True
            except FileNotFoundError:
                continue
            except OSError as exc:
                logger.debug("Disk cache delete failed (%s/%s): %s", namespace, key[:8], exc)
        return removed


def clear_namespace_for_tests(namespace: str) -> None:
    root = _config.CORTEX_CACHE_ROOT / namespace
    with _LOCK:
        if not root.exists():
            return
        for f in root.glob("*.json*"):
            try:
                f.unlink()
            except OSError:
                pass
