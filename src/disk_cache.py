"""Shared on-disk TTL cache under ``CORTEX_CACHE_DIR`` (default: repo ``.cache/``)."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

from .config import CORTEX_CACHE_ROOT, logger

_LOCK = threading.Lock()


def cache_key(name: str, params: dict[str, Any]) -> str:
    blob = json.dumps({"name": name, "params": params}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def cache_path(namespace: str, key: str) -> Path:
    return CORTEX_CACHE_ROOT / namespace / f"{key}.json"


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
        payload = json.loads(raw)
        if time.time() - float(payload.get("ts") or 0) > ttl:
            return None
        return payload.get("data")
    except (ValueError, TypeError):
        return None


def cache_set(namespace: str, key: str, data: Any, ttl_seconds: int | None) -> None:
    ttl = max(0, int(ttl_seconds or 0))
    if ttl <= 0:
        return
    path = cache_path(namespace, key)
    with _LOCK:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({"ts": time.time(), "data": data}), encoding="utf-8")
            tmp.replace(path)
        except OSError as exc:
            logger.debug("Disk cache write failed (%s/%s): %s", namespace, key[:8], exc)


def clear_namespace_for_tests(namespace: str) -> None:
    root = CORTEX_CACHE_ROOT / namespace
    with _LOCK:
        if not root.exists():
            return
        for f in root.glob("*.json*"):
            try:
                f.unlink()
            except OSError:
                pass
