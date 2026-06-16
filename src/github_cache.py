"""On-disk cache for GitHub productivity API rollups."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

from .config import BPO_GITHUB_CACHE_TTL_SECONDS, logger

_CACHE_DIR = Path(__file__).resolve().parents[1] / ".cache" / "github"
_LOCK = threading.Lock()


def cache_key(name: str, params: dict[str, Any]) -> str:
    blob = json.dumps({"name": name, "params": params}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def cache_get(key: str, ttl_seconds: int | None = None) -> Any | None:
    ttl = BPO_GITHUB_CACHE_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    if ttl <= 0:
        return None
    path = _CACHE_DIR / f"{key}.json"
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


def cache_set(key: str, data: Any, ttl_seconds: int | None = None) -> None:
    ttl = BPO_GITHUB_CACHE_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    if ttl <= 0:
        return
    with _LOCK:
        try:
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            tmp = _CACHE_DIR / f"{key}.json.tmp"
            tmp.write_text(json.dumps({"ts": time.time(), "data": data}), encoding="utf-8")
            tmp.replace(_CACHE_DIR / f"{key}.json")
        except OSError as e:
            logger.debug("GitHub cache write failed (%s); continuing without cache", e)


def clear_github_cache_for_tests() -> None:
    with _LOCK:
        if not _CACHE_DIR.exists():
            return
        for f in _CACHE_DIR.glob("*.json*"):
            try:
                f.unlink()
            except OSError:
                pass
