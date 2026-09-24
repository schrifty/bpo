"""Persistent content-addressed cache for KPI situation briefings."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CACHE_VERSION = 1
_MAX_CACHE_FILES = 64


def situation_cache_key(digest: dict[str, Any], *, prompt: str, model: str) -> str:
    """Hash every input that can change the reader-specific briefing."""
    payload = json.dumps(
        {
            "version": _CACHE_VERSION,
            "model": model,
            "prompt": prompt,
            "digest": digest,
        },
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_dir() -> Path:
    # Import at call time so tests and runtime config can replace the cache root.
    from src.config import CORTEX_CACHE_ROOT

    return Path(CORTEX_CACHE_ROOT) / "kpi" / "situation"


def load_situation_cache(key: str) -> dict[str, Any] | None:
    path = _cache_dir() / f"{key}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    analysis = str(payload.get("analysis") or "").strip()
    if payload.get("key") != key or not analysis:
        return None
    return {
        "analysis": analysis,
        "created_at": payload.get("created_at"),
    }


def save_situation_cache(key: str, analysis: str) -> dict[str, Any]:
    """Atomically persist a completed briefing; partial streams are never cached."""
    text = str(analysis or "").strip()
    if not text:
        raise ValueError("cannot cache an empty KPI situation briefing")
    directory = _cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    payload = {"key": key, "analysis": text, "created_at": created_at}
    path = directory / f"{key}.json"
    tmp = directory / f".{key}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
    _prune_cache(directory)
    return {"analysis": text, "created_at": created_at}


def _prune_cache(directory: Path) -> None:
    try:
        files = sorted(
            directory.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in files[_MAX_CACHE_FILES:]:
            path.unlink(missing_ok=True)
    except OSError:
        # A successful cache write must not be turned into a failed briefing
        # merely because best-effort cleanup raced another worker.
        return
