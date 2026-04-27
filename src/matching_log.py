"""Append-only hydrate / field matching audit log (JSONL at repo root: ``matching.log``).

Set ``BPO_MATCHING_LOG=0`` to disable. Thread-safe; safe for parallel slide workers.
Each line is one JSON object with an ``event`` field and a UTC ``ts`` (ISO-8601).
"""

from __future__ import annotations

import datetime
import json
import os
import threading
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_PATH = _REPO_ROOT / "matching.log"

_lock = threading.Lock()
_session_open = False
_enabled = os.environ.get("BPO_MATCHING_LOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)


def enabled() -> bool:
    return _enabled


def log_path() -> Path:
    raw = (os.environ.get("BPO_MATCHING_LOG_PATH") or "").strip()
    return Path(raw) if raw else _DEFAULT_PATH


def _clip(s: str, n: int = 400) -> str:
    t = (s or "").replace("\n", " ")
    return t if len(t) <= n else t[: n - 1] + "…"


def emit(event: str, **fields: Any) -> None:
    """Write one JSON line: ``{"event": ..., "ts": ..., ...}``."""
    if not _enabled:
        return
    global _session_open
    row: dict[str, Any] = {"event": event, **fields}
    row["ts"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    path = log_path()
    try:
        with _lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                if not _session_open:
                    row0 = {
                        "event": "log_session_start",
                        "ts": row["ts"],
                        "log_path": str(path.resolve()),
                        "note": "hydrate / synonym / adapt mapping audit",
                    }
                    f.write(json.dumps(row0, ensure_ascii=False, default=str) + "\n")
                    _session_open = True
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except OSError as e:
        from .config import logger

        logger.warning("matching_log: could not write to %s: %s", path, e)
