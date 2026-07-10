"""Cross-process Pendo aggregation pacing via a token bucket on ``CORTEX_CACHE_ROOT``.

When multiple ECS tasks share EFS at ``/var/cortex/cache``, this coordinates the
integration-key rate limit across containers. Each task still keeps a local
in-process token bucket for thread-level burst smoothing.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from .config import CORTEX_CACHE_ROOT, CORTEX_PENDO_GLOBAL_RATE_LIMIT, logger

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows dev fallback
    fcntl = None  # type: ignore[assignment,misc]

_NAMESPACE = "pendo"
_STATE_NAME = "global_rate_limit.json"
_LOCK_NAME = "global_rate_limit.lock"
_MAX_ACQUIRE_WAIT_S = 300.0


def _state_dir() -> Path:
    return CORTEX_CACHE_ROOT / _NAMESPACE


def _load_state(state_path: Path, *, capacity: float) -> tuple[float, float]:
    now = time.time()
    if not state_path.is_file():
        return float(capacity), now
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        tokens = float(raw.get("tokens", capacity))
        updated = float(raw.get("updated", now))
        return tokens, updated
    except (OSError, ValueError, TypeError):
        return float(capacity), now


def _save_state(state_path: Path, *, tokens: float, updated: float) -> None:
    tmp = state_path.with_suffix(".json.tmp")
    payload = json.dumps({"tokens": tokens, "updated": updated})
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(state_path)


def acquire_global_pendo_token(*, rate_per_sec: float, capacity: float) -> float:
    """Block until one global aggregation token is available. Returns seconds waited."""
    if not CORTEX_PENDO_GLOBAL_RATE_LIMIT or rate_per_sec <= 0 or fcntl is None:
        return 0.0

    state_dir = _state_dir()
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.debug("Pendo global rate limit disabled (cache dir unavailable): %s", exc)
        return 0.0

    lock_path = state_dir / _LOCK_NAME
    state_path = state_dir / _STATE_NAME
    cap = max(1.0, float(capacity))
    total_waited = 0.0
    deadline = time.monotonic() + _MAX_ACQUIRE_WAIT_S

    while True:
        try:
            with open(lock_path, "a+", encoding="utf-8") as lock_fp:
                fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
                try:
                    now = time.time()
                    tokens, updated = _load_state(state_path, capacity=cap)
                    elapsed = max(0.0, now - updated)
                    tokens = min(cap, tokens + elapsed * rate_per_sec)
                    if tokens >= 1.0:
                        tokens -= 1.0
                        _save_state(state_path, tokens=tokens, updated=now)
                        return total_waited
                    needed = (1.0 - tokens) / rate_per_sec
                    _save_state(state_path, tokens=tokens, updated=now)
                finally:
                    fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            logger.debug("Pendo global rate limit skipped: %s", exc)
            return total_waited

        if time.monotonic() >= deadline:
            logger.warning(
                "Pendo global rate limit: waited %.1fs; proceeding without token",
                total_waited,
            )
            return total_waited

        sleep_for = min(max(needed, 0.01), 5.0)
        time.sleep(sleep_for)
        total_waited += sleep_for


def clear_global_pendo_rate_limit_for_tests() -> None:
    """Remove global pacing state (tests only)."""
    root = _state_dir()
    for name in (_STATE_NAME, _LOCK_NAME, "global_rate_limit.json.tmp"):
        path = root / name
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
