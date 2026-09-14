"""Fernet encryption for on-disk TTL caches and Drive integration envelopes.

Without ``CORTEX_CACHE_FERNET_KEY``, caches are skipped (no plaintext writes).
Generate a key with::

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

from __future__ import annotations

import json
import os
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from .config import logger

CACHE_FERNET_ENV = "CORTEX_CACHE_FERNET_KEY"
DISK_CACHE_FORMAT_VERSION = 2
CACHE_ALG_FERNET = "fernet"


def load_cache_fernet() -> Fernet | None:
    raw = (os.environ.get(CACHE_FERNET_ENV) or "").strip()
    if not raw:
        return None
    try:
        return Fernet(raw.encode("ascii"))
    except (ValueError, TypeError) as exc:
        logger.warning("%s is not a valid Fernet key (%s)", CACHE_FERNET_ENV, type(exc).__name__)
        return None


def encrypt_jsonable(obj: Any) -> str | None:
    """Return a Fernet token, or None when the key is missing/invalid."""
    f = load_cache_fernet()
    if f is None:
        return None
    blob = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    return f.encrypt(blob).decode("ascii")


def decrypt_jsonable(token: str) -> Any | None:
    f = load_cache_fernet()
    if f is None:
        return None
    if not isinstance(token, str) or not token.strip():
        return None
    try:
        plain = f.decrypt(token.encode("ascii"))
    except (InvalidToken, ValueError, TypeError):
        return None
    try:
        return json.loads(plain.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
