"""Exchange a LeanDNA Data API secret for a short-lived session Bearer token.

Auth API (Swagger UI ``urls.primaryName=Auth``): ``POST {authBase}/data/session``
with JSON ``{"secret": "<api key>"}`` → ``{"sessionId": "..."}``.

The ``sessionId`` is used as ``Authorization: Bearer`` on ``/api/data/...``.
LeanDNA documents a **one-hour** session lifetime; Cortex caches in-process for
slightly less than that and refreshes on 401.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from .config import logger

SESSION_TTL_SECONDS = 50 * 60
AUTH_SESSION_PATH = "/data/session"

_PostFn = Callable[..., Any]

_cache_lock = threading.Lock()
_session_cache: dict[str, tuple[str, float]] = {}


def derive_auth_api_base_url(data_api_base_url: str) -> str:
    """Map Data API ``…/api`` to Auth API ``…/auth`` on the same host."""
    raw = (data_api_base_url or "").strip()
    if "://" not in raw:
        raw = "https://" + raw.lstrip("/")
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"cannot derive Auth API base from Data API URL {data_api_base_url!r}")
    path = (parsed.path or "").rstrip("/")
    if path.endswith("/api"):
        path = path[: -len("/api")]
    return f"{parsed.scheme}://{parsed.netloc}{path}/auth".rstrip("/")


def reset_auth_session_cache() -> None:
    """Drop cached session IDs (tests and 401 refresh)."""
    with _cache_lock:
        _session_cache.clear()


def _cache_key(auth_base_url: str, secret: str) -> str:
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]
    return f"{auth_base_url.rstrip('/')}|{digest}"


def fetch_data_api_session_id(
    secret: str,
    *,
    auth_base_url: str,
    timeout_seconds: float = 30.0,
    force_refresh: bool = False,
    post: _PostFn | None = None,
) -> str:
    """Return a Data API session id for ``secret`` (the permanent API key)."""
    key = (secret or "").strip()
    if not key:
        raise ValueError("LeanDNA Auth API: API key (secret) is empty")
    if ":" not in key:
        raise ValueError(
            "LeanDNA Auth API: API key must be identifier:passcode as shown once on the "
            "LeanDNA account page (Auth POST /auth/data/session field `secret`). "
            "A session Bearer or cookie is not the API key."
        )
    base = auth_base_url.rstrip("/")
    ck = _cache_key(base, key)
    now = time.monotonic()
    if not force_refresh:
        with _cache_lock:
            hit = _session_cache.get(ck)
            if hit and hit[1] > now:
                return hit[0]

    url = f"{base}{AUTH_SESSION_PATH}"
    post_fn = post or requests.post
    logger.info("LeanDNA Auth API POST %s (exchange API key for sessionId)", url)
    try:
        r = post_fn(
            url,
            json={"secret": key},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=timeout_seconds,
        )
    except requests.RequestException as e:
        raise ValueError(f"LeanDNA Auth API: request failed: {e}") from e

    body_text = (r.text or "").strip()
    if not r.ok:
        snippet = body_text.replace("\n", " ")[:400]
        raise ValueError(
            f"LeanDNA Auth API {r.status_code}: failed to exchange API key for sessionId"
            + (f" ({snippet})" if snippet else "")
        )

    try:
        payload = r.json()
    except ValueError as e:
        raise ValueError("LeanDNA Auth API: session response was not JSON") from e
    if not isinstance(payload, dict):
        raise ValueError("LeanDNA Auth API: session response was not an object")
    session_id = str(payload.get("sessionId") or "").strip()
    if not session_id:
        raise ValueError("LeanDNA Auth API: response missing sessionId")

    expires = time.monotonic() + SESSION_TTL_SECONDS
    with _cache_lock:
        _session_cache[ck] = (session_id, expires)
    return session_id
