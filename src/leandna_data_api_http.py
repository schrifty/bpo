"""Shared LeanDNA Data API request headers.

Supports the same auth modes as the LeanDNA web app:

- **Bearer** — ``LEANDNA_DATA_API_BEARER_TOKEN`` (integration token: paste the **raw token** only;
  a leading ``Bearer `` prefix in the env value is stripped automatically).
- **Session cookie** — ``LEANDNA_DATA_API_COOKIE`` copied from the browser while logged in
  (DevTools → Network → any ``/api/data/...`` request → **Request Headers** → ``Cookie``).
  Optional ``Origin`` / ``Referer`` are sent with cookie auth so the API sees a browser-like request.

You may set **both** cookie and bearer (matches many in-app calls).

Do **not** commit cookie values; keep them in local ``.env`` only.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .config import (
    LEANDNA_DATA_API_BASE_URL,
    LEANDNA_DATA_API_BEARER_TOKEN,
    LEANDNA_DATA_API_COOKIE,
    LEANDNA_DATA_API_ORIGIN,
    LEANDNA_DATA_API_REFERER,
)


def leandna_data_api_credentials_configured() -> bool:
    """True when Bearer and/or session cookie is set (Data API calls may proceed)."""
    return bool((LEANDNA_DATA_API_BEARER_TOKEN or "").strip() or (LEANDNA_DATA_API_COOKIE or "").strip())


def _default_origin_for_cookie() -> str:
    """``https://host`` derived from ``LEANDNA_DATA_API_BASE_URL``."""
    raw = (LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").strip()
    if "://" not in raw:
        raw = "https://" + raw.lstrip("/")
    p = urlparse(raw)
    if p.scheme and p.netloc:
        return f"{p.scheme}://{p.netloc}"
    return ""


def _normalize_bearer_token(raw: str) -> str:
    """Strip surrounding whitespace and a redundant ``Bearer `` prefix from env values.

    Some people paste ``Authorization: Bearer …`` or ``Bearer …`` into ``.env``; we always
    emit ``Authorization: Bearer <token>`` ourselves.
    """
    t = (raw or "").strip()
    lower = t.lower()
    if lower.startswith("bearer "):
        return t[7:].lstrip()
    return t


def build_leandna_data_api_headers(
    *,
    requested_sites: str | None = None,
    user_agent_suffix: str = "leandna-data-api/1.0",
    content_type_json: bool = False,
) -> dict[str, str]:
    """Headers for ``GET`` / ``POST`` / ``PUT`` / ``DELETE`` to ``{LEANDNA_DATA_API_BASE_URL}/data/...``.

    Raises:
        ValueError: if neither bearer token nor session cookie is configured.
    """
    bearer = _normalize_bearer_token(LEANDNA_DATA_API_BEARER_TOKEN or "")
    cookie = (LEANDNA_DATA_API_COOKIE or "").strip()
    if not bearer and not cookie:
        raise ValueError(
            "LeanDNA Data API: set LEANDNA_DATA_API_BEARER_TOKEN and/or LEANDNA_DATA_API_COOKIE in .env. "
            "If in-app calls work but Bearer returns 401, paste the browser Cookie header from a logged-in "
            "session (see src/leandna_data_api_http.py docstring)."
        )

    h: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": f"bpo-{user_agent_suffix}",
    }
    if content_type_json:
        h["Content-Type"] = "application/json"
    if bearer:
        h["Authorization"] = f"Bearer {bearer}"
    if cookie:
        h["Cookie"] = cookie
        origin = (LEANDNA_DATA_API_ORIGIN or "").strip() or _default_origin_for_cookie()
        if origin:
            h["Origin"] = origin
            referer = (LEANDNA_DATA_API_REFERER or "").strip()
            if not referer:
                referer = origin.rstrip("/") + "/application/"
            h["Referer"] = referer
    if requested_sites:
        h["RequestedSites"] = requested_sites.strip()
    return h
