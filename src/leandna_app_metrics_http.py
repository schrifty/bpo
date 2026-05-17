"""LeanDNA classic app API session auth (``/api/2/factndx/...``).

Uses the same mechanism as ``kpi/update-kpi/metric_management.py``: browser
``LDNASESSIONID`` after logging into the web app — **not** the OpenAPI Data API Bearer token.

Configure ``LEANDNA_APP_SESSION_ID`` or a cookie string (``LEANDNA_APP_COOKIE`` or
``LEANDNA_DATA_API_COOKIE`` when it contains ``LDNASESSIONID=``).
"""

from __future__ import annotations

import re

from .config import (
    LEANDNA_APP_COOKIE,
    LEANDNA_APP_SESSION_ID,
    LEANDNA_DATA_API_COOKIE,
)

_LDNA_SESSION_RE = re.compile(r"(?:^|;\s*)LDNASESSIONID=([^;]+)", re.IGNORECASE)


def parse_ldna_session_id(cookie_header: str) -> str | None:
    """Extract ``LDNASESSIONID`` value from a ``Cookie`` header or raw cookie string."""
    raw = (cookie_header or "").strip()
    if not raw:
        return None
    m = _LDNA_SESSION_RE.search(raw)
    if m:
        return m.group(1).strip()
    return None


def resolve_leandna_app_session_id() -> str | None:
    """Session id for app API calls: dedicated env, then app cookie, then Data API cookie."""
    if LEANDNA_APP_SESSION_ID:
        return LEANDNA_APP_SESSION_ID
    for blob in (LEANDNA_APP_COOKIE, LEANDNA_DATA_API_COOKIE):
        sid = parse_ldna_session_id(blob)
        if sid:
            return sid
    return None


def leandna_app_session_configured() -> bool:
    return bool(resolve_leandna_app_session_id())


def build_leandna_app_api_headers(*, user_agent_suffix: str = "leandna-app-metrics/1.0") -> dict[str, str]:
    """Headers for ``/api/2/factndx/...`` (session cookie + XSRF token)."""
    sid = resolve_leandna_app_session_id()
    if not sid:
        raise ValueError(
            "LeanDNA app session not configured — set LEANDNA_APP_SESSION_ID, or "
            "LEANDNA_APP_COOKIE / LEANDNA_DATA_API_COOKIE containing LDNASESSIONID= "
            "(from DevTools while logged into app.leandna.com)."
        )
    return {
        "XSRF-TOKEN": "LDNA",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cookie": f"LDNASESSIONID={sid}",
        "User-Agent": f"bpo/{user_agent_suffix}",
    }
