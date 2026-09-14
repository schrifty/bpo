"""Shared LeanDNA Data API request headers.

Auth modes (first match wins for Bearer):

- **API key** — ``LEANDNA_DATA_API_API_KEY`` (``PR_`` / ``ST_`` prefixed when
  ``EXECUTION_ENV`` is Production or Staging). Cortex calls the Auth API
  ``POST /auth/data/session`` with ``{"secret": <key>}`` and uses the returned
  ``sessionId`` as ``Authorization: Bearer``. Sessions last about an hour;
  a new one is fetched automatically.
- **Bearer token** — ``LEANDNA_DATA_API_BEARER_TOKEN`` (legacy / fallback if no API key).
  Paste the **raw token** only; a leading ``Bearer `` prefix is stripped.

With ``EXECUTION_ENV=Staging`` / ``Production`` / ``CI``, values come from ``ST_*`` /
``PR_*`` prefixed vars in ``src.config`` (see ``resolve_leandna_data_api_base_url``).

Do **not** commit API keys; keep them in local ``.env`` only.
"""

from __future__ import annotations

import requests

from .config import (
    LEANDNA_DATA_API_API_KEY,
    LEANDNA_DATA_API_BEARER_TOKEN,
)
from .leandna_auth_session import (
    derive_auth_api_base_url,
    fetch_data_api_session_id,
    reset_auth_session_cache,
)


def leandna_data_api_credentials_configured() -> bool:
    """True when API key and/or Bearer token is set."""
    return bool(
        (LEANDNA_DATA_API_API_KEY or "").strip()
        or (LEANDNA_DATA_API_BEARER_TOKEN or "").strip()
    )


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


def _resolved_data_api_base_url() -> str:
    from .config import resolve_leandna_data_api_base_url

    return resolve_leandna_data_api_base_url()


def resolve_leandna_data_api_bearer(*, force_refresh_session: bool = False) -> str:
    """Bearer token for Data API calls: Auth session from API key, else env token."""
    api_key = (LEANDNA_DATA_API_API_KEY or "").strip()
    if api_key:
        auth_base = derive_auth_api_base_url(_resolved_data_api_base_url())
        return fetch_data_api_session_id(
            api_key,
            auth_base_url=auth_base,
            force_refresh=force_refresh_session,
        )
    return _normalize_bearer_token(LEANDNA_DATA_API_BEARER_TOKEN or "")


def build_leandna_data_api_headers(
    *,
    requested_sites: str | None = None,
    user_agent_suffix: str = "leandna-data-api/1.0",
    content_type_json: bool = False,
    force_refresh_session: bool = False,
) -> dict[str, str]:
    """Headers for ``GET`` / ``POST`` / ``PUT`` / ``DELETE`` to ``{base}/data/...``.

    Raises:
        ValueError: if neither API key nor bearer token is configured,
            or Auth API exchange fails.
    """
    bearer = resolve_leandna_data_api_bearer(force_refresh_session=force_refresh_session)
    if not bearer:
        raise ValueError(
            "LeanDNA Data API: set LEANDNA_DATA_API_API_KEY (preferred) or "
            "LEANDNA_DATA_API_BEARER_TOKEN in .env. "
            "See docs/SETUP/LEANDNA_SETUP.md."
        )

    h: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": f"cortex-{user_agent_suffix}",
        "Authorization": f"Bearer {bearer}",
    }
    if content_type_json:
        h["Content-Type"] = "application/json"
    if requested_sites:
        h["RequestedSites"] = requested_sites.strip()
    return h


def leandna_data_api_http_request(
    method: str,
    url: str,
    *,
    requested_sites: str | None = None,
    user_agent_suffix: str = "leandna-data-api/1.0",
    content_type_json: bool = False,
    timeout: float | tuple[float, float] = 120.0,
    **request_kw: object,
) -> requests.Response:
    """``requests.request`` with LeanDNA auth; refresh Auth session once on 401."""
    header_kw = dict(
        requested_sites=requested_sites,
        user_agent_suffix=user_agent_suffix,
        content_type_json=content_type_json,
    )
    r = requests.request(
        method,
        url,
        headers=build_leandna_data_api_headers(**header_kw),
        timeout=timeout,
        **request_kw,
    )
    if r.status_code == 401 and (LEANDNA_DATA_API_API_KEY or "").strip():
        reset_auth_session_cache()
        r = requests.request(
            method,
            url,
            headers=build_leandna_data_api_headers(force_refresh_session=True, **header_kw),
            timeout=timeout,
            **request_kw,
        )
    return r
