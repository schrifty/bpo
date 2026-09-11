"""LeanDNA Data API credentials and HTTP for a specific environment bucket (PR_* / ST_*)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

import requests

from .leandna_auth_session import (
    derive_auth_api_base_url,
    fetch_data_api_session_id,
    reset_auth_session_cache,
)
from .leandna_data_api_request import (
    _response_envelope,
    normalize_data_api_relative_path,
)

LeanDNAEnvBucket = Literal["production", "staging"]


@dataclass(frozen=True)
class LeanDNAEnvConfig:
    bucket: LeanDNAEnvBucket
    base_url: str
    bearer_token: str
    cookie: str
    origin: str
    referer: str
    api_key: str = ""


def load_leandna_env_config(bucket: LeanDNAEnvBucket) -> LeanDNAEnvConfig:
    """Load ``PR_*`` (production) or ``ST_*`` (staging) LeanDNA Data API settings from the environment."""
    prefix = "PR_" if bucket == "production" else "ST_"
    base = (os.environ.get(f"{prefix}LEANDNA_DATA_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        raise ValueError(
            f"{prefix}LEANDNA_DATA_API_BASE_URL is required to use LeanDNA {bucket} "
            f"(set in .env; see docs/SETUP/LEANDNA_SETUP.md)."
        )
    api_key = (os.environ.get(f"{prefix}LEANDNA_DATA_API_API_KEY") or "").strip()
    bearer = (os.environ.get(f"{prefix}LEANDNA_DATA_API_BEARER_TOKEN") or "").strip()
    cookie = (os.environ.get(f"{prefix}LEANDNA_DATA_API_COOKIE") or "").strip()
    if not api_key and not bearer and not cookie:
        raise ValueError(
            f"Set {prefix}LEANDNA_DATA_API_API_KEY (preferred), "
            f"{prefix}LEANDNA_DATA_API_BEARER_TOKEN, and/or {prefix}LEANDNA_DATA_API_COOKIE."
        )
    origin = (os.environ.get(f"{prefix}LEANDNA_DATA_API_ORIGIN") or "").strip()
    referer = (os.environ.get(f"{prefix}LEANDNA_DATA_API_REFERER") or "").strip()
    if cookie and not origin:
        origin = _default_origin_from_base(base)
    if cookie and not referer and origin:
        referer = f"{origin.rstrip('/')}/application/"
    return LeanDNAEnvConfig(
        bucket=bucket,
        base_url=base,
        bearer_token=bearer,
        cookie=cookie,
        origin=origin,
        referer=referer,
        api_key=api_key,
    )


def leandna_env_credentials_configured(bucket: LeanDNAEnvBucket) -> bool:
    try:
        load_leandna_env_config(bucket)
        return True
    except ValueError:
        return False


def _default_origin_from_base(base_url: str) -> str:
    raw = base_url.strip()
    if "://" not in raw:
        raw = "https://" + raw.lstrip("/")
    p = urlparse(raw)
    if p.scheme and p.netloc:
        return f"{p.scheme}://{p.netloc}"
    return ""


def _normalize_bearer_token(raw: str) -> str:
    t = (raw or "").strip()
    if t.lower().startswith("bearer "):
        return t[7:].lstrip()
    return t


def build_leandna_env_headers(
    config: LeanDNAEnvConfig,
    *,
    requested_sites: str | None = None,
    user_agent_suffix: str = "leandna-data-api-env/1.0",
    content_type_json: bool = False,
    force_refresh_session: bool = False,
) -> dict[str, str]:
    api_key = (config.api_key or "").strip()
    if api_key:
        bearer = fetch_data_api_session_id(
            api_key,
            auth_base_url=derive_auth_api_base_url(config.base_url),
            force_refresh=force_refresh_session,
        )
    else:
        bearer = _normalize_bearer_token(config.bearer_token)
    if not bearer and not config.cookie:
        raise ValueError(f"LeanDNA {config.bucket}: missing API key, bearer token, and cookie.")
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": f"Mozilla/5.0 (compatible; Cortex/{user_agent_suffix})",
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if config.cookie and not api_key:
        headers["Cookie"] = config.cookie
        if config.origin:
            headers["Origin"] = config.origin
        if config.referer:
            headers["Referer"] = config.referer
    if content_type_json:
        headers["Content-Type"] = "application/json"
    if requested_sites is not None and str(requested_sites).strip():
        headers["RequestedSites"] = str(requested_sites).strip()
    return headers


def _env_http_request(
    config: LeanDNAEnvConfig,
    method: str,
    url: str,
    *,
    requested_sites: str | None,
    user_agent_suffix: str,
    content_type_json: bool,
    timeout_seconds: float,
    extra_kw: dict[str, Any],
) -> requests.Response:
    header_kw = dict(
        requested_sites=requested_sites,
        user_agent_suffix=user_agent_suffix,
        content_type_json=content_type_json,
    )
    r = requests.request(
        method,
        url,
        headers=build_leandna_env_headers(config, **header_kw),
        timeout=timeout_seconds,
        **extra_kw,
    )
    if r.status_code == 401 and (config.api_key or "").strip():
        reset_auth_session_cache()
        r = requests.request(
            method,
            url,
            headers=build_leandna_env_headers(config, force_refresh_session=True, **header_kw),
            timeout=timeout_seconds,
            **extra_kw,
        )
    return r


def env_get_json(
    config: LeanDNAEnvConfig,
    path: str,
    *,
    query: dict[str, Any] | None = None,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
    max_response_chars: int = 500_000,
    user_agent_suffix: str = "leandna-data-api-env/1.0",
) -> dict[str, Any]:
    try:
        rel = normalize_data_api_relative_path(path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    url = f"{config.base_url}/data/{rel}"
    params = {k: v for k, v in (query or {}).items() if v is not None and v != ""}
    extra_kw: dict[str, Any] = {}
    if params:
        extra_kw["params"] = params
    try:
        r = _env_http_request(
            config,
            "GET",
            url,
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
            content_type_json=False,
            timeout_seconds=timeout_seconds,
            extra_kw=extra_kw,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except requests.RequestException as e:
        return {"ok": False, "error": f"request failed: {e}", "url": url}
    return _response_envelope(r, url=url, max_response_chars=max_response_chars)


def env_mutate_json(
    config: LeanDNAEnvConfig,
    method: str,
    path: str,
    *,
    query: dict[str, Any] | None = None,
    json_body: Any | None = None,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
    max_response_chars: int = 500_000,
    user_agent_suffix: str = "leandna-data-api-env/1.0",
) -> dict[str, Any]:
    m = (method or "").strip().upper()
    if m not in ("POST", "PUT", "DELETE"):
        return {"ok": False, "error": f"method must be POST, PUT, or DELETE, not {method!r}"}
    try:
        rel = normalize_data_api_relative_path(path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    url = f"{config.base_url}/data/{rel}"
    params = {k: v for k, v in (query or {}).items() if v is not None and v != ""}
    extra_kw: dict[str, Any] = {}
    if params:
        extra_kw["params"] = params
    if m in ("POST", "PUT") and json_body is not None:
        extra_kw["json"] = json_body
    try:
        r = _env_http_request(
            config,
            m,
            url,
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
            content_type_json=m in ("POST", "PUT") and json_body is not None,
            timeout_seconds=timeout_seconds,
            extra_kw=extra_kw,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except requests.RequestException as e:
        return {"ok": False, "error": f"request failed: {e}", "url": url}
    return _response_envelope(r, url=url, max_response_chars=max_response_chars)
