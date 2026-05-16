"""LeanDNA Data API credentials and HTTP for a specific environment bucket (PR_* / ST_*)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

import requests

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


def load_leandna_env_config(bucket: LeanDNAEnvBucket) -> LeanDNAEnvConfig:
    """Load ``PR_*`` (production) or ``ST_*`` (staging) LeanDNA Data API settings from the environment."""
    prefix = "PR_" if bucket == "production" else "ST_"
    base = (os.environ.get(f"{prefix}LEANDNA_DATA_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        raise ValueError(
            f"{prefix}LEANDNA_DATA_API_BASE_URL is required to use LeanDNA {bucket} "
            f"(set in .env; see docs/SETUP/LEANDNA_SETUP.md)."
        )
    bearer = (os.environ.get(f"{prefix}LEANDNA_DATA_API_BEARER_TOKEN") or "").strip()
    cookie = (os.environ.get(f"{prefix}LEANDNA_DATA_API_COOKIE") or "").strip()
    if not bearer and not cookie:
        raise ValueError(
            f"Set {prefix}LEANDNA_DATA_API_BEARER_TOKEN and/or {prefix}LEANDNA_DATA_API_COOKIE."
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
) -> dict[str, str]:
    bearer = _normalize_bearer_token(config.bearer_token)
    if not bearer and not config.cookie:
        raise ValueError(f"LeanDNA {config.bucket}: missing bearer token and cookie.")
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": f"Mozilla/5.0 (compatible; BPO/{user_agent_suffix})",
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if config.cookie:
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
    try:
        headers = build_leandna_env_headers(
            config,
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    try:
        r = requests.get(url, headers=headers, params=params or None, timeout=timeout_seconds)
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
    if config.bucket == "production":
        from .config import leandna_http_mutations_allowed

        if not leandna_http_mutations_allowed():
            return {
                "ok": False,
                "error": "Refusing to mutate LeanDNA production (set BPO_ALLOW_PRODUCTION_MUTATIONS=true to override).",
            }
    m = (method or "").strip().upper()
    if m not in ("POST", "PUT", "DELETE"):
        return {"ok": False, "error": f"method must be POST, PUT, or DELETE, not {method!r}"}
    try:
        rel = normalize_data_api_relative_path(path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    url = f"{config.base_url}/data/{rel}"
    params = {k: v for k, v in (query or {}).items() if v is not None and v != ""}
    try:
        headers = build_leandna_env_headers(
            config,
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
            content_type_json=m in ("POST", "PUT") and json_body is not None,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    kw: dict[str, Any] = {"headers": headers, "timeout": timeout_seconds}
    if params:
        kw["params"] = params
    if m in ("POST", "PUT") and json_body is not None:
        kw["json"] = json_body
    try:
        r = requests.request(m, url, **kw)
    except requests.RequestException as e:
        return {"ok": False, "error": f"request failed: {e}", "url": url}
    return _response_envelope(r, url=url, max_response_chars=max_response_chars)
