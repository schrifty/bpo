"""Jira Cloud connection — site REST (default) or Atlassian API gateway (opt-in)."""

from __future__ import annotations

import os
from base64 import b64encode
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

from .config import logger

_ATLASSIAN_GATEWAY = "https://api.atlassian.com"
_ACCESSIBLE_RESOURCES_URL = f"{_ATLASSIAN_GATEWAY}/oauth/token/accessible-resources"


def _env(name: str) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return None
    s = v.strip()
    return s or None


def _auth_mode() -> str:
    """``site`` (default): ``{JIRA_URL}/rest/...`` with email+token Basic. ``gateway``: API gateway."""
    raw = (_env("JIRA_AUTH_MODE") or "site").strip().lower()
    if raw in ("gateway", "atlassian", "cloud"):
        return "gateway"
    if raw in ("site", "legacy", "classic", ""):
        return "site"
    raise ValueError(
        f"Invalid JIRA_AUTH_MODE={raw!r}; use site (default) or gateway"
    )


@dataclass(frozen=True)
class JiraConnectionSettings:
    """REST base URL, browse host (issue links), Authorization headers, and auth mode."""

    api_base_url: str
    browse_base_url: str
    headers: dict[str, str]
    cloud_id: str
    auth_mode: str


def _site_base_url() -> str:
    site = (_env("JIRA_URL") or "").rstrip("/")
    if not site:
        raise ValueError(
            "JIRA_URL must be set (e.g. https://yourorg.atlassian.net)"
        )
    return site


def _gateway_auth_header(*, token: str, email: str | None) -> dict[str, str]:
    sa_auth = (os.environ.get("JIRA_SERVICE_ACCOUNT_AUTH") or "bearer").strip().lower()
    if sa_auth in ("basic", "email"):
        em = (email or "").strip()
        if not em:
            raise ValueError(
                "JIRA_EMAIL is required when JIRA_SERVICE_ACCOUNT_AUTH=basic "
                "(service account email from Atlassian Administration)"
            )
        encoded = b64encode(f"{em}:{token}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}
    if sa_auth in ("bearer", "token", ""):
        return {"Authorization": f"Bearer {token}"}
    raise ValueError(
        f"Invalid JIRA_SERVICE_ACCOUNT_AUTH={sa_auth!r}; use bearer or basic"
    )


def fetch_cloud_id_for_site(
    *,
    token: str,
    site_url: str,
    email: str | None = None,
    timeout: float = 30.0,
) -> str:
    """Resolve cloud id from ``GET /oauth/token/accessible-resources`` for a scoped token."""
    site = (site_url or "").strip().rstrip("/")
    if not site:
        raise ValueError("site_url is required to match accessible-resources")
    parsed = urlparse(site if "://" in site else f"https://{site}")
    want_host = (parsed.hostname or "").lower()
    if not want_host:
        raise ValueError(f"Could not parse hostname from JIRA_URL={site_url!r}")

    headers = {**_gateway_auth_header(token=token, email=email), "Accept": "application/json"}
    resp = requests.get(_ACCESSIBLE_RESOURCES_URL, headers=headers, timeout=timeout)
    resp.raise_for_status()
    resources = resp.json()
    if not isinstance(resources, list):
        raise ValueError("accessible-resources returned non-list JSON")

    matches: list[tuple[str, str]] = []
    for row in resources:
        if not isinstance(row, dict):
            continue
        cid = (row.get("id") or "").strip()
        url = (row.get("url") or "").strip().rstrip("/")
        if not cid or not url:
            continue
        host = urlparse(url).hostname or ""
        if host.lower() == want_host:
            matches.append((cid, url))

    if len(matches) == 1:
        return matches[0][0]
    if len(matches) > 1:
        raise ValueError(
            f"Multiple cloud ids match {want_host!r}: {[m[0] for m in matches]}; set JIRA_CLOUD_ID explicitly"
        )

    hosts = sorted(
        {
            (urlparse((r.get("url") or "")).hostname or "").lower()
            for r in resources
            if isinstance(r, dict) and r.get("url")
        }
        - {""}
    )
    raise ValueError(
        f"No accessible-resources entry for {want_host!r}. "
        f"Sites on this token: {', '.join(hosts[:12])}{'…' if len(hosts) > 12 else ''}"
    )


def fetch_cloud_id_from_tenant_info(site_url: str, *, timeout: float = 30.0) -> str:
    """Resolve cloud id from ``GET {site}/_edge/tenant_info`` (no auth; public on many Cloud sites)."""
    site = (site_url or "").strip().rstrip("/")
    if not site:
        raise ValueError("site_url is required for tenant_info cloud id lookup")
    parsed = urlparse(site if "://" in site else f"https://{site}")
    if not (parsed.hostname or "").lower():
        raise ValueError(f"Could not parse hostname from JIRA_URL={site_url!r}")

    resp = requests.get(f"{site}/_edge/tenant_info", timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("tenant_info returned non-object JSON")
    cid = (data.get("cloudId") or "").strip()
    if not cid:
        raise ValueError("tenant_info JSON missing cloudId")
    return cid


def resolve_jira_cloud_id(*, token: str, email: str | None) -> str:
    """``JIRA_CLOUD_ID`` or resolve from ``JIRA_URL`` when ``JIRA_CLOUD_ID_AUTO=true`` (gateway only)."""
    explicit = _env("JIRA_CLOUD_ID")
    if explicit:
        return explicit
    auto = (os.environ.get("JIRA_CLOUD_ID_AUTO") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if auto:
        browse = _site_base_url()
        logger.info("Jira: resolving JIRA_CLOUD_ID (JIRA_CLOUD_ID_AUTO)")
        try:
            return fetch_cloud_id_for_site(token=token, site_url=browse, email=email)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (401, 403):
                logger.warning(
                    "Jira: accessible-resources returned %s; falling back to _edge/tenant_info",
                    status,
                )
                return fetch_cloud_id_from_tenant_info(browse)
            raise
    raise ValueError(
        "JIRA_CLOUD_ID is required for JIRA_AUTH_MODE=gateway "
        "(https://api.atlassian.com/ex/jira/{cloudId}). "
        "Set JIRA_CLOUD_ID in .env, JIRA_CLOUD_ID_AUTO=true, or use JIRA_AUTH_MODE=site."
    )


def _build_site_settings(*, token: str) -> JiraConnectionSettings:
    email = (_env("JIRA_EMAIL") or "").strip()
    if not email:
        raise ValueError(
            "JIRA_EMAIL and JIRA_API_TOKEN must be set in .env for site auth "
            "(JIRA_AUTH_MODE=site, the default)"
        )
    site = _site_base_url()
    encoded = b64encode(f"{email}:{token}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {encoded}",
    }
    logger.info("Jira: site REST (%s)", site)
    return JiraConnectionSettings(
        api_base_url=site,
        browse_base_url=site,
        headers=headers,
        cloud_id="",
        auth_mode="site",
    )


def _build_gateway_settings(*, token: str) -> JiraConnectionSettings:
    browse = _site_base_url()
    cloud_id = resolve_jira_cloud_id(token=token, email=_env("JIRA_EMAIL"))
    api_base = f"{_ATLASSIAN_GATEWAY}/ex/jira/{cloud_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        **_gateway_auth_header(token=token, email=_env("JIRA_EMAIL")),
    }
    logger.info(
        "Jira: API gateway (cloud_id=%s…, browse=%s)",
        cloud_id[:8],
        browse,
    )
    return JiraConnectionSettings(
        api_base_url=api_base,
        browse_base_url=browse,
        headers=headers,
        cloud_id=cloud_id,
        auth_mode="gateway",
    )


def build_jira_connection_settings() -> JiraConnectionSettings:
    """Build REST base URL and Authorization headers from environment."""
    token = _env("JIRA_API_TOKEN") or ""
    if not token:
        raise ValueError("JIRA_API_TOKEN must be set in .env")

    if _auth_mode() == "site":
        return _build_site_settings(token=token)
    return _build_gateway_settings(token=token)


def jira_connection_summary(settings: JiraConnectionSettings) -> dict[str, Any]:
    """Non-secret summary for logs / diagnostics."""
    out: dict[str, Any] = {
        "auth_mode": settings.auth_mode,
        "api_base_url": settings.api_base_url,
        "browse_base_url": settings.browse_base_url,
    }
    if settings.cloud_id:
        out["cloud_id"] = settings.cloud_id
    return out
