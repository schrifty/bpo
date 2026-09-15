"""Environment settings for the KPI web app."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _truthy(name: str, *, environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return env.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _csv_domains(raw: str) -> tuple[str, ...]:
    out: list[str] = []
    for part in raw.split(","):
        d = part.strip().lower().lstrip("@")
        if d and d not in out:
            out.append(d)
    return tuple(out)


@dataclass(frozen=True)
class KPIWebSettings:
    """Runtime config for Google Workspace SSO + local/dev auth."""

    base_url: str
    google_client_id: str
    google_client_secret: str
    session_secret: str
    allowed_domains: tuple[str, ...]
    allow_dev_auth: bool
    dev_user: str
    cookie_name: str
    cookie_secure: bool
    session_ttl_seconds: int
    host: str
    port: int
    registry_path: Path | None
    owners_path: Path | None
    skip_s3: bool

    @property
    def google_oauth_configured(self) -> bool:
        return bool(self.google_client_id and self.google_client_secret)

    @property
    def redirect_uri(self) -> str:
        return f"{self.base_url.rstrip('/')}/auth/callback"


def load_kpi_web_settings(*, environ: dict[str, str] | None = None) -> KPIWebSettings:
    """Load KPI web settings from the environment (fail-loud at request time, not import)."""
    env = environ if environ is not None else os.environ
    base = (env.get("CORTEX_KPI_WEB_BASE_URL") or "http://127.0.0.1:8080").strip()
    domains_raw = (
        env.get("CORTEX_KPI_WEB_ALLOWED_DOMAINS") or "leandna.com"
    ).strip()
    secret = (env.get("CORTEX_KPI_WEB_SESSION_SECRET") or "").strip()
    if not secret and _truthy("CORTEX_KPI_WEB_ALLOW_DEV_AUTH", environ=env):
        # Dev-only default so local ``--dev-user`` works without extra secrets.
        secret = "cortex-kpi-web-dev-session-secret"
    registry = (env.get("CORTEX_KPI_WEB_REGISTRY") or "").strip()
    owners = (env.get("CORTEX_KPI_WEB_OWNERS") or "").strip()
    host = (env.get("CORTEX_KPI_WEB_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    try:
        port = int((env.get("CORTEX_KPI_WEB_PORT") or "8080").strip() or "8080")
    except ValueError as exc:
        raise ValueError("CORTEX_KPI_WEB_PORT must be an integer") from exc
    try:
        ttl = int((env.get("CORTEX_KPI_WEB_SESSION_TTL_SECONDS") or "86400").strip())
    except ValueError as exc:
        raise ValueError("CORTEX_KPI_WEB_SESSION_TTL_SECONDS must be an integer") from exc
    return KPIWebSettings(
        base_url=base.rstrip("/"),
        google_client_id=(env.get("CORTEX_KPI_WEB_GOOGLE_CLIENT_ID") or "").strip(),
        google_client_secret=(env.get("CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET") or "").strip(),
        session_secret=secret,
        allowed_domains=_csv_domains(domains_raw) or ("leandna.com",),
        allow_dev_auth=_truthy("CORTEX_KPI_WEB_ALLOW_DEV_AUTH", environ=env),
        dev_user=(env.get("CORTEX_KPI_WEB_DEV_USER") or "").strip().lower(),
        cookie_name=(env.get("CORTEX_KPI_WEB_COOKIE_NAME") or "cortex_kpi_session").strip()
        or "cortex_kpi_session",
        cookie_secure=_truthy("CORTEX_KPI_WEB_COOKIE_SECURE", environ=env)
        or base.lower().startswith("https://"),
        session_ttl_seconds=max(300, ttl),
        host=host,
        port=port,
        registry_path=Path(registry) if registry else None,
        owners_path=Path(owners) if owners else None,
        skip_s3=_truthy("CORTEX_KPI_WEB_SKIP_S3", environ=env),
    )
