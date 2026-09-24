"""Google Workspace SSO + signed session cookie for the KPI web."""

from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx
import jwt
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from src.kpi_owners import (
    KPIOwnershipError,
    assert_actor_may_view_catalog,
    load_kpi_owners_config,
    normalize_owner_email,
)
from src.kpi_web.settings import KPIWebSettings

logger = logging.getLogger(__name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
OAUTH_SCOPES = "openid email profile"


@dataclass(frozen=True)
class KPIWebUser:
    email: str
    name: str | None
    picture: str | None
    is_catalog_admin: bool
    auth_method: str  # google | dev


class KPIWebAuthError(Exception):
    """Authentication / session failure (maps to 401/403)."""

    def __init__(self, message: str, *, status_code: int = 401) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def email_domain(email: str) -> str:
    parts = email.rsplit("@", 1)
    return parts[1].lower() if len(parts) == 2 else ""


def domain_allowed(email: str, allowed: tuple[str, ...]) -> bool:
    domain = email_domain(email)
    return bool(domain) and domain in {d.lower() for d in allowed}


def encode_session(
    user: KPIWebUser,
    *,
    settings: KPIWebSettings,
    now: int | None = None,
) -> str:
    if not settings.session_secret:
        raise KPIWebAuthError(
            "CORTEX_KPI_WEB_SESSION_SECRET is required (or enable "
            "CORTEX_KPI_WEB_ALLOW_DEV_AUTH for local defaults)",
            status_code=500,
        )
    issued = int(time.time() if now is None else now)
    payload = {
        "sub": user.email,
        "name": user.name,
        "picture": user.picture,
        "auth": user.auth_method,
        "iat": issued,
        "exp": issued + settings.session_ttl_seconds,
    }
    return jwt.encode(payload, settings.session_secret, algorithm="HS256")


def decode_session(token: str, *, settings: KPIWebSettings) -> KPIWebUser:
    if not settings.session_secret:
        raise KPIWebAuthError("session secret not configured", status_code=500)
    try:
        payload = jwt.decode(
            token,
            settings.session_secret,
            algorithms=["HS256"],
            options={"require": ["sub", "exp", "iat"]},
        )
    except jwt.PyJWTError as exc:
        raise KPIWebAuthError(f"invalid session: {exc}") from exc
    email = normalize_owner_email(payload.get("sub"))
    if not email:
        raise KPIWebAuthError("session missing subject email")
    if not domain_allowed(email, settings.allowed_domains):
        raise KPIWebAuthError(
            f"email domain not allowed for {email!r}",
            status_code=403,
        )
    owners = load_kpi_owners_config(path=settings.owners_path)
    try:
        assert_actor_may_view_catalog(email, owners=owners)
    except KPIOwnershipError as exc:
        raise KPIWebAuthError(str(exc), status_code=403) from exc
    name = payload.get("name")
    picture = payload.get("picture")
    auth = str(payload.get("auth") or "google")
    return KPIWebUser(
        email=email,
        name=str(name).strip() if name else None,
        picture=str(picture).strip() if picture else None,
        is_catalog_admin=owners.is_catalog_admin(email),
        auth_method=auth,
    )


def set_session_cookie(response: Response, token: str, *, settings: KPIWebSettings) -> None:
    response.set_cookie(
        settings.cookie_name,
        token,
        max_age=settings.session_ttl_seconds,
        httponly=True,
        samesite="lax",
        secure=settings.cookie_secure,
        path="/",
    )


def clear_session_cookie(response: Response, *, settings: KPIWebSettings) -> None:
    response.delete_cookie(settings.cookie_name, path="/")


def current_user(request: Request) -> KPIWebUser | None:
    """Return the signed-in user, or ``None`` when no session cookie is present."""
    settings: KPIWebSettings = request.app.state.settings
    raw = request.cookies.get(settings.cookie_name)
    if not raw:
        return None
    return decode_session(raw, settings=settings)


def require_user(request: Request) -> KPIWebUser:
    user = current_user(request)
    if user is None:
        raise KPIWebAuthError("authentication required")
    return user


def build_google_authorize_url(*, settings: KPIWebSettings, state: str) -> str:
    if not settings.google_oauth_configured:
        raise KPIWebAuthError(
            "Google OAuth is not configured "
            "(set CORTEX_KPI_WEB_GOOGLE_CLIENT_ID and CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET)",
            status_code=500,
        )
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.redirect_uri,
        "response_type": "code",
        "scope": OAUTH_SCOPES,
        "access_type": "online",
        "include_granted_scopes": "true",
        "state": state,
        "prompt": "select_account",
    }
    # Prefer Workspace account chooser for known domains when a single domain is set.
    if len(settings.allowed_domains) == 1:
        params["hd"] = settings.allowed_domains[0]
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_google_code(code: str, *, settings: KPIWebSettings) -> dict[str, Any]:
    """Exchange an auth code for tokens; return userinfo dict with email/name/picture."""
    if not settings.google_oauth_configured:
        raise KPIWebAuthError("Google OAuth is not configured", status_code=500)
    with httpx.Client(timeout=30.0) as client:
        token_resp = client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": settings.redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code >= 400:
            raise KPIWebAuthError(
                f"Google token exchange failed ({token_resp.status_code}): {token_resp.text}",
                status_code=502,
            )
        tokens = token_resp.json()
        access = tokens.get("access_token")
        if not access:
            raise KPIWebAuthError("Google token response missing access_token", status_code=502)
        info_resp = client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access}"},
        )
        if info_resp.status_code >= 400:
            raise KPIWebAuthError(
                f"Google userinfo failed ({info_resp.status_code}): {info_resp.text}",
                status_code=502,
            )
        return info_resp.json()


def user_from_google_info(
    info: dict[str, Any],
    *,
    settings: KPIWebSettings,
) -> KPIWebUser:
    email = normalize_owner_email(info.get("email"))
    if not email:
        raise KPIWebAuthError("Google account has no email", status_code=403)
    if info.get("email_verified") is False:
        raise KPIWebAuthError("Google email is not verified", status_code=403)
    if not domain_allowed(email, settings.allowed_domains):
        allowed = ", ".join(settings.allowed_domains)
        raise KPIWebAuthError(
            f"email {email!r} is outside allowed Workspace domains ({allowed})",
            status_code=403,
        )
    owners = load_kpi_owners_config(path=settings.owners_path)
    try:
        assert_actor_may_view_catalog(email, owners=owners)
    except KPIOwnershipError as exc:
        raise KPIWebAuthError(str(exc), status_code=403) from exc
    name = info.get("name")
    picture = info.get("picture")
    return KPIWebUser(
        email=email,
        name=str(name).strip() if name else None,
        picture=str(picture).strip() if picture else None,
        is_catalog_admin=owners.is_catalog_admin(email),
        auth_method="google",
    )


def user_from_dev_login(*, settings: KPIWebSettings, email: str | None = None) -> KPIWebUser:
    """Explicit local/dev auth path — never a silent production fallback."""
    if not settings.allow_dev_auth:
        raise KPIWebAuthError(
            "dev auth disabled (set CORTEX_KPI_WEB_ALLOW_DEV_AUTH=true for local use)",
            status_code=403,
        )
    chosen = normalize_owner_email(email or settings.dev_user)
    if not chosen:
        raise KPIWebAuthError(
            "dev auth requires CORTEX_KPI_WEB_DEV_USER or ?email=",
            status_code=400,
        )
    logger.warning(
        "KPI web using DEV auth for %s (CORTEX_KPI_WEB_ALLOW_DEV_AUTH) — not production SSO",
        chosen,
    )
    if not domain_allowed(chosen, settings.allowed_domains):
        allowed = ", ".join(settings.allowed_domains)
        raise KPIWebAuthError(
            f"dev user {chosen!r} is outside allowed domains ({allowed})",
            status_code=403,
        )
    owners = load_kpi_owners_config(path=settings.owners_path)
    try:
        assert_actor_may_view_catalog(chosen, owners=owners)
    except KPIOwnershipError as exc:
        raise KPIWebAuthError(str(exc), status_code=403) from exc
    lead = owners.lead_for(chosen)
    display = lead.display_name if lead else None
    return KPIWebUser(
        email=chosen,
        name=display,
        picture=None,
        is_catalog_admin=owners.is_catalog_admin(chosen),
        auth_method="dev",
    )


def new_oauth_state() -> str:
    return secrets.token_urlsafe(24)


def auth_error_response(exc: KPIWebAuthError) -> JSONResponse:
    return JSONResponse(
        {"error": exc.message, "ok": False},
        status_code=exc.status_code,
    )


def login_redirect_or_error(request: Request) -> Response:
    """Start Google OAuth, or return a clear JSON error when misconfigured."""
    settings: KPIWebSettings = request.app.state.settings
    try:
        state = new_oauth_state()
        url = build_google_authorize_url(settings=settings, state=state)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    response = RedirectResponse(url, status_code=302)
    response.set_cookie(
        "cortex_kpi_oauth_state",
        state,
        max_age=600,
        httponly=True,
        samesite="lax",
        secure=settings.cookie_secure,
        path="/",
    )
    next_url = request.query_params.get("next") or "/kpis"
    if not next_url.startswith("/") or next_url.startswith("//"):
        next_url = "/kpis"
    response.set_cookie(
        "cortex_kpi_oauth_next",
        next_url,
        max_age=600,
        httponly=True,
        samesite="lax",
        secure=settings.cookie_secure,
        path="/",
    )
    return response
