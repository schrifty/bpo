"""Gmail read client for the mailbox granted at Google sign-in.

Reads use the refresh token stored for that Workspace user. The Gmail API
scope is ``gmail.readonly``. Message fetches ask for metadata only: id,
thread, date, and the From, To, Cc, Date, and Subject headers.
"""

from __future__ import annotations

from typing import Any

import httpx

from src.kpi_web.google_tokens import GoogleTokenStoreError, load_refresh_token
from src.kpi_web.settings import KPIWebSettings, load_kpi_web_settings

GMAIL_API = "https://gmail.googleapis.com/gmail/v1"
TOKEN_URL = "https://oauth2.googleapis.com/token"
METADATA_HEADERS = ("From", "To", "Cc", "Date", "Subject")


class GmailClientError(RuntimeError):
    pass


def gmail_configured(email: str, *, session_secret: str) -> bool:
    """True when a refresh token is stored for ``email``."""
    try:
        return bool(load_refresh_token(email, session_secret=session_secret))
    except GoogleTokenStoreError as exc:
        raise GmailClientError(str(exc)) from exc


class GmailClient:
    """Read one user's mailbox with the refresh token from their Google sign-in."""

    def __init__(self, email: str, *, settings: KPIWebSettings | None = None) -> None:
        self.email = str(email or "").strip().lower()
        self.settings = settings or load_kpi_web_settings()
        if not self.settings.google_oauth_configured:
            raise GmailClientError(
                "Google OAuth is not configured "
                "(set CORTEX_KPI_WEB_GOOGLE_CLIENT_ID and CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET)"
            )
        if not self.settings.session_secret:
            raise GmailClientError("session secret is required to read a stored Gmail refresh token")
        try:
            refresh = load_refresh_token(self.email, session_secret=self.settings.session_secret)
        except GoogleTokenStoreError as exc:
            raise GmailClientError(str(exc)) from exc
        if not refresh:
            raise GmailClientError(
                f"no Gmail refresh token is stored for {self.email}; sign in and allow Gmail access"
            )
        self._refresh_token = refresh

    def list_messages(self, query: str, *, max_results: int = 100) -> list[dict[str, Any]]:
        """Message ids matching a Gmail search query, newest first."""
        if max_results < 1:
            raise GmailClientError(f"max_results must be at least 1, got {max_results}")
        payload = self._get(
            "/users/me/messages",
            params={"q": query, "maxResults": min(max_results, 500)},
        )
        messages = payload.get("messages") or []
        if not isinstance(messages, list):
            raise GmailClientError("Gmail messages.list returned a non-list messages field")
        return [row for row in messages if isinstance(row, dict)]

    def get_message(self, message_id: str) -> dict[str, Any]:
        """Metadata for one message, including From, To, Cc, Date, and Subject."""
        cleaned = str(message_id or "").strip()
        if not cleaned:
            raise GmailClientError("message id is required")
        params: list[tuple[str, str]] = [("format", "metadata")]
        params.extend(("metadataHeaders", name) for name in METADATA_HEADERS)
        return self._get(f"/users/me/messages/{cleaned}", params=params)

    def _get(self, path: str, *, params: Any) -> dict[str, Any]:
        access = self._access_token()
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{GMAIL_API}{path}",
                params=params,
                headers={"Authorization": f"Bearer {access}"},
            )
        if response.status_code >= 400:
            raise GmailClientError(
                f"Gmail {path} failed ({response.status_code}): {response.text}"
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise GmailClientError(f"Gmail {path} returned a non-object payload")
        return payload

    def _access_token(self) -> str:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                TOKEN_URL,
                data={
                    "client_id": self.settings.google_client_id,
                    "client_secret": self.settings.google_client_secret,
                    "refresh_token": self._refresh_token,
                    "grant_type": "refresh_token",
                },
            )
        if response.status_code >= 400:
            raise GmailClientError(
                f"Google token refresh failed ({response.status_code}): {response.text}"
            )
        access = response.json().get("access_token")
        if not access:
            raise GmailClientError("Google token refresh response missing access_token")
        return str(access)
