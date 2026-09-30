"""Encrypted store for the Gmail refresh token granted at Google sign-in.

The token is keyed by the Workspace email and encrypted with the KPI web
session secret. It is not written to the session cookie.
"""

from __future__ import annotations

import base64
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from src.config import CORTEX_CACHE_ROOT


class GoogleTokenStoreError(RuntimeError):
    pass


def token_db_path() -> Path:
    return Path(CORTEX_CACHE_ROOT) / "google" / "refresh_tokens.sqlite"


def save_refresh_token(email: str, refresh_token: str, *, session_secret: str) -> None:
    """Replace the stored Gmail refresh token for ``email``."""
    address = _email(email)
    secret = _required_secret(refresh_token, "refresh token")
    path = token_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS google_refresh_token (
                email TEXT PRIMARY KEY,
                token TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO google_refresh_token (email, token, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(email) DO UPDATE SET
                token = excluded.token,
                updated_at = excluded.updated_at
            """,
            (address, _fernet(session_secret).encrypt(secret.encode()).decode("ascii"), _now()),
        )
        conn.commit()
    finally:
        conn.close()


def load_refresh_token(email: str, *, session_secret: str) -> str | None:
    """Return the refresh token for ``email``, or None when that user has not granted Gmail."""
    address = _email(email)
    path = token_db_path()
    if not path.is_file():
        return None
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT token FROM google_refresh_token WHERE email = ?",
            (address,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    try:
        return _fernet(session_secret).decrypt(str(row[0]).encode("ascii")).decode()
    except (InvalidToken, ValueError) as exc:
        raise GoogleTokenStoreError(
            f"could not decrypt the Gmail refresh token for {address}"
        ) from exc


def _fernet(session_secret: str) -> Fernet:
    raw = _required_secret(session_secret, "session secret")
    key = base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    return Fernet(key)


def _email(email: str) -> str:
    address = str(email or "").strip().lower()
    if "@" not in address:
        raise GoogleTokenStoreError("Gmail token store requires an email address")
    return address


def _required_secret(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise GoogleTokenStoreError(f"{label} is required to store a Gmail refresh token")
    return text


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
