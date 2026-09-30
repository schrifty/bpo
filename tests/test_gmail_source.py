"""Gmail is a registered source backed by the sign-in refresh token."""

from pathlib import Path

import httpx
import pytest

from src.gmail_client import GmailClient, GmailClientError
from src.healthscore_web.framework import available_data_source_names, available_source_labels
from src.kpi_web.auth import OAUTH_SCOPES, build_google_authorize_url
from src.kpi_web.google_tokens import load_refresh_token, save_refresh_token
from src.kpi_web.settings import KPIWebSettings


def _settings(tmp_path: Path) -> KPIWebSettings:
    return KPIWebSettings(
        base_url="http://127.0.0.1:8080",
        google_client_id="client-id",
        google_client_secret="client-secret",
        session_secret="session-secret",
        allowed_domains=("leandna.com",),
        allow_dev_auth=False,
        dev_user="",
        cookie_name="cortex_kpi_session",
        cookie_secure=False,
        session_ttl_seconds=3600,
        host="127.0.0.1",
        port=8080,
        registry_path=None,
        owners_path=None,
        skip_s3=True,
    )


def test_gmail_is_an_available_healthscore_source() -> None:
    names = {name.casefold() for name in available_data_source_names()}
    assert "gmail" in names
    assert "Gmail" in available_source_labels()


def test_sign_in_requests_gmail_offline(tmp_path: Path) -> None:
    url = build_google_authorize_url(settings=_settings(tmp_path), state="state")
    assert "gmail.readonly" in OAUTH_SCOPES
    assert "access_type=offline" in url
    assert "prompt=consent+select_account" in url or "prompt=consent%20select_account" in url


def test_refresh_token_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.kpi_web.google_tokens.CORTEX_CACHE_ROOT", tmp_path)
    save_refresh_token("Marc.Schriftman@leandna.com", "refresh-1", session_secret="session-secret")
    assert load_refresh_token("marc.schriftman@leandna.com", session_secret="session-secret") == "refresh-1"
    save_refresh_token("marc.schriftman@leandna.com", "refresh-2", session_secret="session-secret")
    assert load_refresh_token("marc.schriftman@leandna.com", session_secret="session-secret") == "refresh-2"


def test_list_messages_uses_the_stored_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.kpi_web.google_tokens.CORTEX_CACHE_ROOT", tmp_path)
    settings = _settings(tmp_path)
    save_refresh_token("owner@leandna.com", "refresh-1", session_secret=settings.session_secret)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(200, json={"access_token": "access-1"})
        assert request.headers["authorization"] == "Bearer access-1"
        return httpx.Response(200, json={"messages": [{"id": "m1", "threadId": "t1"}]})

    monkeypatch.setattr(httpx.Client, "post", lambda self, url, **kwargs: handler(httpx.Request("POST", url, data=kwargs.get("data"))))
    monkeypatch.setattr(
        httpx.Client,
        "get",
        lambda self, url, **kwargs: handler(httpx.Request("GET", url, params=kwargs.get("params"), headers=kwargs.get("headers"))),
    )
    messages = GmailClient("owner@leandna.com", settings=settings).list_messages("newer_than:7d")
    assert messages == [{"id": "m1", "threadId": "t1"}]
    assert any("oauth2.googleapis.com" in url for url in calls)
    assert any("gmail.googleapis.com" in url for url in calls)


def test_missing_refresh_token_fails_loud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.kpi_web.google_tokens.CORTEX_CACHE_ROOT", tmp_path)
    with pytest.raises(GmailClientError, match="no Gmail refresh token"):
        GmailClient("owner@leandna.com", settings=_settings(tmp_path))
