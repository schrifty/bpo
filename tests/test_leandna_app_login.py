"""Tests for LeanDNA app login helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests


def test_parse_session_from_cookie_jar() -> None:
    from src.leandna_app_login import _extract_session_id

    session = MagicMock()
    cookie = MagicMock()
    cookie.name = "LDNASESSIONID"
    cookie.value = "abc123session"
    session.cookies = [cookie]
    resp = MagicMock(headers={})
    assert _extract_session_id(session, resp) == "abc123session"


def test_login_failure_when_no_cookie() -> None:
    from src.leandna_app_login import login_leandna_app, LeanDNAAppLoginError

    class FakeSession:
        cookies = []
        headers = {}

        def post(self, *args, **kwargs):
            r = MagicMock()
            r.status_code = 200
            r.text = "<title>LeanDNA sign in</title>"
            r.headers = {}
            return r

    import src.leandna_app_login as mod

    orig = requests.Session
    try:
        mod.requests.Session = lambda: FakeSession()  # type: ignore[misc, assignment]
        with pytest.raises(LeanDNAAppLoginError):
            login_leandna_app("u", "p", server="https://app.example.com")
    finally:
        mod.requests.Session = orig


def test_resolve_credentials_requires_password(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.leandna_app_login import LeanDNAAppLoginError, resolve_login_credentials

    monkeypatch.setenv("LEANDNA_APP_EMAIL", "a@b.com")
    monkeypatch.delenv("LEANDNA_APP_PASSWORD", raising=False)
    with pytest.raises(LeanDNAAppLoginError, match="password"):
        resolve_login_credentials()


def test_parse_session_input_raw_and_cookie() -> None:
    from src.leandna_app_login import parse_session_input

    assert parse_session_input("abc123session") == "abc123session"
    assert parse_session_input("LDNASESSIONID=xyz; other=1") == "xyz"
    assert parse_session_input("") is None
    assert parse_session_input("bad=only") is None


def test_sso_login_manual_reads_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    from io import StringIO

    from src.leandna_app_login import login_leandna_app_sso

    opened: list[str] = []

    def fake_open(url: str, new: int = 0, autoraise: bool = True) -> bool:
        opened.append(url)
        return True

    monkeypatch.setattr("src.leandna_app_login.webbrowser.open", fake_open)
    stream = StringIO("paste-me-session-id\n")
    result = login_leandna_app_sso(
        server="https://app.example.com",
        use_playwright=False,
        open_browser=True,
        input_stream=stream,
        username="u@example.com",
    )
    assert result.session_id == "paste-me-session-id"
    assert result.server == "https://app.example.com"
    assert opened == ["https://app.example.com/application/sso.html"]


def test_sso_login_url() -> None:
    from src.leandna_app_login import sso_login_url

    assert sso_login_url("https://app.example.com") == (
        "https://app.example.com/application/sso.html"
    )
