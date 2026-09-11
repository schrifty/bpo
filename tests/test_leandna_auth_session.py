"""Tests for LeanDNA Auth API session exchange."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.leandna_auth_session import (
    derive_auth_api_base_url,
    fetch_data_api_session_id,
    reset_auth_session_cache,
)


def test_derive_auth_api_base_url_from_data_api() -> None:
    assert derive_auth_api_base_url("https://app.leandna.com/api") == "https://app.leandna.com/auth"
    assert (
        derive_auth_api_base_url("https://app.staging.leandna.com/api/")
        == "https://app.staging.leandna.com/auth"
    )


def test_fetch_data_api_session_id_requires_identifier_and_passcode() -> None:
    reset_auth_session_cache()
    with pytest.raises(ValueError, match="identifier:passcode"):
        fetch_data_api_session_id(
            "no-colon-here",
            auth_base_url="https://app.leandna.com/auth",
            post=lambda *a, **k: None,
        )


def test_fetch_data_api_session_id_posts_secret_and_caches() -> None:
    reset_auth_session_cache()
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.text = '{"sessionId": "sess-1"}'
    resp.json.return_value = {"sessionId": "sess-1"}
    post = MagicMock(return_value=resp)

    sid = fetch_data_api_session_id(
        "my-secret:passcode",
        auth_base_url="https://app.leandna.com/auth",
        post=post,
    )
    assert sid == "sess-1"
    post.assert_called_once()
    assert post.call_args.args[0] == "https://app.leandna.com/auth/data/session"
    assert post.call_args.kwargs["json"] == {"secret": "my-secret:passcode"}

    sid2 = fetch_data_api_session_id(
        "my-secret:passcode",
        auth_base_url="https://app.leandna.com/auth",
        post=post,
    )
    assert sid2 == "sess-1"
    assert post.call_count == 1


def test_fetch_data_api_session_id_force_refresh_skips_cache() -> None:
    reset_auth_session_cache()
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.text = '{"sessionId": "sess-2"}'
    resp.json.return_value = {"sessionId": "sess-2"}
    post = MagicMock(return_value=resp)

    fetch_data_api_session_id("k:p", auth_base_url="https://app.leandna.com/auth", post=post)
    fetch_data_api_session_id(
        "k:p",
        auth_base_url="https://app.leandna.com/auth",
        post=post,
        force_refresh=True,
    )
    assert post.call_count == 2


def test_fetch_data_api_session_id_fail_loud_on_http_error() -> None:
    reset_auth_session_cache()
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 400
    resp.text = '{"reason":"Secret token should not be empty or null."}'
    post = MagicMock(return_value=resp)

    with pytest.raises(ValueError, match="Auth API 400"):
        fetch_data_api_session_id("k:p", auth_base_url="https://app.leandna.com/auth", post=post)
