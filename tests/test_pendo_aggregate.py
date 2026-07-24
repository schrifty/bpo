"""Tests for shared Pendo aggregation retry helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.pendo_aggregate import (
    call_with_pendo_retry,
    resolve_pendo_connect_read_timeout,
    resolve_pendo_read_timeout,
)


def test_resolve_pendo_read_timeout_scales_with_window() -> None:
    assert resolve_pendo_read_timeout(14) == 90.0
    assert resolve_pendo_read_timeout(7) == 90.0
    assert resolve_pendo_read_timeout(60) == 228.0
    assert resolve_pendo_read_timeout(120) == 300.0


def test_resolve_pendo_connect_read_timeout_explicit() -> None:
    assert resolve_pendo_connect_read_timeout(timeout=(5, 99.0), read_timeout_days=30, default_read_timeout=90.0) == (
        5,
        99.0,
    )
    assert resolve_pendo_connect_read_timeout(timeout=None, read_timeout_days=60, default_read_timeout=90.0) == (
        10,
        228.0,
    )
    assert resolve_pendo_connect_read_timeout(timeout=None, read_timeout_days=None, default_read_timeout=90.0) == (
        10,
        90.0,
    )


def _http_error(status: int, *, retry_after: str | None = None) -> requests.exceptions.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    if retry_after is not None:
        resp.headers["Retry-After"] = retry_after
    return requests.exceptions.HTTPError(f"{status}", response=resp)


@patch("src.pendo_aggregate.random.uniform", return_value=0.0)
@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_succeeds_after_timeout(mock_sleep, _mock_jitter) -> None:
    fn = MagicMock(
        side_effect=[
            requests.exceptions.ReadTimeout("timed out"),
            {"results": [{"visitorId": "v1"}]},
        ]
    )
    out = call_with_pendo_retry(fn, label="pageEvents", read_timeout=228.0)
    assert out["results"][0]["visitorId"] == "v1"
    assert fn.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("src.pendo_aggregate.random.uniform", return_value=0.0)
@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_raises_after_max_attempts(mock_sleep, _mock_jitter) -> None:
    fn = MagicMock(side_effect=requests.exceptions.ReadTimeout("timed out"))
    with pytest.raises(requests.exceptions.ReadTimeout):
        call_with_pendo_retry(fn, label="featureEvents", max_attempts=3)
    assert fn.call_count == 3
    assert mock_sleep.call_count == 2


@patch("src.pendo_aggregate.random.uniform", return_value=0.0)
@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_retries_on_429(mock_sleep, _mock_jitter) -> None:
    fn = MagicMock(side_effect=[_http_error(429), {"results": []}])
    out = call_with_pendo_retry(fn, label="pageEvents")
    assert out == {"results": []}
    assert fn.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_honors_retry_after_header(mock_sleep) -> None:
    fn = MagicMock(side_effect=[_http_error(503, retry_after="12"), {"results": []}])
    call_with_pendo_retry(fn, label="featureEvents")
    mock_sleep.assert_called_once_with(12.0)


@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_caps_retry_after(mock_sleep) -> None:
    fn = MagicMock(side_effect=[_http_error(429, retry_after="9999"), {"results": []}])
    call_with_pendo_retry(fn, label="pageEvents")
    mock_sleep.assert_called_once_with(60.0)


@patch("src.pendo_aggregate.random.uniform", return_value=0.0)
@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_retries_on_connection_error(mock_sleep, _mock_jitter) -> None:
    fn = MagicMock(side_effect=[requests.exceptions.ConnectionError("reset"), {"results": []}])
    call_with_pendo_retry(fn, label="pageEvents")
    assert fn.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("src.pendo_aggregate.time.sleep")
def test_call_with_pendo_retry_does_not_retry_non_retryable_http(mock_sleep) -> None:
    fn = MagicMock(side_effect=_http_error(400))
    with pytest.raises(requests.exceptions.HTTPError):
        call_with_pendo_retry(fn, label="pageEvents")
    assert fn.call_count == 1
    mock_sleep.assert_not_called()
