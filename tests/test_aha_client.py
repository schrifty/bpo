"""Tests for the Aha! REST client (no network — fake session)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.aha_client import (
    AhaClient,
    AhaClientError,
    aha_configured,
    aha_route_exists,
    check_aha_api,
    normalize_aha_domain,
)
from src.aha_routes import AHA_ROUTES
from src.qa import QARegistry


class _FakeResponse:
    def __init__(self, status_code: int, body, *, text: str = "", headers: dict | None = None):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._body = body
        if text or body is None:
            self.text = text
        else:
            self.text = "{}"
        self.headers = headers or {}

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def request(self, method, url, headers=None, params=None, json=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
                "timeout": timeout,
            }
        )
        return self._responses.pop(0)


def _client(session, **kwargs) -> AhaClient:
    return AhaClient(
        api_key="test-key",
        domain="leandna",
        session=session,
        max_retries=kwargs.pop("max_retries", 0),
        sleeper=kwargs.pop("sleeper", lambda _seconds: None),
        **kwargs,
    )


def test_requires_api_key():
    with pytest.raises(AhaClientError, match="CORTEX_AHA_API_KEY"):
        AhaClient(api_key="", domain="leandna")


def test_domain_strips_host():
    assert normalize_aha_domain("https://leandna.aha.io/api") == "leandna"
    with pytest.raises(AhaClientError, match="subdomain"):
        normalize_aha_domain("not a domain")


def test_route_catalog_covers_published_reads():
    assert aha_route_exists("GET", "/me")
    assert aha_route_exists("GET", "/products/APP")
    assert aha_route_exists("POST", "/products/APP/ideas")
    assert not aha_route_exists("GET", "/not-a-route")
    assert len(AHA_ROUTES["GET"]) == 173
    assert sum(len(paths) for paths in AHA_ROUTES.values()) == 341


def test_get_me_sends_bearer_token():
    session = _FakeSession([_FakeResponse(200, {"user": {"name": "Ada"}})])
    body = _client(session).me()
    assert body["user"]["name"] == "Ada"
    call = session.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "https://leandna.aha.io/api/v1/me"
    assert call["headers"]["Authorization"] == "Bearer test-key"
    assert "User-Agent" in call["headers"]
    assert "Content-Type" not in call["headers"]


def test_unknown_route_is_rejected_before_the_network():
    session = _FakeSession([])
    with pytest.raises(AhaClientError, match="no GET /not-a-route"):
        _client(session).get("/not-a-route")
    assert session.calls == []


def test_list_products_follows_pagination():
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                {"pagination": [{"total_pages": 2, "current_page": 1}], "products": [{"id": "1"}]},
            ),
            _FakeResponse(
                200,
                {"pagination": [{"total_pages": 2, "current_page": 2}], "products": [{"id": "2"}]},
            ),
        ]
    )
    rows = _client(session).list_products()
    assert [row["id"] for row in rows] == ["1", "2"]
    assert session.calls[0]["params"]["page"] == 1
    assert session.calls[0]["params"]["per_page"] == 200
    assert session.calls[1]["params"]["page"] == 2


def test_auth_failure_names_the_account_domain():
    session = _FakeSession([_FakeResponse(401, None, text="")])
    with pytest.raises(AhaClientError, match="leandna.aha.io"):
        _client(session).me()


def test_rate_limit_retries():
    sleeps: list[float] = []
    session = _FakeSession(
        [
            _FakeResponse(429, None, text="slow", headers={"X-Ratelimit-Reset": "1"}),
            _FakeResponse(200, {"user": {"id": "u1"}}),
        ]
    )
    with patch("src.aha_client.time.time", return_value=10), patch(
        "src.aha_client.random.uniform", return_value=0
    ):
        body = _client(session, max_retries=1, sleeper=sleeps.append).me()
    assert body["user"]["id"] == "u1"
    assert sleeps == [0.0]


def test_write_uses_documented_post():
    session = _FakeSession([_FakeResponse(200, {"idea": {"id": "IDEA-1"}})])
    body = _client(session).request(
        "POST", "/products/APP/ideas", json_body={"idea": {"name": "New"}}
    )
    assert body["idea"]["id"] == "IDEA-1"
    assert session.calls[0]["json"] == {"idea": {"name": "New"}}
    assert session.calls[0]["headers"]["Content-Type"] == "application/json"


def test_record_id_cannot_add_path_segments():
    session = _FakeSession([])
    with pytest.raises(AhaClientError, match="product_id"):
        _client(session).get_product("a/b")


def test_check_aha_skips_when_unconfigured():
    with patch("src.aha_client.CORTEX_AHA_API_KEY", None):
        assert aha_configured() is False
        assert check_aha_api() == (True, None)


def test_qa_marks_aha_from_the_report_blob():
    assert QARegistry._aha_source_status(None) == "unavailable"
    assert QARegistry._aha_source_status({"aha": {"configured": True}}) == "ok"
    assert QARegistry._aha_source_status({"aha": {"error": "down"}}) == "unavailable"
    registry = QARegistry()
    registry.begin("Acme")
    snap = registry.summary(report={"customer": "Acme"}, data_source_order=["Aha"])
    assert snap["data_sources"]["Aha"] == "unavailable"
