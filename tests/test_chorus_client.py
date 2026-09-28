"""Tests for the Chorus REST client (no network — fake session)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.chorus_client import (
    ChorusClient,
    ChorusClientError,
    chorus_configured,
    chorus_route_exists,
    check_chorus_api,
)
from src.chorus_routes import CHORUS_ROUTES
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

    def request(self, method, url, headers=None, params=None, json=None, data=None, files=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
                "data": data,
                "files": files,
                "timeout": timeout,
            }
        )
        return self._responses.pop(0)


def _client(session, **kwargs) -> ChorusClient:
    return ChorusClient(
        api_key=kwargs.pop("api_key", "test-key"),
        base_url=kwargs.pop("base_url", "https://chorus.ai"),
        session=session,
        max_retries=kwargs.pop("max_retries", 0),
        sleeper=kwargs.pop("sleeper", lambda _seconds: None),
        **kwargs,
    )


def test_requires_api_key():
    with pytest.raises(ChorusClientError, match="CHORUS_API_KEY"):
        ChorusClient(api_key="", base_url="https://chorus.ai")


def test_base_url_must_be_https():
    with pytest.raises(ChorusClientError, match="https"):
        ChorusClient(api_key="test-key", base_url="http://chorus.ai")


def test_route_catalog_covers_published_reads():
    assert chorus_route_exists("GET", "/api/v1/users/me")
    assert chorus_route_exists("GET", "/v3/engagements")
    assert chorus_route_exists("GET", "/api/v1/conversations/ABC")
    assert chorus_route_exists("POST", "/api/v1/conversations:bulk")
    assert chorus_route_exists("POST", "/v3/upload")
    assert not chorus_route_exists("GET", "/not-a-route")
    assert not chorus_route_exists("GET", "/login")
    assert len(CHORUS_ROUTES["GET"]) == 22
    assert sum(len(paths) for paths in CHORUS_ROUTES.values()) == 61


def test_get_me_sends_raw_token():
    session = _FakeSession([_FakeResponse(200, {"data": {"attributes": {"domain": "leandna.com"}}})])
    body = _client(session, api_key="Bearer test-key").me()
    assert body["data"]["attributes"]["domain"] == "leandna.com"
    call = session.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "https://chorus.ai/api/v1/users/me"
    assert call["headers"]["Authorization"] == "test-key"
    assert call["headers"]["Accept"] == "application/vnd.api+json"
    assert "x-al-version" not in {key.lower() for key in call["headers"]}
    assert "Content-Type" not in call["headers"]


def test_v3_engagements_use_json_accept():
    session = _FakeSession([_FakeResponse(200, {"engagements": []})])
    assert _client(session).list_engagements(min_date=1) == []
    call = session.calls[0]
    assert call["url"] == "https://chorus.ai/v3/engagements"
    assert call["headers"]["Accept"] == "application/json"
    assert call["params"]["min_date"] == 1
    assert "x-al-version" not in {key.lower() for key in call["headers"]}


def test_unknown_route_is_rejected_before_the_network():
    session = _FakeSession([])
    with pytest.raises(ChorusClientError, match="no GET /not-a-route"):
        _client(session).get("/not-a-route")
    assert session.calls == []


def test_engagements_follow_continuation_key_and_stop_when_empty():
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                {"continuation_key": "page-2", "engagements": [{"engagement_id": "a"}]},
            ),
            _FakeResponse(200, {"continuation_key": "page-3", "engagements": []}),
        ]
    )
    rows = _client(session).list_engagements()
    assert [row["engagement_id"] for row in rows] == ["a"]
    assert session.calls[1]["params"]["continuation_key"] == "page-2"
    assert len(session.calls) == 2


def test_emails_follow_cursor():
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                {"data": [{"id": "e1"}], "meta": {"page": {"cursor": "cur"}}},
            ),
            _FakeResponse(200, {"data": [{"id": "e2"}], "meta": {"page": {}}}),
        ]
    )
    rows = _client(session).list_emails(**{"page[size]": 1})
    assert [row["id"] for row in rows] == ["e1", "e2"]
    assert session.calls[0]["params"]["page[size]"] == 1
    assert session.calls[1]["params"]["page[after]"] == "cur"


def test_scorecards_follow_page_number_until_a_short_page():
    session = _FakeSession(
        [
            _FakeResponse(200, {"data": [{"id": "s1"}, {"id": "s2"}]}),
            _FakeResponse(200, {"data": [{"id": "s3"}]}),
        ]
    )
    rows = _client(session).list_all("/api/v1/scorecards", params={"page[size]": 2})
    assert [row["id"] for row in rows] == ["s1", "s2", "s3"]
    assert session.calls[1]["params"]["page[number]"] == 2
    assert len(session.calls) == 2


def test_repeated_page_stops():
    session = _FakeSession(
        [
            _FakeResponse(200, {"data": [{"id": "t1"}]}),
            _FakeResponse(200, {"data": [{"id": "t1"}]}),
        ]
    )
    rows = _client(session).list_all("/api/v1/teams", params={"page[size]": 1})
    assert [row["id"] for row in rows] == ["t1"]
    assert len(session.calls) == 2


def test_users_array_is_a_single_page():
    session = _FakeSession([_FakeResponse(200, [{"id": 1}, {"id": 2}])])
    assert [row["id"] for row in _client(session).list_users()] == [1, 2]
    assert len(session.calls) == 1


def test_list_stops_loud_when_max_pages_is_hit():
    session = _FakeSession(
        [
            _FakeResponse(200, {"data": [{"id": "s1"}]}),
            _FakeResponse(200, {"data": [{"id": "s2"}]}),
        ]
    )
    with pytest.raises(ChorusClientError, match="max_pages"):
        _client(session).list_all("/api/v1/scorecards", params={"page[size]": 1}, max_pages=1)


def test_moments_require_the_shared_on_filter():
    session = _FakeSession([])
    with pytest.raises(ChorusClientError, match="filter\\[shared_on\\]"):
        _client(session).list_moments()
    assert session.calls == []


def test_forbidden_includes_the_api_detail():
    session = _FakeSession(
        [_FakeResponse(403, {"errors": [{"detail": "API may not be called using this token."}]})]
    )
    with pytest.raises(ChorusClientError, match="may not be called"):
        _client(session).get_conversation("ABC")


def test_auth_failure_names_the_env_var():
    session = _FakeSession([_FakeResponse(401, None, text="")])
    with pytest.raises(ChorusClientError, match="CHORUS_API_KEY"):
        _client(session).me()


def test_rate_limit_retries():
    sleeps: list[float] = []
    session = _FakeSession(
        [
            _FakeResponse(429, None, text="slow"),
            _FakeResponse(200, {"data": {"id": "u1"}}),
        ]
    )
    with patch("src.chorus_client.random.uniform", return_value=0):
        body = _client(session, max_retries=1, sleeper=sleeps.append).me()
    assert body["data"]["id"] == "u1"
    assert sleeps == [2.0]


def test_upload_sends_multipart_and_keeps_the_colon_path():
    session = _FakeSession([_FakeResponse(200, {"id": "up1"})])
    body = _client(session).upload_conversation(
        b"audio",
        name="QBR",
        owner_email="rep@leandna.com",
        filename="call.mp3",
        content_type="audio/mpeg",
    )
    assert body["id"] == "up1"
    call = session.calls[0]
    assert call["url"] == "https://chorus.ai/v3/upload"
    assert call["json"] is None
    assert call["data"]["name"] == "QBR"
    assert call["data"]["user"] == "rep@leandna.com"
    assert call["files"]["data"][0] == "call.mp3"
    assert "Content-Type" not in call["headers"]


def test_action_path_keeps_the_colon():
    session = _FakeSession([_FakeResponse(200, {})])
    _client(session).request("POST", "/api/v1/conversations:bulk", json_body={"ids": ["a"]})
    assert session.calls[0]["url"] == "https://chorus.ai/api/v1/conversations:bulk"


def test_record_id_cannot_add_path_segments():
    session = _FakeSession([])
    with pytest.raises(ChorusClientError, match="conversation_id"):
        _client(session).get_conversation("a/b")


def test_check_chorus_skips_when_unconfigured():
    with patch("src.chorus_client.CHORUS_API_KEY", None):
        assert chorus_configured() is False
        assert check_chorus_api() == (True, None)


def test_qa_marks_chorus_from_the_report_blob():
    assert QARegistry._chorus_source_status(None) == "unavailable"
    assert QARegistry._chorus_source_status({"chorus": {"configured": True}}) == "ok"
    assert QARegistry._chorus_source_status({"chorus": {"error": "down"}}) == "unavailable"
    registry = QARegistry()
    registry.begin("Acme")
    snap = registry.summary(report={"customer": "Acme"}, data_source_order=["Chorus"])
    assert snap["data_sources"]["Chorus"] == "unavailable"
