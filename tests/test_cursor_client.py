"""Tests for the Cursor Team Admin API client, fully mocked (no network)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.cursor_client import (
    CursorClient,
    CursorClientError,
    _chunk_ranges,
    _to_epoch_ms,
    clear_cursor_cache,
)


class _Resp:
    def __init__(self, status: int, payload, *, text: str | None = None, headers=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else str(payload)
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def _client(**kw) -> CursorClient:
    # Disable client-side pacing so tests don't sleep between paginated calls, and
    # disable the on-disk cache so tests stay hermetic (no disk I/O / cross-test reuse).
    kw.setdefault("min_request_interval", 0)
    kw.setdefault("cache_ttl_seconds", 0)
    return CursorClient(api_key="test-key", base_url="https://api.cursor.test", **kw)


def test_init_requires_key(monkeypatch) -> None:
    # Ensure no ambient key leaks in from .env / environment.
    monkeypatch.setattr("src.cursor_client.CURSOR_ADMIN_API_KEY", None)
    with pytest.raises(CursorClientError):
        CursorClient(api_key="")


def test_to_epoch_ms_handles_date_datetime_and_epochs() -> None:
    assert _to_epoch_ms(date(2024, 3, 18)) == 1710720000000
    aware = datetime(2024, 3, 18, tzinfo=timezone.utc)
    assert _to_epoch_ms(aware) == 1710720000000
    assert _to_epoch_ms(1710720000) == 1710720000000  # seconds → ms
    assert _to_epoch_ms(1710720000000) == 1710720000000  # already ms
    with pytest.raises(CursorClientError):
        _to_epoch_ms(True)


def test_chunk_ranges_splits_over_30_days() -> None:
    day = 24 * 60 * 60 * 1000
    ranges = _chunk_ranges(0, 45 * day, max_days=30)
    assert len(ranges) == 2
    assert ranges[0][0] == 0
    assert ranges[1][1] == 45 * day


def test_get_team_members_filters_removed(monkeypatch) -> None:
    c = _client()
    payload = {"teamMembers": [
        {"id": 1, "email": "a@x.com", "role": "member", "isRemoved": False},
        {"id": 2, "email": "b@x.com", "role": "owner", "isRemoved": True},
    ]}
    monkeypatch.setattr(c._session, "request", lambda *a, **k: _Resp(200, payload))
    active = c.get_team_members()
    assert [m["email"] for m in active] == ["a@x.com"]
    assert len(c.get_team_members(include_removed=True)) == 2


def test_get_spend_paginates(monkeypatch) -> None:
    c = _client()
    pages = {
        1: {"teamMemberSpend": [{"email": "a@x.com", "overallSpendCents": 100}], "totalPages": 2},
        2: {"teamMemberSpend": [{"email": "b@x.com", "overallSpendCents": 200}], "totalPages": 2},
    }

    def fake_request(method, url, *, json=None, params=None, timeout=None):
        return _Resp(200, pages[json["page"]])

    monkeypatch.setattr(c._session, "request", fake_request)
    rows = c.get_spend()
    assert [r["email"] for r in rows] == ["a@x.com", "b@x.com"]


def test_get_usage_events_paginates_and_caps(monkeypatch) -> None:
    c = _client()

    def fake_request(method, url, *, json=None, params=None, timeout=None):
        page = json["page"]
        has_next = page < 3
        return _Resp(200, {
            "usageEvents": [{"timestamp": str(page), "userEmail": "a@x.com"}],
            "pagination": {"hasNextPage": has_next},
        })

    monkeypatch.setattr(c._session, "request", fake_request)
    events = c.get_usage_events(date(2024, 1, 1), date(2024, 1, 2))
    assert len(events) == 3
    capped = c.get_usage_events(date(2024, 1, 1), date(2024, 1, 2), max_events=2)
    assert len(capped) == 2


def test_get_usage_summary_aggregates(monkeypatch) -> None:
    c = _client()
    data = {"data": [
        {"userId": 1, "isActive": True, "totalLinesAdded": 100, "acceptedLinesAdded": 60,
         "totalAccepts": 8, "totalRejects": 2, "agentRequests": 5},
        {"userId": 2, "isActive": True, "totalLinesAdded": 50, "acceptedLinesAdded": 40,
         "totalAccepts": 2, "totalRejects": 0, "agentRequests": 3},
    ]}
    monkeypatch.setattr(c._session, "request", lambda *a, **k: _Resp(200, data))
    s = c.get_usage_summary(days=7)
    assert s.active_users == 2
    assert s.total_lines_added == 150
    assert s.total_accepts == 10
    assert s.agent_requests == 8
    assert s.acceptance_rate == round(10 / 12, 4)


def test_http_error_fails_loud(monkeypatch) -> None:
    c = _client()
    monkeypatch.setattr(c._session, "request", lambda *a, **k: _Resp(401, None, text="nope"))
    with pytest.raises(CursorClientError) as ei:
        c.get_team_members()
    assert "401" in str(ei.value)


def test_429_retries_then_succeeds(monkeypatch) -> None:
    c = _client(max_retries=3)
    monkeypatch.setattr("src.cursor_client.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] < 3:
            return _Resp(429, {}, text="slow down", headers={"Retry-After": "1"})
        return _Resp(200, {"teamMembers": [{"email": "a@x.com", "isRemoved": False}]})

    monkeypatch.setattr(c._session, "request", flaky)
    members = c.get_team_members()
    assert calls["n"] == 3
    assert [m["email"] for m in members] == ["a@x.com"]


def test_429_exhausts_retries_fails_loud(monkeypatch) -> None:
    c = _client(max_retries=2)
    monkeypatch.setattr("src.cursor_client.time.sleep", lambda *_: None)
    monkeypatch.setattr(c._session, "request", lambda *a, **k: _Resp(429, {}, text="nope"))
    with pytest.raises(CursorClientError) as ei:
        c.get_team_members()
    assert "rate limited" in str(ei.value)


def test_5xx_retries(monkeypatch) -> None:
    c = _client(max_retries=3)
    monkeypatch.setattr("src.cursor_client.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] < 2:
            return _Resp(500, {}, text="boom")
        return _Resp(200, {"teamMembers": []})

    monkeypatch.setattr(c._session, "request", flaky)
    assert c.get_team_members() == []
    assert calls["n"] == 2


def test_cache_short_circuits_second_call(monkeypatch, tmp_path) -> None:
    # With caching on, an identical daily-usage call within the TTL serves from disk
    # and does not hit the network a second time.
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_cursor_cache()
    c = _client(cache_ttl_seconds=3600)
    calls = {"n": 0}

    def fake(method, url, *, json=None, params=None, timeout=None):
        calls["n"] += 1
        return _Resp(200, {"data": [{"userId": 1, "isActive": True}],
                           "pagination": {"hasNextPage": False}})

    monkeypatch.setattr(c._session, "request", fake)
    first = c.get_daily_usage(date(2024, 1, 1), date(2024, 1, 2))
    second = c.get_daily_usage(date(2024, 1, 1), date(2024, 1, 2))
    assert first == second
    assert calls["n"] == 1  # second call short-circuited by cache


def test_cache_disabled_always_requests(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    clear_cursor_cache()
    c = _client(cache_ttl_seconds=0)  # caching off
    calls = {"n": 0}

    def fake(method, url, *, json=None, params=None, timeout=None):
        calls["n"] += 1
        return _Resp(200, {"data": [], "pagination": {"hasNextPage": False}})

    monkeypatch.setattr(c._session, "request", fake)
    c.get_daily_usage(date(2024, 1, 1), date(2024, 1, 2))
    c.get_daily_usage(date(2024, 1, 1), date(2024, 1, 2))
    assert calls["n"] == 2  # no caching → both calls hit the network


def test_throttle_paces_requests(monkeypatch) -> None:
    c = _client(min_request_interval=2.0)
    sleeps: list[float] = []
    monkeypatch.setattr("src.cursor_client.time.sleep", lambda s: sleeps.append(s))
    # Simulate the monotonic clock advancing only via our control: first call sets
    # the timestamp (no sleep), second call within the interval must sleep.
    # First call reads monotonic once (set ts); second call reads twice (wait calc + set ts).
    ticks = iter([100.0, 100.5, 100.5])
    monkeypatch.setattr("src.cursor_client.time.monotonic", lambda: next(ticks))
    monkeypatch.setattr(c._session, "request", lambda *a, **k: _Resp(200, {"teamMembers": []}))
    c.get_team_members()  # first request: no pacing sleep
    c.get_team_members()  # 0.5s later: should sleep ~1.5s
    assert sleeps and abs(sleeps[0] - 1.5) < 1e-6
