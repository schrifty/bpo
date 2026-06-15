"""Tests for the Rippling Platform API client (no network — fake session)."""

from __future__ import annotations

import pytest

from src.rippling_client import RipplingClient, RipplingError


class _FakeResponse:
    def __init__(self, status_code: int, body, *, text: str = ""):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._body = body
        self.text = text or (str(body) if body is not None else "")

    def json(self):
        if self._body is _BAD_JSON:
            raise ValueError("no json")
        return self._body


_BAD_JSON = object()


class _FakeSession:
    """Records requests and returns queued responses (by call order) or a router fn."""

    def __init__(self, responses=None, router=None):
        self._responses = list(responses or [])
        self._router = router
        self.calls: list[dict] = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if self._router is not None:
            return self._router(url, params)
        return self._responses.pop(0)


def _client(session, **kw) -> RipplingClient:
    return RipplingClient(api_key="tok", session=session, **kw)


def test_requires_api_key():
    with pytest.raises(RipplingError):
        RipplingClient(api_key="", session=_FakeSession())


def test_headers_include_bearer_and_optional_version():
    sess = _FakeSession([_FakeResponse(200, [])])
    c = _client(sess, api_version="2025-12-01")
    c.list_teams()
    hdrs = sess.calls[0]["headers"]
    assert hdrs["Authorization"] == "Bearer tok"
    assert hdrs["Accept"] == "application/json"
    assert hdrs["Rippling-Api-Version"] == "2025-12-01"


def test_no_version_header_when_unset():
    sess = _FakeSession([_FakeResponse(200, [])])
    _client(sess).list_teams()
    assert "Rippling-Api-Version" not in sess.calls[0]["headers"]


def test_401_raises_with_key_hint():
    sess = _FakeSession([_FakeResponse(401, None, text="unauthorized")])
    with pytest.raises(RipplingError, match="expired"):
        _client(sess).list_teams()


def test_403_raises_with_scope_hint():
    sess = _FakeSession([_FakeResponse(403, None, text="forbidden")])
    with pytest.raises(RipplingError, match="scopes"):
        _client(sess).list_teams()


def test_non_json_raises():
    sess = _FakeSession([_FakeResponse(200, _BAD_JSON)])
    with pytest.raises(RipplingError, match="non-JSON"):
        _client(sess).list_teams()


def test_limit_offset_pagination_lists():
    # Two full pages of 100, then a short page -> stop.
    page1 = [{"id": i} for i in range(100)]
    page2 = [{"id": 100 + i} for i in range(100)]
    page3 = [{"id": 200}]
    sess = _FakeSession([_FakeResponse(200, page1), _FakeResponse(200, page2), _FakeResponse(200, page3)])
    rows = _client(sess).list_employees()
    assert len(rows) == 201
    # offsets advanced 0, 100, 200
    assert [c["params"]["offset"] for c in sess.calls] == [0, 100, 200]
    assert all(c["params"]["limit"] == 100 for c in sess.calls)


def test_cursor_pagination_dict_bodies():
    def router(url, params):
        if "next" not in url:
            return _FakeResponse(200, {"results": [{"id": 1}], "next_link": "https://api.rippling.com/x?next=1"})
        return _FakeResponse(200, {"results": [{"id": 2}], "next_link": None})

    sess = _FakeSession(router=router)
    rows = _client(sess).list_departments()
    assert [r["id"] for r in rows] == [1, 2]
    assert sess.calls[1]["url"].endswith("next=1")


def test_normalize_employee_tolerates_shapes():
    raw = {
        "id": "E1",
        "firstName": "Ada",
        "lastName": "Lovelace",
        "workEmail": "ada@x.com",
        "title": {"name": "Engineering Manager"},
        "department": {"name": "Engineering"},
        "manager": {"id": "M9"},
        "teams": [{"id": "T1"}, "T2"],
        "roleState": "ACTIVE",
    }
    n = RipplingClient.normalize_employee(raw)
    assert n["name"] == "Ada Lovelace"
    assert n["email"] == "ada@x.com"
    assert n["title"] == "Engineering Manager"
    assert n["department"] == "Engineering"
    assert n["manager_id"] == "M9"
    assert n["team_ids"] == ["T1", "T2"]
    assert n["active"] is True


def test_get_org_roster_groups_by_department_and_picks_lead():
    employees = [
        {"id": "1", "firstName": "Ada", "lastName": "L", "title": "Engineering Manager",
         "department": "Engineering", "roleState": "ACTIVE"},
        {"id": "2", "firstName": "Bob", "lastName": "B", "title": "Software Engineer",
         "department": "Engineering", "roleState": "ACTIVE"},
        {"id": "3", "firstName": "Cy", "lastName": "C", "title": "Sales Rep",
         "department": "Sales", "roleState": "ACTIVE"},
        {"id": "4", "firstName": "Dee", "lastName": "D", "title": "Engineer",
         "department": "Engineering", "roleState": "TERMINATED"},
    ]
    sess = _FakeSession([_FakeResponse(200, employees)])
    roster = _client(sess).get_org_roster(group_by="department")
    assert roster["group_by"] == "department"
    # Terminated Dee excluded by active_only default.
    assert roster["total_employees"] == 3
    by_name = {g["name"]: g for g in roster["groups"]}
    eng = by_name["Engineering"]
    assert eng["headcount"] == 2
    assert set(eng["members"]) == {"Ada L", "Bob B"}
    assert eng["lead"] == "Ada L"  # "Manager" beats "Engineer"
    assert by_name["Sales"]["lead"] == ""  # no lead-ish title


def test_get_org_roster_by_team_resolves_team_names():
    def router(url, params):
        if url.endswith("/employees"):
            return _FakeResponse(200, [
                {"id": "1", "firstName": "Ada", "lastName": "L", "title": "Lead",
                 "teams": ["T1"], "roleState": "ACTIVE"},
                {"id": "2", "firstName": "Bob", "lastName": "B", "title": "Engineer",
                 "teams": ["T1", "T2"], "roleState": "ACTIVE"},
            ])
        if url.endswith("/teams"):
            return _FakeResponse(200, [{"id": "T1", "name": "Platform"}, {"id": "T2", "name": "Data"}])
        return _FakeResponse(200, [])

    sess = _FakeSession(router=router)
    roster = _client(sess).get_org_roster(group_by="team")
    by_name = {g["name"]: g for g in roster["groups"]}
    assert by_name["Platform"]["headcount"] == 2
    assert by_name["Data"]["headcount"] == 1
    assert by_name["Platform"]["lead"] == "Ada L"


def test_invalid_group_by_raises():
    sess = _FakeSession([])
    with pytest.raises(RipplingError, match="group_by"):
        _client(sess).get_org_roster(group_by="zones")


def test_check_returns_ok_on_success():
    sess = _FakeSession([_FakeResponse(200, [{"id": "1"}])])
    ok, err = _client(sess).check()
    assert ok is True and err is None


def test_check_returns_error_on_failure():
    sess = _FakeSession([_FakeResponse(500, None, text="boom")])
    ok, err = _client(sess).check()
    assert ok is False and "500" in err
