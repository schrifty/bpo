"""Tests for the GitHub REST API client (no network — fake session)."""

from __future__ import annotations

from datetime import datetime, timezone

from unittest.mock import patch

import pytest

from src.github_client import (
    GitHubClient,
    GitHubError,
    _parse_link_next,
    _resolve_repo_specs,
    build_github_activity_report,
    check_github_api,
    github_configured,
)


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        body,
        *,
        text: str = "",
        headers: dict | None = None,
        ok: bool | None = None,
    ):
        self.status_code = status_code
        self.ok = ok if ok is not None else 200 <= status_code < 300
        self._body = body
        self.text = text or (str(body) if body is not None else "")
        self.headers = headers or {}

    def json(self):
        return self._body


class _FakeSession:
    def __init__(self, router=None, responses=None):
        self._router = router
        self._responses = list(responses or [])
        self.calls: list[dict] = []

    def request(self, method, url, headers=None, params=None, timeout=None):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "params": params, "timeout": timeout}
        )
        if self._router is not None:
            return self._router(method, url, params)
        return self._responses.pop(0)


def _client(session, **kw) -> GitHubClient:
    return GitHubClient(token="ghp_test", base_url="https://api.github.test", session=session, **kw)


def test_requires_token():
    with pytest.raises(GitHubError, match="GITHUB_TOKEN"):
        GitHubClient(token="")


def test_headers_include_bearer_and_api_version():
    sess = _FakeSession(responses=[_FakeResponse(200, {"login": "bot"})])
    c = _client(sess)
    c.get_authenticated_user()
    hdrs = sess.calls[0]["headers"]
    assert hdrs["Authorization"] == "Bearer ghp_test"
    assert hdrs["Accept"] == "application/vnd.github+json"
    assert hdrs["X-GitHub-Api-Version"] == "2022-11-28"


def test_401_raises_with_hint():
    sess = _FakeSession(responses=[_FakeResponse(401, None, text="bad")])
    with pytest.raises(GitHubError, match="401"):
        _client(sess).get_authenticated_user()


def test_parse_link_next():
    link = '<https://api.github.com/repos?page=2>; rel="next", <https://api.github.com/repos?page=5>; rel="last"'
    assert _parse_link_next(link) == "https://api.github.com/repos?page=2"
    assert _parse_link_next(None) is None


def test_paginate_follows_link_header():
    page1 = [{"id": 1}, {"id": 2}]
    page2 = [{"id": 3}]
    sess = _FakeSession(
        responses=[
            _FakeResponse(
                200,
                page1,
                headers={"Link": '<https://api.github.test/orgs/acme/repos?page=2>; rel="next"'},
            ),
            _FakeResponse(200, page2),
        ]
    )
    rows = _client(sess).list_org_repos("acme")
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sess.calls[1]["url"].endswith("page=2")


def test_resolve_repo_specs_from_env():
    specs = _resolve_repo_specs(org="acme", repos_env="acme/web,other", client=None)
    assert specs == [("acme", "web"), ("acme", "other")]


def test_resolve_repo_specs_requires_owner_when_no_org():
    with pytest.raises(GitHubError, match="owner/repo"):
        _resolve_repo_specs(org=None, repos_env="solo-repo", client=None)


def test_list_commits_passes_since_param():
    sess = _FakeSession(responses=[_FakeResponse(200, [])])
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _client(sess).list_commits("acme", "web", since=since, max_commits=10)
    assert sess.calls[0]["params"]["since"] == "2024-01-01T00:00:00Z"


def test_build_activity_report_returns_none_when_unconfigured(monkeypatch):
    monkeypatch.setattr("src.github_client.GITHUB_TOKEN", None)
    assert build_github_activity_report() is None


def test_build_activity_report_aggregates(monkeypatch):
    monkeypatch.setattr("src.github_client.GITHUB_TOKEN", "ghp_test")
    recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT12:00:00Z")

    def router(method, url, params):
        if url.endswith("/user"):
            return _FakeResponse(200, {"login": "svc-bot"})
        if "/repos/acme/web" in url and url.endswith("/repos/acme/web"):
            return _FakeResponse(
                200,
                {
                    "full_name": "acme/web",
                    "default_branch": "main",
                    "open_issues_count": 2,
                    "pushed_at": recent,
                },
            )
        if url.endswith("/repos/acme/web/commits"):
            return _FakeResponse(
                200,
                [
                    {
                        "commit": {
                            "author": {
                                "email": "dev@example.com",
                                "date": recent,
                            }
                        }
                    }
                ],
            )
        if url.endswith("/repos/acme/web/pulls"):
            return _FakeResponse(
                200,
                [
                    {
                        "state": "closed",
                        "merged_at": recent,
                        "updated_at": recent,
                        "user": {"login": "dev"},
                    },
                    {"state": "open", "merged_at": None, "updated_at": recent, "user": {"login": "dev"}},
                ],
            )
        if url.endswith("/repos/acme/web/releases"):
            return _FakeResponse(
                200,
                [{"published_at": recent, "tag_name": "v1.0.0"}],
            )
        raise AssertionError(f"unexpected url: {url}")

    report = build_github_activity_report(
        org="acme",
        repos_env="acme/web",
        window_days=30,
        client=_client(_FakeSession(router=router)),
    )
    assert report is not None
    assert report["user_login"] == "svc-bot"
    assert report["totals"]["commits"] == 1
    assert report["totals"]["prs_merged"] == 1
    assert report["totals"]["prs_open"] == 1
    assert report["totals"]["releases"] == 1
    assert report["by_email"]["dev@example.com"]["commits"] == 1


def test_check_github_api_delegates_to_client(monkeypatch):
    monkeypatch.setattr("src.github_client.GITHUB_TOKEN", "ghp_test")
    with patch.object(GitHubClient, "get_authenticated_user", return_value={"login": "bot"}):
        ok, msg = check_github_api()
    assert ok is True
    assert msg is None


def test_github_configured():
    assert github_configured() in (True, False)
