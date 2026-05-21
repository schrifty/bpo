"""Jira Atlassian API gateway connection (no direct site REST)."""

from __future__ import annotations

import pytest


def _gateway_env(monkeypatch, *, cloud_id: str = "cloud-uuid-123") -> None:
    monkeypatch.delenv("JIRA_AUTH_MODE", raising=False)
    monkeypatch.setenv("JIRA_CLOUD_ID", cloud_id)
    monkeypatch.setenv("JIRA_URL", "https://acme.atlassian.net")
    monkeypatch.setenv("JIRA_API_TOKEN", "scoped-token")
    monkeypatch.setenv("JIRA_SERVICE_ACCOUNT_AUTH", "bearer")
    monkeypatch.delenv("JIRA_EMAIL", raising=False)


def test_gateway_bearer_auth(monkeypatch):
    _gateway_env(monkeypatch)

    from src.jira_connection import build_jira_connection_settings

    s = build_jira_connection_settings()
    assert s.api_base_url == "https://api.atlassian.com/ex/jira/cloud-uuid-123"
    assert s.browse_base_url == "https://acme.atlassian.net"
    assert s.headers["Authorization"] == "Bearer scoped-token"
    assert s.cloud_id == "cloud-uuid-123"


def test_site_mode_rejected(monkeypatch):
    _gateway_env(monkeypatch)
    monkeypatch.setenv("JIRA_AUTH_MODE", "site")

    from src.jira_connection import build_jira_connection_settings

    with pytest.raises(ValueError, match="no longer supported"):
        build_jira_connection_settings()


def test_requires_cloud_id_without_auto(monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://acme.atlassian.net")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.delenv("JIRA_CLOUD_ID", raising=False)
    monkeypatch.delenv("JIRA_CLOUD_ID_AUTO", raising=False)

    from src.jira_connection import build_jira_connection_settings

    with pytest.raises(ValueError, match="JIRA_CLOUD_ID is required"):
        build_jira_connection_settings()


def test_fetch_cloud_id_for_site(monkeypatch):
    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {"id": "id-1", "url": "https://other.atlassian.net"},
                {"id": "id-2", "url": "https://acme.atlassian.net"},
            ]

    monkeypatch.setattr("src.jira_connection.requests.get", lambda *a, **k: _Resp())

    from src.jira_connection import fetch_cloud_id_for_site

    assert (
        fetch_cloud_id_for_site(token="tok", site_url="https://acme.atlassian.net", email=None)
        == "id-2"
    )


def test_cloud_id_auto_falls_back_to_tenant_info_on_401(monkeypatch):
    _gateway_env(monkeypatch)
    monkeypatch.delenv("JIRA_CLOUD_ID", raising=False)
    monkeypatch.setenv("JIRA_CLOUD_ID_AUTO", "true")

    class _Unauthorized:
        response = type("R", (), {"status_code": 401})()

        def raise_for_status(self):
            import requests

            raise requests.HTTPError(response=self.response)

    def _accessible(*a, **k):
        return _Unauthorized()

    def _tenant_info(*a, **k):
        class _Ok:
            def raise_for_status(self):
                return None

            def json(self):
                return {"cloudId": "from-tenant-info"}

        return _Ok()

    monkeypatch.setattr("src.jira_connection.requests.get", lambda url, **kw: (
        _tenant_info() if "tenant_info" in url else _accessible()
    ))

    from src.jira_connection import build_jira_connection_settings

    s = build_jira_connection_settings()
    assert s.cloud_id == "from-tenant-info"


def test_jira_client_uses_api_base_for_search(monkeypatch):
    _gateway_env(monkeypatch, cloud_id="cid")

    import importlib

    import src.jira_client as jc_mod

    importlib.reload(jc_mod)
    jc_mod.reset_shared_jira_client()
    jc = jc_mod.JiraClient()
    assert jc.api_base_url == "https://api.atlassian.com/ex/jira/cid"
    assert jc.base_url == "https://acme.atlassian.net"
