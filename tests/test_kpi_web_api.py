"""Tests for KPI web API authz, filters, and fail-loud value handling."""

from __future__ import annotations

from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.kpi_owners import reset_for_tests
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings

_OWNERS_YAML = """
catalog_admin: marc.schriftman@leandna.com
leads:
  - email: lead.eng@leandna.com
    display_name: Eng Lead
    packs: [engineering]
topic_packs:
  engineering:
    description: Eng pack
    required_any_tags: [engineering]
  support:
    description: Support pack
    required_any_tags: [support]
"""

_REGISTRY = """
metrics:
  "Alpha Eng":
    description: Eng KPI
    mgmt_guidance: Hit it.
    owner: marc.schriftman@leandna.com
    metric-id: 1
    metric-generator: null
    tags: [engineering]
    target: 10
    direction: higher
  "Beta Support":
    description: Support KPI
    mgmt_guidance: Keep queue healthy.
    owner: lead.eng@leandna.com
    metric-id: null
    metric-generator: null
    tags: [support]
"""


@pytest.fixture(autouse=True)
def _clear_owners_cache() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    owners = tmp_path / "kpi_owners.yaml"
    owners.write_text(_OWNERS_YAML.strip() + "\n", encoding="utf-8")
    registry = tmp_path / "my-metrics.yaml"
    registry.write_text(_REGISTRY.strip() + "\n", encoding="utf-8")
    return owners, registry


def _client(tmp_path: Path, *, actor: str = "marc.schriftman@leandna.com") -> TestClient:
    owners, registry = _write_fixture(tmp_path)
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": actor,
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
        "CORTEX_KPI_WEB_SKIP_S3": "true",
    }
    settings = load_kpi_web_settings(environ=env)
    app = create_app(settings=settings)
    return TestClient(app)


def _login(client: TestClient) -> None:
    res = client.get("/auth/dev-login", follow_redirects=False)
    assert res.status_code == 302
    assert "cortex_kpi_session" in res.cookies or "set-cookie" in {
        k.lower() for k in res.headers.keys()
    }


def test_unauthenticated_api_fails_loud(tmp_path: Path) -> None:
    client = _client(tmp_path)
    res = client.get("/api/kpis")
    assert res.status_code == 401
    body = res.json()
    assert body["ok"] is False
    assert "authentication required" in body["error"]


def test_dev_login_and_read_all_for_lead(tmp_path: Path) -> None:
    client = _client(tmp_path, actor="lead.eng@leandna.com")
    _login(client)

    me = client.get("/api/me")
    assert me.status_code == 200
    me_body = me.json()
    assert me_body["email"] == "lead.eng@leandna.com"
    assert me_body["is_catalog_admin"] is False
    assert me_body["permissions"]["read"] == "all"
    assert me_body["permissions"]["edit"] == "own"

    listed = client.get("/api/kpis")
    assert listed.status_code == 200
    body = listed.json()
    assert body["ok"] is True
    assert body["resolved"] is False
    names = {k["name"] for k in body["kpis"]}
    # Lead may read Marc's KPI as well as own (read-all).
    assert names == {"Alpha Eng", "Beta Support"}


def test_owner_and_tag_filters(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _login(client)

    by_owner = client.get("/api/kpis", params={"owner": "marc.schriftman@leandna.com"})
    assert by_owner.status_code == 200
    assert [k["name"] for k in by_owner.json()["kpis"]] == ["Alpha Eng"]

    by_me = client.get("/api/kpis", params={"owner": "me"})
    assert by_me.status_code == 200
    assert [k["name"] for k in by_me.json()["kpis"]] == ["Alpha Eng"]

    by_tag = client.get("/api/kpis", params={"tag": "support"})
    assert by_tag.status_code == 200
    assert [k["name"] for k in by_tag.json()["kpis"]] == ["Beta Support"]


def test_unknown_viewer_denied(tmp_path: Path) -> None:
    owners, registry = _write_fixture(tmp_path)
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "stranger@leandna.com",
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
    }
    app = create_app(settings=load_kpi_web_settings(environ=env))
    client = TestClient(app)
    res = client.get("/auth/dev-login", follow_redirects=False)
    assert res.status_code == 403
    assert "unauthorized KPI viewer" in res.json()["error"]


def test_wrong_domain_denied(tmp_path: Path) -> None:
    owners, registry = _write_fixture(tmp_path)
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "marc.schriftman@leandna.com",
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "other.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
    }
    app = create_app(settings=load_kpi_web_settings(environ=env))
    client = TestClient(app)
    res = client.get("/auth/dev-login", follow_redirects=False)
    assert res.status_code == 403
    assert "outside allowed domains" in res.json()["error"]


def test_detail_empty_value_fail_loud_no_fake(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _login(client)
    # No generator / no store row → empty, not a fabricated number.
    res = client.get("/api/kpis/Alpha%20Eng", params={"mode": "live"})
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    kpi = body["kpi"]
    assert kpi["observation"]["ok"] is False
    assert kpi["observation"]["value"] is None
    assert kpi["observation"]["error"] is None
    assert kpi["value_status"] == "empty"
    assert "no metric-generator" in " ".join(kpi["observation"]["warnings"])


def test_detail_not_found(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _login(client)
    res = client.get("/api/kpis/Does%20Not%20Exist")
    assert res.status_code == 404
    assert res.json()["ok"] is False


def test_meta_and_health(tmp_path: Path) -> None:
    client = _client(tmp_path)
    assert client.get("/api/health").json()["ok"] is True
    _login(client)
    meta = client.get("/api/meta")
    assert meta.status_code == 200
    body = meta.json()
    assert body["catalog_admin"] == "marc.schriftman@leandna.com"
    assert any(t["tag"] == "engineering" for t in body["tags"])
    counts = [t["count"] for t in body["tags"]]
    assert counts == sorted(counts, reverse=True)
    assert any(o["email"] == "marc.schriftman@leandna.com" for o in body["owners"])


def test_google_login_unconfigured_fails_loud(tmp_path: Path) -> None:
    client = _client(tmp_path)
    res = client.get("/auth/login")
    assert res.status_code == 500
    assert "Google OAuth is not configured" in res.json()["error"]


def test_index_serves_ui(tmp_path: Path) -> None:
    client = _client(tmp_path)
    res = client.get("/")
    assert res.status_code == 200
    html = res.text
    assert "Cortex KPIs" in html
    assert 'id="filter-tags"' in html
    assert "<select id=\"filter-tag\"" not in html
    assert 'id="confirm-delete-dialog"' in html
    assert 'id="btn-delete-confirm"' in html


def test_stored_values_fail_loud_when_store_missing(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _login(client)
    res = client.get("/api/kpis", params={"mode": "stored", "values": "1"})
    assert res.status_code == 500
    body = res.json()
    assert body["ok"] is False
    assert "KPI store" in body["error"] or "does not exist" in body["error"]
