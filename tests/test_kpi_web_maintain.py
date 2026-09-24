"""Tests for KPI web maintain (write) API — authz, dry-run, happy path."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from starlette.testclient import TestClient

from src.kpi_observation import KPIObservation
from src.kpi_owners import reset_for_tests
from src.kpi_store import connect, list_kpis, stored_kpi_from_observation, upsert_kpi
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings
from src.metrics_registry_write import public_metric_entry
from tests.test_kpi_web_api import _client, _login, _write_fixture

_OWNERS_YAML = """
catalog_admin: marc.schriftman@leandna.com
leads:
  - email: lead.eng@leandna.com
    display_name: Eng Lead
    packs: [engineering]
  - email: lead.support@leandna.com
    display_name: Support Lead
    packs: [support]
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
    owner: lead.support@leandna.com
    metric-id: null
    metric-generator: null
    tags: [support]
  "Gamma Eng Lead":
    description: Lead-owned eng KPI
    mgmt_guidance: Own it.
    owner: lead.eng@leandna.com
    metric-id: null
    metric-generator: null
    tags: [engineering]
"""


@pytest.fixture(autouse=True)
def _clear_owners_cache() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


def _write_maintain_fixture(tmp_path: Path) -> tuple[Path, Path]:
    owners = tmp_path / "kpi_owners.yaml"
    owners.write_text(_OWNERS_YAML.strip() + "\n", encoding="utf-8")
    registry = tmp_path / "my-metrics.yaml"
    registry.write_text(_REGISTRY.strip() + "\n", encoding="utf-8")
    return owners, registry


def _maintain_client(tmp_path: Path, *, actor: str) -> tuple[TestClient, Path]:
    owners, registry = _write_maintain_fixture(tmp_path)
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
    app = create_app(settings=load_kpi_web_settings(environ=env))
    return TestClient(app), registry


def test_add_edit_delete_happy_path_yaml_shape(tmp_path: Path) -> None:
    client, registry = _maintain_client(tmp_path, actor="marc.schriftman@leandna.com")
    _login(client)

    added = client.post(
        "/api/kpis",
        json={
            "name": "Web Added KPI",
            "description": "From web",
            "mgmt_guidance": "Guide",
            "owner": "marc.schriftman@leandna.com",
            "tags": ["engineering", "ai"],
            "target": 42,
            "direction": "higher",
            "unit": "percent",
            "metric_id": 777,
            "metric_generator": "get_open_help",
        },
    )
    assert added.status_code == 201, added.text
    change = added.json()["change"]
    assert change["action"] == "add"
    assert change["dry_run"] is False
    assert change["actor"] == "marc.schriftman@leandna.com"
    assert change["entry"] == public_metric_entry(
        {
            "description": "From web",
            "mgmt_guidance": "Guide",
            "owner": "marc.schriftman@leandna.com",
            "metric-id": 777,
            "metric-generator": "get_open_help",
            "tags": ["engineering", "ai"],
            "unit": "percent",
            "target": 42.0,
            "direction": "higher",
        }
    )

    doc = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert "Web Added KPI" in doc["metrics"]
    assert doc["metrics"]["Web Added KPI"]["owner"] == "marc.schriftman@leandna.com"
    assert doc["metrics"]["Web Added KPI"]["metric-generator"] == "get_open_help"
    assert "owner: marc.schriftman@leandna.com" in registry.read_text(encoding="utf-8")

    edited = client.patch(
        "/api/kpis/Web%20Added%20KPI",
        json={"target": 43, "new_name": "Web Updated KPI"},
    )
    assert edited.status_code == 200, edited.text
    assert edited.json()["change"]["name"] == "Web Updated KPI"
    assert edited.json()["change"]["previous_name"] == "Web Added KPI"
    doc = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert "Web Updated KPI" in doc["metrics"]
    assert "Web Added KPI" not in doc["metrics"]
    assert doc["metrics"]["Web Updated KPI"]["target"] == 43
    # Immutable fields survive a rename untouched.
    assert doc["metrics"]["Web Updated KPI"]["description"] == "From web"

    deleted = client.delete("/api/kpis/Web%20Updated%20KPI")
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["change"]["action"] == "delete"
    doc = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert "Web Updated KPI" not in doc["metrics"]


def test_dry_run_does_not_write(tmp_path: Path) -> None:
    client, registry = _maintain_client(tmp_path, actor="marc.schriftman@leandna.com")
    _login(client)
    before = registry.read_text(encoding="utf-8")

    res = client.post(
        "/api/kpis?dry_run=1",
        json={
            "name": "Dry Run KPI",
            "tags": ["engineering"],
            "owner": "marc.schriftman@leandna.com",
        },
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["change"]["dry_run"] is True
    assert body["change"]["entry"]["owner"] == "marc.schriftman@leandna.com"
    assert registry.read_text(encoding="utf-8") == before

    patch = client.patch(
        "/api/kpis/Alpha%20Eng",
        json={"target": 99, "dry_run": True},
    )
    assert patch.status_code == 200
    assert patch.json()["change"]["dry_run"] is True
    assert registry.read_text(encoding="utf-8") == before

    delete = client.delete("/api/kpis/Alpha%20Eng?dry_run=true")
    assert delete.status_code == 200
    assert delete.json()["change"]["dry_run"] is True
    assert registry.read_text(encoding="utf-8") == before


def test_unauthorized_mutate_fails_loud(tmp_path: Path) -> None:
    client, registry = _maintain_client(tmp_path, actor="lead.eng@leandna.com")
    _login(client)
    before = registry.read_text(encoding="utf-8")

    # Lead may not edit Marc's KPI.
    edit = client.patch(
        "/api/kpis/Alpha%20Eng",
        json={"tags": ["engineering", "nope"]},
    )
    assert edit.status_code == 403
    assert "may not edit" in edit.json()["error"]

    # Lead may not create for another owner.
    add = client.post(
        "/api/kpis",
        json={
            "name": "Stolen",
            "owner": "marc.schriftman@leandna.com",
            "tags": ["engineering"],
        },
    )
    assert add.status_code == 403
    assert "only create KPIs owned by themselves" in add.json()["error"]

    # Lead may not delete another owner's KPI.
    delete = client.delete("/api/kpis/Beta%20Support")
    assert delete.status_code == 403
    assert "may not delete" in delete.json()["error"]

    assert registry.read_text(encoding="utf-8") == before


def test_lead_edit_own_kpi(tmp_path: Path) -> None:
    client, registry = _maintain_client(tmp_path, actor="lead.eng@leandna.com")
    _login(client)

    res = client.patch(
        "/api/kpis/Gamma%20Eng%20Lead",
        json={
            "metric_id": 55,
            "tags": ["engineering", "ai"],
            "target": 12,
            "direction": "lower",
        },
    )
    assert res.status_code == 200, res.text
    entry = res.json()["change"]["entry"]
    assert entry["metric-id"] == 55
    assert entry["tags"] == ["engineering", "ai"]
    assert entry["owner"] == "lead.eng@leandna.com"
    doc = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert doc["metrics"]["Gamma Eng Lead"]["target"] == 12
    assert doc["metrics"]["Gamma Eng Lead"]["metric-id"] == 55

    # A generator is chosen at creation, not bolted on later through the web app.
    blocked = client.patch(
        "/api/kpis/Gamma%20Eng%20Lead",
        json={"metric_generator": "get_open_help"},
    )
    assert blocked.status_code == 400
    assert "immutable" in blocked.json()["error"]


def _value_client(
    tmp_path: Path,
    *,
    actor: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TestClient, Path]:
    """Maintain client whose SQLite store lives under *tmp_path*, not the real cache."""
    cache_root = tmp_path / "cache"
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", cache_root)
    store = cache_root / "kpi" / "observations.sqlite"
    connect(store).close()
    client, _ = _maintain_client(tmp_path, actor=actor)
    return client, store


def test_set_value_overrides_without_losing_generated_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, store = _value_client(
        tmp_path, actor="marc.schriftman@leandna.com", monkeypatch=monkeypatch
    )
    _login(client)

    conn = connect(store)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Alpha Eng",
            grain="daily",
            period_key="2026-09-24",
            observation=KPIObservation(value=9.0, origin="live"),
            generator="get_alpha",
            tags=["engineering"],
        ),
    )
    conn.close()

    res = client.put("/api/kpis/Alpha%20Eng/value", json={"value": "12.5"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["name"] == "Alpha Eng"
    assert body["grain"] == "daily"
    assert body["value"] == 12.5
    assert body["generated_value"] == 9.0
    assert body["overridden"] is True
    assert body["override_by"] == "marc.schriftman@leandna.com"

    conn = connect(store)
    rows = list_kpis(conn, metric_name="Alpha Eng")
    conn.close()
    assert len(rows) == 1
    assert rows[0].observation.value == 9.0
    assert rows[0].effective_observation.display_value == 12.5

    cleared = client.put("/api/kpis/Alpha%20Eng/value", json={"value": None})
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["overridden"] is False
    assert cleared.json()["value"] == 9.0

    conn = connect(store)
    rows = list_kpis(conn, metric_name="Alpha Eng")
    conn.close()
    assert [r.effective_observation.display_value for r in rows] == [9.0]


def test_edit_rejects_immutable_fields(tmp_path: Path) -> None:
    client, registry = _maintain_client(tmp_path, actor="marc.schriftman@leandna.com")
    _login(client)
    before = registry.read_text(encoding="utf-8")

    for payload in (
        {"description": "new"},
        {"grain": "weekly"},
        {"metric_generator": "get_open_help"},
        {"owner": "lead.eng@leandna.com"},
        {"clear_owner": True},
    ):
        res = client.patch("/api/kpis/Alpha%20Eng", json=payload)
        assert res.status_code == 400, (payload, res.text)
        assert "immutable" in res.json()["error"]
    assert registry.read_text(encoding="utf-8") == before

    allowed = client.patch(
        "/api/kpis/Alpha%20Eng",
        json={"new_name": "Alpha Engineering", "tags": ["engineering", "ai"], "target": 25},
    )
    assert allowed.status_code == 200, allowed.text
    entry = allowed.json()["change"]["entry"]
    assert entry["tags"] == ["engineering", "ai"]
    assert entry["target"] == 25
    doc = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert "Alpha Engineering" in doc["metrics"]


def test_set_value_rejects_bad_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _value_client(
        tmp_path, actor="marc.schriftman@leandna.com", monkeypatch=monkeypatch
    )
    _login(client)

    missing = client.put("/api/kpis/Alpha%20Eng/value", json={})
    assert missing.status_code == 400
    assert "value is required" in missing.json()["error"]

    not_a_number = client.put("/api/kpis/Alpha%20Eng/value", json={"value": "abc"})
    assert not_a_number.status_code == 400
    assert "must be a number" in not_a_number.json()["error"]

    unknown = client.put("/api/kpis/Does%20Not%20Exist/value", json={"value": 1})
    assert unknown.status_code == 404
    assert unknown.json()["ok"] is False


def test_set_value_ownership_and_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client, store = _value_client(
        tmp_path, actor="lead.eng@leandna.com", monkeypatch=monkeypatch
    )

    anon = client.put("/api/kpis/Gamma%20Eng%20Lead/value", json={"value": 1})
    assert anon.status_code == 401
    assert anon.json()["ok"] is False

    _login(client)

    # Lead may not write a value for Marc's KPI.
    other = client.put("/api/kpis/Alpha%20Eng/value", json={"value": 1})
    assert other.status_code == 403
    assert "may not edit" in other.json()["error"]

    own = client.put("/api/kpis/Gamma%20Eng%20Lead/value", json={"value": 4})
    assert own.status_code == 200, own.text

    conn = connect(store)
    names = {r.metric_name for r in list_kpis(conn)}
    conn.close()
    assert names == {"Gamma Eng Lead"}


def test_unauthenticated_write_fails_loud(tmp_path: Path) -> None:
    client = _client(tmp_path)
    res = client.post("/api/kpis", json={"name": "X", "tags": ["engineering"]})
    assert res.status_code == 401
    assert res.json()["ok"] is False


def test_ui_mentions_maintain(tmp_path: Path) -> None:
    owners, registry = _write_fixture(tmp_path)
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "marc.schriftman@leandna.com",
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
    }
    client = TestClient(create_app(settings=load_kpi_web_settings(environ=env)))
    res = client.get("/")
    assert res.status_code == 200
    assert "Add KPI" in res.text
    assert "Report a Bug or File a Feature Request" in res.text
    assert "view &amp; maintain" not in res.text
    assert "view & maintain" not in res.text
