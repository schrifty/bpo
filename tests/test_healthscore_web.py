"""Separate Health Score framework, store, and web API."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.healthscore_web.framework import load_framework
from src.healthscore_web.store import (
    connect,
    observations_for_entity,
    set_override,
    upsert_reading,
)
from src.healthscore_web.usage_level import get_usage_level, usage_level_points
from src.kpi_store import GRAINS
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings
from tests.test_kpi_web_api import _OWNERS_YAML, _REGISTRY, _login


def _client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dev_user: str = "marc.schriftman@leandna.com",
) -> TestClient:
    owners = tmp_path / "owners.yaml"
    owners.write_text(_OWNERS_YAML, encoding="utf-8")
    registry = tmp_path / "metrics.yaml"
    registry.write_text(_REGISTRY, encoding="utf-8")
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path / "cache")
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": dev_user,
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret-for-healthscore",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
        "CORTEX_KPI_WEB_SKIP_S3": "true",
    }
    return TestClient(create_app(settings=load_kpi_web_settings(environ=env)))


def _entities() -> list[dict[str, object]]:
    return [
        {
            "id": "001-active",
            "name": "Acme Entity",
            "entity_name": "Acme",
            "parent_name": "Acme Parent",
            "ultimate_parent_name": None,
            "contract_status": "Active",
            "contract_end_date": "2027-01-01",
            "arr": 100000,
        }
    ]


def test_framework_preserves_draft_weight_and_unknown_roi_weight() -> None:
    framework = load_framework()
    assert framework["input_count"] == 27
    assert framework["override_count"] == 4
    assert framework["configured_weight"] == 83
    roi = next(row for row in framework["inputs"] if row["key"] == "roi_multiple")
    assert roi["weight"] is None
    assert roi["status"] == "needs_weight"
    usage = next(row for row in framework["inputs"] if row["key"] == "usage_level")
    assert usage["metric-generator"] == "get_usage_level"
    assert usage["owner"] == "lindsay.brown@leandna.com"
    assert usage["grain"] == "weekly"


def test_framework_uses_kpi_registry_field_names() -> None:
    """The two catalogs merge later, so field names and grains must line up."""
    framework = load_framework()
    for row in [*framework["inputs"], *framework["overrides"]]:
        assert row["description"], row["key"]
        assert "definition" not in row, row["key"]
        assert "cadence" not in row, row["key"]
        assert row["owner"].endswith("@leandna.com"), row["key"]
        assert row["grain"] is None or row["grain"] in GRAINS, row["key"]
        if row["grain"] is None:
            assert row["cadence_note"], row["key"]
        assert isinstance(row["tags"], list) and row["tags"], row["key"]
        assert "metric-id" in row and "metric-generator" in row, row["key"]


def test_healthscore_store_is_separate_and_tracks_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    upsert_reading(
        conn,
        entity_id="001",
        entity_name="Acme",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-01",
        value=95,
        points=6,
        generator="get_usage_level",
        tags=["healthscore"],
        meta={"factory_count": 2},
    )
    rows = observations_for_entity(conn, "001")
    conn.close()
    assert rows[0]["period_key"] == "2026-W36"
    assert rows[0]["effective_value"] == 95
    assert rows[0]["tags"] == ["healthscore"]
    assert rows[0]["meta"]["factory_count"] == 2
    assert rows[0]["source_mode"] == "automated"
    assert (tmp_path / "healthscore" / "observations.sqlite").is_file()
    assert not (tmp_path / "kpi" / "observations.sqlite").exists()


def test_healthscore_store_columns_match_kpi_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    from src.kpi_store import connect as kpi_connect

    hs = connect()
    kpi = kpi_connect(tmp_path / "kpi" / "observations.sqlite")
    try:
        hs_cols = {row[1] for row in hs.execute("PRAGMA table_info(healthscore_observation)")}
        kpi_cols = {row[1] for row in kpi.execute("PRAGMA table_info(kpi_observation)")}
    finally:
        hs.close()
        kpi.close()
    shared = {
        "metric_name",
        "grain",
        "period_key",
        "captured_at",
        "value",
        "generator",
        "tags_json",
        "meta_json",
        "error",
        "as_of",
        "override_value",
        "override_by",
        "override_at",
    }
    assert shared <= hs_cols
    assert shared <= kpi_cols
    # Health Score only adds the entity dimension and the weighted points.
    assert hs_cols - kpi_cols == {
        "entity_id",
        "entity_name",
        "points",
        "override_points",
        "override_note",
    }


def test_healthscore_routes_and_manual_component_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _client(tmp_path, monkeypatch)
    assert client.get("/", follow_redirects=False).headers["location"] == "/kpis"
    assert "Cortex KPIs" in client.get("/kpis").text
    healthscore = client.get("/healthscore").text
    assert "Cortex Healthscore" in healthscore
    assert "Customer Success · Entity-level model" not in healthscore
    assert client.get("/healthscore/api/framework").status_code == 401

    _login(client)
    entities = client.get("/healthscore/api/entities")
    assert entities.status_code == 200
    assert entities.json()["entities"][0]["name"] == "Acme Entity"
    assert entities.json()["source"].startswith("Salesforce")

    first = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-08-31", "value": 95, "points": 6, "note": "August close"},
    )
    assert first.status_code == 200, first.text
    assert first.json()["observation"]["period_key"] == "2026-W36"
    second = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "value": 50, "points": 2, "note": "September close"},
    )
    assert second.status_code == 200, second.text

    score = client.get("/healthscore/api/entities/001-active/score")
    assert score.status_code == 200, score.text
    body = score.json()
    assert body["score"] == pytest.approx(33.33, abs=0.01)
    assert body["covered_weight"] == 6
    assert body["coverage_pct"] == pytest.approx(7.23, abs=0.01)
    usage = next(row for row in body["components"] if row["key"] == "usage_level")
    assert usage["latest"]["period_key"] == "2026-W40"
    assert usage["latest"]["override_by"] == "marc.schriftman@leandna.com"
    assert usage["contribution"] == 2
    assert len(body["history"]) == 2
    assert len(body["observations"]) == 2


def test_healthscore_rejects_non_salesforce_entity_and_excess_points(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _client(tmp_path, monkeypatch)
    _login(client)
    missing = client.put(
        "/healthscore/api/entities/not-salesforce/components/usage_level",
        json={"points": 1},
    )
    assert missing.status_code == 400
    assert "active Salesforce Customer Entity" in missing.json()["error"]
    excess = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"points": 7},
    )
    assert excess.status_code == 400
    assert "cannot exceed 6" in excess.json()["error"]


def test_usage_level_points_follow_framework_bands() -> None:
    assert usage_level_points(None) == 0
    assert usage_level_points(0) == 1
    assert usage_level_points(30) == 1
    assert usage_level_points(31) == 2
    assert usage_level_points(65) == 3
    assert usage_level_points(80) == 4
    assert usage_level_points(92) == 5
    assert usage_level_points(93) == 6


def test_get_usage_level_scores_csr_match_and_warns_on_join_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    entities = [
        *_entities(),
        {
            "id": "001-unmatched",
            "name": "No CSR Entity",
            "entity_name": "Missing",
            "parent_name": None,
            "ultimate_parent_name": None,
        },
    ]
    week_rows = [
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant A",
            "weeklyActiveBuyersPercent": json.dumps({"endValue": 95, "empty": False}),
            "endDate": "2026-09-21",
        },
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant B",
            "automatedHealthScores": json.dumps(
                [{"name": "Usage", "healthScore": 40.0}]
            ),
            "endDate": "2026-09-21",
        },
    ]
    dry = get_usage_level(entities=entities, week_rows=week_rows, persist=False)
    assert dry["owner"] == "lindsay.brown@leandna.com"
    assert dry["grain"] == "weekly"
    assert dry["scored"] == 1
    assert dry["unmatched"] == 1
    assert dry["warnings"][0]["id"] == "001-unmatched"
    reading = dry["readings"][0]
    assert reading["value"] == pytest.approx(67.5)
    assert reading["points"] == 4
    assert reading["period_key"] == "2026-W39"
    assert reading["as_of"] == "2026-09-21"

    persisted = get_usage_level(entities=entities, week_rows=week_rows, persist=True)
    assert persisted["scored"] == 1
    conn = connect()
    rows = observations_for_entity(conn, "001-active")
    conn.close()
    assert rows[0]["source_mode"] == "automated"
    assert rows[0]["generator"] == "get_usage_level"
    assert rows[0]["points"] == 4
    assert rows[0]["meta"]["factory_count"] == 2


def test_manual_override_shadows_generated_reading_without_erasing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    set_override(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-21",
        value=10,
        points=1,
        by="lindsay.brown@leandna.com",
        note="CS override",
    )
    conn.close()
    week_rows = [
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant A",
            "weeklyActiveBuyersPercent": json.dumps({"endValue": 99, "empty": False}),
            "endDate": "2026-09-21",
        }
    ]
    result = get_usage_level(entities=_entities(), week_rows=week_rows, persist=True)
    assert result["overridden"] == 1
    conn = connect()
    rows = observations_for_entity(conn, "001-active")
    conn.close()
    assert len(rows) == 1
    assert rows[0]["source_mode"] == "manual"
    assert rows[0]["effective_value"] == 10
    assert rows[0]["effective_points"] == 1
    assert rows[0]["note"] == "CS override"
    # The generated reading survives underneath the override.
    assert rows[0]["value"] == pytest.approx(99)
    assert rows[0]["generator"] == "get_usage_level"


def test_null_override_clears_manual_reading_like_kpi_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _client(tmp_path, monkeypatch)
    _login(client)
    conn = connect()
    upsert_reading(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-21",
        value=95,
        points=6,
        generator="get_usage_level",
    )
    conn.close()
    over = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"period_key": "2026-W39", "points": 2},
    )
    assert over.status_code == 200, over.text
    assert over.json()["observation"]["overridden"] is True
    assert over.json()["observation"]["effective_points"] == 2

    cleared = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"period_key": "2026-W39", "points": None, "value": None},
    )
    assert cleared.status_code == 200, cleared.text
    body = cleared.json()
    assert body["cleared"] is True
    assert body["observation"]["overridden"] is False
    assert body["observation"]["effective_points"] == 6
    assert body["observation"]["override_by"] is None


def _framework_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from src.healthscore_web.framework import DEFAULT_FRAMEWORK_PATH

    target = tmp_path / "framework.yaml"
    target.write_text(DEFAULT_FRAMEWORK_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setenv("CORTEX_HEALTHSCORE_FRAMEWORK", str(target))
    return target


def test_update_component_rewrites_yaml_and_reweights(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.framework import HealthScoreFrameworkError, update_component

    target = _framework_copy(tmp_path, monkeypatch)
    framework = update_component(
        "usage_level", name="Usage breadth", pillar="Adoption", weight=8, path=target
    )
    usage = next(row for row in framework["inputs"] if row["key"] == "usage_level")
    assert usage["name"] == "Usage breadth"
    assert usage["pillar"] == "Adoption"
    assert usage["weight"] == 8
    assert framework["configured_weight"] == 85
    text = target.read_text(encoding="utf-8")
    assert text.startswith("# LeanDNA Customer Health Score")
    assert "configured_weight: 85" in text
    # Untouched rows keep their fields.
    roi = next(row for row in framework["inputs"] if row["key"] == "roi_multiple")
    assert roi["weight"] is None

    blank = update_component("usage_level", weight=None, path=target)
    assert blank["configured_weight"] == 77

    with pytest.raises(HealthScoreFrameworkError):
        update_component("usage_level", name="", path=target)
    with pytest.raises(HealthScoreFrameworkError):
        update_component("merger_acquisition", weight=3, path=target)
    with pytest.raises(HealthScoreFrameworkError):
        update_component("missing_component", name="x", path=target)
    # Overrides can still be renamed.
    renamed = update_component("merger_acquisition", name="M&A", path=target)
    assert next(row for row in renamed["overrides"] if row["key"] == "merger_acquisition")["name"] == "M&A"


def test_framework_edit_api_requires_catalog_admin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    _framework_copy(tmp_path, monkeypatch)
    client = _client(tmp_path, monkeypatch)
    assert client.put("/healthscore/api/framework/components/usage_level", json={"weight": 7}).status_code == 401
    _login(client)  # marc.schriftman is catalog_admin in _OWNERS_YAML
    res = client.put(
        "/healthscore/api/framework/components/usage_level",
        json={"name": "Usage level", "pillar": "Product Adoption & Usage", "weight": 7},
    )
    assert res.status_code == 200, res.text
    assert res.json()["component"]["weight"] == 7
    assert res.json()["framework"]["configured_weight"] == 84
    bad = client.put("/healthscore/api/framework/components/usage_level", json={"weight": "seven"})
    assert bad.status_code == 400

    # A lead who is not the catalog admin is refused.
    lead = _client(tmp_path, monkeypatch, dev_user="lead.eng@leandna.com")
    _login(lead)
    assert lead.get("/api/me").json()["is_catalog_admin"] is False
    denied = lead.put("/healthscore/api/framework/components/usage_level", json={"weight": 1})
    assert denied.status_code == 403
    assert "catalog admin" in denied.json()["error"]


def test_generate_usage_level_api_is_authenticated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    monkeypatch.setattr(
        "src.healthscore_web.api.run_usage_level_snapshot",
        lambda **kwargs: {
            "ok": True,
            "generator": "get_usage_level",
            "scored": 1,
            "unmatched": 0,
            "warnings": [],
            "readings": [],
        },
    )
    client = _client(tmp_path, monkeypatch)
    assert client.post("/healthscore/api/generate/usage_level").status_code == 401
    _login(client)
    res = client.post("/healthscore/api/generate/usage_level")
    assert res.status_code == 200
    assert res.json()["generator"] == "get_usage_level"

