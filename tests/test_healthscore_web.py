"""Separate Health Score framework, store, and web API."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.healthscore_web.framework import load_framework
from src.healthscore_web.store import connect, observations_for_entity
from src.healthscore_web.usage_level import get_usage_level, usage_level_points
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings
from tests.test_kpi_web_api import _OWNERS_YAML, _REGISTRY, _login


def _client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    owners = tmp_path / "owners.yaml"
    owners.write_text(_OWNERS_YAML, encoding="utf-8")
    registry = tmp_path / "metrics.yaml"
    registry.write_text(_REGISTRY, encoding="utf-8")
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path / "cache")
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "marc.schriftman@leandna.com",
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
    assert usage["generator"] == "get_usage_level"
    assert usage["owner"] == "Lindsay Brown"
    assert usage["owner_email"] == "lindsay.brown@leandna.com"


def test_healthscore_store_is_separate_and_tracks_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    conn.execute(
        """
        INSERT INTO healthscore_observation (
            entity_id, entity_name, component_key, period_date, raw_value_json,
            points, source_mode, note, entered_by, entered_at
        ) VALUES ('001', 'Acme', 'usage_level', '2026-09-01', '95', 6, 'manual',
                  'baseline', 'lead@leandna.com', '2026-09-01T00:00:00Z')
        """
    )
    conn.commit()
    rows = observations_for_entity(conn, "001")
    conn.close()
    assert rows[0]["raw_value"] == 95
    assert rows[0]["source_mode"] == "manual"
    assert (tmp_path / "healthscore" / "observations.sqlite").is_file()
    assert not (tmp_path / "kpi" / "observations.sqlite").exists()


def test_healthscore_routes_and_manual_component_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _client(tmp_path, monkeypatch)
    assert client.get("/", follow_redirects=False).headers["location"] == "/kpis"
    assert "Cortex KPIs" in client.get("/kpis").text
    assert "Cortex Health Score" in client.get("/healthscore").text
    assert client.get("/healthscore/api/framework").status_code == 401

    _login(client)
    entities = client.get("/healthscore/api/entities")
    assert entities.status_code == 200
    assert entities.json()["entities"][0]["name"] == "Acme Entity"
    assert entities.json()["source"].startswith("Salesforce")

    first = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={
            "period_date": "2026-08-31",
            "raw_value": 95,
            "points": 6,
            "note": "August close",
        },
    )
    assert first.status_code == 200, first.text
    second = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={
            "period_date": "2026-09-30",
            "raw_value": 50,
            "points": 2,
            "note": "September close",
        },
    )
    assert second.status_code == 200, second.text

    score = client.get("/healthscore/api/entities/001-active/score")
    assert score.status_code == 200, score.text
    body = score.json()
    assert body["score"] == pytest.approx(33.33, abs=0.01)
    assert body["covered_weight"] == 6
    assert body["coverage_pct"] == pytest.approx(7.23, abs=0.01)
    usage = next(row for row in body["components"] if row["key"] == "usage_level")
    assert usage["latest"]["period_date"] == "2026-09-30"
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
    assert dry["owner"] == "Lindsay Brown"
    assert dry["scored"] == 1
    assert dry["unmatched"] == 1
    assert dry["warnings"][0]["id"] == "001-unmatched"
    reading = dry["readings"][0]
    assert reading["raw_value"] == pytest.approx(67.5)
    assert reading["points"] == 4
    assert reading["period_date"] == "2026-09-21"

    persisted = get_usage_level(entities=entities, week_rows=week_rows, persist=True)
    assert persisted["scored"] == 1
    conn = connect()
    rows = observations_for_entity(conn, "001-active")
    conn.close()
    assert rows[0]["source_mode"] == "automated"
    assert rows[0]["entered_by"] == "get_usage_level"
    assert rows[0]["points"] == 4


def test_usage_level_generator_does_not_clobber_manual_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    conn.execute(
        """
        INSERT INTO healthscore_observation (
            entity_id, entity_name, component_key, period_date, raw_value_json,
            points, source_mode, note, entered_by, entered_at
        ) VALUES ('001-active', 'Acme Entity', 'usage_level', '2026-09-21', '10',
                  1, 'manual', 'CS override', 'lindsay.brown@leandna.com',
                  '2026-09-21T00:00:00Z')
        """
    )
    conn.commit()
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
    assert result["skipped_manual"] == 1
    conn = connect()
    rows = observations_for_entity(conn, "001-active")
    conn.close()
    assert rows[0]["source_mode"] == "manual"
    assert rows[0]["raw_value"] == 10


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

