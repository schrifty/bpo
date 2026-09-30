"""Separate Health Score framework, store, and web API."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.healthscore_web.champion_login import (
    ChampionLoginGeneratorError,
    champion_login_points,
    get_champion_login_continuity,
)
from src.healthscore_web.roi_multiple import get_roi_multiple, roi_multiple_points
from src.healthscore_web.summit_attendance import (
    SummitAttendanceGeneratorError,
    get_summit_attendance,
    is_summit_campaign,
    member_counts,
)
from src.healthscore_web.framework import load_framework
from src.healthscore_web.store import (
    connect,
    observations_for_entity,
    set_override,
    upsert_reading,
)
from src.healthscore_web.usage_level import get_usage_level, usage_level_points
from src.healthscore_web.usage_trend import (
    get_usage_trend,
    percent_change,
    usage_trend_points,
)
from src.kpi_store import GRAINS
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings
from tests.test_kpi_web_api import _OWNERS_YAML, _REGISTRY, _login

_SCORE_OWNER = "lindsay.brown@leandna.com"
_OWNERS_WITH_SCORE_OWNER = _OWNERS_YAML.replace(
    "  - email: lead.eng@leandna.com\n",
    "  - email: lindsay.brown@leandna.com\n"
    "    display_name: Lindsay Brown\n"
    "    packs: [support]\n"
    "  - email: lead.eng@leandna.com\n",
)


def _client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dev_user: str = "marc.schriftman@leandna.com",
) -> TestClient:
    owners = tmp_path / "owners.yaml"
    owners.write_text(_OWNERS_WITH_SCORE_OWNER, encoding="utf-8")
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


def _score_owner_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    return _client(tmp_path, monkeypatch, dev_user=_SCORE_OWNER)


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
    assert framework["input_count"] == 28
    assert framework["override_count"] == 4
    assert framework["configured_weight"] == 88
    roi = next(row for row in framework["inputs"] if row["key"] == "roi_multiple")
    assert roi["weight"] is None
    assert roi["status"] == "needs_weight"
    usage = next(row for row in framework["inputs"] if row["key"] == "usage_level")
    assert usage["metric-generator"] == "get_usage_level"
    assert usage["owner"] == "lindsay.brown@leandna.com"
    assert usage["grain"] == "weekly"
    trend = next(row for row in framework["inputs"] if row["key"] == "usage_trend")
    assert trend["metric-generator"] == "get_usage_trend"
    assert trend["status"] == "defined"
    assert trend["grain"] == "weekly"
    champion = next(row for row in framework["inputs"] if row["key"] == "champion_login_continuity")
    assert champion["metric-generator"] == "get_champion_login_continuity"
    assert champion["status"] == "defined"
    assert champion["automation"] == "automated"
    assert "trailing 7 days" in champion["description"]
    assert champion["data_source"] == ["Salesforce"]
    turnover = next(row for row in framework["inputs"] if row["key"] == "champion_turnover")
    assert turnover["metric-generator"] == "get_champion_turnover"
    assert turnover["status"] == "defined"
    assert turnover["automation"] == "automated"
    assert turnover["data_source"] == ["Salesforce"]
    assert "external research" not in turnover["description"].casefold()
    assert turnover["pillar"] == "Relationship & Engagement"
    assert turnover["weight"] == 4
    assert turnover["max_points"] == 5
    assert turnover["min_points"] == -5
    assert "trailing 3" in turnover["description"]
    assert "-5" in turnover["scoring"]
    departure = next(
        row for row in framework["overrides"] if row["key"] == "champion_departure_external"
    )
    assert departure["metric-generator"] == "get_champion_turnover"
    assert departure["data_source"] == ["Salesforce", "Web Research"]
    assert departure["automation"] == "automated"
    assert departure["status"] == "defined"
    assert departure["grain"] == "weekly"
    assert "trailing" in departure["description"]
    assert "external" not in departure["description"].casefold()
    assert champion["automation"] == "automated"
    roi = next(row for row in framework["inputs"] if row["key"] == "roi_multiple")
    assert roi["metric-generator"] == "get_roi_multiple"
    assert roi["data_source"] == ["CS Report", "Salesforce"]
    assert "Previous-period inventory-action savings" in roi["description"]
    assert roi["weight"] is None
    summit = next(row for row in framework["inputs"] if row["key"] == "summit_attendance")
    assert summit["metric-generator"] == "get_summit_attendance"
    ideas = next(row for row in framework["inputs"] if row["key"] == "product_ideas")
    assert ideas["metric-generator"] == "get_product_ideas"
    assert ideas["data_source"] == ["Pendo"]
    assert "Pendo feedback" in ideas["description"]
    reference = next(row for row in framework["inputs"] if row["key"] == "reference_willingness")
    assert reference["metric-generator"] == "get_reference_willingness"
    assert reference["data_source"] == ["Salesforce"]
    assert reference["grain"] == "monthly"
    engagement = next(row for row in framework["inputs"] if row["key"] == "enhancement_engagement")
    assert engagement["metric-generator"] == "get_enhancement_engagement"
    assert engagement["name"] == "Enhancement engagement & delivery"
    assert engagement["weight"] == 5
    assert engagement["status"] == "needs_weight"
    assert engagement["max_points"] == 10
    assert engagement["data_source"] == ["Aha", "Salesforce"]
    cadence = next(row for row in framework["inputs"] if row["key"] == "meeting_cadence")
    assert cadence["metric-generator"] == "get_meeting_cadence"
    assert cadence["data_source"] == ["Chorus"]
    assert cadence["max_points"] == 2
    assert cadence["weight"] == 2
    sla = next(row for row in framework["inputs"] if row["key"] == "sla_adherence")
    assert sla["metric-generator"] == "get_sla_adherence"
    assert sla["data_source"] == ["JIRA"]
    assert sla["status"] == "defined"
    assert sla["max_points"] == 3
    assert sla["weight"] == 3
    escalation = next(row for row in framework["inputs"] if row["key"] == "escalation_rate")
    assert escalation["data_source"] == ["JIRA"]
    assert escalation["metric-generator"] == "get_escalation_rate"
    assert escalation["max_points"] == 2
    assert escalation["status"] == "defined"
    volume = next(row for row in framework["inputs"] if row["key"] == "ticket_volume_trend")
    assert volume["data_source"] == ["JIRA"]
    assert volume["metric-generator"] == "get_ticket_volume_trend"
    assert volume["max_points"] == 3
    assert volume["status"] == "defined"
    assert "Attended" in summit["description"]
    known = {name.casefold() for name in framework["available_data_sources"]}
    assert {"salesforce", "cs report", "pendo", "aha"} <= known
    labels = framework["available_source_labels"]
    assert "Aha" in labels
    assert "JIRA" in labels and "Atlassian Jira" not in labels
    assert "Data API" in labels and "LeanDNA" not in labels
    assert labels.index("Pendo") < labels.index("Salesforce") < labels.index("Aha")
    verified = next(row for row in framework["inputs"] if row["key"] == "verified_outcomes")
    assert verified["data_source"] == ["Verified-outcome log"]
    assert "verified-outcome log" not in known
    assert "not yet defined" not in known


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
        assert isinstance(row["data_source"], list) and row["data_source"], row["key"]
        assert row["automation"] in ("automated", "manual", "blocked"), row["key"]


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
    client = _score_owner_client(tmp_path, monkeypatch)
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
    assert body["coverage_pct"] == pytest.approx(6.82, abs=0.01)
    usage = next(row for row in body["components"] if row["key"] == "usage_level")
    assert usage["latest"]["period_key"] == "2026-W40"
    assert usage["latest"]["override_by"] == _SCORE_OWNER
    assert usage["contribution"] == 2
    assert len(body["history"]) == 2
    assert len(body["observations"]) == 2


def test_healthscore_report_lists_entities_by_score_then_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entities = [
        {**_entities()[0], "id": "001-red", "name": "Red Entity"},
        {**_entities()[0], "id": "001-yellow", "name": "Yellow Entity"},
        {**_entities()[0], "id": "001-green", "name": "Green Entity"},
        {**_entities()[0], "id": "001-wide", "name": "Wide Entity"},
        {**_entities()[0], "id": "001-none", "name": "Unscored Entity"},
    ]
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", lambda: entities)
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.get("/healthscore/api/report").status_code == 401
    page = client.get("/healthscore")
    assert 'href="/healthscore/report"' in page.text
    assert "Healthscore Report" in client.get("/healthscore/report").text

    _login(client)
    # usage_level weight 6, max 6. Wide also fills champion_turnover at 5 of 5
    # (weight 4) and meeting_cadence (2). Order is the shown 0–100 score,
    # highest first, then entity name. Unscored stays last.
    for entity_id, points in (("001-red", 0), ("001-yellow", 4), ("001-green", 6), ("001-wide", 3)):
        saved = client.put(
            f"/healthscore/api/entities/{entity_id}/components/usage_level",
            json={"as_of": "2026-09-30", "points": points},
        )
        assert saved.status_code == 200, saved.text
    for metric, points in (("champion_turnover", 5), ("meeting_cadence", 2)):
        saved = client.put(
            f"/healthscore/api/entities/001-wide/components/{metric}",
            json={"as_of": "2026-09-30", "points": points},
        )
        assert saved.status_code == 200, saved.text

    report = client.get("/healthscore/api/report")
    assert report.status_code == 200, report.text
    body = report.json()
    assert [row["name"] for row in body["entities"]] == [
        "Green Entity",
        "Wide Entity",
        "Yellow Entity",
        "Red Entity",
        "Unscored Entity",
    ]
    assert [row["shown_score"] for row in body["entities"]] == [100, 75, 67, 0, None]
    assert [row["weighted_score"] for row in body["entities"]] == [6, 9, 4, 0, None]
    assert [row["band"] for row in body["entities"]] == ["green", "green", "yellow", "red", None]
    assert body["counts"] == {"red": 1, "yellow": 1, "green": 2, "unscored": 1}
    assert body["entity_count"] == 5


def test_champion_turnover_minus_five_lowers_the_score(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _score_owner_client(tmp_path, monkeypatch)
    _login(client)
    saved = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "points": 6},
    )
    assert saved.status_code == 200, saved.text
    left = client.put(
        "/healthscore/api/entities/001-active/components/champion_turnover",
        json={"as_of": "2026-09-30", "points": -5},
    )
    assert left.status_code == 200, left.text
    body = client.get("/healthscore/api/entities/001-active/score").json()
    turnover = next(row for row in body["components"] if row["key"] == "champion_turnover")
    assert turnover["contribution"] == -4
    assert turnover["pillar"] == "Relationship & Engagement"
    assert body["score"] == 20
    assert body["covered_weight"] == 10
    too_low = client.put(
        "/healthscore/api/entities/001-active/components/champion_turnover",
        json={"as_of": "2026-09-30", "points": -6},
    )
    assert too_low.status_code == 400


def test_override_flag_sets_healthscore_to_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entities = [
        {**_entities()[0], "id": "001-open", "name": "Open Entity"},
        {**_entities()[0], "id": "001-flagged", "name": "Flagged Entity"},
    ]
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", lambda: entities)
    client = _score_owner_client(tmp_path, monkeypatch)
    _login(client)
    for entity_id in ("001-open", "001-flagged"):
        saved = client.put(
            f"/healthscore/api/entities/{entity_id}/components/usage_level",
            json={"as_of": "2026-09-30", "points": 6},
        )
        assert saved.status_code == 200, saved.text
    raised = client.put(
        "/healthscore/api/entities/001-flagged/components/merger_acquisition",
        json={"as_of": "2026-09-30", "value": 1},
    )
    assert raised.status_code == 200, raised.text

    score = client.get("/healthscore/api/entities/001-flagged/score")
    assert score.status_code == 200, score.text
    body = score.json()
    assert body["score"] == 0
    assert body["score_zeroed"] is True
    assert "M&A activity / parent company change" in body["zeroed_by"]
    assert body["coverage_pct"] == pytest.approx(6.82, abs=0.01)
    assert any(row["score"] == 0 and row["score_zeroed"] for row in body["history"])

    report = client.get("/healthscore/api/report")
    assert report.status_code == 200, report.text
    by_name = {row["name"]: row for row in report.json()["entities"]}
    assert by_name["Open Entity"]["weighted_score"] == 6
    assert by_name["Flagged Entity"]["weighted_score"] == 0
    assert by_name["Flagged Entity"]["shown_score"] == 0
    names = [row["name"] for row in report.json()["entities"]]
    assert names.index("Open Entity") < names.index("Flagged Entity")

    cleared = client.put(
        "/healthscore/api/entities/001-flagged/components/merger_acquisition",
        json={"as_of": "2026-09-30", "value": 0},
    )
    assert cleared.status_code == 200, cleared.text
    restored = client.get("/healthscore/api/entities/001-flagged/score")
    assert restored.json()["score"] == 100
    assert restored.json()["score_zeroed"] is False


def test_healthscore_rejects_non_salesforce_entity_and_excess_points(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    client = _score_owner_client(tmp_path, monkeypatch)
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


def test_latest_csr_file_per_iso_week_picks_newest_file_in_week() -> None:
    from src.cs_report_client import latest_csr_file_per_iso_week

    files = [
        {
            "id": "old-thu",
            "name": "customer-success-report-2026-09-24T04:00:00Z",
            "modifiedTime": "2026-09-24T04:00:00.000Z",
        },
        {
            "id": "new-fri",
            "name": "customer-success-report-2026-09-25T04:00:00Z",
            "modifiedTime": "2026-09-25T04:00:00.000Z",
        },
        {
            "id": "w37",
            "name": "customer-success-report-2026-09-13T04:00:00Z",
            "modifiedTime": "2026-09-13T04:00:00.000Z",
        },
    ]
    one = latest_csr_file_per_iso_week(files, weeks=1)
    assert [row["id"] for row in one] == ["new-fri"]
    assert one[0]["period_key"] == "2026-W39"
    two = latest_csr_file_per_iso_week(files, weeks=2)
    assert [row["id"] for row in two] == ["new-fri", "w37"]


def test_backfill_usage_level_fails_loud_when_weeks_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.snapshot import backfill_usage_level_history
    from src.healthscore_web.usage_level import UsageLevelGeneratorError

    monkeypatch.setattr(
        "src.cs_report_client.list_csr_report_files",
        lambda: [
            {
                "id": "only",
                "name": "customer-success-report-2026-09-25T04:00:00Z",
                "modifiedTime": "2026-09-25T04:00:00.000Z",
            }
        ],
    )
    with pytest.raises(UsageLevelGeneratorError, match="need 15"):
        backfill_usage_level_history(weeks=15, dry_run=True, entities=_entities())


def test_csr_site_matches_matamoros_codes_and_carrier_singapore() -> None:
    from src.healthscore_web.usage_level import csr_site_matches_entity

    matamoros = {
        "name": "Johnson Controls: JCI Matamoros (1301&1302)",
        "entity_name": "Johnson Controls: JCI Matamoros (1301&1302)",
        "parent_name": "Johnson Controls - Security",
        "ultimate_parent_name": None,
    }
    assert csr_site_matches_entity({"entity": "JCI Matamoros (SP)"}, matamoros)
    carrier = {
        "name": "Carrier Transicold (Singapore)",
        "entity_name": "Carrier: Carrier Singapore Container",
        "parent_name": "REF (Carrier)",
        "ultimate_parent_name": None,
    }
    assert csr_site_matches_entity({"entity": "Carrier Singapore, Container"}, carrier)
    lithia = {
        "name": "Johnson Controls - DC Lithia",
        "entity_name": "Johnson Controls: JCI DC Lithia (1304)",
        "parent_name": "Johnson Controls Fire Suppression",
        "ultimate_parent_name": None,
    }
    assert csr_site_matches_entity({"entity": "JCI DC Lithia (1304)"}, lithia)
    assert not csr_site_matches_entity({"entity": "JCI DC Lithia Springs (1114)"}, lithia)
    fougeres = {
        "name": "Safran Electronics and Defense : Fougeres",
        "entity_name": "Safran Electronics and Defense: Fougères",
        "parent_name": "Safran Electronics & Defense, Avionics",
        "ultimate_parent_name": None,
    }
    assert csr_site_matches_entity(
        {"entity": "Safran Electronics and Defense: Fougères"}, fougeres
    )


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
    client = _score_owner_client(tmp_path, monkeypatch)
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


def test_update_component_edits_description_owner_and_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.framework import HealthScoreFrameworkError, update_component

    target = _framework_copy(tmp_path, monkeypatch)
    framework = update_component(
        "usage_level",
        description="  Share of licensed seats active in the period.  ",
        owner="owner@leandna.com",
        grain="Monthly",
        path=target,
    )
    usage = next(row for row in framework["inputs"] if row["key"] == "usage_level")
    assert usage["description"] == "Share of licensed seats active in the period."
    assert usage["owner"] == "owner@leandna.com"
    assert usage["grain"] == "monthly"

    # Blank grain clears it back to "not defined".
    cleared = update_component("usage_level", grain="", path=target)
    assert next(row for row in cleared["inputs"] if row["key"] == "usage_level")["grain"] is None

    # Override flags accept the same descriptive fields.
    flagged = update_component("merger_acquisition", owner="flags@leandna.com", grain="daily", path=target)
    flag = next(row for row in flagged["overrides"] if row["key"] == "merger_acquisition")
    assert flag["owner"] == "flags@leandna.com"
    assert flag["grain"] == "daily"

    with pytest.raises(HealthScoreFrameworkError):
        update_component("usage_level", description="   ", path=target)
    with pytest.raises(HealthScoreFrameworkError):
        update_component("usage_level", owner="", path=target)
    with pytest.raises(HealthScoreFrameworkError):
        update_component("usage_level", grain="fortnightly", path=target)


def test_update_component_status_follows_generator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.framework import HealthScoreFrameworkError, update_component

    target = _framework_copy(tmp_path, monkeypatch)
    blocked = update_component("usage_level", automation="blocked", path=target)
    usage = next(row for row in blocked["inputs"] if row["key"] == "usage_level")
    assert usage["automation"] == "blocked"
    assert usage["metric-generator"]
    with pytest.raises(HealthScoreFrameworkError, match="cannot be Manual"):
        update_component("usage_level", automation="manual", path=target)
    restored = update_component("usage_level", automation="Automated", path=target)
    assert next(row for row in restored["inputs"] if row["key"] == "usage_level")["automation"] == "automated"

    manual = update_component("product_anchor_adoption", automation="manual", path=target)
    anchor = next(row for row in manual["inputs"] if row["key"] == "product_anchor_adoption")
    assert anchor["automation"] == "manual"
    assert anchor["status"] == "needs_definition"
    assert not anchor.get("metric-generator")
    with pytest.raises(HealthScoreFrameworkError, match="cannot be Automated"):
        update_component("product_anchor_adoption", automation="automated", path=target)
    with pytest.raises(HealthScoreFrameworkError, match="status must be"):
        update_component("product_anchor_adoption", automation="paused", path=target)


def test_framework_status_edit_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _framework_copy(tmp_path, monkeypatch)
    client = _client(tmp_path, monkeypatch)
    _login(client)
    saved = client.put(
        "/healthscore/api/framework/components/usage_level",
        json={"automation": "blocked"},
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["component"]["automation"] == "blocked"
    denied_rule = client.put(
        "/healthscore/api/framework/components/usage_level",
        json={"automation": "manual"},
    )
    assert denied_rule.status_code == 400
    assert "cannot be Manual" in denied_rule.json()["error"]

    lead = _client(tmp_path, monkeypatch, dev_user="lead.eng@leandna.com")
    _login(lead)
    denied = lead.put(
        "/healthscore/api/framework/components/usage_level",
        json={"automation": "blocked"},
    )
    assert denied.status_code == 403


def test_framework_targets_come_from_full_points_thresholds() -> None:
    from src.healthscore_web.framework import load_framework

    framework = load_framework()
    rows = {row["key"]: row for row in framework["inputs"]}
    assert (rows["meeting_cadence"]["target"], rows["meeting_cadence"]["target_direction"]) == (80, "higher")
    assert (rows["sla_adherence"]["target"], rows["sla_adherence"]["target_direction"]) == (90, "higher")
    assert (rows["call_talk_ratio"]["target"], rows["call_talk_ratio"]["target_direction"]) == (50, "lower")
    assert rows["summit_attendance"]["target"] is None
    assert rows["summit_attendance"]["target_direction"] is None


def test_framework_rejects_target_without_direction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.framework import HealthScoreFrameworkError, load_framework

    target = _framework_copy(tmp_path, monkeypatch)
    text = target.read_text(encoding="utf-8")
    target.write_text(text.replace("  target_direction: higher\n", "", 1), encoding="utf-8")
    with pytest.raises(HealthScoreFrameworkError, match="target_direction"):
        load_framework(target)


def test_deactivate_component_keeps_underlying_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.api import _score_numbers
    from src.healthscore_web.framework import (
        HealthScoreFrameworkError,
        load_framework,
        update_component,
    )

    target = _framework_copy(tmp_path, monkeypatch)
    original = next(row for row in load_framework(target)["inputs"] if row["key"] == "usage_level")
    automation = original["automation"]
    active = _score_numbers(load_framework(target), {"usage_level": original["max_points"]})

    original_weight = original["weight"]
    original_total = active["configured_weight"]

    hidden = update_component("usage_level", deactivated=True, path=target)
    usage = next(row for row in hidden["inputs"] if row["key"] == "usage_level")
    assert usage["deactivated"] is True
    assert usage["automation"] == automation
    assert usage["status"] == original["status"]
    assert usage["weight"] == 0
    assert usage["deactivated_weight"] == original_weight
    assert hidden["configured_weight"] == original_total - original_weight
    paused = _score_numbers(hidden, {"usage_level": original["max_points"]})
    assert paused["score"] is None
    assert paused["covered_weight"] == 0
    with pytest.raises(HealthScoreFrameworkError, match="reactivate"):
        update_component("usage_level", weight=12, path=target)

    restored = update_component("usage_level", deactivated=False, path=target)
    usage = next(row for row in restored["inputs"] if row["key"] == "usage_level")
    assert not usage.get("deactivated")
    assert "deactivated_weight" not in usage
    assert usage["weight"] == original_weight
    assert usage["automation"] == automation
    assert restored["configured_weight"] == original_total
    assert _score_numbers(restored, {"usage_level": original["max_points"]})["score"] == active["score"]


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


def test_score_write_requires_component_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    admin = _client(tmp_path, monkeypatch)
    _login(admin)
    denied = admin.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "points": 1},
    )
    assert denied.status_code == 403
    assert "KPI owner" in denied.json()["error"]
    assert "lindsay.brown@leandna.com" in denied.json()["error"]

    lead = _client(tmp_path, monkeypatch, dev_user="lead.eng@leandna.com")
    _login(lead)
    also_denied = lead.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "points": 1},
    )
    assert also_denied.status_code == 403

    owner = _score_owner_client(tmp_path, monkeypatch)
    _login(owner)
    saved = owner.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "points": 1},
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["observation"]["override_by"] == _SCORE_OWNER

    blocked_generate = admin.post("/healthscore/api/generate/usage_level")
    assert blocked_generate.status_code == 403


def test_usage_trend_points_follow_confirmed_band() -> None:
    assert usage_trend_points(None) is None
    assert usage_trend_points(10) == 3
    assert usage_trend_points(-10) == 3
    assert usage_trend_points(10.1) == 5
    assert usage_trend_points(-10.1) == 0
    assert percent_change(80, 72) == pytest.approx(-10)
    assert percent_change(0, 0) == 0
    assert percent_change(0, 40) == 100


def test_get_usage_trend_needs_two_weeks_then_scores_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    upsert_reading(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-01",
        value=80,
        points=4,
        generator="get_usage_level",
    )
    conn.close()
    dry = get_usage_trend(entities=_entities(), as_of=date.fromisoformat("2026-09-24"), persist=False)
    assert dry["scored"] == 0
    assert dry["insufficient"] == 1
    assert dry["readings"][0]["points"] is None
    assert "insufficient" in (dry["readings"][0]["error"] or "")

    conn = connect()
    upsert_reading(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-24",
        value=96,
        points=6,
        generator="get_usage_level",
    )
    # A points override on usage_level must not change the trend series.
    set_override(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_level",
        grain="weekly",
        as_of="2026-09-24",
        value=10,
        points=1,
        by="tester@leandna.com",
    )
    conn.close()
    persisted = get_usage_trend(
        entities=_entities(), as_of=date.fromisoformat("2026-09-24"), persist=True
    )
    assert persisted["scored"] == 1
    reading = persisted["readings"][0]
    assert reading["value"] == pytest.approx(20)
    assert reading["points"] == 5
    assert reading["meta"]["baseline_usage_pct"] == 80
    assert reading["meta"]["current_usage_pct"] == 96
    assert reading["meta"]["weeks_used"] == 2


def _csr_week_rows(pct: float, end_date: str) -> list[dict[str, object]]:
    return [
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant A",
            "weeklyActiveBuyersPercent": json.dumps({"endValue": pct, "empty": False}),
            "endDate": end_date,
        }
    ]


def test_backfill_usage_trend_history_scores_each_week_and_skips_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.healthscore_web.snapshot import backfill_usage_trend_history
    from src.healthscore_web.store import observations_for_metric

    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    # Sundays 2026-08-30 (W35) .. 2026-09-27 (W39); W37 has no workbook.
    workbooks = {
        "w35": (50.0, "2026-08-30"),
        "w36": (60.0, "2026-09-06"),
        "w38": (72.0, "2026-09-20"),
        "w39": (90.0, "2026-09-27"),
    }
    monkeypatch.setattr(
        "src.cs_report_client.list_csr_report_files",
        lambda: [
            {
                "id": file_id,
                "name": f"customer-success-report-{day}T04:00:00Z",
                "modifiedTime": f"{day}T04:00:00.000Z",
            }
            for file_id, (_, day) in workbooks.items()
        ],
    )
    downloads: list[str] = []

    def _load(file_id: str, *, name: str | None = None) -> list[dict[str, object]]:
        downloads.append(file_id)
        pct, day = workbooks[file_id]
        return _csr_week_rows(pct, day)

    monkeypatch.setattr("src.cs_report_client.load_csr_report_by_file_id", _load)

    # W39 already has a trend reading; the backfill must leave it alone.
    conn = connect()
    upsert_reading(
        conn,
        entity_id="001-active",
        entity_name="Acme Entity",
        metric_name="usage_trend",
        grain="weekly",
        as_of="2026-09-27",
        value=1.0,
        points=3,
        generator="manual-seed",
    )
    conn.close()

    result = backfill_usage_trend_history(
        weeks=4, as_of=date.fromisoformat("2026-09-27"), entities=_entities()
    )
    assert result["ok"] is True
    assert result["weeks_requested"] == 4
    assert result["source_weeks_requested"] == 16
    assert result["source_weeks_available"] == 4
    assert result["warnings"] and "shorter" in result["warnings"][0]
    assert sorted(downloads) == sorted(workbooks)
    assert result["source_weeks_loaded"] == ["2026-W35", "2026-W36", "2026-W38", "2026-W39"]
    assert result["weeks_skipped"] == ["2026-W39"]
    assert [row["period_key"] for row in result["weeks"]] == [
        "2026-W36",
        "2026-W37",
        "2026-W38",
    ]
    # Weeks without a workbook still score from the surrounding usage_level history.
    assert result["weeks"][1]["as_of"] == "2026-09-13"

    conn = connect()
    try:
        trend = {
            row["period_key"]: row
            for row in observations_for_metric(conn, "001-active", "usage_trend")
        }
        level_periods = [
            row["period_key"]
            for row in observations_for_metric(conn, "001-active", "usage_level")
        ]
    finally:
        conn.close()
    assert level_periods == ["2026-W35", "2026-W36", "2026-W38", "2026-W39"]
    assert trend["2026-W36"]["value"] == pytest.approx(20)
    assert trend["2026-W36"]["points"] == 5
    assert trend["2026-W37"]["meta"]["current_period"] == "2026-W36"
    assert trend["2026-W38"]["value"] == pytest.approx(44)
    assert trend["2026-W38"]["meta"]["weeks_used"] == 3
    assert trend["2026-W39"]["generator"] == "manual-seed"

    # A rerun downloads nothing and rewrites nothing.
    downloads.clear()
    again = backfill_usage_trend_history(
        weeks=4, as_of=date.fromisoformat("2026-09-27"), entities=_entities()
    )
    assert downloads == []
    assert again["weeks_scored"] == 0
    assert again["weeks_skipped"] == ["2026-W36", "2026-W37", "2026-W38", "2026-W39"]


def test_generate_usage_trend_api_is_authenticated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    monkeypatch.setattr(
        "src.healthscore_web.api.run_usage_trend_snapshot",
        lambda **kwargs: {
            "ok": True,
            "generator": "get_usage_trend",
            "scored": 1,
            "insufficient": 0,
            "warnings": [],
            "readings": [],
        },
    )
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.post("/healthscore/api/generate/usage_trend").status_code == 401
    _login(client)
    res = client.post("/healthscore/api/generate/usage_trend")
    assert res.status_code == 200
    assert res.json()["generator"] == "get_usage_trend"


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
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.post("/healthscore/api/generate/usage_level").status_code == 401
    _login(client)
    res = client.post("/healthscore/api/generate/usage_level")
    assert res.status_code == 200
    assert res.json()["generator"] == "get_usage_level"


def test_champion_login_points_follow_framework_bands() -> None:
    assert champion_login_points(named=True, days_since_login=0) == 2
    assert champion_login_points(named=True, days_since_login=7) == 2
    assert champion_login_points(named=True, days_since_login=8) == 0
    assert champion_login_points(named=True, days_since_login=None) == 0
    assert champion_login_points(named=False, days_since_login=None) is None


def test_get_champion_login_continuity_scores_sponsor_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    entities = [
        {"id": "001-recent", "name": "Recent Sponsor"},
        {"id": "001-stale", "name": "Stale Sponsor"},
        {"id": "001-none", "name": "No Sponsor"},
    ]
    rows = [
        {
            "Id": "001-recent",
            "Executive_Sponsor__c": "003-recent",
            "Executive_Sponsor_Last_Login__c": "2026-09-22T15:00:00.000+0000",
        },
        {
            "Id": "001-stale",
            "Executive_Sponsor_Email__c": "sponsor@example.com",
            "Executive_Sponsor_Last_Login__c": "2026-09-01T15:00:00.000+0000",
        },
        {"Id": "001-none"},
    ]
    dry = get_champion_login_continuity(
        entities=entities,
        sponsor_rows=rows,
        champion_contacts=[],
        as_of=date.fromisoformat("2026-09-25"),
        persist=False,
    )
    by_id = {row["entity_id"]: row for row in dry["readings"]}
    assert dry["scored"] == 2
    assert dry["unscored"] == 1
    assert by_id["001-recent"]["points"] == 2
    assert by_id["001-recent"]["value"] == 3
    assert by_id["001-stale"]["points"] == 0
    assert by_id["001-stale"]["value"] == 24
    assert by_id["001-none"]["points"] is None
    assert "no executive sponsor" in by_id["001-none"]["error"]
    assert dry["warnings"][0]["id"] == "001-none"

    persisted = get_champion_login_continuity(
        entities=entities[:1],
        sponsor_rows=rows[:1],
        champion_contacts=[],
        as_of=date.fromisoformat("2026-09-25"),
        persist=True,
    )
    assert persisted["readings"][0]["generator"] == "get_champion_login_continuity"
    assert persisted["readings"][0]["points"] == 2


def test_champion_login_accepts_buyer_and_buying_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    entities = [
        {"id": "001-buyer", "name": "Buyer Only"},
        {"id": "001-both", "name": "Sponsor And Buyer"},
        {"id": "001-end-user", "name": "End User"},
    ]
    sponsors = [
        {"Id": "001-buyer"},
        {
            "Id": "001-both",
            "Executive_Sponsor__c": "003-sponsor",
            "Executive_Sponsor_Last_Login__c": "2026-09-01T15:00:00.000+0000",
        },
        {"Id": "001-end-user"},
    ]
    contacts = [
        {
            "Id": "003-buyer",
            "AccountId": "001-buyer",
            "User_Role__c": "Buyer,Engineer",
            "Pendo_Last_Visit__c": "2026-09-24T12:00:00.000+0000",
        },
        {
            "Id": "003-champion",
            "AccountId": "001-both",
            "Buyer_Role_HubSpot_Sync__c": "Champion",
            "Pendo_Last_Visit__c": "2026-09-23T12:00:00.000+0000",
        },
        {
            "Id": "003-end-user",
            "AccountId": "001-end-user",
            "Buyer_Role_HubSpot_Sync__c": "End User",
            "User_Role__c": "Planner",
            "Pendo_Last_Visit__c": "2026-09-24T12:00:00.000+0000",
        },
        {
            "Id": "003-technical",
            "AccountId": "001-buyer",
            "Buyer_Role_HubSpot_Sync__c": "Technical Buyer",
            "Pendo_Last_Visit__c": "2026-09-10T12:00:00.000+0000",
        },
    ]
    result = get_champion_login_continuity(
        entities=entities,
        sponsor_rows=sponsors,
        champion_contacts=contacts,
        as_of=date.fromisoformat("2026-09-25"),
        persist=False,
    )
    by_id = {row["entity_id"]: row for row in result["readings"]}
    assert by_id["001-buyer"]["points"] == 2
    assert by_id["001-buyer"]["value"] == 1
    assert by_id["001-buyer"]["meta"]["login_contact_id"] == "003-buyer"
    assert by_id["001-both"]["points"] == 2
    assert by_id["001-both"]["value"] == 2
    assert by_id["001-both"]["meta"]["login_contact_id"] == "003-champion"
    assert by_id["001-end-user"]["points"] is None
    assert by_id["001-end-user"]["error"]


def test_champion_login_fails_loud_when_salesforce_returns_no_rows() -> None:
    with pytest.raises(ChampionLoginGeneratorError, match="no Customer Entity"):
        get_champion_login_continuity(
            entities=_entities(),
            sponsor_rows=[],
            champion_contacts=[],
            as_of=date.fromisoformat("2026-09-25"),
        )


def test_generate_champion_login_api_is_authenticated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    monkeypatch.setattr(
        "src.healthscore_web.api.run_champion_login_snapshot",
        lambda **kwargs: {
            "ok": True,
            "generator": "get_champion_login_continuity",
            "scored": 1,
            "unscored": 0,
            "warnings": [],
            "readings": [],
        },
    )
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.post("/healthscore/api/generate/champion_login_continuity").status_code == 401
    _login(client)
    res = client.post("/healthscore/api/generate/champion_login_continuity")
    assert res.status_code == 200
    assert res.json()["generator"] == "get_champion_login_continuity"


def test_generate_champion_turnover_api_is_authenticated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    monkeypatch.setattr(
        "src.healthscore_web.api.run_champion_turnover_snapshot",
        lambda **kwargs: {
            "ok": True,
            "generator": "get_champion_turnover",
            "scored": 1,
            "unscored": 0,
            "warnings": [],
            "readings": [],
        },
    )
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.post("/healthscore/api/generate/champion_turnover").status_code == 401
    _login(client)
    res = client.post("/healthscore/api/generate/champion_turnover")
    assert res.status_code == 200
    assert res.json()["generator"] == "get_champion_turnover"


def test_roi_multiple_points_follow_framework_bands() -> None:
    assert roi_multiple_points(None) is None
    assert roi_multiple_points(7) == 6
    assert roi_multiple_points(6.9) == 4
    assert roi_multiple_points(5) == 4
    assert roi_multiple_points(3) == 2
    assert roi_multiple_points(2.99) == 0


def test_get_roi_multiple_divides_previous_period_savings_by_arr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    entities = [
        {**_entities()[0], "arr": 25},
        {"id": "001-no-arr", "name": "No ARR", "entity_name": "No ARR"},
    ]
    week_rows = [
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant A",
            "inventoryActionPreviousReportingPeriodSavings": json.dumps(
                {"endValue": 100, "empty": False}
            ),
        },
        {
            "delta": "week",
            "customer": "Acme Parent",
            "entity": "Acme Entity",
            "factoryName": "Plant B",
            "inventoryActionPreviousReportingPeriodSavings": json.dumps(
                {"endValue": 50, "empty": False}
            ),
        },
        {
            "delta": "week",
            "customer": "Other",
            "entity": "No ARR",
            "factoryName": "Plant C",
            "inventoryActionPreviousReportingPeriodSavings": json.dumps(
                {"endValue": 10, "empty": False}
            ),
        },
    ]
    result = get_roi_multiple(
        entities=entities,
        week_rows=week_rows,
        as_of=date.fromisoformat("2026-09-25"),
        persist=False,
    )
    by_id = {row["entity_id"]: row for row in result["readings"]}
    assert result["scored"] == 1
    assert result["unscored"] == 1
    assert by_id["001-active"]["value"] == 6
    assert by_id["001-active"]["points"] == 4
    assert by_id["001-active"]["meta"]["savings"] == 150
    assert by_id["001-no-arr"]["points"] is None
    assert by_id["001-no-arr"]["error"] == "Salesforce ARR is missing"


def test_summit_attendance_uses_registrants_before_and_attendees_after() -> None:
    assert is_summit_campaign("Event - Manufacturing Excellence Summit 2026", "Event")
    assert not is_summit_campaign("EV - HO - Manufacturing Excellence Summit - Welcome Dinner", "Event")
    assert member_counts("Registered", mode="registered")
    assert member_counts("Attended - Alex", mode="attended")
    assert not member_counts("Registered", mode="attended")
    entities = [
        {"id": "001-site", "name": "Site", "parent_id": "001-parent"},
        {"id": "001-other", "name": "Other site", "parent_id": "001-other-parent"},
    ]
    campaigns = [
        {
            "Id": "701-future",
            "Name": "Event - Manufacturing Excellence Summit 2027",
            "Type": "Event",
            "StartDate": "2027-03-02",
        },
        {
            "Id": "701-dinner",
            "Name": "Manufacturing Excellence Summit - Welcome Dinner",
            "Type": "Event",
            "StartDate": "2026-03-01",
        },
    ]
    members = [
        {
            "CampaignId": "701-future",
            "Status": "Registered",
            "account_id": "001-parent",
            "account_type": "Customer",
        },
        {
            "CampaignId": "701-dinner",
            "Status": "Attended",
            "account_id": "001-other",
            "account_type": "Customer Entity",
        },
    ]
    before = get_summit_attendance(
        entities=entities,
        campaigns=campaigns,
        members=members,
        as_of=date.fromisoformat("2026-09-27"),
        persist=False,
    )
    by_id = {row["entity_id"]: row for row in before["readings"]}
    assert by_id["001-site"]["points"] == 1
    assert by_id["001-other"]["points"] == 0
    assert before["readings"][0]["meta"]["campaigns"][0]["mode"] == "registered"

    past = get_summit_attendance(
        entities=entities,
        campaigns=[
            {
                "Id": "701-past",
                "Name": "Event - Manufacturing Excellence Summit 2026",
                "Type": "Event",
                "StartDate": "2026-03-02",
            }
        ],
        members=[
            {
                "CampaignId": "701-past",
                "Status": "Registered",
                "account_id": "001-site",
                "account_type": "Customer Entity",
            },
            {
                "CampaignId": "701-past",
                "Status": "Attended",
                "account_id": "001-other",
                "account_type": "Customer Entity",
            },
        ],
        as_of=date.fromisoformat("2026-09-27"),
        persist=False,
    )
    past_by_id = {row["entity_id"]: row for row in past["readings"]}
    assert past_by_id["001-site"]["points"] == 0
    assert past_by_id["001-other"]["points"] == 1
    assert past["warnings"] == []

    stale = get_summit_attendance(
        entities=entities[:1],
        campaigns=[
            {
                "Id": "701-past",
                "Name": "Event - Manufacturing Excellence Summit 2026",
                "Type": "Event",
                "StartDate": "2026-03-02",
            }
        ],
        members=[
            {
                "CampaignId": "701-past",
                "Status": "Registered",
                "account_id": "001-site",
                "account_type": "Customer Entity",
            }
        ],
        as_of=date.fromisoformat("2026-09-27"),
        persist=False,
    )
    assert stale["readings"][0]["points"] == 0
    assert "no Attended members" in stale["warnings"][0]["warning"]


def test_summit_attendance_fails_loud_without_a_campaign() -> None:
    with pytest.raises(SummitAttendanceGeneratorError, match="no Manufacturing Excellence Summit"):
        get_summit_attendance(
            entities=[{"id": "001-site", "name": "Site", "parent_id": None}],
            campaigns=[],
            members=[],
            as_of=date.fromisoformat("2026-09-27"),
        )

