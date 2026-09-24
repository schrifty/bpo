"""KPI web situation digest (week/month compare) and Claude briefing API."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml
from starlette.testclient import TestClient

from src.kpi_observation import KPIObservation
from src.kpi_owners import reset_for_tests
from src.kpi_store import (
    GRAIN_DAILY,
    GRAIN_MONTH,
    GRAIN_WEEKLY,
    connect,
    stored_kpi_from_observation,
    upsert_kpi,
)
from src.kpi_web.app import create_app
from src.kpi_web.settings import load_kpi_web_settings
from src.kpi_web.situation import (
    KpiSituationError,
    build_situation_digest,
    compare_series,
    generate_situation_analysis,
    period_as_date,
    _Point,
)
from tests.test_kpi_web_api import _OWNERS_YAML, _login


@pytest.fixture(autouse=True)
def _clear_owners_cache() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


def test_period_as_date_by_grain() -> None:
    assert period_as_date(GRAIN_DAILY, "2026-09-24") == date(2026, 9, 24)
    assert period_as_date(GRAIN_WEEKLY, "2026-W37") == date.fromisocalendar(2026, 37, 1)
    assert period_as_date(GRAIN_MONTH, "2026-08") == date(2026, 8, 31)


def test_daily_month_end_history_has_no_week_ago() -> None:
    points = [
        _Point(date(2026, 9, 24), 10.0, "2026-09-24"),
        _Point(date(2026, 8, 31), 8.0, "2026-08-31"),
        _Point(date(2026, 7, 31), 7.0, "2026-07-31"),
    ]
    got = compare_series(points, grain=GRAIN_DAILY, as_of=date(2026, 9, 24))
    assert got["current"]["value"] == 10.0
    assert got["week_ago"] is None
    assert got["month_ago"]["period_key"] == "2026-08-31"
    assert got["month_change_pct"] == 25.0


def test_weekly_series_picks_prior_week_and_month() -> None:
    points = [
        _Point(date(2026, 9, 21), 5.0, "2026-W39"),
        _Point(date(2026, 9, 14), 4.0, "2026-W38"),
        _Point(date(2026, 8, 24), 2.0, "2026-W35"),
    ]
    got = compare_series(points, grain=GRAIN_WEEKLY, as_of=date(2026, 9, 24))
    assert got["week_ago"]["period_key"] == "2026-W38"
    assert got["month_ago"]["period_key"] == "2026-W35"


def test_monthly_skips_week_ago() -> None:
    points = [
        _Point(date(2026, 8, 31), 100.0, "2026-08"),
        _Point(date(2026, 7, 31), 80.0, "2026-07"),
    ]
    got = compare_series(points, grain=GRAIN_MONTH, as_of=date(2026, 9, 24))
    assert got["week_ago"] is None
    assert got["month_ago"]["value"] == 80.0
    assert "monthly/quarterly" in (got["week_note"] or "")


def test_digest_only_includes_generator_kpis_with_readings(tmp_path: Path) -> None:
    registry = yaml.safe_load(
        """
metrics:
  "Wired Daily":
    metric-generator: get_wired
    grain: daily
    tags: [engineering]
    target: 10
    direction: higher
  "Pending":
    metric-generator: null
    grain: daily
"""
    )
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Wired Daily",
            grain=GRAIN_DAILY,
            period_key="2026-09-24",
            observation=KPIObservation(value=12.0, origin="live"),
            generator="get_wired",
        ),
    )
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Wired Daily",
            grain=GRAIN_DAILY,
            period_key="2026-08-31",
            observation=KPIObservation(value=9.0, origin="live"),
            generator="get_wired",
        ),
    )
    digest = build_situation_digest(conn, registry, as_of=date(2026, 9, 24))
    conn.close()
    assert digest["kpi_count"] == 1
    assert digest["skipped_no_generator"] == 1
    row = digest["kpis"][0]
    assert row["name"] == "Wired Daily"
    assert row["target_outcome"] == "made"
    assert row["month_ago"]["value"] == 9.0


def test_generate_situation_fails_loud_without_readings() -> None:
    with pytest.raises(KpiSituationError, match="no stored generator"):
        generate_situation_analysis({"kpi_count": 0, "kpis": []})


def test_generate_situation_uses_invoke() -> None:
    text = generate_situation_analysis(
        {"kpi_count": 1, "kpis": [{"name": "X"}]},
        invoke=lambda **_: "Support tickets are down vs last month.",
    )
    assert "Support tickets" in text


def test_generate_situation_empty_llm_fails() -> None:
    with pytest.raises(KpiSituationError, match="empty"):
        generate_situation_analysis(
            {"kpi_count": 1, "kpis": [{"name": "X"}]},
            invoke=lambda **_: "  ",
        )


def test_situation_api_returns_claude_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = tmp_path / "kpi_owners.yaml"
    owners.write_text(_OWNERS_YAML.strip() + "\n", encoding="utf-8")
    registry = tmp_path / "my-metrics.yaml"
    registry.write_text(
        """
metrics:
  "Wired Daily":
    description: d
    owner: marc.schriftman@leandna.com
    metric-generator: get_wired
    grain: daily
    tags: [engineering]
""",
        encoding="utf-8",
    )
    cache_root = tmp_path / "cache"
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", cache_root)
    store = cache_root / "kpi" / "observations.sqlite"
    conn = connect(store)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Wired Daily",
            grain=GRAIN_DAILY,
            period_key="2026-09-24",
            observation=KPIObservation(value=12.0, origin="live"),
            generator="get_wired",
        ),
    )
    conn.close()
    monkeypatch.setattr(
        "src.kpi_web.api.generate_situation_analysis",
        lambda digest, **_: "Month-end spend is up versus August.",
    )
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "marc.schriftman@leandna.com",
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
        "CORTEX_KPI_WEB_SKIP_S3": "true",
    }
    client = TestClient(create_app(settings=load_kpi_web_settings(environ=env)))
    _login(client)
    res = client.get("/api/situation")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is True
    assert "Month-end spend" in body["analysis"]
    assert body["kpi_count"] == 1


def test_situation_api_fails_loud_on_claude_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = tmp_path / "kpi_owners.yaml"
    owners.write_text(_OWNERS_YAML.strip() + "\n", encoding="utf-8")
    registry = tmp_path / "my-metrics.yaml"
    registry.write_text(
        """
metrics:
  "Wired Daily":
    owner: marc.schriftman@leandna.com
    metric-generator: get_wired
    grain: daily
""",
        encoding="utf-8",
    )
    cache_root = tmp_path / "cache"
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", cache_root)
    store = cache_root / "kpi" / "observations.sqlite"
    conn = connect(store)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Wired Daily",
            grain=GRAIN_DAILY,
            period_key="2026-09-24",
            observation=KPIObservation(value=1.0, origin="live"),
            generator="get_wired",
        ),
    )
    conn.close()

    def _boom(digest, **_):
        raise KpiSituationError("ANTHROPIC_API_KEY is not set")

    monkeypatch.setattr("src.kpi_web.api.generate_situation_analysis", _boom)
    env = {
        "CORTEX_KPI_WEB_ALLOW_DEV_AUTH": "true",
        "CORTEX_KPI_WEB_DEV_USER": "marc.schriftman@leandna.com",
        "CORTEX_KPI_WEB_BASE_URL": "http://testserver",
        "CORTEX_KPI_WEB_SESSION_SECRET": "test-session-secret",
        "CORTEX_KPI_WEB_ALLOWED_DOMAINS": "leandna.com",
        "CORTEX_KPI_WEB_REGISTRY": str(registry),
        "CORTEX_KPI_WEB_OWNERS": str(owners),
        "CORTEX_KPI_WEB_SKIP_S3": "true",
    }
    client = TestClient(create_app(settings=load_kpi_web_settings(environ=env)))
    _login(client)
    res = client.get("/api/situation")
    assert res.status_code == 500
    assert "ANTHROPIC_API_KEY" in res.json()["error"]
    assert res.json()["ok"] is False
