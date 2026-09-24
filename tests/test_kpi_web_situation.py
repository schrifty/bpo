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
    iter_situation_analysis,
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


def test_period_key_wins_over_fresh_as_of() -> None:
    # Rows captured before the period-key fix carry a stale key and a fresh as_of;
    # the key names the period, so it must decide the row's age.
    assert period_as_date(GRAIN_MONTH, "2024-09", "2026-09-20") == date(2024, 9, 30)
    assert period_as_date(GRAIN_DAILY, "not-a-date", "2026-09-20") == date(2026, 9, 20)


def test_stale_period_key_rows_do_not_become_current(tmp_path: Path) -> None:
    registry = yaml.safe_load(
        """
metrics:
  "AI Spend %":
    metric-generator: get_ai_spend_pct
    grain: monthly
    tags: [engineering, ai]
    target: 5
    direction: higher
"""
    )
    conn = connect(tmp_path / "kpi.sqlite")
    for key, value in (("2026-08", 2.72), ("2026-07", 3.07)):
        upsert_kpi(
            conn,
            stored_kpi_from_observation(
                metric_name="AI Spend %",
                grain=GRAIN_MONTH,
                period_key=key,
                observation=KPIObservation(value=value, origin="live"),
                generator="get_ai_spend_pct",
            ),
        )
    # Simulate a pre-fix row: stale key, recent as_of, zero value.
    conn.execute(
        "UPDATE kpi_observation SET as_of = '2026-09-20' WHERE period_key = '2026-07'"
    )
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="AI Spend %",
            grain=GRAIN_MONTH,
            period_key="2024-09",
            observation=KPIObservation(value=0.0, origin="live"),
            generator="get_ai_spend_pct",
        ),
    )
    conn.execute(
        "UPDATE kpi_observation SET as_of = '2026-09-20' WHERE period_key = '2024-09'"
    )
    conn.commit()
    digest = build_situation_digest(conn, registry, as_of=date(2026, 9, 24))
    conn.close()
    row = digest["kpis"][0]
    assert row["current"]["period_key"] == "2026-08"
    assert row["month_ago"]["period_key"] == "2026-07"


def test_strip_markdown_chrome() -> None:
    from src.kpi_web.situation import strip_markdown_chrome

    raw = "**Your team**\n\n- SLA at **84.9%**\n\n---\n\n## Company\nSeven made."
    assert strip_markdown_chrome(raw) == "Your team\n\n- SLA at 84.9%\n\nCompany\nSeven made."


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
    assert row["team"] == "Engineering"
    assert row["target_outcome"] == "made"
    assert row["month_ago"]["value"] == 9.0
    assert row["month_move_is_good"] is True
    assert digest["viewer"] is None
    company = digest["company"]
    assert company["targets_made"] == ["Wired Daily"]
    assert company["biggest_improvements_month"][0]["name"] == "Wired Daily"
    assert company["by_team"]["Engineering"]["improving_month"] == 1
    assert digest["pending_kpis_by_team"] == {"Other": ["Pending"]}


def test_digest_marks_viewer_team_and_owned_kpis(tmp_path: Path) -> None:
    registry = yaml.safe_load(
        """
metrics:
  "Open HELP":
    owner: lead.support@leandna.com
    metric-generator: get_open_help
    grain: daily
    tags: [support]
    target: 50
    direction: lower
    mgmt_guidance: Triage anything older than 30 days every Monday.
  "PRs Merged":
    owner: marc.schriftman@leandna.com
    metric-generator: get_prs_merged
    grain: monthly
    tags: [engineering]
"""
    )
    conn = connect(tmp_path / "kpi.sqlite")
    for name, grain, key, value in (
        ("Open HELP", GRAIN_DAILY, "2026-09-24", 64.0),
        ("PRs Merged", GRAIN_MONTH, "2026-08", 300.0),
    ):
        upsert_kpi(
            conn,
            stored_kpi_from_observation(
                metric_name=name,
                grain=grain,
                period_key=key,
                observation=KPIObservation(value=value, origin="live"),
                generator="g",
            ),
        )
    digest = build_situation_digest(
        conn,
        registry,
        as_of=date(2026, 9, 24),
        viewer={
            "email": "Lead.Support@leandna.com",
            "name": "Lead",
            "is_catalog_admin": False,
            "packs": ["support"],
        },
    )
    conn.close()
    assert digest["viewer"]["teams"] == ["Support"]
    assert digest["viewer"]["owned_kpis"] == ["Open HELP"]
    assert digest["viewer"]["role"] == "team lead"
    open_help = next(k for k in digest["kpis"] if k["name"] == "Open HELP")
    assert open_help["owned_by_viewer"] is True
    assert open_help["target_outcome"] == "missed"
    assert "Triage" in open_help["mgmt_guidance"]
    assert digest["company"]["targets_missed"] == ["Open HELP"]


def test_generate_situation_fails_loud_without_readings() -> None:
    with pytest.raises(KpiSituationError, match="no stored generator"):
        generate_situation_analysis({"kpi_count": 0, "kpis": []})


def test_generate_situation_uses_invoke_with_reader_prompt() -> None:
    seen: dict[str, str] = {}

    def fake(*, system: str, user: str) -> str:
        seen["system"] = system
        seen["user"] = user
        return "Support tickets are down vs last month."

    text = generate_situation_analysis(
        {"kpi_count": 1, "kpis": [{"name": "X"}], "viewer": {"teams": ["Support"]}},
        invoke=fake,
    )
    assert "Support tickets" in text
    assert "How is my team doing?" in seen["system"]
    assert "Watch this week" in seen["system"]
    assert "mgmt_guidance" in seen["system"]
    assert '"teams":["Support"]' in seen["user"]


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
    captured: dict[str, object] = {}

    def fake_generate(digest, **_):
        captured["digest"] = digest
        return "Month-end spend is up versus August."

    monkeypatch.setattr("src.kpi_web.api.generate_situation_analysis", fake_generate)
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
    viewer = captured["digest"]["viewer"]  # type: ignore[index]
    assert viewer["email"] == "marc.schriftman@leandna.com"
    assert viewer["role"].startswith("catalog admin")
    assert viewer["owned_kpis"] == ["Wired Daily"]


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


def test_iter_situation_analysis_cleans_markdown_across_chunks() -> None:
    # Markdown arrives split mid-token, so cleaning has to buffer whole lines.
    chunks = ["# Your te", "am\n\n**Supp", "ort** is up.\n---\nDone."]
    out = "".join(
        iter_situation_analysis(
            {"kpi_count": 1, "kpis": [{"name": "X"}]},
            stream=lambda **_: iter(chunks),
        )
    )
    assert out == "Your team\n\nSupport is up.\nDone."


def test_iter_situation_analysis_empty_stream_fails() -> None:
    with pytest.raises(KpiSituationError, match="empty"):
        list(
            iter_situation_analysis(
                {"kpi_count": 1, "kpis": [{"name": "X"}]},
                stream=lambda **_: iter(["   ", "\n"]),
            )
        )


def test_iter_situation_analysis_fails_loud_without_readings() -> None:
    with pytest.raises(KpiSituationError, match="no stored generator"):
        list(iter_situation_analysis({"kpi_count": 0, "kpis": []}))


def _situation_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
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
    conn = connect(cache_root / "kpi" / "observations.sqlite")
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
    return client


def _ndjson(text: str) -> list[dict]:
    import json as _json

    return [_json.loads(line) for line in text.splitlines() if line.strip()]


def test_situation_stream_api_emits_ndjson_deltas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_iter(digest, **_):
        yield "Your team\n"
        yield "Support is up.\n"

    monkeypatch.setattr("src.kpi_web.api.iter_situation_analysis", fake_iter)
    res = _situation_client(tmp_path, monkeypatch).get("/api/situation/stream")
    assert res.status_code == 200, res.text
    events = _ndjson(res.text)
    assert events[0]["type"] == "start"
    assert events[0]["kpi_count"] == 1
    assert "".join(e["text"] for e in events if e["type"] == "delta") == (
        "Your team\nSupport is up.\n"
    )
    assert events[-1]["type"] == "done"


def test_situation_stream_api_reports_error_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The 200 header is already sent when Claude dies, so the failure has to
    # ride in-band rather than silently truncating the briefing.
    def fake_iter(digest, **_):
        yield "Your team\n"
        raise KpiSituationError("Claude KPI situation stream failed: boom")

    monkeypatch.setattr("src.kpi_web.api.iter_situation_analysis", fake_iter)
    res = _situation_client(tmp_path, monkeypatch).get("/api/situation/stream")
    assert res.status_code == 200
    events = _ndjson(res.text)
    assert events[-1]["type"] == "error"
    assert "boom" in events[-1]["error"]
    assert not any(e["type"] == "done" for e in events)


def test_situation_stream_api_fails_loud_when_store_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _situation_client(tmp_path, monkeypatch)
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path / "missing-cache")
    res = client.get("/api/situation/stream")
    assert res.status_code == 500
    assert res.json()["ok"] is False
