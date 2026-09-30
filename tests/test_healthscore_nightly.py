"""Nightly Health Score grain gating and daily site scores."""

from datetime import date

import pytest

from src.healthscore_web.nightly import (
    components_due,
    grain_is_due,
    persist_site_healthscores,
    site_healthscore_value,
)
from src.healthscore_web.store import connect, observations_for_metric, upsert_reading


def test_grain_is_due_on_its_period_boundary() -> None:
    monday = date(2026, 9, 28)
    month_start = date(2026, 11, 1)
    quarter_start = date(2026, 10, 1)
    midweek = date(2026, 9, 30)
    assert grain_is_due("daily", midweek)
    assert grain_is_due("weekly", monday)
    assert not grain_is_due("weekly", midweek)
    assert grain_is_due("monthly", month_start)
    assert not grain_is_due("monthly", monday)
    assert grain_is_due("quarterly", quarter_start)
    assert not grain_is_due("quarterly", month_start)
    assert not grain_is_due(None, midweek)


def test_components_due_follow_grain_and_skip_inputs_without_a_generator() -> None:
    monday = components_due(date(2026, 9, 28))
    assert "usage_level" in monday
    assert "champion_turnover" in monday
    assert "champion_departure_external" not in monday
    assert "product_ideas" not in monday
    assert "customer_data_delivery" not in monday
    first = components_due(date(2026, 11, 1))
    assert "product_ideas" in first
    assert "call_sentiment" in first
    assert "usage_level" not in first
    assert "roi_multiple" not in first
    assert "roi_multiple" in components_due(date(2026, 10, 1))


def test_site_score_ignores_unscored_weight_and_stays_unscored_without_readings() -> None:
    assert site_healthscore_value({"contribution": 10, "covered_weight": 29}) == 34.48
    assert site_healthscore_value({"contribution": 10, "covered_weight": 29, "score_zeroed": True}) == 0
    assert site_healthscore_value({"contribution": 0, "covered_weight": 0}) is None


def test_daily_site_score_is_stored_only_when_an_input_is_scored(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    entities = [
        {"id": "001-scored", "name": "Scored Entity"},
        {"id": "001-open", "name": "Open Entity"},
    ]
    conn = connect()
    try:
        upsert_reading(
            conn,
            entity_id="001-scored",
            entity_name="Scored Entity",
            metric_name="usage_level",
            grain="weekly",
            as_of=date(2026, 9, 28),
            value=100,
            points=6,
            generator="get_usage_level",
        )
    finally:
        conn.close()
    result = persist_site_healthscores(entities=entities, as_of=date(2026, 9, 30))
    assert result["written"] == 1
    assert result["unscored"] == 1
    assert result["period_key"] == "2026-09-30"
    conn = connect()
    try:
        rows = observations_for_metric(conn, "001-scored", "site_healthscore")
        assert len(rows) == 1
        assert rows[0]["value"] == 100
        assert observations_for_metric(conn, "001-open", "site_healthscore") == []
    finally:
        conn.close()
