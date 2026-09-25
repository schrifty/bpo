"""CSR generator for Health Score ``usage_trend`` (velocity).

Uses the generated CSR usage percent already stored as weekly ``usage_level``
readings. Percent change is from the oldest usable week in a trailing 13-week
window to the newest. Fewer than two numeric weeks stays unscored.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from src.healthscore_web.store import (
    connect,
    observations_for_metric,
    period_key_for,
    upsert_reading,
)
from src.healthscore_web.usage_level import METRIC_NAME as USAGE_LEVEL_METRIC

logger = logging.getLogger(__name__)

METRIC_NAME = "usage_trend"
GRAIN = "weekly"
GENERATOR_NAME = "get_usage_trend"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "adoption"]
WINDOW_WEEKS = 13
FLAT_BAND_PCT = 10.0
SOURCE_METRIC = USAGE_LEVEL_METRIC


class UsageTrendGeneratorError(RuntimeError):
    pass


def usage_trend_points(pct_change: float | None) -> int | None:
    """Map percent change onto the draft bands. None means unscored."""
    if pct_change is None:
        return None
    if pct_change > FLAT_BAND_PCT:
        return 5
    if pct_change < -FLAT_BAND_PCT:
        return 0
    return 3


def percent_change(baseline: float, current: float) -> float:
    """Percent change of usage percent. A rise from 0 is recorded as +100."""
    if baseline == 0:
        return 0.0 if current == 0 else 100.0
    return round((current - baseline) / baseline * 100.0, 4)


def _numeric_value(row: dict[str, Any]) -> float | None:
    raw = row.get("value")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _window_rows(
    rows: list[dict[str, Any]], *, as_of: date
) -> list[dict[str, Any]]:
    earliest = period_key_for(GRAIN, as_of - timedelta(weeks=WINDOW_WEEKS - 1))
    latest = period_key_for(GRAIN, as_of)
    return [
        row
        for row in rows
        if earliest <= str(row.get("period_key") or "") <= latest
    ]


def get_usage_trend(
    *,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score Usage trend from stored weekly Usage Level percents."""
    if not entities:
        raise UsageTrendGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate usage_trend"
        )
    today = as_of or date.today()
    current_key = period_key_for(GRAIN, today)
    insufficient: list[dict[str, str]] = []
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect()
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            history = _window_rows(
                observations_for_metric(conn, entity_id, SOURCE_METRIC),
                as_of=today,
            )
            series = [
                (str(row["period_key"]), _numeric_value(row))
                for row in history
                if _numeric_value(row) is not None
            ]
            error = None
            value: float | None = None
            points: int | None = None
            meta: dict[str, Any] = {
                "source_metric": SOURCE_METRIC,
                "window_weeks": WINDOW_WEEKS,
                "flat_band_pct": FLAT_BAND_PCT,
                "owner": OWNER_EMAIL,
            }
            if len(series) < 2:
                error = (
                    "insufficient usage_level history for usage_trend "
                    f"(need 2 weekly percents in the trailing {WINDOW_WEEKS} weeks, "
                    f"have {len(series)})"
                )
                logger.warning(
                    "Health Score usage_trend: %s for Salesforce entity %s (%s)",
                    error,
                    entity.get("name"),
                    entity_id,
                )
                insufficient.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
            else:
                baseline_key, baseline = series[0]
                current_src_key, current = series[-1]
                value = percent_change(baseline, current)
                points = usage_trend_points(value)
                meta.update(
                    {
                        "baseline_period": baseline_key,
                        "baseline_usage_pct": baseline,
                        "current_period": current_src_key,
                        "current_usage_pct": current,
                        "weeks_used": len(series),
                    }
                )
                if baseline == 0 and current > 0:
                    meta["baseline_zero"] = True
            payload = {
                "entity_id": entity_id,
                "entity_name": entity.get("name"),
                "metric_name": METRIC_NAME,
                "grain": GRAIN,
                "period_key": current_key,
                "as_of": today.isoformat(),
                "value": value,
                "points": points,
                "generator": generator,
                "tags": list(TAGS),
                "meta": meta,
                "error": error,
            }
            if not persist:
                readings.append(payload)
                continue
            saved = upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                value=value,
                points=None if points is None else float(points),
                generator=generator,
                tags=TAGS,
                meta=meta,
                error=error,
            )
            if saved.get("overridden"):
                overridden.append(entity_id)
            readings.append(saved)
    finally:
        conn.close()
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "scored": sum(1 for row in readings if row.get("points") is not None),
        "insufficient": len(insufficient),
        "overridden": len(overridden),
        "warnings": insufficient,
        "readings": readings,
    }
