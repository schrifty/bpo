"""Nightly Health Score run.

Input generators run only when their grain rolls onto a new period: daily
every night, weekly on Monday, monthly on the 1st, and quarterly on the first
day of January, April, July, and October. Inputs with no generator are left
alone.

After the due generators, every active Customer Entity gets a daily health
score from the inputs that already have readings. Unscored inputs stay out of
both the sum and the divisor. Those daily scores are what the report sparkline
plots.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from typing import Any

from src.healthscore_web.api import (
    _apply_override_flags,
    _raised_override_names,
    _score_numbers,
)
from src.healthscore_web.framework import load_framework
from src.healthscore_web.snapshot import SUPPORTED, run_healthscore_snapshot
from src.healthscore_web.store import connect, period_key_for, upsert_reading

logger = logging.getLogger(__name__)

METRIC_NAME = "site_healthscore"
GRAIN = "daily"
GENERATOR_NAME = "site_healthscore"
TAGS = ["customer-success", "healthscore"]
SPARKLINE_DAYS = 90


class HealthscoreNightlyError(RuntimeError):
    pass


def grain_is_due(grain: str | None, as_of: date) -> bool:
    """True when ``as_of`` is the day that grain's period key changes."""
    name = str(grain or "").strip().lower()
    if name == "daily":
        return True
    if name == "weekly":
        return as_of.weekday() == 0
    if name == "monthly":
        return as_of.day == 1
    if name == "quarterly":
        return as_of.day == 1 and as_of.month in (1, 4, 7, 10)
    return False


def components_due(as_of: date, framework: dict[str, Any] | None = None) -> list[str]:
    """Input keys whose generator should run on ``as_of``.

    One generator is listed once. ``champion_turnover`` also writes the
    departure flag, so that second key is not run again.
    """
    payload = framework if framework is not None else load_framework()
    due: list[str] = []
    seen_generators: set[str] = set()
    for row in payload.get("inputs") or []:
        if row.get("deactivated"):
            continue
        generator = str(row.get("metric-generator") or "").strip()
        key = str(row.get("key") or "").strip()
        if not generator or not key or generator in seen_generators:
            continue
        if key not in SUPPORTED:
            continue
        if not grain_is_due(row.get("grain"), as_of):
            continue
        seen_generators.add(generator)
        due.append(key)
    return due


def site_healthscore_value(numbers: dict[str, Any]) -> float | None:
    """0–100 score on scored inputs. None when nothing is scored.

    A raised override flag is 0. Unscored weight is not in the divisor.
    """
    if numbers.get("score_zeroed"):
        return 0.0
    covered = float(numbers.get("covered_weight") or 0)
    if covered <= 0:
        return None
    return round(float(numbers.get("contribution") or 0) / covered * 100, 2)


def persist_site_healthscores(
    *,
    entities: list[dict[str, Any]],
    as_of: date | None = None,
    framework: dict[str, Any] | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Write one daily health score per active entity that has scored inputs."""
    today = as_of or date.today()
    payload = framework if framework is not None else load_framework()
    conn = connect()
    try:
        from src.healthscore_web.store import latest_effective_by_entity

        points, flag_values = latest_effective_by_entity(conn)
        written = 0
        unscored = 0
        for entity in entities:
            entity_id = str(entity["id"])
            numbers = _apply_override_flags(
                _score_numbers(payload, points.get(entity_id, {})),
                _raised_override_names(payload, flag_values.get(entity_id, {})),
            )
            value = site_healthscore_value(numbers)
            if value is None:
                unscored += 1
                continue
            if not persist:
                written += 1
                continue
            upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                period_key=period_key_for(GRAIN, today),
                value=value,
                points=value,
                generator=GENERATOR_NAME,
                tags=TAGS,
                meta={
                    "covered_weight": numbers.get("covered_weight"),
                    "coverage_pct": numbers.get("coverage_pct"),
                    "score_zeroed": bool(numbers.get("score_zeroed")),
                },
            )
            written += 1
    finally:
        conn.close()
    return {
        "ok": True,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "as_of": today.isoformat(),
        "period_key": period_key_for(GRAIN, today),
        "written": written,
        "unscored": unscored,
        "dry_run": not persist,
    }


def sparkline_since(as_of: date | None = None, days: int = SPARKLINE_DAYS) -> str:
    today = as_of or date.today()
    return (today - timedelta(days=max(1, days) - 1)).isoformat()


def run_healthscore_nightly(
    *,
    as_of: date | None = None,
    dry_run: bool = False,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run due input generators, then store a daily health score for every site."""
    today = as_of or date.today()
    if entities is None:
        from src.healthscore_web.snapshot import _active_entities

        entities = _active_entities()
    due = components_due(today)
    ran: dict[str, Any] = {}
    failures: list[str] = []
    for key in due:
        try:
            ran[key] = run_healthscore_snapshot(
                component=key, dry_run=dry_run, as_of=today, entities=entities
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Health Score nightly generator %s failed", key)
            failures.append(f"{key}: {exc}")
    scores = persist_site_healthscores(entities=entities, as_of=today, persist=not dry_run)
    result = {
        "ok": not failures,
        "as_of": today.isoformat(),
        "due": due,
        "generators": {key: {"ok": payload.get("ok")} for key, payload in ran.items()},
        "failures": failures,
        "site_healthscore": scores,
    }
    if failures:
        raise HealthscoreNightlyError(
            "Health Score nightly generator failures: " + "; ".join(failures)
        )
    return result


def run_healthscore_nightly_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cortex healthscore-nightly",
        description=(
            "Run Health Score generators whose grain is due, then store a daily "
            "health score for every active Customer Entity."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", dest="as_of", default=None, help="YYYY-MM-DD")
    args = parser.parse_args(list(argv) if argv is not None else None)
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        result = run_healthscore_nightly(as_of=as_of, dry_run=args.dry_run)
    except HealthscoreNightlyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str, indent=2))
    return 0
