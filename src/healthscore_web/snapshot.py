"""Run Health Score generators into the separate Health Score SQLite store."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Sequence

from src.healthscore_web.usage_level import (
    METRIC_NAME as USAGE_LEVEL_METRIC,
    UsageLevelGeneratorError,
    get_usage_level,
)
from src.healthscore_web.usage_trend import (
    METRIC_NAME as USAGE_TREND_METRIC,
    UsageTrendGeneratorError,
    get_usage_trend,
)

SUPPORTED = (USAGE_LEVEL_METRIC, USAGE_TREND_METRIC, "all")


def _active_entities() -> list[dict[str, Any]]:
    from src.healthscore_web.api import _active_salesforce_entities

    try:
        return _active_salesforce_entities()
    except Exception as exc:  # noqa: BLE001
        raise UsageLevelGeneratorError(
            f"Salesforce Customer Entity inventory failed: {exc}"
        ) from exc


def run_usage_level_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    week_rows: list[dict[str, Any]] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_usage_level(
        entities=entities,
        week_rows=week_rows,
        as_of=as_of,
        persist=not dry_run,
    )


def run_usage_trend_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_usage_trend(
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
    )


def backfill_usage_level_history(
    *,
    weeks: int,
    dry_run: bool = False,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Persist usage_level from the latest CS Report workbook in each ISO week."""
    from src.cs_report_client import (
        latest_csr_file_per_iso_week,
        list_csr_report_files,
        load_csr_report_by_file_id,
    )

    if weeks < 1:
        raise UsageLevelGeneratorError("history weeks must be at least 1")
    if entities is None:
        entities = _active_entities()
    files = list_csr_report_files()
    snapshots = latest_csr_file_per_iso_week(files, weeks=weeks)
    if not snapshots:
        raise UsageLevelGeneratorError(
            "no CS Report workbooks found to backfill usage_level"
        )
    if len(snapshots) < weeks:
        raise UsageLevelGeneratorError(
            f"CS Report folder has {len(snapshots)} ISO weeks with workbooks, "
            f"need {weeks} for usage_level history"
        )
    loaded: list[dict[str, Any]] = []
    for snap in reversed(snapshots):
        rows = load_csr_report_by_file_id(str(snap["id"]), name=str(snap["name"]))
        result = get_usage_level(
            entities=entities,
            week_rows=rows,
            as_of=snap["as_of"],
            period_as_of=snap["as_of"],
            persist=not dry_run,
        )
        loaded.append(
            {
                "period_key": snap["period_key"],
                "as_of": snap["as_of"].isoformat(),
                "file": snap["name"],
                "scored": result.get("scored"),
                "unmatched": result.get("unmatched"),
                "ok": result.get("ok"),
            }
        )
    return {
        "ok": True,
        "generator": "get_usage_level",
        "weeks_requested": weeks,
        "weeks_loaded": len(loaded),
        "dry_run": dry_run,
        "weeks": loaded,
    }


def run_healthscore_snapshot(
    *,
    component: str = "all",
    dry_run: bool = False,
    as_of: date | None = None,
) -> dict[str, Any]:
    name = str(component or "all").strip().lower()
    if name not in SUPPORTED:
        raise ValueError(
            f"unknown Health Score component {component!r} (supported: {', '.join(SUPPORTED)})"
        )
    entities = _active_entities()
    results: dict[str, Any] = {"ok": True, "component": name}
    if name in (USAGE_LEVEL_METRIC, "all"):
        results[USAGE_LEVEL_METRIC] = run_usage_level_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (USAGE_TREND_METRIC, "all"):
        results[USAGE_TREND_METRIC] = run_usage_trend_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    return results


def run_healthscore_snapshot_cli(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "cortex healthscore-snapshot",
) -> int:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Run Health Score generators into the Health Score store. "
            "Does not write KPI observations. Default runs usage_level then "
            "usage_trend so the trend sees this week's stored usage percent."
        ),
    )
    parser.add_argument(
        "--component",
        default="all",
        help="usage_level, usage_trend, or all (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", dest="as_of", default=None, help="YYYY-MM-DD fallback period")
    parser.add_argument(
        "--history-weeks",
        type=int,
        default=None,
        help="Backfill usage_level from one CS Report workbook per ISO week (newest weeks)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        if args.history_weeks:
            result: dict[str, Any] = backfill_usage_level_history(
                weeks=args.history_weeks, dry_run=args.dry_run
            )
            name = str(args.component or "all").strip().lower()
            if name in (USAGE_TREND_METRIC, "all"):
                result[USAGE_TREND_METRIC] = run_usage_trend_snapshot(
                    dry_run=args.dry_run, as_of=as_of
                )
        else:
            result = run_healthscore_snapshot(
                component=args.component, dry_run=args.dry_run, as_of=as_of
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (UsageLevelGeneratorError, UsageTrendGeneratorError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str, indent=2))
    return 0
