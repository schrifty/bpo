"""Run Health Score generators into the separate Health Score SQLite store."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Sequence

from src.healthscore_web.usage_level import (
    METRIC_NAME,
    UsageLevelGeneratorError,
    get_usage_level,
)


def run_usage_level_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    week_rows: list[dict[str, Any]] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        from src.healthscore_web.api import _active_salesforce_entities

        try:
            entities = _active_salesforce_entities()
        except Exception as exc:  # noqa: BLE001
            raise UsageLevelGeneratorError(
                f"Salesforce Customer Entity inventory failed: {exc}"
            ) from exc
    return get_usage_level(
        entities=entities,
        week_rows=week_rows,
        as_of=as_of,
        persist=not dry_run,
    )


def run_healthscore_snapshot_cli(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "cortex healthscore-snapshot",
) -> int:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Run Health Score generators into the Health Score store. "
            "Does not write KPI observations."
        ),
    )
    parser.add_argument(
        "--component",
        default=METRIC_NAME,
        help="Health Score component generator to run (default: usage_level)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", dest="as_of", default=None, help="YYYY-MM-DD fallback period")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.component != METRIC_NAME:
        print(
            f"error: unknown Health Score component {args.component!r} "
            f"(supported: {METRIC_NAME})",
            file=sys.stderr,
        )
        return 2
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        result = run_usage_level_snapshot(dry_run=args.dry_run, as_of=as_of)
    except UsageLevelGeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str, indent=2))
    return 0
