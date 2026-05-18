#!/usr/bin/env python3
"""Per-team development cycle time from selected Jira scrum boards.

Default boards: **44** (LEAN), **36**, **46**, **322** (CUSTOMER). Cycle time is
calendar days in **In Progress** (status category ``indeterminate``) before first
**Done**, computed from issue changelogs for tickets resolved in the trailing window.

Includes **SUT** and **Sub-task** (implementation work). Excludes **Epic**,
**Hypercare**, **Data Sync Escalation**, **Data Access**, and **Request for
Information** by default. Drops upper-tail cycle times above **mean + 4σ**
(``--no-outlier-filter`` to disable). Use ``--all-issue-types`` to stop type exclusions.

Requires ``JIRA_*`` in ``.env``.

Examples::

  get-dev-cycle-times
  get-dev-cycle-times --days 90 --format json
  get-dev-cycle-times --months 6
  get-dev-cycle-times --months 6 --format json --output cycle-times-6mo.json
  get-dev-cycle-times --board 44 --include-issues
  discover-dev-teams --project CUSTOMER
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.jira_client import get_shared_jira_client  # noqa: E402
from src.jira_cycle_time import DEV_CYCLE_TIME_BOARDS, get_dev_team_cycle_times  # noqa: E402


def _print_filter_legend(payload: dict[str, Any]) -> None:
    excluded = payload.get("excluded_issue_types") or []
    print(
        "(median days in In Progress / In Review before first Done; from issue changelogs)"
    )
    if excluded:
        print(f"Excluded types: {', '.join(excluded)}")
    sigma = payload.get("outlier_sigma")
    if sigma:
        print(f"Outlier trim: drop cycle time > mean + {sigma}σ (per board)")


def _print_history_brief(payload: dict[str, Any]) -> None:
    months = payload.get("months", 6)
    print(f"Development cycle time — last {months} months (by resolution month)")
    _print_filter_legend(payload)
    print()

    if payload.get("error"):
        print(f"Error: {payload['error']}")
        return

    for team in payload.get("teams") or []:
        if team.get("error"):
            print(f"## {team.get('team', '?')} — ERROR: {team['error']}")
            print()
            continue
        label = team.get("team") or team.get("board_name")
        trunc = " TRUNCATED" if team.get("truncated") else ""
        print(f"## {label} (board {team.get('board_id')}){trunc}")
        overall = team.get("overall") or {}
        if overall.get("median_days") is not None:
            print(
                f"   Overall ({team.get('window_days')}d window): "
                f"median {overall.get('median_days')}d  "
                f"mean {overall.get('mean_days')}d  "
                f"n={team.get('measured_total')}"
            )
        print(f"   {'Period':<10} {'Done':>6} {'Meas':>6} {'Median':>8} {'Mean':>8} {'P85':>8}")
        for row in team.get("history") or []:
            med = row.get("median_days")
            med_s = f"{med}d" if med is not None else "—"
            mean_s = f"{row.get('mean_days')}d" if row.get("mean_days") is not None else "—"
            p85_s = f"{row.get('p85_days')}d" if row.get("p85_days") is not None else "—"
            print(
                f"   {row.get('period', ''):<10} "
                f"{row.get('completed', 0):>6} "
                f"{row.get('measured', 0):>6} "
                f"{med_s:>8} "
                f"{mean_s:>8} "
                f"{p85_s:>8}"
            )
        print()


def _print_brief(payload: dict[str, Any]) -> None:
    if payload.get("mode") == "history":
        _print_history_brief(payload)
        return

    days = payload.get("window_days", 30)
    print(f"Development cycle time — resolved in trailing {days}d")
    _print_filter_legend(payload)
    print()

    if payload.get("error"):
        print(f"Error: {payload['error']}")
        return

    for team in payload.get("teams") or []:
        if team.get("error"):
            print(f"## {team.get('team', '?')} (board {team.get('board_id')}) — ERROR")
            print(f"   {team['error']}")
            print()
            continue
        label = team.get("team") or team.get("board_name")
        print(f"## {label} (board {team.get('board_id')}, {team.get('project_key')})")
        print(f"   {team.get('board_name')}")
        measured = team.get("measured", 0)
        completed = team.get("completed_in_window", 0)
        skipped = team.get("skipped_no_in_progress", 0)
        trunc = " (truncated)" if team.get("truncated") else ""
        print(
            f"   Completed: {completed}{trunc}  |  Measured: {measured}  |  "
            f"No in-progress segment: {skipped}"
        )
        med = team.get("median_days")
        mean = team.get("mean_days")
        p85 = team.get("p85_days")
        if med is not None:
            print(
                f"   Median: {med}d   Mean: {mean}d   P85: {p85}d   "
                f"Min: {team.get('min_days')}d   Max: {team.get('max_days')}d"
            )
        else:
            print("   No measurable cycle times in window")
        top = team.get("top_issues") or []
        if top:
            print("   Slowest:")
            for row in top[:5]:
                print(f"     {row.get('key')}: {row.get('cycle_days')}d")
        print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cycle time per development board (default: 44, 36, 46, 322).",
    )
    ap.add_argument("--days", type=int, default=30, metavar="N", help="Trailing window for snapshot mode (default: 30)")
    ap.add_argument(
        "--months",
        type=int,
        default=None,
        metavar="N",
        help="Monthly history for last N calendar months (e.g. 6); fetches once per board",
    )
    ap.add_argument(
        "--board",
        action="append",
        type=int,
        default=[],
        metavar="ID",
        help="Board id (repeatable; default: all DEV_CYCLE_TIME_BOARDS)",
    )
    ap.add_argument(
        "--max-issues",
        type=int,
        default=500,
        metavar="N",
        help="Max completed issues per board (default: 500)",
    )
    ap.add_argument("--workers", type=int, default=6, metavar="N", help="Parallel changelog fetches")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument(
        "--include-issues",
        action="store_true",
        help="Include per-issue cycle times in JSON snapshot mode (up to 50 per board)",
    )
    ap.add_argument(
        "--all-issue-types",
        action="store_true",
        help="Do not exclude Epic, Hypercare, etc. (SUT and Sub-task are always included)",
    )
    ap.add_argument(
        "--outlier-sigma",
        type=float,
        default=None,
        metavar="N",
        help="Drop cycle times above mean+N*std (default: 4; 0 or --no-outlier-filter disables)",
    )
    ap.add_argument(
        "--no-outlier-filter",
        action="store_true",
        help="Keep all measured cycle times (no σ trimming)",
    )
    ap.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Write JSON payload to FILE (stdout still prints brief unless --format json)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--timeout", type=float, default=60.0, metavar="SEC")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    try:
        jira = get_shared_jira_client()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    board_ids = ns.board if ns.board else None
    if ns.months is not None and ns.months < 1:
        print("--months must be >= 1", file=sys.stderr)
        return 1
    if ns.months is not None:
        print(
            f"Fetching up to {max(ns.max_issues, 2000)} issues/board for "
            f"{ns.months}-month history (changelog per issue; may take several minutes)…",
            file=sys.stderr,
        )
    try:
        payload = get_dev_team_cycle_times(
            jira,
            board_ids=board_ids,
            days=ns.days,
            months=ns.months,
            max_issues_per_board=ns.max_issues,
            workers=ns.workers,
            timeout=ns.timeout,
            include_all_issue_types=ns.all_issue_types,
            outlier_sigma=ns.outlier_sigma,
            disable_outlier_filter=ns.no_outlier_filter,
        )
    except Exception as e:
        print(f"Cycle time fetch failed: {e}", file=sys.stderr)
        return 1

    if not ns.include_issues:
        for team in payload.get("teams") or []:
            if isinstance(team, dict):
                team.pop("issues", None)

    if ns.output:
        out_path = Path(ns.output)
        out_path.write_text(
            json.dumps(payload, indent=2, default=str, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote {out_path}", file=sys.stderr)

    if ns.format == "json":
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    else:
        _print_brief(payload)
        if payload.get("mode") != "history":
            print("Configured boards:", file=sys.stderr)
            for b in DEV_CYCLE_TIME_BOARDS:
                print(f"  {b['board_id']}: {b['team_label']}", file=sys.stderr)

    if payload.get("error"):
        return 1
    if any(t.get("error") for t in payload.get("teams") or []):
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
