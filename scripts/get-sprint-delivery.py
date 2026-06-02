#!/usr/bin/env python3
"""Per-team sprint delivery % from configured Jira scrum boards.

Uses each board's **latest closed sprint**. Shows delivered/committed per team and
the **unweighted average** posted by ``metrics-upsert`` to LeanDNA metric 2086.

Requires ``JIRA_*`` in ``.env``.

Examples::

  get-sprint-delivery
  get-sprint-delivery --format json
  get-sprint-delivery --board 44 --board 46
"""
from __future__ import annotations

import argparse
import json
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
from src.jira_sprint_delivery import (  # noqa: E402
    SPRINT_DELIVERY_BOARDS,
    get_sprint_delivery_by_team,
)


def _format_sprint_label(sprint: dict[str, Any] | None) -> str:
    if not isinstance(sprint, dict):
        return "(unknown sprint)"
    name = str(sprint.get("name") or "").strip() or "(unnamed)"
    end = sprint.get("end")
    if end:
        return f"{name} (ended {end})"
    return name


def _print_brief(payload: dict[str, Any]) -> None:
    if payload.get("error"):
        print(f"Error: {payload['error']}", file=sys.stderr)
        return

    average = payload.get("average_delivery_pct")
    print(payload.get("definition") or "Sprint delivery % by board")
    excluded = payload.get("excluded_issue_types") or []
    if excluded:
        print(f"Excluded issue types: {', '.join(excluded)}")
    print()

    for team in payload.get("teams") or []:
        label = team.get("team") or team.get("board_id")
        if team.get("error"):
            print(f"  {label}: ERROR — {team['error']}")
            continue
        sprint = _format_sprint_label(team.get("sprint"))
        delivered = team.get("delivered")
        committed = team.get("committed")
        pct = team.get("delivery_pct")
        line = f"  {label}: {delivered}/{committed} ({pct}%)  —  {sprint}"
        print(line)
        if team.get("truncated"):
            total = team.get("reported_total")
            print(
                f"           warning: sprint has {total} issues but only {committed} were fetched "
                f"(rate may be overstated; raise --max-issues)",
                file=sys.stderr,
            )

    print()
    if average is not None:
        print(f"Average (metrics-upsert value): {average}%")
        print("(unweighted mean of per-board delivery %; LeanDNA POST uses numerator={0}, denominator=100)".format(average))


def main() -> int:
    ap = argparse.ArgumentParser(description="Sprint delivery % per development board.")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument(
        "--board",
        type=int,
        action="append",
        dest="board_ids",
        metavar="ID",
        help=f"Board id (default: {[b['board_id'] for b in SPRINT_DELIVERY_BOARDS]})",
    )
    ap.add_argument("--max-issues", type=int, default=500, dest="max_issues")
    ap.add_argument("--timeout", type=float, default=60.0, metavar="SEC")
    ap.add_argument("--all-issue-types", action="store_true")
    ns = ap.parse_args()

    try:
        jira = get_shared_jira_client()
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    payload = get_sprint_delivery_by_team(
        jira,
        board_ids=ns.board_ids,
        max_issues_per_board=ns.max_issues,
        timeout=ns.timeout,
        include_all_issue_types=ns.all_issue_types,
    )

    if ns.format == "json":
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_brief(payload)

    return 1 if payload.get("error") else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
