#!/usr/bin/env python3
"""Per-team sprint delivery % from configured Jira scrum boards.

Default: each board's **latest closed sprint**. Use flags to pick active sprint,
a specific sprint number/week/name, or **historical** closed sprints.

Requires ``JIRA_*`` in ``.env``.

Examples::

  get-sprint-delivery
  get-sprint-delivery --active
  get-sprint-delivery --board 44 --sprint-number 595
  get-sprint-delivery --board 36 --week 14
  get-sprint-delivery --sprint-name "Week of Jun 1"
  get-sprint-delivery --history 10 --board 44
  get-sprint-delivery --format json --history 5
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
    SprintSelector,
    get_sprint_delivery_by_team,
    get_sprint_delivery_history,
)


def _format_sprint_label(sprint: dict[str, Any] | None, *, ended: bool = True) -> str:
    if not isinstance(sprint, dict):
        return "(unknown sprint)"
    name = str(sprint.get("name") or "").strip() or "(unnamed)"
    start = sprint.get("start")
    end = sprint.get("end")
    state = str(sprint.get("state") or "").lower()
    if start and end:
        range_s = f"{start} → {end}"
    elif end:
        range_s = f"ended {end}" if ended else str(end)
    elif start:
        range_s = f"from {start}"
    else:
        range_s = ""
    suffix = f" [{state}]" if state and state != "closed" else ""
    return f"{name}  ({range_s}){suffix}".strip()


def _print_truncation_warning(team_row: dict[str, Any]) -> None:
    if not team_row.get("truncated"):
        return
    total = team_row.get("reported_total")
    committed = team_row.get("committed")
    print(
        f"           warning: sprint has {total} issues but only {committed} were fetched "
        f"(rate may be overstated; raise --max-issues)",
        file=sys.stderr,
    )


def _print_team_row(team: dict[str, Any], *, indent: str = "  ") -> None:
    label = team.get("team") or team.get("board_id")
    if team.get("error"):
        print(f"{indent}{label}: ERROR — {team['error']}")
        return
    sprint = _format_sprint_label(team.get("sprint"), ended=team.get("sprint", {}).get("state") == "closed")
    delivered = team.get("delivered")
    committed = team.get("committed")
    pct = team.get("delivery_pct")
    print(f"{indent}{label}: {delivered}/{committed} ({pct}%)  —  {sprint}")
    _print_truncation_warning(team)


def _print_brief(payload: dict[str, Any]) -> None:
    if payload.get("error"):
        print(f"Error: {payload['error']}", file=sys.stderr)
        return

    print(payload.get("definition") or "Sprint delivery % by board")
    excluded = payload.get("excluded_issue_types") or []
    if excluded:
        print(f"Excluded issue types: {', '.join(excluded)}")
    print()

    if payload.get("mode") == "history":
        for board in payload.get("boards") or []:
            label = board.get("team") or board.get("board_id")
            bid = board.get("board_id")
            print(f"=== {label} [{bid}] ===")
            if board.get("error"):
                print(f"  ERROR — {board['error']}")
                continue
            for row in board.get("sprints") or []:
                if row.get("error"):
                    sprint = _format_sprint_label(row.get("sprint"))
                    print(f"  {sprint}: ERROR — {row['error']}")
                    continue
                sprint = _format_sprint_label(row.get("sprint"))
                print(
                    f"  {sprint}: {row.get('delivered')}/{row.get('committed')} ({row.get('delivery_pct')}%)"
                )
                _print_truncation_warning(row)
            print()
        return

    for team in payload.get("teams") or []:
        _print_team_row(team)

    average = payload.get("average_delivery_pct")
    print()
    if average is not None:
        print(f"Average (metrics-upsert value): {average}%")
        if payload.get("mode") == "snapshot" and not payload.get("sprint_selector"):
            print(
                "(unweighted mean of per-board delivery %; "
                "LeanDNA POST uses numerator={0}, denominator=100)".format(average)
            )


def _build_selector(ns: argparse.Namespace) -> SprintSelector | None:
    selectors = sum(
        1
        for v in (ns.sprint_id, ns.sprint_number, ns.week, ns.sprint_name, ns.active)
        if v
    )
    if selectors > 1:
        raise SystemExit(
            "error: use only one of --active, --sprint-id, --sprint-number, --week, --sprint-name",
        )
    if ns.active:
        return SprintSelector(active=True)
    if ns.sprint_id is not None:
        return SprintSelector(sprint_id=ns.sprint_id)
    if ns.sprint_number is not None:
        return SprintSelector(sprint_number=ns.sprint_number)
    if ns.week is not None:
        return SprintSelector(week=ns.week)
    if ns.sprint_name:
        return SprintSelector(sprint_name=ns.sprint_name)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sprint delivery % per development board.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Board ids: "
            + ", ".join(f"{b['board_id']}={b['team_label']}" for b in SPRINT_DELIVERY_BOARDS)
        ),
    )
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument(
        "--board",
        type=int,
        action="append",
        dest="board_ids",
        metavar="ID",
        help=f"Board id (default: {[b['board_id'] for b in SPRINT_DELIVERY_BOARDS]})",
    )
    ap.add_argument(
        "--history",
        type=int,
        metavar="N",
        help="Show last N closed sprints per board (newest first)",
    )
    ap.add_argument(
        "--active",
        action="store_true",
        help="Use the active sprint instead of latest closed",
    )
    ap.add_argument("--sprint-id", type=int, metavar="ID", help="Jira sprint id")
    ap.add_argument(
        "--sprint-number",
        type=int,
        metavar="N",
        help="Match LEAN-style sprint name (e.g. 595 → Sprint595)",
    )
    ap.add_argument(
        "--week",
        metavar="LABEL",
        help='Match week sprint name (e.g. 14 → "Week 14", or "Week of Jun 1")',
    )
    ap.add_argument(
        "--sprint-name",
        metavar="TEXT",
        help="Case-insensitive substring match on sprint name",
    )
    ap.add_argument("--max-issues", type=int, default=500, dest="max_issues")
    ap.add_argument("--timeout", type=float, default=60.0, metavar="SEC")
    ap.add_argument("--all-issue-types", action="store_true")
    ns = ap.parse_args()

    if ns.history is not None and _build_selector(ns) is not None:
        print("error: --history cannot be combined with sprint selector flags", file=sys.stderr)
        return 2

    try:
        jira = get_shared_jira_client()
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    common = dict(
        board_ids=ns.board_ids,
        max_issues_per_board=ns.max_issues,
        timeout=ns.timeout,
        include_all_issue_types=ns.all_issue_types,
    )

    if ns.history is not None:
        payload = get_sprint_delivery_history(
            jira,
            history_count=ns.history,
            **common,
        )
    else:
        payload = get_sprint_delivery_by_team(
            jira,
            sprint_selector=_build_selector(ns),
            **common,
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
