#!/usr/bin/env python3
"""Discover Jira development teams via boards, sprints, and backlogs.

Uses the Jira Software Agile REST API (``/rest/agile/1.0/board``) to inventory
scrum/kanban boards, active sprints, and backlog sizes. This is the first step
before building per-team cycle-time metrics (similar to ``get-help-ttr``).

Requires ``JIRA_*`` in ``.env``.

Examples::

  discover-dev-teams
  discover-dev-teams --project LEAN
  discover-dev-teams --format json --include-inactive
  discover-dev-teams --probe-teams-api
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

from src.jira_agile_discovery import (  # noqa: E402
    discover_development_boards,
    try_discover_atlassian_teams,
)
from src.jira_client import get_shared_jira_client  # noqa: E402


def _print_brief(payload: dict[str, Any]) -> None:
    if payload.get("error"):
        print(f"Error: {payload['error']}")
        return

    print(f"Jira: {payload.get('jira_base')}")
    filt = payload.get("project_filter")
    if filt:
        print(f"Project filter: {filt}")
    print(f"Boards (active or with backlog/sprint): {payload.get('board_count', 0)}")
    print()

    by_project = payload.get("by_project") or {}
    for project_key in sorted(by_project.keys()):
        rows = by_project[project_key]
        print(f"=== {project_key} ({len(rows)} board(s)) ===")
        for b in rows:
            bid = b.get("board_id")
            name = b.get("name")
            btype = b.get("type")
            active_flag = "active" if b.get("active") else "idle"
            sprints = b.get("sprints") or {}
            act = sprints.get("active") or []
            closed = sprints.get("recent_closed") or []
            backlog = b.get("backlog") or {}
            bl_total = backlog.get("total") if backlog.get("ok") else backlog.get("error")
            sprint_line = ""
            if act:
                sprint_line = f"  sprint: {act[0].get('name')} ({act[0].get('start')} → {act[0].get('end')})"
            elif closed:
                sprint_line = f"  last closed: {closed[-1].get('name')} (ended {closed[-1].get('end')})"
            print(f"  [{bid}] {name} ({btype}, {active_flag})  backlog={bl_total}{sprint_line}")
        print()

    teams_probe = payload.get("teams_api")
    if teams_probe:
        print("=== Atlassian Teams API probes ===")
        for row in teams_probe.get("teams_api_probes") or []:
            print(f"  {row.get('path')}: {row.get('status') or row.get('error')} {row.get('note', '')}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Discover Jira boards/sprints/backlogs for development teams.",
    )
    ap.add_argument(
        "--project",
        default=None,
        metavar="KEY",
        help="Limit boards to one Jira project (e.g. LEAN)",
    )
    ap.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include boards with no active sprint and empty/unknown backlog",
    )
    ap.add_argument(
        "--no-sprint-probe",
        action="store_true",
        help="Skip per-board sprint API calls",
    )
    ap.add_argument(
        "--no-backlog-probe",
        action="store_true",
        help="Skip per-board backlog API calls",
    )
    ap.add_argument(
        "--probe-teams-api",
        action="store_true",
        help="Also try Atlassian Teams REST paths (often 403/404 with API token)",
    )
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    logging.getLogger("cortex").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    try:
        jira = get_shared_jira_client()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        payload = discover_development_boards(
            jira,
            project_key=ns.project,
            include_inactive=ns.include_inactive,
            sprint_probe=not ns.no_sprint_probe,
            backlog_probe=not ns.no_backlog_probe,
        )
    except Exception as e:
        print(f"Discovery failed: {e}", file=sys.stderr)
        return 1

    if ns.probe_teams_api:
        payload["teams_api"] = try_discover_atlassian_teams(jira)

    if ns.format == "json":
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    else:
        _print_brief(payload)

    return 1 if payload.get("error") else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
