"""Report EventBridge rules that run Cortex ECS batch jobs.

Catalog defaults mirror ``infra/terraform/variables.tf`` → ``scheduled_jobs``.
Live rows come from AWS when credentials and rules are available.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

# Keep in sync with infra/terraform/variables.tf scheduled_jobs defaults.
SCHEDULED_JOBS_CATALOG: dict[str, dict[str, Any]] = {
    "engineering-portfolio": {
        "schedule_expression": "cron(0 2 * * ? *)",
        "command": ["engineering-portfolio"],
        "enabled": True,
        "summary": "Engineering portfolio deck",
    },
    "export-nightly": {
        "schedule_expression": "cron(0 3 * * ? *)",
        "command": ["export-nightly"],
        "enabled": True,
        "summary": "LLM export (decks --export, 90-day window)",
    },
    "portfolio-batch": {
        "schedule_expression": "cron(0 4 * * ? *)",
        "command": ["portfolio-batch"],
        "enabled": True,
        "summary": "Portfolio deck batch",
    },
    "export-weekly": {
        "schedule_expression": "cron(0 6 ? * SUN *)",
        "command": ["export-weekly"],
        "enabled": False,
        "summary": "LLM export (legacy weekly; disabled in Terraform defaults)",
    },
}


@dataclass
class ScheduleRow:
    job_key: str
    rule_name: str | None
    state: str | None
    schedule_expression: str
    command: list[str]
    summary: str
    source: str  # "aws" | "catalog"


def _default_region() -> str:
    return (
        os.environ.get("CORTEX_AWS_REGION", "").strip()
        or os.environ.get("AWS_DEFAULT_REGION", "").strip()
        or os.environ.get("AWS_REGION", "").strip()
        or "us-east-1"
    )


def _default_name_prefix() -> str:
    return os.environ.get("CORTEX_SCHEDULE_NAME_PREFIX", "").strip() or "cortex"


def _command_from_target_input(raw_input: str | None) -> list[str]:
    if not raw_input:
        return []
    try:
        payload = json.loads(raw_input)
    except json.JSONDecodeError:
        return []
    overrides = payload.get("containerOverrides") or []
    if not overrides:
        return []
    command = overrides[0].get("command")
    if isinstance(command, list):
        return [str(x) for x in command]
    return []


def _fetch_via_aws_cli(*, name_prefix: str, region: str) -> tuple[list[ScheduleRow], str | None]:
    import subprocess

    try:
        proc = subprocess.run(
            [
                "aws",
                "events",
                "list-rules",
                "--name-prefix",
                f"{name_prefix}-",
                "--region",
                region,
                "--output",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return [], f"AWS CLI unavailable ({exc}); showing catalog only"

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return [], f"AWS CLI lookup failed ({err}); showing catalog only"

    payload = json.loads(proc.stdout or "{}")
    rows: list[ScheduleRow] = []
    for rule in payload.get("Rules") or []:
        name = str(rule.get("Name") or "")
        if not name.startswith(f"{name_prefix}-"):
            continue
        job_key = name[len(name_prefix) + 1 :]
        schedule = str(rule.get("ScheduleExpression") or "")
        if not schedule:
            continue
        state = str(rule.get("State") or "")
        command: list[str] = []
        target_proc = subprocess.run(
            [
                "aws",
                "events",
                "list-targets-by-rule",
                "--rule",
                name,
                "--region",
                region,
                "--output",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if target_proc.returncode == 0:
            targets_payload = json.loads(target_proc.stdout or "{}")
            for target in targets_payload.get("Targets") or []:
                command = _command_from_target_input(target.get("Input"))
                if command:
                    break
        catalog = SCHEDULED_JOBS_CATALOG.get(job_key, {})
        rows.append(
            ScheduleRow(
                job_key=job_key,
                rule_name=name,
                state=state,
                schedule_expression=schedule,
                command=command or list(catalog.get("command") or []),
                summary=str(catalog.get("summary") or job_key),
                source="aws",
            )
        )
    rows.sort(key=lambda r: (r.schedule_expression, r.job_key))
    return rows, None


def fetch_aws_schedule_rows(*, name_prefix: str, region: str) -> tuple[list[ScheduleRow], str | None]:
    """Return live EventBridge rules for ``{prefix}-{job}`` and an optional error note."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    except ImportError:
        return _fetch_via_aws_cli(name_prefix=name_prefix, region=region)

    client = boto3.client("events", region_name=region)
    rows: list[ScheduleRow] = []
    try:
        paginator = client.get_paginator("list_rules")
        for page in paginator.paginate(NamePrefix=f"{name_prefix}-"):
            for rule in page.get("Rules") or []:
                name = str(rule.get("Name") or "")
                if not name.startswith(f"{name_prefix}-"):
                    continue
                job_key = name[len(name_prefix) + 1 :]
                schedule = str(rule.get("ScheduleExpression") or "")
                if not schedule:
                    continue
                state = str(rule.get("State") or "")
                targets = client.list_targets_by_rule(Rule=name).get("Targets") or []
                command: list[str] = []
                for target in targets:
                    command = _command_from_target_input(target.get("Input"))
                    if command:
                        break
                catalog = SCHEDULED_JOBS_CATALOG.get(job_key, {})
                rows.append(
                    ScheduleRow(
                        job_key=job_key,
                        rule_name=name,
                        state=state,
                        schedule_expression=schedule,
                        command=command or list(catalog.get("command") or []),
                        summary=str(catalog.get("summary") or job_key),
                        source="aws",
                    )
                )
    except (NoCredentialsError, ClientError, BotoCoreError) as exc:
        return [], f"AWS lookup failed ({exc}); showing catalog only"

    rows.sort(key=lambda r: (r.schedule_expression, r.job_key))
    return rows, None


def build_schedule_rows(*, name_prefix: str, region: str) -> tuple[list[ScheduleRow], list[str]]:
    """Merge AWS rules with catalog entries not deployed in AWS."""
    notes: list[str] = []
    aws_rows, aws_note = fetch_aws_schedule_rows(name_prefix=name_prefix, region=region)
    if aws_note:
        notes.append(aws_note)

    by_job = {row.job_key: row for row in aws_rows}
    merged: list[ScheduleRow] = list(aws_rows)

    for job_key, spec in SCHEDULED_JOBS_CATALOG.items():
        if job_key in by_job:
            continue
        if not spec.get("enabled", True):
            continue
        merged.append(
            ScheduleRow(
                job_key=job_key,
                rule_name=None,
                state=None,
                schedule_expression=str(spec.get("schedule_expression") or ""),
                command=[str(x) for x in (spec.get("command") or [])],
                summary=str(spec.get("summary") or job_key),
                source="catalog",
            )
        )

    for job_key, spec in SCHEDULED_JOBS_CATALOG.items():
        if job_key in by_job or spec.get("enabled", True):
            continue
        merged.append(
            ScheduleRow(
                job_key=job_key,
                rule_name=None,
                state="DISABLED",
                schedule_expression=str(spec.get("schedule_expression") or ""),
                command=[str(x) for x in (spec.get("command") or [])],
                summary=str(spec.get("summary") or job_key),
                source="catalog",
            )
        )

    merged.sort(key=lambda r: (r.schedule_expression, r.job_key))
    return merged, notes


def format_schedule_table(rows: list[ScheduleRow]) -> str:
    headers = ("JOB", "RULE", "STATE", "SCHEDULE (UTC)", "COMMAND")
    table_rows: list[tuple[str, ...]] = []
    for row in rows:
        table_rows.append(
            (
                row.job_key,
                row.rule_name or "(not in AWS)",
                row.state or ("catalog" if row.source == "catalog" else "—"),
                row.schedule_expression,
                " ".join(row.command) if row.command else "—",
            )
        )
    widths = [len(h) for h in headers]
    for tr in table_rows:
        for i, cell in enumerate(tr):
            widths[i] = max(widths[i], len(cell))

    def fmt(cells: tuple[str, ...]) -> str:
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    lines = [fmt(headers), fmt(tuple("-" * w for w in widths))]
    for tr in table_rows:
        lines.append(fmt(tr))
    return "\n".join(lines)


def schedule_main(argv: list[str] | None = None, *, prog: str = "decks --schedule") -> int:
    parser = argparse.ArgumentParser(prog=prog, description="Show EventBridge schedules for Cortex ECS jobs.")
    parser.add_argument(
        "--prefix",
        default=_default_name_prefix(),
        help="EventBridge rule name prefix (default: CORTEX_SCHEDULE_NAME_PREFIX or cortex)",
    )
    parser.add_argument(
        "--region",
        default=_default_region(),
        help="AWS region (default: CORTEX_AWS_REGION, AWS_DEFAULT_REGION, or us-east-1)",
    )
    args = parser.parse_args(argv)

    rows, notes = build_schedule_rows(name_prefix=args.prefix.strip(), region=args.region.strip())
    print(f"EventBridge schedules (prefix={args.prefix}, region={args.region})")
    print("Cron expressions are UTC.")
    print()
    if rows:
        for row in rows:
            if row.source == "catalog" and row.rule_name is None and row.state != "DISABLED":
                print(f"# {row.job_key}: {row.summary} — not deployed in AWS")
        print(format_schedule_table(rows))
    else:
        print("No schedules found.")

    if notes:
        print()
        for note in notes:
            print(f"Note: {note}")

    if not rows:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(schedule_main())
