"""Report EventBridge rules that run Cortex ECS batch jobs.

Catalog defaults mirror ``infra/terraform/variables.tf`` → ``scheduled_jobs``.
Live rows come from AWS when credentials and rules are available.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any

from src.ecs_aws_defaults import default_name_prefix, default_region

# Keep in sync with infra/terraform/variables.tf scheduled_jobs defaults.
SCHEDULED_JOBS_CATALOG: dict[str, dict[str, Any]] = {
    "export-nightly": {
        "schedule_expression": "cron(0 1 * * ? *)",
        "command": ["export-nightly"],
        "enabled": True,
        "rule_name": "cortex-export-nightly",
        "summary": "LLM export (cortex export-all, 90-day window)",
    },
    "engineering-portfolio": {
        "schedule_expression": "cron(30 1 * * ? *)",
        "command": ["engineering-portfolio"],
        "enabled": True,
        "rule_name": "cortex-engineering-portfolio",
        "summary": "Engineering portfolio deck",
    },
    "ford-pendo-7d": {
        "schedule_expression": "cron(0 2 * * ? *)",
        "command": ["ford-pendo-7d"],
        "enabled": True,
        "rule_name": "cortex-ford-pendo-7d",
        "summary": "Ford Pendo usage export (cortex --export-pendo --customer Ford --days 7 --compare-days 7)",
    },
    "ford-pendo-30d": {
        "schedule_expression": "cron(30 2 * * ? *)",
        "command": ["ford-pendo-30d"],
        "enabled": True,
        "rule_name": "cortex-ford-pendo-30d",
        "summary": "Ford Pendo usage export (cortex --export-pendo --customer Ford --days 30 --compare-days 30)",
    },
    "metrics-eng-cycle-lead-weekly": {
        "schedule_expression": "cron(0 5 ? * MON *)",
        "command": ["metrics-eng-cycle-lead-weekly"],
        "enabled": True,
        "rule_name": "cortex-metrics-eng-cycle-lead-weekly",
        "summary": "LeanDNA metrics upsert: 2024, 2179, 2028, 2035 — weekly Sun 11pm CT",
    },
}


def _catalog_rule_name_index() -> dict[str, str]:
    """Map explicit EventBridge rule names to catalog job keys."""
    out: dict[str, str] = {}
    for job_key, spec in SCHEDULED_JOBS_CATALOG.items():
        rule_name = spec.get("rule_name")
        if rule_name:
            out[str(rule_name)] = job_key
    return out


def _expected_rule_name(job_key: str, spec: dict[str, Any], *, name_prefix: str) -> str:
    rule_name = spec.get("rule_name")
    if rule_name:
        return str(rule_name)
    return f"{name_prefix}-{job_key}"


def _schedule_row_from_rule(
    *,
    rule: dict[str, Any],
    job_key: str,
    command: list[str],
) -> ScheduleRow:
    catalog = SCHEDULED_JOBS_CATALOG.get(job_key, {})
    return ScheduleRow(
        job_key=job_key,
        rule_name=str(rule.get("Name") or ""),
        state=str(rule.get("State") or ""),
        schedule_expression=str(rule.get("ScheduleExpression") or ""),
        command=command or [str(x) for x in (catalog.get("command") or [])],
        summary=str(catalog.get("summary") or job_key),
        source="aws",
    )


@dataclass
class ScheduleRow:
    job_key: str
    rule_name: str | None
    state: str | None
    schedule_expression: str
    command: list[str]
    summary: str
    source: str  # "aws" | "catalog"


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
    seen_jobs: set[str] = set()
    custom_rules = _catalog_rule_name_index()
    for rule in payload.get("Rules") or []:
        name = str(rule.get("Name") or "")
        if name in custom_rules:
            job_key = custom_rules[name]
        elif name.startswith(f"{name_prefix}-"):
            job_key = name[len(name_prefix) + 1 :]
        else:
            continue
        schedule = str(rule.get("ScheduleExpression") or "")
        if not schedule:
            continue
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
        rows.append(_schedule_row_from_rule(rule=rule, job_key=job_key, command=command))
        seen_jobs.add(job_key)

    for rule_name, job_key in custom_rules.items():
        if job_key in seen_jobs:
            continue
        describe_proc = subprocess.run(
            [
                "aws",
                "events",
                "describe-rule",
                "--name",
                rule_name,
                "--region",
                region,
                "--output",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if describe_proc.returncode != 0:
            continue
        rule = json.loads(describe_proc.stdout or "{}")
        command: list[str] = []
        target_proc = subprocess.run(
            [
                "aws",
                "events",
                "list-targets-by-rule",
                "--rule",
                rule_name,
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
        rows.append(_schedule_row_from_rule(rule=rule, job_key=job_key, command=command))
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
    seen_jobs: set[str] = set()
    custom_rules = _catalog_rule_name_index()
    try:
        paginator = client.get_paginator("list_rules")
        for page in paginator.paginate(NamePrefix=f"{name_prefix}-"):
            for rule in page.get("Rules") or []:
                name = str(rule.get("Name") or "")
                if name in custom_rules:
                    job_key = custom_rules[name]
                elif name.startswith(f"{name_prefix}-"):
                    job_key = name[len(name_prefix) + 1 :]
                else:
                    continue
                schedule = str(rule.get("ScheduleExpression") or "")
                if not schedule:
                    continue
                targets = client.list_targets_by_rule(Rule=name).get("Targets") or []
                command: list[str] = []
                for target in targets:
                    command = _command_from_target_input(target.get("Input"))
                    if command:
                        break
                rows.append(_schedule_row_from_rule(rule=rule, job_key=job_key, command=command))
                seen_jobs.add(job_key)

        for rule_name, job_key in custom_rules.items():
            if job_key in seen_jobs:
                continue
            try:
                rule = client.describe_rule(Name=rule_name)
            except ClientError:
                continue
            targets = client.list_targets_by_rule(Rule=rule_name).get("Targets") or []
            command = []
            for target in targets:
                command = _command_from_target_input(target.get("Input"))
                if command:
                    break
            rows.append(_schedule_row_from_rule(rule=rule, job_key=job_key, command=command))
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
                rule_name=_expected_rule_name(job_key, spec, name_prefix=name_prefix),
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
                rule_name=_expected_rule_name(job_key, spec, name_prefix=name_prefix),
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


def schedule_main(argv: list[str] | None = None, *, prog: str = "cortex --schedule") -> int:
    parser = argparse.ArgumentParser(prog=prog, description="Show EventBridge schedules for Cortex ECS jobs.")
    parser.add_argument(
        "--prefix",
        default=default_name_prefix(),
        help="EventBridge rule name prefix (default: CORTEX_SCHEDULE_NAME_PREFIX, terraform name_prefix, or cortex)",
    )
    parser.add_argument(
        "--region",
        default=default_region(),
        help="AWS region (default: CORTEX_AWS_REGION, terraform aws_region, or us-east-1)",
    )
    args = parser.parse_args(argv)

    rows, notes = build_schedule_rows(name_prefix=args.prefix.strip(), region=args.region.strip())
    print(f"EventBridge schedules (prefix={args.prefix}, region={args.region})")
    print("Cron expressions are UTC.")
    print()
    if rows:
        for row in rows:
            if row.source == "catalog" and row.state is None:
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
