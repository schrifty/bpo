"""Report ECS Fargate tasks currently running Cortex batch jobs."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.ecs_aws_defaults import (
    default_cluster_name,
    default_region,
    default_task_family,
    format_cluster_not_found_error,
    is_cluster_not_found_error,
)


@dataclass
class RunningRow:
    task_id: str
    job: str
    status: str
    started_at: str
    task_definition: str
    started_by: str


def _job_from_container_command(command: list[str] | None) -> str:
    if not command:
        return "—"
    parts = [str(x) for x in command]
    if parts[0].endswith("run_job.sh"):
        return parts[1] if len(parts) > 1 else "—"
    if "run-job" in parts:
        for i, part in enumerate(parts):
            if part in ("--job", "-j") and i + 1 < len(parts):
                return parts[i + 1]
    if len(parts) == 1 and not parts[0].startswith("-"):
        return parts[0]
    return " ".join(parts)


def _format_started_at(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except ValueError:
            return raw
    return str(value)


def _running_row_from_task(task: dict[str, Any]) -> RunningRow:
    task_arn = str(task.get("taskArn") or "")
    task_id = task_arn.rsplit("/", 1)[-1] if task_arn else "—"
    containers = task.get("containers") or []
    command: list[str] = []
    for container in containers:
        raw = container.get("command")
        if isinstance(raw, list) and raw:
            command = [str(x) for x in raw]
            break
    overrides = task.get("overrides") or {}
    for container in overrides.get("containerOverrides") or []:
        raw = container.get("command")
        if isinstance(raw, list) and raw:
            command = [str(x) for x in raw]
            break
    task_def_arn = str(task.get("taskDefinitionArn") or "")
    task_def = task_def_arn.rsplit("/", 1)[-1] if task_def_arn else "—"
    return RunningRow(
        task_id=task_id,
        job=_job_from_container_command(command),
        status=str(task.get("lastStatus") or task.get("desiredStatus") or "—"),
        started_at=_format_started_at(task.get("startedAt") or task.get("createdAt")),
        task_definition=task_def,
        started_by=str(task.get("startedBy") or "—"),
    )


def _fetch_via_aws_cli(*, cluster: str, region: str, task_family: str) -> tuple[list[RunningRow], str | None]:
    import json
    import subprocess

    list_args = [
        "aws",
        "ecs",
        "list-tasks",
        "--cluster",
        cluster,
        "--desired-status",
        "RUNNING",
        "--region",
        region,
        "--output",
        "json",
    ]
    if task_family:
        list_args.extend(["--family", task_family])

    try:
        list_proc = subprocess.run(list_args, check=False, capture_output=True, text=True)
    except OSError as exc:
        return [], f"AWS CLI unavailable ({exc})"

    if list_proc.returncode != 0:
        err = (list_proc.stderr or list_proc.stdout or "").strip()
        if is_cluster_not_found_error(err):
            return [], format_cluster_not_found_error(cluster=cluster, region=region)
        return [], f"AWS CLI lookup failed ({err})"

    task_arns = json.loads(list_proc.stdout or "{}").get("taskArns") or []
    if not task_arns:
        return [], None

    describe_proc = subprocess.run(
        [
            "aws",
            "ecs",
            "describe-tasks",
            "--cluster",
            cluster,
            "--tasks",
            *task_arns,
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
        err = (describe_proc.stderr or describe_proc.stdout or "").strip()
        return [], f"AWS CLI lookup failed ({err})"

    payload = json.loads(describe_proc.stdout or "{}")
    rows = [_running_row_from_task(task) for task in payload.get("tasks") or []]
    rows.sort(key=lambda r: (r.started_at, r.task_id))
    return rows, None


def fetch_running_rows(*, cluster: str, region: str, task_family: str) -> tuple[list[RunningRow], str | None]:
    """Return RUNNING ECS tasks for the decks task family."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    except ImportError:
        return _fetch_via_aws_cli(cluster=cluster, region=region, task_family=task_family)

    client = boto3.client("ecs", region_name=region)
    rows: list[RunningRow] = []
    try:
        list_kwargs: dict[str, Any] = {"cluster": cluster, "desiredStatus": "RUNNING"}
        if task_family:
            list_kwargs["family"] = task_family
        task_arns: list[str] = []
        paginator = client.get_paginator("list_tasks")
        for page in paginator.paginate(**list_kwargs):
            task_arns.extend(page.get("taskArns") or [])
        if not task_arns:
            return [], None

        for i in range(0, len(task_arns), 100):
            chunk = task_arns[i : i + 100]
            described = client.describe_tasks(cluster=cluster, tasks=chunk)
            rows.extend(_running_row_from_task(task) for task in described.get("tasks") or [])
    except (NoCredentialsError, ClientError, BotoCoreError) as exc:
        msg = str(exc)
        if is_cluster_not_found_error(msg):
            return [], format_cluster_not_found_error(cluster=cluster, region=region)
        return [], f"AWS lookup failed ({exc})"

    rows.sort(key=lambda r: (r.started_at, r.task_id))
    return rows, None


def format_running_table(rows: list[RunningRow]) -> str:
    headers = ("TASK ID", "JOB", "STATUS", "STARTED (UTC)", "TASK DEF", "STARTED BY")
    table_rows: list[tuple[str, ...]] = []
    for row in rows:
        table_rows.append(
            (
                row.task_id,
                row.job,
                row.status,
                row.started_at,
                row.task_definition,
                row.started_by,
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


def running_main(argv: list[str] | None = None, *, prog: str = "cortex --running") -> int:
    parser = argparse.ArgumentParser(prog=prog, description="List ECS Fargate tasks running Cortex batch jobs.")
    parser.add_argument(
        "--cluster",
        default=default_cluster_name(),
        help="ECS cluster name (default: CORTEX_ECS_CLUSTER, terraform name_prefix, or cortex)",
    )
    parser.add_argument(
        "--family",
        default=default_task_family(),
        help="Task definition family filter (default: CORTEX_ECS_TASK_FAMILY or {prefix}-decks)",
    )
    parser.add_argument(
        "--region",
        default=default_region(),
        help="AWS region (default: CORTEX_AWS_REGION, terraform aws_region, or us-east-1)",
    )
    args = parser.parse_args(argv)

    rows, error = fetch_running_rows(
        cluster=args.cluster.strip(),
        region=args.region.strip(),
        task_family=args.family.strip(),
    )
    print(f"Running ECS tasks (cluster={args.cluster}, family={args.family}, region={args.region})")
    print()
    if error:
        print(f"Error: {error}")
        return 1
    if rows:
        print(format_running_table(rows))
    else:
        print("No tasks running.")
    return 0


if __name__ == "__main__":
    sys.exit(running_main())
