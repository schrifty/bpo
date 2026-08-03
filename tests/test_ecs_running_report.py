"""Tests for ECS running-task reporting."""

from __future__ import annotations

from datetime import datetime, timezone

from src.ecs_running_report import (
    RunningRow,
    _job_from_container_command,
    _running_row_from_task,
    format_running_table,
    running_main,
)


def test_job_from_container_command_reads_run_job_override():
    assert _job_from_container_command(["engineering-portfolio"]) == "engineering-portfolio"
    assert _job_from_container_command(["/app/scripts/run_job.sh", "export-nightly"]) == "export-nightly"
    assert _job_from_container_command(["python3", "cortex.py", "run-job", "--job", "portfolio-batch"]) == (
        "portfolio-batch"
    )


def test_running_row_from_task_uses_overrides():
    task = {
        "taskArn": "arn:aws:ecs:us-east-1:123:task/cluster/abc123",
        "lastStatus": "RUNNING",
        "startedAt": datetime(2026, 6, 17, 3, 15, tzinfo=timezone.utc),
        "taskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/cortex-decks:4",
        "startedBy": "events.amazonaws.com",
        "overrides": {
            "containerOverrides": [{"name": "cortex-decks", "command": ["export-nightly"]}],
        },
    }
    row = _running_row_from_task(task)
    assert row.task_id == "abc123"
    assert row.job == "export-nightly"
    assert row.status == "RUNNING"
    assert "2026-06-17 03:15:00 UTC" in row.started_at
    assert row.task_definition == "cortex-decks:4"


def test_format_running_table_aligns_columns():
    rows = [
        RunningRow(
            task_id="abc123",
            job="export-nightly",
            status="RUNNING",
            started_at="2026-06-17 03:15:00 UTC",
            task_definition="cortex-decks:4",
            started_by="events.amazonaws.com",
        )
    ]
    text = format_running_table(rows)
    assert "abc123" in text
    assert "export-nightly" in text
    assert "RUNNING" in text


def test_running_main_prints_table(monkeypatch, capsys):
    monkeypatch.setattr(
        "src.ecs_running_report.fetch_running_rows",
        lambda **kwargs: (
            [
                RunningRow(
                    task_id="abc123",
                    job="engineering-portfolio",
                    status="RUNNING",
                    started_at="2026-06-17 02:00:00 UTC",
                    task_definition="cortex-decks:4",
                    started_by="events.amazonaws.com",
                )
            ],
            None,
        ),
    )
    code = running_main(["--cluster", "cortex", "--region", "us-east-1"])
    out = capsys.readouterr().out
    assert code == 0
    assert "abc123" in out
    assert "engineering-portfolio" in out


def test_running_main_prints_empty_state(monkeypatch, capsys):
    monkeypatch.setattr(
        "src.ecs_running_report.fetch_running_rows",
        lambda **kwargs: ([], None),
    )
    code = running_main(["--cluster", "cortex", "--region", "us-east-1"])
    out = capsys.readouterr().out
    assert code == 0
    assert "No tasks running." in out


def test_running_main_reports_aws_error(monkeypatch, capsys):
    monkeypatch.setattr(
        "src.ecs_running_report.fetch_running_rows",
        lambda **kwargs: ([], "AWS lookup failed (test)"),
    )
    code = running_main(["--cluster", "cortex", "--region", "us-east-1"])
    out = capsys.readouterr().out
    assert code == 1
    assert "Error: AWS lookup failed (test)" in out
