"""Tests for failure-triggered one-shot job retries."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.job_retry_scheduler import (
    failures_are_retryable,
    is_retryable_failure_text,
    maybe_schedule_job_retry_after_failure,
    schedule_job_retry,
)


@pytest.mark.parametrize(
    "text,expected",
    [
        (
            'ford-pendo-export-7d: googleapiclient.errors.HttpError: <HttpError 503 when requesting https://sheets.googleapis.com/',
            True,
        ),
        ("HttpError 429 rateLimitExceeded", True),
        ("timeout after 7200s", True),
        ("Pendo shared snapshot require: missing manifest", False),
        ("preflight: Jira: not configured", False),
        ("Completed with 1 error(s) of 10 customer(s)", False),
        (
            "Pendo detailed export failed for Ford Motor Company: <HttpError 503 when requesting https://sheets.googleapis.com/",
            True,
        ),
    ],
)
def test_is_retryable_failure_text(text: str, expected: bool) -> None:
    assert is_retryable_failure_text(text) is expected


def test_failures_are_retryable_requires_all_retryable() -> None:
    assert failures_are_retryable(
        [
            "step: HttpError 503 unavailable",
            "other: timeout after 30s",
        ]
    )
    assert not failures_are_retryable(
        [
            "step: HttpError 503 unavailable",
            "preflight: Jira down",
        ]
    )
    assert not failures_are_retryable([])


def test_any_failure_retryable_uses_step_tail_for_top_arr_style() -> None:
    from src.job_retry_scheduler import any_failure_retryable, failure_texts_for_retry_classification
    from src.job_runner import StepResult

    step = StepResult(
        name="pendo-top-10-arr-export-30d",
        command="export-pendo-top-arr",
        success=False,
        exit_code=1,
        duration_s=100.0,
        error="Completed with 1 error(s) of 10 customer(s)",
        stdout_tail=(
            "Pendo detailed export failed for Ford Motor Company: "
            "<HttpError 503 when requesting https://sheets.googleapis.com/"
        ),
    )
    texts = failure_texts_for_retry_classification(
        ["pendo-top-10-arr-export-30d: Completed with 1 error(s) of 10 customer(s)"],
        step_results=[step],
    )
    assert any_failure_retryable(texts)


def test_schedule_job_retry_skips_when_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("CORTEX_ECS_CLUSTER_ARN", raising=False)
    monkeypatch.delenv("CORTEX_JOB_RETRY_ENABLED", raising=False)
    result = schedule_job_retry(
        job_name="ford-pendo-7d",
        run_id="abc123",
        failures=["ford: HttpError 503"],
    )
    assert result.scheduled is False
    assert "disabled" in result.reason or "not configured" in result.reason


def test_schedule_job_retry_skips_when_budget_exhausted(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_JOB_RETRY_ENABLED", "1")
    monkeypatch.setenv("CORTEX_RETRY_ATTEMPT", "1")
    monkeypatch.setenv("CORTEX_JOB_RETRY_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("CORTEX_ECS_CLUSTER_ARN", "arn:aws:ecs:us-east-1:1:cluster/cortex")
    result = schedule_job_retry(
        job_name="ford-pendo-7d",
        run_id="abc123",
        failures=["ford: HttpError 503"],
    )
    assert result.scheduled is False
    assert "exhausted" in result.reason


def test_schedule_job_retry_creates_scheduler_at_expression(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_JOB_RETRY_ENABLED", "1")
    monkeypatch.setenv("CORTEX_JOB_RETRY_DELAY_MINUTES", "15")
    monkeypatch.setenv("CORTEX_JOB_RETRY_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("CORTEX_ECS_CLUSTER_ARN", "arn:aws:ecs:us-east-1:1:cluster/cortex")
    monkeypatch.setenv(
        "CORTEX_ECS_TASK_DEFINITION_ARN",
        "arn:aws:ecs:us-east-1:1:task-definition/cortex-decks",
    )
    monkeypatch.setenv("CORTEX_ECS_SUBNETS", "subnet-a,subnet-b")
    monkeypatch.setenv("CORTEX_ECS_SECURITY_GROUPS", "sg-1")
    monkeypatch.setenv("CORTEX_SCHEDULER_ROLE_ARN", "arn:aws:iam::1:role/cortex-scheduler-ecs")
    monkeypatch.setenv("CORTEX_JOB_RETRY_SCHEDULE_GROUP", "cortex-job-retries")
    monkeypatch.delenv("CORTEX_RETRY_ATTEMPT", raising=False)

    client = MagicMock()
    now = datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
    result = schedule_job_retry(
        job_name="ford-pendo-7d",
        run_id="17f85f8455b94209be1239c10ae9dc97",
        failures=[
            'ford-pendo-export-7d: googleapiclient.errors.HttpError: <HttpError 503 when requesting https://sheets.googleapis.com/'
        ],
        scheduler_client=client,
        now=now,
    )
    assert result.scheduled is True
    assert result.attempt == 1
    assert result.run_at_utc == "2026-08-14T12:15:00Z"
    assert result.schedule_name and result.schedule_name.startswith("cortex-retry-ford-pendo-7d-")
    client.create_schedule.assert_called_once()
    kwargs = client.create_schedule.call_args.kwargs
    assert kwargs["ScheduleExpression"] == "at(2026-08-14T12:15:00)"
    assert kwargs["GroupName"] == "cortex-job-retries"
    assert kwargs["ActionAfterCompletion"] == "DELETE"
    assert kwargs["Target"]["Arn"] == "arn:aws:ecs:us-east-1:1:cluster/cortex"
    import json

    payload = json.loads(kwargs["Target"]["Input"])
    env = {e["name"]: e["value"] for e in payload["containerOverrides"][0]["environment"]}
    assert env["CORTEX_RETRY_OF"] == "17f85f8455b94209be1239c10ae9dc97"
    assert env["CORTEX_RETRY_ATTEMPT"] == "1"
    assert payload["containerOverrides"][0]["command"] == ["ford-pendo-7d"]


def test_maybe_schedule_wrapper_logs_skip(monkeypatch) -> None:
    monkeypatch.setenv("CORTEX_JOB_RETRY_DISABLE", "1")
    result = maybe_schedule_job_retry_after_failure(
        job_name="ford-pendo-7d",
        run_id="x",
        failures=["HttpError 503"],
    )
    assert result.scheduled is False
