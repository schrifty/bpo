"""Failure-triggered one-shot ECS retries via EventBridge Scheduler.

When a scheduled job exits with a *retryable* failure (transient Google Sheets/Drive
outages, timeouts, etc.), schedule a single delayed re-run of the same job YAML.
Retries are capped (default: one) and skipped when ECS/Scheduler env is unset (local).
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from .config import logger

# Transient / infrastructure failures worth a delayed full-job re-run.
_RETRYABLE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"HttpError\s*50[234]",
        r"HttpError\s*429",
        r"status[= ]*50[234]",
        r"\b429\b.*(?:rate|quota|too many)",
        r"service is currently unavailable",
        r"temporarily unavailable",
        r"timeout after \d+s",
        r"timed?\s*out",
        r"Read timed out",
        r"ConnectionResetError|Connection aborted|BrokenPipeError",
        r"ConnectionReset|ECONNRESET|ETIMEDOUT",
        r"SSLError|RemoteDisconnected",
    )
)

_NON_RETRYABLE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"^preflight:",
        r"missing manifest",
        r"snapshot require:",
        r"credentials|unauthorized|401\b|Invalid JWT|access denied",
        r"not configured|is not set",
        r"Unsupported command",
        r"disk cache disabled",
    )
)


@dataclass(frozen=True)
class RetryScheduleResult:
    scheduled: bool
    reason: str
    schedule_name: str | None = None
    attempt: int | None = None
    run_at_utc: str | None = None


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def retry_attempt_from_env() -> int:
    raw = (os.environ.get("CORTEX_RETRY_ATTEMPT") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def retry_of_from_env() -> str | None:
    raw = (os.environ.get("CORTEX_RETRY_OF") or "").strip()
    return raw or None


def max_retry_attempts() -> int:
    raw = (os.environ.get("CORTEX_JOB_RETRY_MAX_ATTEMPTS") or "").strip()
    if not raw:
        return 1
    try:
        return max(0, int(raw))
    except ValueError:
        return 1


def retry_delay_minutes() -> int:
    raw = (os.environ.get("CORTEX_JOB_RETRY_DELAY_MINUTES") or "").strip()
    if not raw:
        return 15
    try:
        return max(1, int(raw))
    except ValueError:
        return 15


def job_retry_enabled() -> bool:
    if _truthy("CORTEX_JOB_RETRY_DISABLE"):
        return False
    # Explicit off
    raw = (os.environ.get("CORTEX_JOB_RETRY_ENABLED") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    # Default: enabled when ECS launch env is present (Fargate), else off for local.
    if raw in ("1", "true", "yes", "on"):
        return True
    return bool((os.environ.get("CORTEX_ECS_CLUSTER_ARN") or "").strip())


def is_retryable_failure_text(text: str) -> bool:
    """Return True when *text* looks like a transient infra failure."""
    msg = (text or "").strip()
    if not msg:
        return False
    for pat in _NON_RETRYABLE_PATTERNS:
        if pat.search(msg):
            return False
    return any(pat.search(msg) for pat in _RETRYABLE_PATTERNS)


def failures_are_retryable(failures: Sequence[str]) -> bool:
    """True when every listed failure string is retryable (strict AND)."""
    msgs = [str(f).strip() for f in failures if str(f).strip()]
    if not msgs:
        return False
    return all(is_retryable_failure_text(m) for m in msgs)


def failure_texts_for_retry_classification(
    failures: Sequence[str],
    *,
    step_results: Sequence[Any] | None = None,
) -> list[str]:
    """Flatten summary failures plus step error/detail/tails for retry classification.

    Jobs like ``pendo-top-10-arr`` often summarize as ``Completed with 1 error(s)`` while
    the retryable Sheets 503 lives in step stdout / detail messages.
    """
    texts: list[str] = [str(f) for f in failures if str(f).strip()]
    for step in step_results or []:
        for attr in ("error", "stdout_tail", "stderr_tail"):
            val = getattr(step, attr, None)
            if val:
                texts.append(str(val))
        details = getattr(step, "detail_messages", None) or []
        for d in details:
            if d:
                texts.append(str(d))
    return texts


def any_failure_retryable(texts: Sequence[str]) -> bool:
    """True when at least one failure text is retryable and none are hard non-retryable-only.

    Uses OR across texts for retryable signals (so a top-level '1 error(s)' summary can
    still retry when a step tail contains HttpError 503), but blocks if any text matches
    a hard non-retryable pattern (preflight, missing snapshot, auth).
    """
    msgs = [str(t).strip() for t in texts if str(t).strip()]
    if not msgs:
        return False
    for msg in msgs:
        for pat in _NON_RETRYABLE_PATTERNS:
            if pat.search(msg):
                return False
    return any(is_retryable_failure_text(m) for m in msgs)


def _require_env(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise RuntimeError(f"missing required env {name} for job retry scheduling")
    return val


def _schedule_name(job_name: str, run_id: str, attempt: int) -> str:
    # Scheduler names: letters, numbers, hyphens; max 64.
    safe_job = re.sub(r"[^a-zA-Z0-9_-]+", "-", job_name).strip("-")[:24] or "job"
    short = (run_id or "run")[:8]
    return f"cortex-retry-{safe_job}-{short}-a{attempt}"[:64]


def schedule_job_retry(
    *,
    job_name: str,
    run_id: str,
    failures: Sequence[str],
    step_results: Sequence[Any] | None = None,
    scheduler_client: Any | None = None,
    now: datetime | None = None,
) -> RetryScheduleResult:
    """Schedule a one-shot ECS re-run of *job_name* if the failure is retryable.

    No-ops (``scheduled=False``) when disabled, already at max attempts, not retryable,
    or ECS/Scheduler env is incomplete.
    """
    attempt = retry_attempt_from_env()
    next_attempt = attempt + 1
    max_attempts = max_retry_attempts()

    if not job_retry_enabled():
        return RetryScheduleResult(False, "job retry disabled or not configured for this runtime")
    if max_attempts <= 0:
        return RetryScheduleResult(False, "CORTEX_JOB_RETRY_MAX_ATTEMPTS is 0")
    if attempt >= max_attempts:
        return RetryScheduleResult(
            False,
            f"retry budget exhausted (attempt={attempt}, max={max_attempts})",
            attempt=attempt,
        )
    classify_texts = failure_texts_for_retry_classification(failures, step_results=step_results)
    if not any_failure_retryable(classify_texts):
        return RetryScheduleResult(False, "failure not classified as retryable", attempt=attempt)

    try:
        cluster_arn = _require_env("CORTEX_ECS_CLUSTER_ARN")
        task_def = _require_env("CORTEX_ECS_TASK_DEFINITION_ARN")
        subnets_raw = _require_env("CORTEX_ECS_SUBNETS")
        sg_raw = _require_env("CORTEX_ECS_SECURITY_GROUPS")
        role_arn = _require_env("CORTEX_SCHEDULER_ROLE_ARN")
    except RuntimeError as exc:
        return RetryScheduleResult(False, str(exc), attempt=attempt)

    subnets = [s.strip() for s in subnets_raw.split(",") if s.strip()]
    security_groups = [s.strip() for s in sg_raw.split(",") if s.strip()]
    if not subnets or not security_groups:
        return RetryScheduleResult(False, "empty CORTEX_ECS_SUBNETS or CORTEX_ECS_SECURITY_GROUPS", attempt=attempt)

    assign_public = (os.environ.get("CORTEX_ECS_ASSIGN_PUBLIC_IP") or "ENABLED").strip().upper()
    if assign_public not in ("ENABLED", "DISABLED"):
        assign_public = "ENABLED"
    container = (os.environ.get("CORTEX_ECS_CONTAINER_NAME") or "cortex-decks").strip() or "cortex-decks"
    group = (os.environ.get("CORTEX_JOB_RETRY_SCHEDULE_GROUP") or "default").strip() or "default"
    region = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1").strip()

    delay = retry_delay_minutes()
    when = (now or datetime.now(timezone.utc)) + timedelta(minutes=delay)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    else:
        when = when.astimezone(timezone.utc)
    # EventBridge Scheduler `at()` is second-precision UTC, no zone suffix.
    at_expr = when.strftime("at(%Y-%m-%dT%H:%M:%S)")
    run_at_iso = when.strftime("%Y-%m-%dT%H:%M:%SZ")
    name = _schedule_name(job_name, run_id, next_attempt)
    new_run_id = uuid.uuid4().hex

    target_input = {
        "containerOverrides": [
            {
                "name": container,
                "command": [job_name],
                "environment": [
                    {"name": "CORTEX_RETRY_OF", "value": run_id},
                    {"name": "CORTEX_RETRY_ATTEMPT", "value": str(next_attempt)},
                    {"name": "CORTEX_RUN_ID", "value": new_run_id},
                ],
            }
        ]
    }

    client = scheduler_client
    if client is None:
        import boto3

        client = boto3.client("scheduler", region_name=region)

    try:
        client.create_schedule(
            Name=name,
            GroupName=group,
            ScheduleExpression=at_expr,
            FlexibleTimeWindow={"Mode": "OFF"},
            ActionAfterCompletion="DELETE",
            Target={
                "Arn": cluster_arn,
                "RoleArn": role_arn,
                "EcsParameters": {
                    "TaskDefinitionArn": task_def,
                    "LaunchType": "FARGATE",
                    "TaskCount": 1,
                    "NetworkConfiguration": {
                        "awsvpcConfiguration": {
                            "Subnets": subnets,
                            "SecurityGroups": security_groups,
                            "AssignPublicIp": assign_public,
                        }
                    },
                },
                "Input": json.dumps(target_input, separators=(",", ":")),
            },
        )
    except Exception as exc:
        logger.error(
            "Failed to schedule job retry for %s (attempt %d): %s",
            job_name,
            next_attempt,
            exc,
        )
        return RetryScheduleResult(
            False,
            f"scheduler create_schedule failed: {exc}",
            schedule_name=name,
            attempt=next_attempt,
            run_at_utc=run_at_iso,
        )

    logger.info(
        "Scheduled one-shot job retry name=%s job=%s attempt=%d at=%s retry_of=%s",
        name,
        job_name,
        next_attempt,
        run_at_iso,
        run_id,
    )
    print(
        f"CORTEX_JOB_RETRY_SCHEDULED="
        f'{{"job":"{job_name}","schedule_name":"{name}","attempt":{next_attempt},'
        f'"run_at_utc":"{run_at_iso}","retry_of":"{run_id}"}}',
        flush=True,
    )
    return RetryScheduleResult(
        True,
        "scheduled",
        schedule_name=name,
        attempt=next_attempt,
        run_at_utc=run_at_iso,
    )


def maybe_schedule_job_retry_after_failure(
    *,
    job_name: str,
    run_id: str,
    failures: Sequence[str],
    step_results: Sequence[Any] | None = None,
    scheduler_client: Any | None = None,
) -> RetryScheduleResult:
    """Public entry used by ``run_job`` after a failed summary."""
    result = schedule_job_retry(
        job_name=job_name,
        run_id=run_id,
        failures=failures,
        step_results=step_results,
        scheduler_client=scheduler_client,
    )
    if not result.scheduled:
        logger.info(
            "Job retry not scheduled for %s: %s",
            job_name,
            result.reason,
        )
    return result
