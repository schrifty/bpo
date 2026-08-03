"""Last-night ECS job outcomes for the morning report (CloudWatch ``CORTEX_RUN_SUMMARY``)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from src.ecs_aws_defaults import default_region
from src.ecs_schedule_report import SCHEDULED_JOBS_CATALOG

logger = logging.getLogger("cortex")

# Morning report itself — excluded from "last night" expectations.
_MORNING_REPORT_JOB = "metrics-daily-digest"
_DEFAULT_LOG_GROUP = "/cortex/decks"
# Overnight batch window on the as-of UTC calendar day (before 12:00 morning report).
_WINDOW_START_HOUR_UTC = 2
_WINDOW_END_HOUR_UTC = 11
_WINDOW_END_MINUTE_UTC = 45

_LINE_WIDTH = 128


@dataclass(frozen=True)
class OvernightJobOutcome:
    job: str
    status: str  # OK | FAIL | MISSING | DISABLED | SKIPPED | ERROR
    duration_s: float | None = None
    finished_utc: datetime | None = None
    failures: tuple[str, ...] = ()
    detail: str | None = None


def overnight_window_utc(as_of: date) -> tuple[datetime, datetime]:
    """Return [start, end) UTC covering last night's scheduled batch for *as_of*."""
    start = datetime(as_of.year, as_of.month, as_of.day, _WINDOW_START_HOUR_UTC, 0, tzinfo=timezone.utc)
    end = datetime(
        as_of.year,
        as_of.month,
        as_of.day,
        _WINDOW_END_HOUR_UTC,
        _WINDOW_END_MINUTE_UTC,
        tzinfo=timezone.utc,
    )
    return start, end


def _schedule_runs_on_date(schedule_expression: str, as_of: date) -> bool:
    expr = (schedule_expression or "").upper()
    if "MON" in expr:
        return as_of.weekday() == 0  # Monday
    return True


def expected_overnight_jobs(*, as_of: date) -> list[tuple[str, dict[str, Any]]]:
    """Catalog jobs that should have run overnight before the morning report."""
    out: list[tuple[str, dict[str, Any]]] = []
    for job_key, spec in SCHEDULED_JOBS_CATALOG.items():
        if job_key == _MORNING_REPORT_JOB:
            continue
        sched = str(spec.get("schedule_expression") or "")
        if not _schedule_runs_on_date(sched, as_of):
            continue
        out.append((job_key, spec))
    return out


def extract_run_summary(message: str) -> dict[str, Any] | None:
    """Parse ``CORTEX_RUN_SUMMARY={…}`` from a raw or JSON-wrapped CloudWatch line."""
    msg = message
    try:
        outer = json.loads(message)
        if isinstance(outer, dict):
            inner = outer.get("message") or outer.get("msg")
            if isinstance(inner, str) and "CORTEX_RUN_SUMMARY=" in inner:
                msg = inner
    except (TypeError, ValueError, json.JSONDecodeError):
        pass

    idx = msg.find("CORTEX_RUN_SUMMARY=")
    if idx < 0:
        return None
    payload = msg[idx + len("CORTEX_RUN_SUMMARY=") :]
    if not payload.startswith("{"):
        return None

    depth = 0
    end = None
    in_str = False
    esc = False
    for i, ch in enumerate(payload):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return None
    try:
        data = json.loads(payload[:end])
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _job_name_from_summary(data: dict[str, Any]) -> str | None:
    job = data.get("job")
    if isinstance(job, str) and job.strip():
        return job.strip()
    scope = data.get("scope")
    if isinstance(scope, str) and scope.startswith("job:"):
        return scope[4:].strip() or None
    return None


def fetch_run_summaries_from_logs(
    *,
    start: datetime,
    end: datetime,
    log_group: str = _DEFAULT_LOG_GROUP,
    region: str | None = None,
    logs_client: Any | None = None,
) -> list[tuple[datetime, dict[str, Any]]]:
    """Return ``(event_time_utc, summary_dict)`` for CORTEX_RUN_SUMMARY lines in the window."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as e:
        raise RuntimeError(f"boto3 required to read overnight job logs: {e}") from e

    client = logs_client or boto3.client("logs", region_name=region or default_region())
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    out: list[tuple[datetime, dict[str, Any]]] = []
    next_token: str | None = None
    try:
        while True:
            kwargs: dict[str, Any] = {
                "logGroupName": log_group,
                "startTime": start_ms,
                "endTime": end_ms,
                "filterPattern": "CORTEX_RUN_SUMMARY",
            }
            if next_token:
                kwargs["nextToken"] = next_token
            resp = client.filter_log_events(**kwargs)
            for event in resp.get("events") or []:
                summary = extract_run_summary(str(event.get("message") or ""))
                if not summary:
                    continue
                ts_ms = event.get("timestamp")
                if ts_ms is None:
                    continue
                ts = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
                out.append((ts, summary))
            next_token = resp.get("nextToken")
            if not next_token:
                break
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"CloudWatch log read failed for {log_group}: {e}") from e
    return out


def build_overnight_job_outcomes(
    *,
    as_of: date,
    summaries: list[tuple[datetime, dict[str, Any]]] | None = None,
    fetch_error: str | None = None,
) -> list[OvernightJobOutcome]:
    """Merge expected overnight jobs with observed run summaries."""
    if fetch_error:
        return [
            OvernightJobOutcome(
                job="(cloudwatch)",
                status="ERROR",
                detail=fetch_error,
            )
        ]

    by_job: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for ts, data in summaries or []:
        name = _job_name_from_summary(data)
        if not name or name == _MORNING_REPORT_JOB:
            continue
        prev = by_job.get(name)
        if prev is None or ts >= prev[0]:
            by_job[name] = (ts, data)

    outcomes: list[OvernightJobOutcome] = []
    for job_key, spec in expected_overnight_jobs(as_of=as_of):
        if not bool(spec.get("enabled", True)):
            outcomes.append(OvernightJobOutcome(job=job_key, status="DISABLED"))
            continue
        hit = by_job.pop(job_key, None)
        if hit is None:
            outcomes.append(OvernightJobOutcome(job=job_key, status="MISSING"))
            continue
        ts, data = hit
        ok = bool(data.get("success"))
        fails_raw = data.get("failures") or []
        fails = tuple(str(f) for f in fails_raw) if isinstance(fails_raw, list) else ()
        dur = data.get("duration_s")
        outcomes.append(
            OvernightJobOutcome(
                job=job_key,
                status="OK" if ok else "FAIL",
                duration_s=float(dur) if isinstance(dur, (int, float)) else None,
                finished_utc=ts,
                failures=fails,
            )
        )

    return outcomes


def collect_overnight_job_outcomes(
    *,
    as_of: date,
    log_group: str = _DEFAULT_LOG_GROUP,
    region: str | None = None,
    logs_client: Any | None = None,
) -> list[OvernightJobOutcome]:
    """Fetch CloudWatch summaries and build overnight outcomes (ERROR row on failure)."""
    start, end = overnight_window_utc(as_of)
    try:
        summaries = fetch_run_summaries_from_logs(
            start=start,
            end=end,
            log_group=log_group,
            region=region,
            logs_client=logs_client,
        )
    except Exception as e:  # noqa: BLE001 — surface in report, don't abort morning KPIs
        logger.warning("Overnight job status unavailable: %s", e)
        return build_overnight_job_outcomes(as_of=as_of, fetch_error=str(e))
    return build_overnight_job_outcomes(as_of=as_of, summaries=summaries)


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds / 60.0:.1f}m"


def _fmt_finished(ts: datetime | None) -> str:
    if ts is None:
        return "—"
    return ts.astimezone(timezone.utc).strftime("%H:%M")


def _wrap_text(text: str, *, width: int, prefix: str = "") -> list[str]:
    """Wrap *text* to *width* without dropping characters (hard-wrap only if needed)."""
    if width <= 0:
        return [prefix + text] if text else [prefix.rstrip()]
    avail = max(1, width - len(prefix))
    if len(text) <= avail:
        return [f"{prefix}{text}"]
    lines: list[str] = []
    rest = text
    while rest:
        chunk = rest[:avail]
        rest = rest[avail:]
        lines.append(f"{prefix}{chunk}")
    return lines


def format_overnight_jobs_section(
    outcomes: list[OvernightJobOutcome],
    *,
    line_width: int = _LINE_WIDTH,
) -> list[str]:
    """Plain-text section lines for the morning report (*line_width* columns)."""
    status_w = max(len("STATUS"), max((len(o.status) for o in outcomes), default=0))
    dur_w = max(len("DURATION"), max((len(_fmt_duration(o.duration_s)) for o in outcomes), default=0))
    fin_w = max(len("FINISHED"), max((len(_fmt_finished(o.finished_utc)) for o in outcomes), default=0))
    gaps = 2 * 3  # three "  " separators
    job_w = max(len("JOB"), max((len(o.job) for o in outcomes), default=0))
    natural = job_w + status_w + dur_w + fin_w + gaps
    if natural < line_width:
        job_w += line_width - natural
    elif natural > line_width:
        # Last resort: shrink JOB only so STATUS/DURATION/FINISHED stay full-width.
        job_w = max(len("JOB"), line_width - status_w - dur_w - fin_w - gaps)

    header = (
        f"{'JOB':<{job_w}}  "
        f"{'STATUS':<{status_w}}  "
        f"{'DURATION':>{dur_w}}  "
        f"{'FINISHED':>{fin_w}}"
    )
    rule = "-" * len(header)

    lines = ["LAST NIGHT'S JOBS", header, rule]
    if not outcomes:
        lines.append("(none)")
        return lines

    for row in outcomes:
        job = row.job
        if len(job) > job_w:
            job = job[: job_w - 1] + "…"  # only when job name itself exceeds budget
        line = (
            f"{job:<{job_w}}  "
            f"{row.status:<{status_w}}  "
            f"{_fmt_duration(row.duration_s):>{dur_w}}  "
            f"{_fmt_finished(row.finished_utc):>{fin_w}}"
        )
        lines.append(line)
        if row.detail:
            lines.extend(_wrap_text(row.detail, width=line_width, prefix="  "))
        for fail in row.failures:
            lines.extend(_wrap_text(f"- {fail}", width=line_width, prefix="  "))
    return lines


def overnight_failure_count(outcomes: list[OvernightJobOutcome]) -> int:
    return sum(1 for o in outcomes if o.status in {"FAIL", "MISSING", "ERROR"})
