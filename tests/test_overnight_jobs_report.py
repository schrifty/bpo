"""Tests for overnight ECS job status in the morning report."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from src.overnight_jobs_report import (
    OvernightJobOutcome,
    build_overnight_job_outcomes,
    extract_run_summary,
    format_overnight_jobs_section,
    overnight_failure_count,
    overnight_window_utc,
)


def test_overnight_window_utc() -> None:
    start, end = overnight_window_utc(date(2026, 8, 3))
    assert start.isoformat() == "2026-08-03T02:00:00+00:00"
    assert end.isoformat() == "2026-08-03T11:45:00+00:00"


def test_extract_run_summary_from_json_log() -> None:
    summary = {
        "event": "run_complete",
        "success": True,
        "job": "export-nightly",
        "duration_s": 540.0,
    }
    wrapped = json.dumps(
        {
            "timestamp": "2026-08-03T06:10:02.990Z",
            "level": "INFO",
            "message": "CORTEX_RUN_SUMMARY=" + json.dumps(summary, separators=(",", ":")),
        }
    )
    out = extract_run_summary(wrapped)
    assert out is not None
    assert out["job"] == "export-nightly"
    assert out["success"] is True


def test_build_outcomes_marks_fail_and_missing() -> None:
    as_of = date(2026, 8, 3)
    summaries = [
        (
            datetime(2026, 8, 3, 6, 41, tzinfo=timezone.utc),
            {
                "success": False,
                "job": "engineering-portfolio",
                "duration_s": 120.0,
                "failures": ["deck: boom"],
            },
        ),
        (
            datetime(2026, 8, 3, 3, 13, tzinfo=timezone.utc),
            {"success": True, "job": "pendo-snapshot-refresh", "duration_s": 718.6},
        ),
    ]
    outcomes = build_overnight_job_outcomes(as_of=as_of, summaries=summaries)
    by_job = {o.job: o for o in outcomes}
    assert by_job["pendo-snapshot-refresh"].status == "OK"
    assert by_job["engineering-portfolio"].status == "FAIL"
    assert by_job["export-nightly"].status == "MISSING"
    assert overnight_failure_count(outcomes) >= 2
    assert "metrics-eng-cycle-lead-weekly" not in by_job


def test_format_overnight_section_width() -> None:
    outcomes = [
        OvernightJobOutcome(
            job="pendo-snapshot-refresh",
            status="OK",
            duration_s=718.6,
            finished_utc=datetime(2026, 8, 3, 3, 13, tzinfo=timezone.utc),
        ),
        OvernightJobOutcome(
            job="engineering-portfolio",
            status="FAIL",
            duration_s=143.6,
            finished_utc=datetime(2026, 8, 3, 6, 41, tzinfo=timezone.utc),
            failures=("deck failure: " + ("x" * 100),),
        ),
    ]
    lines = format_overnight_jobs_section(outcomes)
    assert lines[0] == "LAST NIGHT'S JOBS"
    assert len(lines[1]) == 128
    assert len(lines[2]) == 128
    joined = "\n".join(lines)
    assert "deck failure: " in joined
    assert "x" * 100 in joined.replace("\n", "").replace("  ", "")
    for line in lines:
        assert len(line) <= 128, repr(line)
