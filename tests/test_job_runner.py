"""Tests for declarative job runner."""

from __future__ import annotations

import pytest

from src.job_runner import (
    _build_failures_payload,
    _extract_step_failure_messages,
    _summarize_step_error,
    build_step_argv,
    load_job_spec,
    run_job,
)


def test_load_nightly_core_spec() -> None:
    spec = load_job_spec("nightly-core")
    assert spec.name == "nightly-core"
    assert len(spec.steps) == 3
    assert spec.steps[0]["deck_id"] == "engineering-portfolio"


def test_build_step_argv_portfolio() -> None:
    argv = build_step_argv({"command": "portfolio", "days": 30, "csm": "Alex"})
    assert argv == ["--portfolio", "--days", "30", "--csm", "Alex"]


def test_build_step_argv_export_all() -> None:
    argv = build_step_argv({"command": "export-all", "days": 90})
    assert argv == ["export-all", "--days", "90"]


def test_build_step_argv_export_legacy_alias() -> None:
    argv = build_step_argv({"command": "export", "days": 90})
    assert argv == ["export-all", "--days", "90"]


def test_build_step_argv_metrics_upsert() -> None:
    argv = build_step_argv(
        {
            "command": "metrics-upsert",
            "metric": "Engineering Cycle Time (Sprint)",
            "days": 30,
        }
    )
    assert argv == [
        "metrics-upsert",
        "--metric",
        "Engineering Cycle Time (Sprint)",
        "--days",
        "30",
    ]


def test_load_metrics_eng_cycle_lead_weekly_job() -> None:
    spec = load_job_spec("metrics-eng-cycle-lead-weekly")
    assert spec.name == "metrics-eng-cycle-lead-weekly"
    assert len(spec.steps) == 4
    assert build_step_argv(spec.steps[0]) == [
        "metrics-upsert",
        "--metric",
        "Engineering Cycle Time (Sprint)",
        "--days",
        "30",
    ]
    assert build_step_argv(spec.steps[1]) == [
        "metrics-upsert",
        "--metric",
        "Engineering Lead Time (Days)",
        "--days",
        "30",
    ]
    assert build_step_argv(spec.steps[2]) == [
        "metrics-upsert",
        "--metric",
        "Tickets Beyond Service Thresholds",
    ]
    assert build_step_argv(spec.steps[3]) == [
        "metrics-upsert",
        "--metric",
        "Customer-Reported Bugs",
    ]


def test_run_job_dry_run(capsys) -> None:
    code = run_job("engineering-portfolio", dry_run=True)
    assert code == 0
    out = capsys.readouterr().out
    assert "engineering-portfolio" in out
    assert "python3 cortex.py" in out


def test_extract_step_failure_messages_from_preflight_output() -> None:
    stdout = (
        "Data source check failed — not running:\n"
        "  • Jira: HTTPSConnectionPool(host='api.atlassian.com', port=443): Read timed out.\n"
    )
    messages = _extract_step_failure_messages(stdout, "")
    assert any("Jira" in m for m in messages)


def test_extract_step_failure_messages_from_deck_fail_line() -> None:
    stdout = "Done in 12s\n  FAIL: Rate limit: quota exceeded. Wait and retry.\n"
    messages = _extract_step_failure_messages(stdout, "")
    assert messages == ["FAIL: Rate limit: quota exceeded. Wait and retry."]


def test_build_failures_payload_includes_failed_step_details() -> None:
    from src.job_runner import StepResult

    step = StepResult(
        name="engineering-portfolio",
        command="deck",
        success=False,
        exit_code=1,
        duration_s=12.3,
        error="Jira: timeout",
        detail_messages=["Jira: timeout"],
        stdout_tail="  FAIL: deck error\n",
        stderr_tail="warning: something\n",
    )
    payload = _build_failures_payload(
        "engineering-portfolio",
        "adb13090daa44a03ba677d4d9c813c4a",
        failures=["engineering-portfolio: Jira: timeout"],
        step_results=[step],
    )
    assert payload["failures"] == ["engineering-portfolio: Jira: timeout"]
    assert payload["steps"][0]["name"] == "engineering-portfolio"
    assert payload["steps"][0]["detail_messages"] == ["Jira: timeout"]
    assert "stdout_tail" in payload["steps"][0]


def test_summarize_step_error_prefers_detail_messages() -> None:
    err = _summarize_step_error(
        exit_code=1,
        detail_messages=["Jira: timeout", "FAIL: no slides"],
        stderr_tail="",
        stdout_tail="",
    )
    assert err.startswith("Jira: timeout")


def test_build_step_argv_unknown_command() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        build_step_argv({"command": "unknown"})
