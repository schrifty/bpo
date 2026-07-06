"""Tests for EventBridge schedule reporting."""

from __future__ import annotations

from src.ecs_schedule_report import (
    ScheduleRow,
    _command_from_target_input,
    build_schedule_rows,
    format_schedule_table,
    schedule_main,
)


def test_format_schedule_table_aligns_columns():
    rows = [
        ScheduleRow(
            job_key="export-nightly",
            rule_name="cortex-export-nightly",
            state="ENABLED",
            schedule_expression="cron(0 3 * * ? *)",
            command=["export-nightly"],
            summary="export",
            source="aws",
        ),
        ScheduleRow(
            job_key="other-job",
            rule_name="cortex-other-job",
            state="ENABLED",
            schedule_expression="cron(0 4 * * ? *)",
            command=["other-job"],
            summary="other",
            source="aws",
        ),
    ]
    text = format_schedule_table(rows)
    assert "export-nightly" in text
    assert "cortex-export-nightly" in text
    assert "cron(0 3 * * ? *)" in text


def test_build_schedule_rows_merges_catalog_when_aws_empty(monkeypatch):
    monkeypatch.setattr(
        "src.ecs_schedule_report.fetch_aws_schedule_rows",
        lambda **kwargs: ([], "AWS lookup failed (test); showing catalog only"),
    )
    rows, notes = build_schedule_rows(name_prefix="cortex", region="us-east-1")
    assert any(r.job_key == "export-nightly" for r in rows)
    assert any(r.job_key == "engineering-portfolio" for r in rows)
    assert any(r.job_key == "ford-pendo-7d" for r in rows)
    assert any(r.job_key == "ford-pendo-30d" for r in rows)
    top_arr = next(r for r in rows if r.job_key == "pendo-top-arr-30d")
    assert top_arr.rule_name == "cortex-pendo-top-arr-30d"
    assert top_arr.schedule_expression == "cron(0 9 * * ? *)"
    weekly = next(r for r in rows if r.job_key == "metrics-eng-cycle-lead-weekly")
    assert weekly.rule_name == "cortex-metrics-eng-cycle-lead-weekly"
    assert weekly.schedule_expression == "cron(0 5 ? * MON *)"
    eng = next(r for r in rows if r.job_key == "engineering-portfolio")
    assert eng.rule_name == "cortex-engineering-portfolio"
    assert eng.schedule_expression == "cron(30 1 * * ? *)"
    export = next(r for r in rows if r.job_key == "export-nightly")
    assert export.rule_name == "cortex-export-nightly"
    assert export.schedule_expression == "cron(0 1 * * ? *)"
    ford_7d = next(r for r in rows if r.job_key == "ford-pendo-7d")
    assert ford_7d.rule_name == "cortex-ford-pendo-7d"
    ford_30d = next(r for r in rows if r.job_key == "ford-pendo-30d")
    assert ford_30d.rule_name == "cortex-ford-pendo-30d"
    assert ford_30d.schedule_expression == "cron(30 2 * * ? *)"
    assert notes


def test_command_from_target_input_parses_ecs_override():
    raw = '{"containerOverrides":[{"name":"cortex-decks","command":["export-nightly"]}]}'
    assert _command_from_target_input(raw) == ["export-nightly"]


def test_command_from_target_input_empty_when_invalid():
    assert _command_from_target_input(None) == []
    assert _command_from_target_input("{bad json") == []


def test_schedule_main_prints_table(monkeypatch, capsys):
    monkeypatch.setattr(
        "src.ecs_schedule_report.build_schedule_rows",
        lambda **kwargs: (
            [
                ScheduleRow(
                    job_key="export-nightly",
                    rule_name="cortex-export-nightly",
                    state="ENABLED",
                    schedule_expression="cron(0 3 * * ? *)",
                    command=["export-nightly"],
                    summary="export",
                    source="aws",
                )
            ],
            [],
        ),
    )
    code = schedule_main(["--prefix", "cortex", "--region", "us-east-1"])
    out = capsys.readouterr().out
    assert code == 0
    assert "cortex-export-nightly" in out
    assert "cron(0 3 * * ? *)" in out
