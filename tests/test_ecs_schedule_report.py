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
            job_key="llm-context-portfolio-daily",
            rule_name="cortex-llm-context-portfolio-daily",
            state="ENABLED",
            schedule_expression="cron(0 7 * * ? *)",
            command=["llm-context-portfolio-daily"],
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
    assert "llm-context-portfolio-daily" in text
    assert "cortex-llm-context-portfolio-daily" in text
    assert "cron(0 7 * * ? *)" in text


def test_build_schedule_rows_merges_catalog_when_aws_empty(monkeypatch):
    monkeypatch.setattr(
        "src.ecs_schedule_report.fetch_aws_schedule_rows",
        lambda **kwargs: ([], "AWS lookup failed (test); showing catalog only"),
    )
    rows, notes = build_schedule_rows(name_prefix="cortex", region="us-east-1")
    assert any(r.job_key == "pendo-snapshot-refresh" for r in rows)
    assert any(r.job_key == "llm-context-portfolio-daily" for r in rows)
    assert any(r.job_key == "engineering-portfolio" for r in rows)
    assert any(r.job_key == "pendo-ford-7d" for r in rows)
    assert any(r.job_key == "pendo-ford-30d" for r in rows)
    assert any(r.job_key == "morning-report" for r in rows)
    snap = next(r for r in rows if r.job_key == "pendo-snapshot-refresh")
    assert snap.rule_name == "cortex-pendo-snapshot-refresh"
    assert snap.schedule_expression == "cron(0 3 * * ? *)"
    top_arr = next(r for r in rows if r.job_key == "pendo-top-arr-detailed")
    assert top_arr.rule_name == "cortex-pendo-top-arr-detailed"
    assert top_arr.schedule_expression == "cron(0 9 * * ? *)"
    csr_6 = next(r for r in rows if r.job_key == "csr-dump-0600")
    assert csr_6.rule_name == "cortex-csr-dump-0600"
    assert csr_6.schedule_expression == "cron(0 11 * * ? *)"
    assert csr_6.source == "catalog"
    assert not any(r.job_key == "pendo-top-arr-30d" for r in rows)
    assert not any(r.job_key == "carrier-pendo-detailed-30d" for r in rows)
    assert not any(r.job_key == "metrics-eng-cycle-lead-weekly" for r in rows)
    digest = next(r for r in rows if r.job_key == "morning-report")
    assert digest.rule_name == "cortex-morning-report"
    assert digest.schedule_expression == "cron(0 12 * * ? *)"
    assert digest.state == "DISABLED"
    eng = next(r for r in rows if r.job_key == "engineering-portfolio")
    assert eng.rule_name == "cortex-engineering-portfolio"
    assert eng.schedule_expression == "cron(30 7 * * ? *)"
    export = next(r for r in rows if r.job_key == "llm-context-portfolio-daily")
    assert export.rule_name == "cortex-llm-context-portfolio-daily"
    assert export.schedule_expression == "cron(0 7 * * ? *)"
    ford_7d = next(r for r in rows if r.job_key == "pendo-ford-7d")
    assert ford_7d.rule_name == "cortex-pendo-ford-7d"
    ford_30d = next(r for r in rows if r.job_key == "pendo-ford-30d")
    assert ford_30d.rule_name == "cortex-pendo-ford-30d"
    assert ford_30d.schedule_expression == "cron(30 8 * * ? *)"
    assert notes


def test_command_from_target_input_parses_ecs_override():
    raw = '{"containerOverrides":[{"name":"cortex-decks","command":["llm-context-portfolio-daily"]}]}'
    assert _command_from_target_input(raw) == ["llm-context-portfolio-daily"]


def test_command_from_target_input_empty_when_invalid():
    assert _command_from_target_input(None) == []
    assert _command_from_target_input("{bad json") == []


def test_schedule_main_prints_table(monkeypatch, capsys):
    monkeypatch.setattr(
        "src.ecs_schedule_report.build_schedule_rows",
        lambda **kwargs: (
            [
                ScheduleRow(
                    job_key="llm-context-portfolio-daily",
                    rule_name="cortex-llm-context-portfolio-daily",
                    state="ENABLED",
                    schedule_expression="cron(0 7 * * ? *)",
                    command=["llm-context-portfolio-daily"],
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
    assert "cortex-llm-context-portfolio-daily" in out
    assert "cron(0 7 * * ? *)" in out
