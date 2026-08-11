"""Tests for morning KPI digest (target/direction, formatting, SES wiring)."""

from __future__ import annotations

import pytest

from src.metrics_digest import (
    DigestRow,
    format_digest_body,
    format_digest_subject,
    is_off_target,
    metrics_deck_display_title,
    metrics_deck_scorecard_month,
    partition_digest_rows,
    run_metrics_digest,
    scalar_from_parts,
)
from src.metrics_registry import (
    registry_metric_direction,
    registry_metric_target,
    validate_metric_target_direction,
)
from src.metrics_upsert import MetricParts
from src.ses_email import SesEmailError, send_email


def test_registry_metric_target_and_direction() -> None:
    entry = {"target": 160, "direction": "lower"}
    assert registry_metric_target(entry) == 160.0
    assert registry_metric_direction(entry) == "lower"
    assert validate_metric_target_direction(entry) is None


def test_registry_target_requires_direction() -> None:
    err = validate_metric_target_direction({"target": 10})
    assert err is not None
    assert "direction" in err


def test_registry_invalid_direction() -> None:
    with pytest.raises(ValueError, match="higher"):
        registry_metric_direction({"direction": "sideways"})


def test_digest_currency_value_and_target_display() -> None:
    from src.metrics_digest import _format_digest_number
    from src.metrics_registry import digest_display_unit, registry_metric_unit

    assert registry_metric_unit({"unit": "currency"}) == "currency"
    assert registry_metric_unit({"unit": "percent"}) == "percent"
    assert registry_metric_unit({}) is None
    assert digest_display_unit("% Growth Allocation", {}) == "percent"
    assert digest_display_unit("AI Spend %", {}) == "percent"
    assert digest_display_unit("PRs Merged", {}) is None
    assert _format_digest_number(10342.41, unit="currency") == "$10,342.41"
    assert _format_digest_number(50.0, unit="currency") == "$50"
    assert _format_digest_number(73.81, unit="percent") == "73.81%"
    assert _format_digest_number(80.0, unit="percent") == "80%"
    assert _format_digest_number(901253073.8) == "901,253,073.8"
    assert _format_digest_number(281.0) == "281"
    row = DigestRow(
        "AI Spend / Issue",
        None,
        33.7769,
        50.0,
        "lower",
        False,
        unit="currency",
    )
    assert row.value_display == "$33.78"
    assert row.target_display == "$50"
    pct = DigestRow(
        "Weekly Active AI Users",
        None,
        73.81,
        80.0,
        "higher",
        True,
        unit="percent",
    )
    assert pct.value_display == "73.81%"
    assert pct.target_display == "80%"


def test_metrics_deck_display_title_uses_scorecard_month_name() -> None:
    from datetime import date

    assert metrics_deck_scorecard_month("2026-08-01") == date(2026, 7, 1)
    assert metrics_deck_scorecard_month("2026-07-01") == date(2026, 6, 1)
    assert metrics_deck_scorecard_month("2026-01-15") == date(2025, 12, 1)
    assert metrics_deck_display_title("akkr", "2026-08-01") == "AKKR Metrics - July"
    assert metrics_deck_display_title("akkr", "2026-07-01") == "AKKR Metrics - June"
    assert metrics_deck_display_title("akkr", "2026-06-01") == "AKKR Metrics - May"
    assert metrics_deck_display_title(None, "2026-08-01") == "KPI Metrics - July"


def test_generate_digest_row_prefers_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spend/issue returns n/d plus value; digest must use value (USD), not (n/d)*100."""
    from unittest.mock import MagicMock

    from src.metrics_digest import generate_digest_row

    monkeypatch.setattr(
        "src.metrics_digest.invoke_metric_generator",
        lambda *a, **k: {"numerator": 9491.0, "denominator": 281.0, "value": 33.78},
    )
    row = generate_digest_row(
        "AI Spend / Issue",
        {
            "metric-generator": "get_ai_spend_per_issue",
            "target": 50,
            "direction": "lower",
            "unit": "currency",
        },
        registry={"metrics": {}},
        ctx=MagicMock(),
    )
    assert row.value == 33.78
    assert row.value_display == "$33.78"
    assert row.target_display == "$50"
    assert row.off_target is False


def test_is_off_target_lower_and_higher() -> None:
    assert is_off_target(200.0, target=160.0, direction="lower") is True
    assert is_off_target(100.0, target=160.0, direction="lower") is False
    assert is_off_target(40.0, target=50.0, direction="higher") is True
    assert is_off_target(60.0, target=50.0, direction="higher") is False


def test_is_off_target_errors_and_missing_target() -> None:
    assert is_off_target(None, target=1.0, direction="lower", error="boom") is True
    assert is_off_target(10.0, target=None, direction=None) is False


def test_scalar_from_parts() -> None:
    assert scalar_from_parts(MetricParts(42.0, 1.0)) == 42.0
    assert scalar_from_parts(MetricParts(1.0, 2.0)) == 50.0


def test_partition_and_format_off_target_first() -> None:
    rows = [
        DigestRow("Zebra", 1, 10.0, 5.0, "lower", True),
        DigestRow("Alpha", 2, 3.0, 5.0, "lower", False),
        DigestRow("Mid", None, None, 1.0, "higher", True, error="fail"),
    ]
    off, rest = partition_digest_rows(rows)
    assert [r.name for r in off] == ["Mid", "Zebra"]
    assert [r.name for r in rest] == ["Alpha"]

    body = format_digest_body(rows, as_of="2026-08-03", overnight=[])
    assert body.startswith("Morning report — 2026-08-03")
    assert "LAST NIGHT'S JOBS" in body
    assert "OFF TARGET" in body
    assert "NAME" in body and "ID" in body and "VALUE" in body and "TARGET" in body
    assert "DIR" in body and "TAGS" in body and "DESCRIPTION" in body
    assert body.index("LAST NIGHT'S JOBS") < body.index("OFF TARGET")
    assert body.index("Mid") < body.index("ALL OTHER")
    assert body.index("Zebra") < body.index("Alpha")
    assert "error" in body
    assert "fail" in body
    # Columnar rows should include padded name then id cells
    assert "Zebra" in body and "1" in body
    assert "—" in body  # missing metric id for Mid

    subj = format_digest_subject(rows, as_of="2026-08-03", overnight=[])
    assert subj == "Morning report 2026-08-03 — 2 off target"


def test_morning_report_leads_with_overnight_jobs() -> None:
    from datetime import datetime, timezone

    from src.overnight_jobs_report import OvernightJobOutcome

    rows = [DigestRow("Alpha", 2, 3.0, 5.0, "lower", False)]
    overnight = [
        OvernightJobOutcome(
            job="export-nightly",
            status="OK",
            duration_s=540.0,
            finished_utc=datetime(2026, 8, 3, 6, 10, tzinfo=timezone.utc),
        ),
        OvernightJobOutcome(
            job="engineering-portfolio",
            status="FAIL",
            duration_s=143.6,
            finished_utc=datetime(2026, 8, 3, 6, 41, tzinfo=timezone.utc),
            failures=("deck: FAIL 401",),
        ),
    ]
    body = format_digest_body(rows, as_of="2026-08-03", overnight=overnight)
    assert body.index("LAST NIGHT'S JOBS") < body.index("OFF TARGET")
    assert "export-nightly" in body and "OK" in body
    assert "FAIL" in body and "deck: FAIL 401" in body
    subj = format_digest_subject(rows, as_of="2026-08-03", overnight=overnight)
    assert subj == "Morning report 2026-08-03 — 1 job issue(s), 0 off target"


def test_digest_report_uses_compact_natural_widths() -> None:
    from src.metrics_digest import column_widths_for_digest_rows, format_digest_lines

    rows = [
        DigestRow("Short", 1, 10.0, 5.0, "lower", True, tags=("akkr",)),
        DigestRow("Median TTR", 2171, 12345.67, 9999.0, "higher", False, tags=("support",)),
        DigestRow(
            "Engineering Cycle Time (Sprint)",
            2024,
            0.67,
            5.0,
            "lower",
            False,
            tags=("engineering", "impact"),
        ),
    ]
    widths = column_widths_for_digest_rows(rows)
    assert "TAGS" in widths.header
    assert widths.header.index("DIR") < widths.header.index("TAGS")
    assert len(widths.rule) == len(widths.header)
    for row in rows:
        lines = format_digest_lines(row, widths=widths)
        assert len(lines[0]) == len(widths.header)
        assert row.name in lines[0]
        assert any(row.value_display in line for line in lines)
        assert row.tags_display in lines[0]


def test_digest_akkr_tags_appear_after_dir() -> None:
    from src.metrics_digest import format_digest_lines

    row = DigestRow(
        "Weekly Active AI Users",
        None,
        79.4,
        80.0,
        "higher",
        True,
        tags=("engineering", "ai", "akkr"),
    )
    lines = format_digest_lines(row)
    primary = lines[0]
    assert "higher" in primary
    assert "akkr" in primary
    assert primary.index("higher") < primary.index("akkr")


def test_digest_error_detail_follows_row_truncated_to_132() -> None:
    from src.metrics_digest import (
        _DIGEST_DETAIL_MAX,
        column_widths_for_digest_rows,
        format_digest_body,
        format_digest_lines,
    )

    long_error = "GitHubError: GitHub API HTTP 403 for https://api.github.com/orgs/leandna-apex/repos " + (
        "SAML enforcement. " * 20
    )
    rows = [
        DigestRow(
            "Weekly Active AI Users",
            None,
            None,
            1.0,
            "higher",
            True,
            error=long_error,
            description="% of Engineering using Cursor in the last 7 days.",
        ),
        DigestRow("Median TTR", 2171, 48.0, 160.0, "lower", False),
        DigestRow("AI Token Usage", None, 485092511.0, 1.0, "higher", False),
    ]
    widths = column_widths_for_digest_rows(rows)
    assert "DESCRIPTION" in widths.header
    assert widths.value <= 24
    lines = format_digest_lines(rows[0], widths=widths)
    assert len(lines) == 2  # primary (with description) + error
    assert len(lines[0]) == len(widths.header)
    assert "error" in lines[0]
    assert "Engineering" in lines[0] and "Cursor" in lines[0]
    detail = lines[1].lstrip()
    assert len(detail) == _DIGEST_DETAIL_MAX
    assert detail.endswith("…")
    assert detail.startswith("GitHubError:")
    assert long_error not in "\n".join(lines)  # full text not dumped


def test_digest_prints_registry_description_truncated() -> None:
    from src.metrics_digest import _DIGEST_DESCRIPTION_MAX, format_digest_lines

    assert _DIGEST_DESCRIPTION_MAX == 185
    long_desc = "A" * 250
    row = DigestRow(
        "Issues Shipped",
        None,
        281.0,
        20.0,
        "higher",
        False,
        description=long_desc,
    )
    lines = format_digest_lines(row)
    assert len(lines) == 1
    assert "281" in lines[0]
    assert ("A" * (_DIGEST_DESCRIPTION_MAX - 1) + "…") in lines[0]



def test_run_metrics_digest_filters_by_tag(monkeypatch) -> None:
    registry = {
        "metrics": {
            "AKKR KPI": {
                "metric-id": 1,
                "metric-generator": "fake_akkr",
                "target": 50,
                "direction": "higher",
                "tags": ["akkr"],
            },
            "Other KPI": {
                "metric-id": 2,
                "metric-generator": "fake_other",
                "target": 10,
                "direction": "lower",
                "tags": ["support"],
            },
        }
    }

    def fake_invoke(name, *, registry, ctx):  # noqa: ARG001
        return {"value": 99}

    monkeypatch.setattr("src.metrics_digest.invoke_metric_generator", fake_invoke)
    result = run_metrics_digest(
        dry_run=True,
        as_of="2026-08-03",
        registry=registry,
        skip_overnight=True,
        tag="AKKR",
    )
    names = [r.name for r in result.rows]
    assert names == ["AKKR KPI"]
    assert "tag: akkr" in result.body
    assert "filter: akkr" in result.body
    assert "tag akkr" in result.subject
    assert "Other KPI" not in result.body
    assert "LAST NIGHT'S JOBS" not in result.body


def test_tag_filter_omits_overnight_even_when_passed(monkeypatch) -> None:
    from datetime import datetime, timezone

    from src.overnight_jobs_report import OvernightJobOutcome

    registry = {
        "metrics": {
            "AKKR KPI": {
                "metric-generator": "fake",
                "target": 1,
                "direction": "higher",
                "tags": ["akkr"],
            }
        }
    }
    monkeypatch.setattr(
        "src.metrics_digest.invoke_metric_generator",
        lambda *a, **k: {"value": 2},
    )
    overnight = [
        OvernightJobOutcome(
            job="export-nightly",
            status="FAIL",
            duration_s=1.0,
            finished_utc=datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
        )
    ]
    result = run_metrics_digest(
        dry_run=True,
        as_of="2026-08-03",
        registry=registry,
        overnight=overnight,
        tag="akkr",
    )
    assert "LAST NIGHT'S JOBS" not in result.body
    assert "job issue" not in result.subject
    assert "AKKR KPI" in result.body


def test_run_metrics_digest_dry_run_with_mocked_generators(monkeypatch) -> None:
    registry = {
        "metrics": {
            "High KPI": {
                "metric-id": 1,
                "metric-generator": "fake_high",
                "target": 50,
                "direction": "higher",
            },
            "Low KPI": {
                "metric-id": 2,
                "metric-generator": "fake_low",
                "target": 10,
                "direction": "lower",
            },
        }
    }

    def fake_invoke(name, *, registry, ctx):  # noqa: ARG001
        if name == "fake_high":
            return {"value": 40}
        return {"value": 25}

    monkeypatch.setattr("src.metrics_digest.invoke_metric_generator", fake_invoke)
    result = run_metrics_digest(dry_run=True, as_of="2026-08-03", registry=registry, skip_overnight=True)
    assert result.sent is False
    assert result.error is None
    assert "2 off target" in result.subject
    assert "Morning report" in result.subject
    # High: 40 < 50 (higher) → off; Low: 25 > 10 (lower) → off
    assert "OFF TARGET" in result.body
    assert "High KPI" in result.body
    assert "Low KPI" in result.body
    assert "ALL OTHER GENERATED METRICS" in result.body


def test_run_metrics_digest_sends_via_ses(monkeypatch) -> None:
    registry = {
        "metrics": {
            "OK KPI": {
                "metric-id": 9,
                "metric-generator": "fake_ok",
                "target": 10,
                "direction": "higher",
            }
        }
    }
    monkeypatch.setattr(
        "src.metrics_digest.invoke_metric_generator",
        lambda name, **kwargs: {"value": 20},  # noqa: ARG005
    )
    monkeypatch.setenv("CORTEX_METRICS_DIGEST_TO", "you@example.com")
    monkeypatch.setenv("CORTEX_METRICS_DIGEST_FROM", "from@example.com")

    calls: list[dict] = []

    def fake_send(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "message_id": "abc-123"}

    result = run_metrics_digest(
        dry_run=False,
        as_of="2026-08-03",
        registry=registry,
        send_fn=fake_send,
        skip_overnight=True,
    )
    assert result.sent is True
    assert result.message_id == "abc-123"
    assert calls and calls[0]["to"] == ["you@example.com"]
    assert "Morning report" in calls[0]["subject"]


def test_send_email_fails_loud_without_from(monkeypatch) -> None:
    monkeypatch.delenv("CORTEX_METRICS_DIGEST_FROM", raising=False)
    monkeypatch.setenv("CORTEX_METRICS_DIGEST_TO", "you@example.com")
    with pytest.raises(SesEmailError, match="FROM"):
        send_email(to="you@example.com", subject="x", body="y", from_addr="")


def test_metrics_digest_job_argv() -> None:
    from src.job_runner import build_step_argv, load_job_spec

    spec = load_job_spec("metrics-daily-digest")
    assert spec.name == "metrics-daily-digest"
    argv = build_step_argv(spec.steps[0])
    assert argv[0] == "metrics-digest"
    assert "--days" in argv
    assert "30" in argv
