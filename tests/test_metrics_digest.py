"""Tests for morning KPI digest (target/direction, formatting, SES wiring)."""

from __future__ import annotations

import pytest

from src.metrics_digest import (
    DigestRow,
    format_digest_body,
    format_digest_subject,
    is_off_target,
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

    body = format_digest_body(rows, as_of="2026-08-03")
    assert "OFF TARGET" in body
    assert "NAME" in body and "ID" in body and "VALUE" in body and "TARGET" in body
    assert body.index("Mid") < body.index("ALL OTHER")
    assert body.index("Zebra") < body.index("Alpha")
    assert "error: fail" in body
    # Columnar rows should include padded name then id cells
    assert "Zebra" in body and "1" in body
    assert "—" in body  # missing metric id for Mid

    subj = format_digest_subject(rows, as_of="2026-08-03")
    assert subj == "KPI digest 2026-08-03 — 2 off target"


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
    result = run_metrics_digest(dry_run=True, as_of="2026-08-03", registry=registry)
    assert result.sent is False
    assert result.error is None
    assert "2 off target" in result.subject
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
    )
    assert result.sent is True
    assert result.message_id == "abc-123"
    assert calls and calls[0]["to"] == ["you@example.com"]
    assert "KPI digest" in calls[0]["subject"]


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
