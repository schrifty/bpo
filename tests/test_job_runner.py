"""Tests for declarative job runner."""

from __future__ import annotations

import pytest

from src.job_runner import build_step_argv, load_job_spec, run_job


def test_load_nightly_core_spec() -> None:
    spec = load_job_spec("nightly-core")
    assert spec.name == "nightly-core"
    assert len(spec.steps) == 3
    assert spec.steps[0]["deck_id"] == "engineering-portfolio"


def test_build_step_argv_portfolio() -> None:
    argv = build_step_argv({"command": "portfolio", "days": 30, "csm": "Alex"})
    assert argv == ["--portfolio", "--days", "30", "--csm", "Alex"]


def test_run_job_dry_run(capsys) -> None:
    code = run_job("engineering-portfolio", dry_run=True)
    assert code == 0
    out = capsys.readouterr().out
    assert "engineering-portfolio" in out
    assert "python3 decks.py" in out


def test_build_step_argv_unknown_command() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        build_step_argv({"command": "unknown"})
