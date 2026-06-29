"""Tests for run context propagation."""

from __future__ import annotations

from src.run_context import init_run_context, run_context_fields, set_run_phase


def test_init_run_context_and_fields() -> None:
    rid = init_run_context(job_name="nightly-core", deck_id="engineering-portfolio")
    assert rid
    set_run_phase("enrichment")
    fields = run_context_fields()
    assert fields["run_id"] == rid
    assert fields["job_name"] == "nightly-core"
    assert fields["deck_id"] == "engineering-portfolio"
    assert fields["phase"] == "enrichment"
