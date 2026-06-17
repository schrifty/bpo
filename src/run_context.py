"""Run-scoped context for batch jobs (run_id, job name, deck, customer, phase)."""

from __future__ import annotations

import os
import uuid
from contextvars import ContextVar
from typing import Any

_run_id: ContextVar[str | None] = ContextVar("bpo_run_id", default=None)
_job_name: ContextVar[str | None] = ContextVar("bpo_job_name", default=None)
_deck_id: ContextVar[str | None] = ContextVar("bpo_deck_id", default=None)
_customer: ContextVar[str | None] = ContextVar("bpo_customer", default=None)
_phase: ContextVar[str | None] = ContextVar("bpo_phase", default=None)


def new_run_id() -> str:
    return uuid.uuid4().hex


def _env_run_id() -> str | None:
    raw = os.environ.get("BPO_RUN_ID", "").strip()
    return raw or None


def current_run_id() -> str:
    rid = _run_id.get()
    if rid:
        return rid
    env_rid = _env_run_id()
    if env_rid:
        return env_rid
    rid = new_run_id()
    _run_id.set(rid)
    return rid


def init_run_context(
    *,
    run_id: str | None = None,
    job_name: str | None = None,
    deck_id: str | None = None,
    customer: str | None = None,
    phase: str | None = None,
) -> str:
    """Seed context for a top-level batch run; returns the active run_id."""
    rid = (run_id or _env_run_id() or new_run_id()).strip()
    _run_id.set(rid)
    if job_name is not None:
        _job_name.set(job_name)
    if deck_id is not None:
        _deck_id.set(deck_id)
    if customer is not None:
        _customer.set(customer)
    if phase is not None:
        _phase.set(phase)
    return rid


def set_run_phase(phase: str | None) -> None:
    _phase.set(phase)


def set_run_deck(deck_id: str | None) -> None:
    _deck_id.set(deck_id)


def set_run_customer(customer: str | None) -> None:
    _customer.set(customer)


def run_context_fields() -> dict[str, str]:
    out: dict[str, str] = {}
    rid = _run_id.get() or _env_run_id()
    if rid:
        out["run_id"] = rid
    for key, var in (
        ("job_name", _job_name),
        ("deck_id", _deck_id),
        ("customer", _customer),
        ("phase", _phase),
    ):
        val = var.get()
        if val:
            out[key] = val
    step = os.environ.get("BPO_STEP_NAME", "").strip()
    if step and "phase" not in out:
        out["phase"] = step
    job_env = os.environ.get("BPO_JOB_NAME", "").strip()
    if job_env and "job_name" not in out:
        out["job_name"] = job_env
    return out


def enrich_log_record(record: Any) -> None:
    for key, val in run_context_fields().items():
        if not hasattr(record, key) or getattr(record, key) in (None, ""):
            setattr(record, key, val)
