"""Guardrails for live LeanDNA Data API integration tests."""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]


def assert_leandna_staging_integration_bucket(*, bucket: str, execution_env: str) -> None:
    """Apply staging-only policy given a resolved config bucket."""
    raw_env = (execution_env or "").strip()
    if bucket == "production":
        pytest.fail(
            f"LeanDNA integration tests refuse to run when EXECUTION_ENV={raw_env!r} "
            "(production / PR_* credentials). Set EXECUTION_ENV=Staging with "
            "ST_LEANDNA_DATA_API_* in .env before running live tests."
        )
    if bucket != "staging":
        pytest.skip(
            "LeanDNA integration tests require EXECUTION_ENV=Staging (ST_* credentials); "
            f"got EXECUTION_ENV={raw_env!r} (bucket={bucket!r}). "
            "Set EXECUTION_ENV=Staging in .env to run live tests."
        )


def require_leandna_staging_integration_env(
    *,
    dotenv_path: Path | None = None,
    reload_config: bool = True,
) -> None:
    """Ensure live LeanDNA tests target staging only.

    - ``EXECUTION_ENV=Production`` or ``CI`` (production bucket): ``pytest.fail``.
    - Legacy / unset / unknown buckets: ``pytest.skip``.
    - Staging: return (tests use ``ST_*`` credentials).
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")

    env_file = dotenv_path or (_ROOT / ".env")
    load_dotenv(env_file, override=True)

    if reload_config:
        import src.config as cfg

        importlib.reload(cfg)
    else:
        import src.config as cfg

    assert_leandna_staging_integration_bucket(
        bucket=cfg.CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET,
        execution_env=os.environ.get("EXECUTION_ENV") or "",
    )
