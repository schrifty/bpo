"""Tests for LeanDNA live-integration environment guardrails."""

from __future__ import annotations

import pytest


def test_staging_bucket_allows_integration() -> None:
    from tests.leandna_integration_env import assert_leandna_staging_integration_bucket

    assert_leandna_staging_integration_bucket(bucket="staging", execution_env="Staging")


def test_production_bucket_fails_integration() -> None:
    from tests.leandna_integration_env import assert_leandna_staging_integration_bucket

    with pytest.raises(pytest.fail.Exception, match="refuse to run"):
        assert_leandna_staging_integration_bucket(bucket="production", execution_env="Production")


def test_ci_bucket_fails_integration() -> None:
    from tests.leandna_integration_env import assert_leandna_staging_integration_bucket

    with pytest.raises(pytest.fail.Exception, match="refuse to run"):
        assert_leandna_staging_integration_bucket(bucket="production", execution_env="CI")


def test_legacy_bucket_skips_integration() -> None:
    from tests.leandna_integration_env import assert_leandna_staging_integration_bucket

    with pytest.raises(pytest.skip.Exception, match="require EXECUTION_ENV=Staging"):
        assert_leandna_staging_integration_bucket(bucket="legacy", execution_env="")


def test_unknown_bucket_skips_integration() -> None:
    from tests.leandna_integration_env import assert_leandna_staging_integration_bucket

    with pytest.raises(pytest.skip.Exception, match="require EXECUTION_ENV=Staging"):
        assert_leandna_staging_integration_bucket(bucket="none", execution_env="dev")
