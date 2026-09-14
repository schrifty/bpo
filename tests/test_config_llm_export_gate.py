"""Production/ECS opt-in for Slack and §7 third-party LLM calls in export-all."""

from __future__ import annotations

import pytest


def test_local_non_aws_allows_llm_export_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "legacy")
    monkeypatch.setattr(cfg, "_is_aws_runtime", lambda: False)
    monkeypatch.delenv("CORTEX_ALLOW_PRODUCTION_LLM_EXPORT", raising=False)
    assert cfg.production_llm_customer_export_requires_opt_in() is False
    assert cfg.production_llm_customer_export_allowed() is True
    assert cfg.production_llm_customer_export_block_reason() is None


def test_production_blocks_llm_export_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.setattr(cfg, "_is_aws_runtime", lambda: False)
    monkeypatch.delenv("CORTEX_ALLOW_PRODUCTION_LLM_EXPORT", raising=False)
    assert cfg.production_llm_customer_export_requires_opt_in() is True
    assert cfg.production_llm_customer_export_allowed() is False
    reason = cfg.production_llm_customer_export_block_reason()
    assert reason is not None
    assert "CORTEX_ALLOW_PRODUCTION_LLM_EXPORT" in reason


def test_ecs_blocks_llm_export_even_without_execution_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "legacy")
    monkeypatch.setattr(cfg, "_is_aws_runtime", lambda: True)
    monkeypatch.delenv("CORTEX_ALLOW_PRODUCTION_LLM_EXPORT", raising=False)
    assert cfg.production_llm_customer_export_allowed() is False


def test_production_llm_export_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.setattr(cfg, "_is_aws_runtime", lambda: False)
    monkeypatch.setenv("CORTEX_ALLOW_PRODUCTION_LLM_EXPORT", "true")
    assert cfg.production_llm_customer_export_allowed() is True
    assert cfg.production_llm_customer_export_block_reason() is None
