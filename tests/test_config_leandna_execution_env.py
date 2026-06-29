"""Unit tests for LeanDNA Data API env resolution (EXECUTION_ENV / ST_* / PR_*)."""

from __future__ import annotations

import pytest


def test_resolve_base_url_legacy_empty_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "legacy")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    assert cfg.resolve_leandna_data_api_base_url() == "https://app.leandna.com/api"


def test_resolve_base_url_staging_requires_base(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "staging")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="ST_LEANDNA_DATA_API_BASE_URL"):
        cfg.resolve_leandna_data_api_base_url()


def test_resolve_base_url_production_requires_base(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="PR_LEANDNA_DATA_API_BASE_URL"):
        cfg.resolve_leandna_data_api_base_url()


def test_resolve_base_url_none_bucket_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "none")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="EXECUTION_ENV"):
        cfg.resolve_leandna_data_api_base_url()


def test_execution_env_production_blocks_mutations(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.delenv("CORTEX_ALLOW_PRODUCTION_MUTATIONS", raising=False)
    assert cfg.execution_env_disallows_http_mutations() is True
    assert cfg.leandna_http_mutations_allowed() is False
    blocked = cfg.leandna_http_mutation_blocked_envelope(method="POST", path="Metric/1/MetricDataPoint")
    assert blocked is not None
    assert blocked["ok"] is False
    assert "POST" in blocked["error"]


def test_execution_env_staging_allows_mutations(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "staging")
    assert cfg.execution_env_disallows_http_mutations() is False
    assert cfg.leandna_http_mutations_allowed() is True
    assert cfg.leandna_http_mutation_blocked_envelope(method="DELETE") is None


def test_production_mutations_override(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.setenv("CORTEX_ALLOW_PRODUCTION_MUTATIONS", "true")
    assert cfg.leandna_http_mutations_allowed() is True
    assert cfg.leandna_http_mutation_blocked_envelope(method="POST") is None


def test_data_api_get_json_returns_envelope_when_base_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.leandna_data_api_request import data_api_get_json

    def _boom() -> str:
        raise ValueError("no base")

    monkeypatch.setattr("src.leandna_data_api_request.data_api_base_url", _boom)
    out = data_api_get_json("Metric")
    assert out["ok"] is False
    assert "no base" in out["error"]
