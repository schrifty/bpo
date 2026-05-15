"""Unit tests for LeanDNA Data API env resolution (EXECUTION_ENV / ST_* / PR_*)."""

from __future__ import annotations

import pytest


def test_resolve_base_url_legacy_empty_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "legacy")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    assert cfg.resolve_leandna_data_api_base_url() == "https://app.leandna.com/api"


def test_resolve_base_url_staging_requires_base(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "staging")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="ST_LEANDNA_DATA_API_BASE_URL"):
        cfg.resolve_leandna_data_api_base_url()


def test_resolve_base_url_production_requires_base(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "production")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="PR_LEANDNA_DATA_API_BASE_URL"):
        cfg.resolve_leandna_data_api_base_url()


def test_resolve_base_url_none_bucket_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.config as cfg

    monkeypatch.setattr(cfg, "BPO_LEANDNA_DATA_API_EXECUTION_BUCKET", "none")
    monkeypatch.setattr(cfg, "LEANDNA_DATA_API_BASE_URL", "")
    with pytest.raises(ValueError, match="EXECUTION_ENV"):
        cfg.resolve_leandna_data_api_base_url()


def test_data_api_get_json_returns_envelope_when_base_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.leandna_data_api_request import data_api_get_json

    def _boom() -> str:
        raise ValueError("no base")

    monkeypatch.setattr("src.leandna_data_api_request.data_api_base_url", _boom)
    out = data_api_get_json("Metric")
    assert out["ok"] is False
    assert "no base" in out["error"]
