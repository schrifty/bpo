"""Tests for sync_secret_key bundle targeting."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _sync_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "sync_secret_key.py"
    spec = importlib.util.spec_from_file_location("sync_secret_key", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_resolve_secret_id_uses_bundle_name_when_no_terraform(monkeypatch) -> None:
    mod = _sync_module()
    monkeypatch.delenv("CORTEX_SECRETS_ARN", raising=False)
    monkeypatch.setattr(mod, "_terraform_output", lambda name: "")
    assert mod.resolve_secret_id(None, ["OPENAI_API_KEY"]) == "cortex/prod/llm"
    assert mod.resolve_secret_id(None, ["JIRA_API_TOKEN"]) == "cortex/prod/integrations"
    assert mod.resolve_secret_id(None, ["SLACK_BOT_TOKEN"]) == "cortex/prod/slack"


def test_resolve_secret_id_rejects_mixed_bundles() -> None:
    import pytest

    mod = _sync_module()
    with pytest.raises(ValueError, match="span secret bundles"):
        mod.resolve_secret_id(None, ["OPENAI_API_KEY", "JIRA_API_TOKEN"])
