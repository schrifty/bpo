"""Tests for AWS env bootstrap helpers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _bootstrap_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_aws_env.py"
    spec = importlib.util.spec_from_file_location("bootstrap_aws_env", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_apply_secret_payload_writes_google_sa(monkeypatch, tmp_path) -> None:
    mod = _bootstrap_module()
    monkeypatch.setattr(mod.tempfile, "mkstemp", lambda **kw: (1, str(tmp_path / "sa.json")))
    mod.apply_secret_payload(
        {
            "PENDO_INTEGRATION_KEY": "pendo-key",
            "GOOGLE_SERVICE_ACCOUNT_JSON": {"type": "service_account", "client_email": "x@y.iam.gserviceaccount.com"},
        }
    )
    import os

    assert os.environ["PENDO_INTEGRATION_KEY"] == "pendo-key"
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    sa = json.loads((tmp_path / "sa.json").read_text(encoding="utf-8"))
    assert sa["client_email"] == "x@y.iam.gserviceaccount.com"


def test_build_secret_env_and_shell_exports() -> None:
    mod = _bootstrap_module()
    env = mod.build_secret_env(
        {
            "PENDO_INTEGRATION_KEY": "pendo-key",
            "OPENAI_API_KEY": "sk-test",
        }
    )
    assert env["PENDO_INTEGRATION_KEY"] == "pendo-key"
    assert env["CORTEX_SKIP_DOTENV"] == "1"
    rendered = mod.render_shell_exports(env)
    assert "export OPENAI_API_KEY=" in rendered
    assert "export PENDO_INTEGRATION_KEY=" in rendered

