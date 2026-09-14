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
    import os

    mod = _bootstrap_module()
    env = mod.build_secret_env(
        {
            "PENDO_INTEGRATION_KEY": "pendo-key",
            "GOOGLE_SERVICE_ACCOUNT_JSON": {
                "type": "service_account",
                "client_email": "x@y.iam.gserviceaccount.com",
            },
        },
        sa_dir=str(tmp_path),
    )
    for key, val in env.items():
        monkeypatch.setenv(key, val)
    assert os.environ["PENDO_INTEGRATION_KEY"] == "pendo-key"
    sa_path = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    assert sa_path.parent == tmp_path
    sa = json.loads(sa_path.read_text(encoding="utf-8"))
    assert sa["client_email"] == "x@y.iam.gserviceaccount.com"


def test_google_sa_temp_file_mode_is_0600(tmp_path) -> None:
    import os
    import stat

    mod = _bootstrap_module()
    env = mod.build_secret_env(
        {"GOOGLE_SERVICE_ACCOUNT_JSON": {"type": "service_account"}},
        sa_dir=str(tmp_path),
    )
    path = Path(env["GOOGLE_APPLICATION_CREDENTIALS"])
    assert path.parent == tmp_path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert json.loads(path.read_text(encoding="utf-8"))["type"] == "service_account"


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


def test_secret_arns_from_env_merges_list_and_single(monkeypatch) -> None:
    mod = _bootstrap_module()
    monkeypatch.setenv(
        "CORTEX_SECRETS_ARNS",
        "arn:aws:secretsmanager:us-east-1:1:secret:google, arn:aws:secretsmanager:us-east-1:1:secret:integrations",
    )
    monkeypatch.setenv("CORTEX_SECRETS_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:integrations")
    arns = mod.secret_arns_from_env()
    assert arns == [
        "arn:aws:secretsmanager:us-east-1:1:secret:google",
        "arn:aws:secretsmanager:us-east-1:1:secret:integrations",
    ]


def test_load_secret_env_merges_multiple_arns(monkeypatch) -> None:
    mod = _bootstrap_module()
    payloads = {
        "arn:google": {"GOOGLE_SERVICE_ACCOUNT_JSON": {"client_email": "sa@x"}},
        "arn:int": {"PENDO_INTEGRATION_KEY": "pendo-key"},
        "arn:llm": {"OPENAI_API_KEY": "sk-test"},
    }

    def _payload(arn: str):
        return payloads[arn]

    monkeypatch.setattr(mod, "_payload_from_arn", _payload)
    env = mod.load_secret_env(secrets_arn="arn:google,arn:int,arn:llm")
    assert env["PENDO_INTEGRATION_KEY"] == "pendo-key"
    assert env["OPENAI_API_KEY"] == "sk-test"
    assert env["GOOGLE_APPLICATION_CREDENTIALS"]


