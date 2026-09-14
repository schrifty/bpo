"""Tests for split Secrets Manager JSON builder."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _builder_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "build_secrets_manager_json.py"
    spec = importlib.util.spec_from_file_location("build_secrets_manager_json", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_build_secret_bundles_splits_env(tmp_path: Path) -> None:
    bsm = _builder_module()
    sa = tmp_path / "sa.json"
    sa.write_text('{"type":"service_account","client_email":"sa@x"}', encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "PENDO_INTEGRATION_KEY=pendo",
                f"GOOGLE_APPLICATION_CREDENTIALS={sa}",
                "OPENAI_API_KEY=sk-test",
                "SLACK_BOT_TOKEN=xoxb-test",
                "CORTEX_SECRETS_ARN=ignore-me",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    bundles = bsm.build_secret_bundles(env)
    assert bundles["google"]["GOOGLE_SERVICE_ACCOUNT_JSON"]["client_email"] == "sa@x"
    assert bundles["integrations"]["PENDO_INTEGRATION_KEY"] == "pendo"
    assert bundles["llm"]["OPENAI_API_KEY"] == "sk-test"
    assert bundles["slack"]["SLACK_BOT_TOKEN"] == "xoxb-test"
    assert "CORTEX_SECRETS_ARN" not in bundles["integrations"]


def test_build_secrets_manager_json_writes_dir(tmp_path: Path, monkeypatch) -> None:
    bsm = _builder_module()
    env = tmp_path / ".env"
    env.write_text("JIRA_API_TOKEN=jira\nOPENAI_API_KEY=sk\n", encoding="utf-8")
    out = tmp_path / "secrets"
    monkeypatch.setattr("sys.argv", ["build_secrets_manager_json.py", "--env", str(env), "-o", str(out)])
    bsm.main()
    integ = json.loads((out / "integrations.json").read_text(encoding="utf-8"))
    llm = json.loads((out / "llm.json").read_text(encoding="utf-8"))
    assert integ["JIRA_API_TOKEN"] == "jira"
    assert llm["OPENAI_API_KEY"] == "sk"
