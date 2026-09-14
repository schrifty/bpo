"""Tests for Secrets Manager bundle split."""

from __future__ import annotations

from src.secrets_bundles import (
    BUNDLE_GOOGLE,
    BUNDLE_INTEGRATIONS,
    BUNDLE_LLM,
    BUNDLE_SLACK,
    bundle_for_key,
    default_secret_name,
    split_secret_payload,
)


def test_bundle_for_key_routes_credentials() -> None:
    assert bundle_for_key("GOOGLE_SERVICE_ACCOUNT_JSON") == BUNDLE_GOOGLE
    assert bundle_for_key("OPENAI_API_KEY") == BUNDLE_LLM
    assert bundle_for_key("CURSOR_ADMIN_API_KEY") == BUNDLE_LLM
    assert bundle_for_key("SLACK_BOT_TOKEN") == BUNDLE_SLACK
    assert bundle_for_key("JIRA_API_TOKEN") == BUNDLE_INTEGRATIONS
    assert bundle_for_key("PENDO_INTEGRATION_KEY") == BUNDLE_INTEGRATIONS
    assert bundle_for_key("SF_PRIVATE_KEY") == BUNDLE_INTEGRATIONS
    assert bundle_for_key("PR_LEANDNA_DATA_API_API_KEY") == BUNDLE_INTEGRATIONS


def test_split_secret_payload_drops_task_env_and_groups_keys() -> None:
    bundles = split_secret_payload(
        {
            "CORTEX_SECRETS_ARN": "should-drop",
            "CORTEX_SECRETS_ARNS": "should-drop",
            "GOOGLE_SERVICE_ACCOUNT_JSON": {"type": "service_account"},
            "OPENAI_API_KEY": "sk-test",
            "SLACK_BOT_TOKEN": "xoxb-test",
            "JIRA_API_TOKEN": "jira",
            "GOOGLE_DRIVE_OWNER_EMAIL": "robot@example.com",
        }
    )
    assert "CORTEX_SECRETS_ARN" not in bundles[BUNDLE_INTEGRATIONS]
    assert bundles[BUNDLE_GOOGLE] == {"GOOGLE_SERVICE_ACCOUNT_JSON": {"type": "service_account"}}
    assert bundles[BUNDLE_LLM] == {"OPENAI_API_KEY": "sk-test"}
    assert bundles[BUNDLE_SLACK] == {"SLACK_BOT_TOKEN": "xoxb-test"}
    assert bundles[BUNDLE_INTEGRATIONS]["JIRA_API_TOKEN"] == "jira"
    assert bundles[BUNDLE_INTEGRATIONS]["GOOGLE_DRIVE_OWNER_EMAIL"] == "robot@example.com"


def test_default_secret_name() -> None:
    assert default_secret_name("integrations") == "cortex/prod/integrations"
    assert default_secret_name("llm", prefix="cortex-tf", environment="staging") == "cortex-tf/staging/llm"
