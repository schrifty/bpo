"""LLM export Slack attachment."""

from __future__ import annotations

from unittest.mock import patch

from src.llm_export_slack import attach_slack_top_customers_for_llm_export, llm_export_slack_enabled


def test_llm_export_slack_disabled_by_env():
    with patch.dict("os.environ", {"CORTEX_LLM_EXPORT_SLACK": "false"}, clear=False):
        assert llm_export_slack_enabled() is False


def test_attach_slack_skipped_when_not_configured():
    report: dict = {
        "days": 30,
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "BigCo", "arr": 100000, "active": True},
            ],
        },
    }
    with patch("src.llm_export_slack.llm_export_slack_enabled", return_value=True), patch(
        "src.slack_client.slack_configured", return_value=False
    ):
        summary = attach_slack_top_customers_for_llm_export(report)
    assert summary["slack_configured"] is False
    assert report["slack"].get("skipped") == "slack_not_configured"


def test_attach_slack_top_customer():
    report: dict = {
        "days": 30,
        "customers": [{"customer": "BigCo"}],
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "BigCo", "arr": 100000, "active": True},
            ],
        },
    }
    slack_payload = {
        "source": "slack",
        "conversation_summaries": [{"channel_name": "bigco-cs", "summary_lines": ["line"]}],
    }
    with patch("src.llm_export_slack.llm_export_slack_enabled", return_value=True), patch(
        "src.slack_client.slack_configured", return_value=True
    ), patch(
        "src.slack_client.get_customer_slack_conversations", return_value=slack_payload
    ):
        summary = attach_slack_top_customers_for_llm_export(report)
    assert summary["customers_with_slack_data"] == 1
    assert "BigCo" in report["slack"]["customers"]
