"""Tests for LLM export Slack attachment and summarization."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.llm_export_slack import (
    attach_slack_top_customers_for_llm_export,
    llm_export_slack_enabled,
    llm_export_slack_top_n,
)
from src.llm_export_slack_summarize import summarize_customer_slack_for_llm_export


def test_llm_export_slack_top_n_defaults_to_10():
    with patch.dict("os.environ", {}, clear=False):
        # Remove override if present in test env
        import os

        os.environ.pop("CORTEX_LLM_EXPORT_SLACK_TOP_N", None)
        assert llm_export_slack_top_n() == 10


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


def test_attach_slack_top_customer_with_llm_summary_and_performance():
    report: dict = {
        "days": 30,
        "customers": [{"customer": "BigCo"}],
        "_llm_export_salesforce_revenue_book": {
            "matched_customer_contract_rollups": [
                {"customer": "BigCo", "arr": 100000, "active": True, "current_arr": 100000},
            ],
        },
    }
    slack_payload = {
        "source": "slack",
        "days": 180,
        "conversation_summaries": [
            {"channel_name": "bigco-cs", "message_count": 2, "summary_lines": ["2025-01-01 u1: hello"]},
        ],
    }
    llm_summary = {
        "status": "ok",
        "summary_markdown": "- Customer asked about rollout",
        "themes": ["rollout"],
        "open_items": [],
        "sentiment": "neutral",
        "llm_seconds": 1.2,
    }
    with patch("src.llm_export_slack.llm_export_slack_enabled", return_value=True), patch(
        "src.slack_client.slack_configured", return_value=True
    ), patch(
        "src.slack_client.get_customer_slack_conversations", return_value=slack_payload
    ), patch(
        "src.llm_export_slack_summarize.summarize_customer_slack_for_llm_export",
        return_value=llm_summary,
    ):
        summary = attach_slack_top_customers_for_llm_export(report)
    assert summary["customers_with_slack_data"] == 1
    assert summary["customers_llm_summarized"] == 1
    assert summary["performance"]["wall_seconds_total"] >= 0
    assert summary["performance"]["per_customer"][0]["messages"] == 2
    assert "BigCo" in report["slack"]["customers"]
    assert report["slack"]["customers"]["BigCo"]["llm_summary"]["status"] == "ok"
    assert report["slack"]["lookback_days"] == 180


def test_summarize_customer_slack_no_messages():
    out = summarize_customer_slack_for_llm_export(
        "Acme",
        {"conversation_summaries": []},
        lookback_days=180,
    )
    assert out["status"] == "ok"
    assert "No human Slack messages" in out["summary_markdown"]


def test_summarize_customer_slack_llm_call():
    slack_payload = {
        "conversation_summaries": [
            {
                "channel_name": "acme-cs",
                "summary_lines": ["2025-06-01 U123: Need training"],
            }
        ]
    }
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(
            message=MagicMock(
                content='{"summary_markdown":"- Training requested","themes":["training"],'
                '"open_items":["Schedule training"],"sentiment":"neutral"}'
            )
        )
    ]
    with patch(
        "src.llm_export_slack_summarize.llm_export_slack_llm_enabled", return_value=True
    ), patch("src.config.llm_client") as mock_client, patch(
        "src.llm_export_slack_summarize._llm_create_with_retry", return_value=mock_resp
    ):
        out = summarize_customer_slack_for_llm_export("Acme", slack_payload, lookback_days=180)
    assert out["status"] == "ok"
    assert "Training" in out["summary_markdown"]
    mock_client.assert_called_once()
