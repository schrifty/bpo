"""Slack client — configuration, channel matching, and API helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.slack_client import (
    _name_matches_customer,
    check_slack_api,
    get_customer_slack_conversations,
    match_channels_for_customer,
    reset_slack_channel_cache,
    slack_configured,
)


def test_slack_not_configured():
    with patch("src.slack_client.SLACK_BOT_TOKEN", None):
        assert not slack_configured()
        assert check_slack_api() == (True, None)
        out = get_customer_slack_conversations("Acme")
        assert out.get("skipped") == "slack_not_configured"


def test_name_matches_customer_word_boundary():
    assert _name_matches_customer("acme-support", "Acme", ["Acme"])
    assert not _name_matches_customer("integrated-packaging", "AGI", ["AGI"])


def test_match_channels_for_customer_uses_aliases():
    reset_slack_channel_cache()
    channels = [
        {"id": "C1", "name": "random"},
        {"id": "C2", "name": "johnson-controls-cs"},
    ]
    with patch("src.slack_client._list_channels", return_value=channels), patch(
        "src.slack_client._load_slack_alias_map",
        return_value={"jci": ["johnson-controls"]},
    ):
        matched = match_channels_for_customer("JCI")
    assert len(matched) == 1
    assert matched[0]["name"] == "johnson-controls-cs"


def test_check_slack_api_ok():
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client._slack_api", return_value={"ok": True, "team": "T"}
    ):
        ok, msg = check_slack_api()
    assert ok is True
    assert msg is None


def test_get_customer_slack_conversations_digest():
    reset_slack_channel_cache()
    channels = [{"id": "C1", "name": "acme-cs", "is_private": False}]
    history = [
        {
            "type": "message",
            "user": "U1",
            "text": "Hello from CS",
            "ts": "1710000000.000001",
        },
    ]
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client.match_channels_for_customer", return_value=channels
    ), patch("src.slack_client._fetch_channel_history", return_value=history):
        out = get_customer_slack_conversations("Acme", days=7)
    assert out["customer"] == "Acme"
    summaries = out.get("conversation_summaries") or []
    assert len(summaries) == 1
    assert summaries[0]["message_count"] == 1
    assert "Hello from CS" in (summaries[0].get("summary_text") or "")


def test_check_slack_api_failure():
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client._slack_api", return_value={"ok": False, "error": "invalid_auth"}
    ):
        ok, msg = check_slack_api()
    assert ok is False
    assert msg and "invalid_auth" in msg


def test_slack_api_read_methods_use_disk_cache():
    from src import slack_client, slack_cache

    slack_cache.clear_slack_cache_for_tests()
    resp = MagicMock()
    resp.json.return_value = {"ok": True, "channels": [{"id": "C1", "name": "acme"}]}
    resp.raise_for_status.return_value = None
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client.CORTEX_SLACK_CACHE_TTL_SECONDS", 3600
    ), patch("src.slack_client.requests.post", return_value=resp) as post:
        first = slack_client._slack_api("conversations.list", params={"limit": 200})
        second = slack_client._slack_api("conversations.list", params={"limit": 200})
    assert first == second
    # Second identical read is served from disk cache — only one network call.
    assert post.call_count == 1
    slack_cache.clear_slack_cache_for_tests()


def test_slack_api_never_caches_auth_test():
    from src import slack_client, slack_cache

    slack_cache.clear_slack_cache_for_tests()
    resp = MagicMock()
    resp.json.return_value = {"ok": True, "team": "T"}
    resp.raise_for_status.return_value = None
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client.CORTEX_SLACK_CACHE_TTL_SECONDS", 3600
    ), patch("src.slack_client.requests.post", return_value=resp) as post:
        slack_client._slack_api("auth.test")
        slack_client._slack_api("auth.test")
    # Preflight must always hit the live API — never cached.
    assert post.call_count == 2
