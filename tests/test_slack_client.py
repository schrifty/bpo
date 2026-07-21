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
        {"id": "C1", "name": "random", "is_private": False},
        {"id": "C2", "name": "johnson-controls-cs", "is_private": False},
        {"id": "C3", "name": "dover-implementation", "is_private": False},
        {"id": "C4", "name": "spirit", "is_private": False},
        {"id": "C5", "name": "spirit-data-load", "is_private": False},
        {"id": "C6", "name": "cs-spirit-ops", "is_private": False},
        {"id": "C7", "name": "spirit-implementation", "is_private": False, "is_archived": True},
        {"id": "C8", "name": "aspirin-team", "is_private": False},
    ]
    with patch("src.slack_client._list_channels", return_value=channels), patch(
        "src.slack_client._load_slack_alias_map",
        return_value={
            "jci": ["johnson-controls"],
            "spirit": ["spirit"],
        },
    ):
        matched = match_channels_for_customer("JCI")
        spirit_matched = match_channels_for_customer("Spirit")
    assert [c["name"] for c in matched] == ["johnson-controls-cs"]
    # Token "spirit" matches any channel containing that token; archived skipped.
    assert [c["name"] for c in spirit_matched] == ["cs-spirit-ops", "spirit", "spirit-data-load"]


def test_alias_spirit_matches_spirit_token_anywhere():
    from src.slack_client import _alias_fragment_matches_channel

    assert _alias_fragment_matches_channel("spirit", "spirit")
    assert _alias_fragment_matches_channel("spirit-data-load", "spirit")
    assert _alias_fragment_matches_channel("spirit_abm", "spirit")
    assert _alias_fragment_matches_channel("cs-spirit-ops", "spirit")
    assert not _alias_fragment_matches_channel("aspirin-team", "spirit")


def test_match_channels_skips_archived():
    reset_slack_channel_cache()
    channels = [
        {"id": "C1", "name": "spirit", "is_private": False, "is_archived": False},
        {"id": "C2", "name": "spirit-implementation", "is_private": False, "is_archived": True},
        {"id": "C3", "name": "spirit-data-load", "is_private": False},
    ]
    with patch("src.slack_client._list_channels", return_value=channels), patch(
        "src.slack_client._load_slack_alias_map",
        return_value={"spirit": ["spirit", "spirit-data-load", "spirit-implementation"]},
    ):
        matched = match_channels_for_customer("Spirit")
    assert [c["name"] for c in matched] == ["spirit", "spirit-data-load"]


def test_list_channels_drops_archived_even_if_api_returns_them():
    import src.slack_client as slack_client

    reset_slack_channel_cache()
    pages = [
        {
            "ok": True,
            "channels": [
                {"id": "C1", "name": "spirit", "is_archived": False},
                {"id": "C2", "name": "spirit-implementation", "is_archived": True},
            ],
            "response_metadata": {},
        }
    ]

    def _api(method, params=None):
        assert method == "conversations.list"
        assert params and params.get("exclude_archived") is True
        return pages.pop(0)

    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client._slack_api", side_effect=_api
    ):
        channels = slack_client._list_channels(force_refresh=True)
    assert [c["name"] for c in channels] == ["spirit"]
    assert all(not c.get("is_archived") for c in channels)


def test_check_slack_api_ok():
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client._slack_api", return_value={"ok": True, "team": "T"}
    ):
        ok, msg = check_slack_api()
    assert ok is True
    assert msg is None


def test_get_customer_slack_conversations_digest():
    reset_slack_channel_cache()
    channels = [{"id": "C1", "name": "acme-cs", "is_private": False, "is_member": False}]
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
    assert out["channels_invite_needed"][0]["name"] == "acme-cs"
    assert out["channels_matched"][0]["invite_needed"] is True


def test_get_customer_slack_conversations_no_visible_match():
    reset_slack_channel_cache()
    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client.match_channels_for_customer", return_value=[]
    ):
        out = get_customer_slack_conversations("Safran", days=7)
    assert out.get("no_visible_channel_match") is True
    assert out["channels_invite_needed"] == []
    assert "invisible" in (out.get("note") or "").lower()


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


def test_list_channels_stops_when_pagination_cursor_repeats():
    from src import slack_client

    reset_slack_channel_cache()
    page = {
        "ok": True,
        "channels": [{"id": "C1", "name": "acme", "is_private": False, "is_member": False}],
        "response_metadata": {"next_cursor": "same"},
    }

    def fake_list(method, *, params=None):
        assert method == "conversations.list"
        return page

    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client._slack_api", side_effect=fake_list
    ):
        channels = slack_client._list_channels(force_refresh=True)
    assert len(channels) == 1
    assert channels[0]["name"] == "acme"


def test_fetch_channel_history_joins_public_channel_on_not_in_channel():
    from src import slack_client

    channel = {"id": "C1", "name": "acme-cs", "is_private": False, "is_member": False}
    history = [{"type": "message", "user": "U1", "text": "Hi", "ts": "1710000000.000001"}]
    calls: list[str] = []

    def fake_api(method, *, params=None):
        calls.append(method)
        if method == "conversations.history":
            if len(calls) == 1:
                return {"ok": False, "error": "not_in_channel"}
            return {"ok": True, "messages": history}
        if method == "conversations.join":
            return {"ok": True, "channel": {"id": "C1"}}
        raise AssertionError(method)

    with patch("src.slack_client.SLACK_BOT_TOKEN", "xoxb-test"), patch(
        "src.slack_client.CORTEX_SLACK_AUTO_JOIN_PUBLIC_CHANNELS", True
    ), patch("src.slack_client._slack_api", side_effect=fake_api):
        out = slack_client._fetch_channel_history("C1", oldest=0.0, limit=10, channel=channel)
    assert out == history
    assert calls == ["conversations.history", "conversations.join", "conversations.history"]
    assert channel.get("is_member") is True


def test_jci_alias_matches_johnson_controls_channel():
    import src.slack_client as slack_client

    reset_slack_channel_cache()
    slack_client._alias_map = None
    channels = [
        {"id": "C1", "name": "random", "is_private": False},
        {"id": "C2", "name": "johnson-controls", "is_private": False},
    ]
    with patch("src.slack_client._list_channels", return_value=channels):
        matched = match_channels_for_customer("JCI")
    assert [c["name"] for c in matched] == ["johnson-controls"]
