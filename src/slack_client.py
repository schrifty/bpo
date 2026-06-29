"""Slack Web API — recent channel conversations and per-customer digests."""

from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

from .config_paths import SLACK_CUSTOMER_ALIASES_FILE
from .config import (
    CORTEX_SLACK_MAX_CHANNELS_PER_CUSTOMER,
    CORTEX_SLACK_MAX_MESSAGES_PER_CHANNEL,
    CORTEX_SLACK_LOOKBACK_DAYS,
    SLACK_API_BASE_URL,
    SLACK_BOT_TOKEN,
    logger,
)

_SLACK_ALIAS_FILE = SLACK_CUSTOMER_ALIASES_FILE
_alias_map: dict[str, list[str]] | None = None
_alias_lock = threading.Lock()

_CHANNEL_CACHE: list[dict[str, Any]] | None = None
_CHANNEL_CACHE_LOCK = threading.Lock()

_SKIP_MESSAGE_SUBTYPES = frozenset(
    {
        "channel_join",
        "channel_leave",
        "group_join",
        "group_leave",
        "pinned_item",
        "channel_topic",
        "channel_purpose",
        "channel_name",
        "channel_archive",
        "channel_unarchive",
        "ekm_access_denied",
        "me_message",
    }
)

_WORD_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)


def slack_configured() -> bool:
    return bool(SLACK_BOT_TOKEN and str(SLACK_BOT_TOKEN).strip())


def slack_enabled_for_reports() -> bool:
    """True when a bot token is set and Slack is not explicitly disabled."""
    import os

    if not slack_configured():
        return False
    raw = (os.environ.get("CORTEX_SLACK_DISABLED") or "").strip().lower()
    return raw not in ("1", "true", "yes", "on")


def check_slack_api() -> tuple[bool, str | None]:
    """Return (True, None) if Slack is not configured or ``auth.test`` succeeds."""
    if not slack_configured():
        return True, None
    try:
        data = _slack_api("auth.test")
        if data.get("ok"):
            return True, None
        err = str(data.get("error") or "auth.test failed")
        return False, f"Slack: {err}"[:120]
    except Exception as e:
        logger.warning("Slack preflight failed: %s", e)
        return False, f"Slack: {str(e)[:120]}"


def _slack_api(method: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    if not slack_configured():
        raise RuntimeError("Slack not configured (set SLACK_BOT_TOKEN)")
    url = f"{SLACK_API_BASE_URL.rstrip('/')}/{method}"
    token = str(SLACK_BOT_TOKEN).strip()
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
        json=params or {},
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Slack {method}: non-object response")
    return data


def _load_slack_alias_map() -> dict[str, list[str]]:
    global _alias_map
    with _alias_lock:
        if _alias_map is not None:
            return _alias_map
        out: dict[str, list[str]] = {}
        if _SLACK_ALIAS_FILE.is_file():
            try:
                raw = yaml.safe_load(_SLACK_ALIAS_FILE.read_text(encoding="utf-8")) or {}
                if isinstance(raw, dict):
                    for key, val in raw.items():
                        k = str(key or "").strip()
                        if not k:
                            continue
                        if isinstance(val, str):
                            out[k.lower()] = [val.strip()]
                        elif isinstance(val, list):
                            out[k.lower()] = [str(v).strip() for v in val if str(v).strip()]
            except Exception as e:
                logger.warning("Slack aliases YAML unreadable: %s", e)
        _alias_map = out
        return out


def _tokens(text: str) -> list[str]:
    return [t.lower() for t in _WORD_RE.findall(text or "") if len(t) >= 2]


def _name_matches_customer(channel_name: str, customer_name: str, extra_terms: list[str]) -> bool:
    ch_tokens = set(_tokens(channel_name))
    if not ch_tokens:
        return False

    def _terms_match(term: str) -> bool:
        term_tokens = _tokens(term)
        if not term_tokens:
            return False
        if len(term_tokens) == 1:
            return term_tokens[0] in ch_tokens
        matched = sum(1 for t in term_tokens if t in ch_tokens)
        return matched >= max(1, len(term_tokens) - 1)

    for term in extra_terms:
        if _terms_match(term):
            return True
    return _terms_match(customer_name)


def _list_channels(*, force_refresh: bool = False) -> list[dict[str, Any]]:
    global _CHANNEL_CACHE
    with _CHANNEL_CACHE_LOCK:
        if _CHANNEL_CACHE is not None and not force_refresh:
            return list(_CHANNEL_CACHE)

    channels: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(50):
        params: dict[str, Any] = {
            "types": "public_channel,private_channel",
            "exclude_archived": True,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        data = _slack_api("conversations.list", params=params)
        if not data.get("ok"):
            raise RuntimeError(f"Slack conversations.list: {data.get('error')}")
        batch = data.get("channels") if isinstance(data.get("channels"), list) else []
        for ch in batch:
            if isinstance(ch, dict) and ch.get("id"):
                channels.append(
                    {
                        "id": ch.get("id"),
                        "name": ch.get("name") or "",
                        "is_private": bool(ch.get("is_private")),
                        "num_members": ch.get("num_members"),
                    }
                )
        cursor = (data.get("response_metadata") or {}).get("next_cursor") if isinstance(data.get("response_metadata"), dict) else None
        if not cursor:
            break
        time.sleep(0.2)

    with _CHANNEL_CACHE_LOCK:
        _CHANNEL_CACHE = channels
    return list(channels)


def reset_slack_channel_cache() -> None:
    """Clear cached channel list (tests or long-running workers)."""
    global _CHANNEL_CACHE
    with _CHANNEL_CACHE_LOCK:
        _CHANNEL_CACHE = None


def match_channels_for_customer(customer_name: str) -> list[dict[str, Any]]:
    """Return Slack channels whose names match *customer_name* or alias terms."""
    name = (customer_name or "").strip()
    if not name:
        return []
    aliases = _load_slack_alias_map()
    extra = list(aliases.get(name.lower(), []))
    extra.append(name)
    channels = _list_channels()
    matched: list[dict[str, Any]] = []
    for ch in channels:
        ch_name = str(ch.get("name") or "")
        if _name_matches_customer(ch_name, name, extra):
            matched.append(dict(ch))
    matched.sort(key=lambda c: (0 if name.lower() in str(c.get("name") or "").lower() else 1, str(c.get("name") or "").lower()))
    return matched[: max(1, int(CORTEX_SLACK_MAX_CHANNELS_PER_CUSTOMER))]


def _format_ts(ts: str | float | None) -> str:
    try:
        sec = float(ts)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _message_line(msg: dict[str, Any]) -> str | None:
    subtype = msg.get("subtype")
    if subtype in _SKIP_MESSAGE_SUBTYPES:
        return None
    text = (msg.get("text") or "").strip()
    if not text and subtype == "bot_message":
        text = (msg.get("username") or "bot").strip()
    if not text:
        return None
    user = msg.get("user") or msg.get("bot_id") or msg.get("username") or "unknown"
    when = _format_ts(msg.get("ts"))
    line = f"{when} {user}: {text}"
    if len(line) > 600:
        line = line[:597] + "..."
    if msg.get("thread_ts") and msg.get("thread_ts") != msg.get("ts"):
        line += " (thread reply)"
    return line


def _fetch_channel_history(channel_id: str, *, oldest: float, limit: int) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    cursor: str | None = None
    while len(messages) < limit:
        page_limit = min(200, limit - len(messages))
        params: dict[str, Any] = {
            "channel": channel_id,
            "oldest": str(oldest),
            "limit": page_limit,
        }
        if cursor:
            params["cursor"] = cursor
        data = _slack_api("conversations.history", params=params)
        if not data.get("ok"):
            err = str(data.get("error") or "unknown")
            if err == "not_in_channel":
                return []
            raise RuntimeError(f"Slack conversations.history: {err}")
        batch = data.get("messages") if isinstance(data.get("messages"), list) else []
        for msg in batch:
            if isinstance(msg, dict):
                messages.append(msg)
        cursor = (data.get("response_metadata") or {}).get("next_cursor") if isinstance(data.get("response_metadata"), dict) else None
        if not batch or not cursor:
            break
        time.sleep(0.15)
    return messages[:limit]


def _summarize_channel(
    channel: dict[str, Any],
    *,
    days: int,
    max_messages: int,
) -> dict[str, Any]:
    ch_id = str(channel.get("id") or "")
    ch_name = str(channel.get("name") or ch_id)
    oldest = time.time() - max(1, int(days)) * 86400
    try:
        raw = _fetch_channel_history(ch_id, oldest=oldest, limit=max_messages)
    except Exception as e:
        return {
            "channel_id": ch_id,
            "channel_name": ch_name,
            "error": str(e)[:300],
            "message_count": 0,
            "summary_lines": [],
            "summary_text": "",
        }

    lines: list[str] = []
    for msg in reversed(raw):
        line = _message_line(msg)
        if line:
            lines.append(line)

    summary_text = "\n".join(f"- {ln}" for ln in lines) if lines else "(no messages in lookback window)"
    return {
        "channel_id": ch_id,
        "channel_name": ch_name,
        "is_private": bool(channel.get("is_private")),
        "message_count": len(lines),
        "summary_lines": lines,
        "summary_text": summary_text,
    }


def get_customer_slack_conversations(
    customer_name: str,
    *,
    days: int | None = None,
) -> dict[str, Any]:
    """Recent Slack conversation digests for channels matched to *customer_name*."""
    name = (customer_name or "").strip()
    lookback = int(days if days is not None else CORTEX_SLACK_LOOKBACK_DAYS)
    lookback = max(1, min(lookback, 90))
    empty: dict[str, Any] = {
        "source": "slack",
        "customer": name,
        "days": lookback,
        "configured": slack_configured(),
        "channels_matched": [],
        "conversation_summaries": [],
    }
    if not name:
        return {**empty, "error": "customer name required"}
    if not slack_configured():
        return {**empty, "skipped": "slack_not_configured"}

    channels = match_channels_for_customer(name)
    empty["channels_matched"] = [{"id": c.get("id"), "name": c.get("name")} for c in channels]
    if not channels:
        empty["note"] = (
            "No Slack channels matched this customer name or config/slack_customer_aliases.yaml entries. "
            "Add aliases or ensure the bot is in customer channels."
        )
        return empty

    summaries: list[dict[str, Any]] = []
    max_msg = max(5, int(CORTEX_SLACK_MAX_MESSAGES_PER_CHANNEL))
    for ch in channels:
        summaries.append(_summarize_channel(ch, days=lookback, max_messages=max_msg))

    combined_parts = [
        f"### #{s.get('channel_name')}\n{s.get('summary_text')}"
        for s in summaries
        if isinstance(s, dict) and not s.get("error")
    ]
    return {
        **empty,
        "conversation_summaries": summaries,
        "combined_summary_markdown": "\n\n".join(combined_parts) if combined_parts else "",
    }
