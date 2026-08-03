"""LLM summaries of per-customer Slack conversation digests for export-all."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from .config import LLM_MODEL, logger
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence


class SlackSummaryLlmError(RuntimeError):
    """Slack LLM summary did not return usable content (strict mode)."""


def llm_export_slack_llm_enabled() -> bool:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SLACK_LLM") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return True


def _allow_slack_llm_fallback() -> bool:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SLACK_LLM_ALLOW_FALLBACK") or "").strip().lower()
    return raw in ("1", "true", "yes", "on", "allow")


def _max_input_chars() -> int:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_SLACK_LLM_MAX_INPUT_CHARS") or "").strip()
    if not raw:
        return 60_000
    try:
        return max(4000, min(int(raw), 200_000))
    except ValueError:
        return 60_000


def _conversation_lines_for_llm(slack_payload: dict[str, Any]) -> tuple[list[str], int]:
    """Flatten channel summaries into chronological lines for the LLM (newest last)."""
    summaries = slack_payload.get("conversation_summaries")
    if not isinstance(summaries, list):
        return [], 0
    lines: list[str] = []
    total = 0
    for block in summaries:
        if not isinstance(block, dict) or block.get("error"):
            continue
        ch = str(block.get("channel_name") or block.get("channel_id") or "channel")
        raw_lines = block.get("summary_lines") if isinstance(block.get("summary_lines"), list) else []
        for ln in raw_lines:
            if not isinstance(ln, str) or not ln.strip():
                continue
            lines.append(f"[#{ch}] {ln.strip()}")
            total += 1
    return lines, total


def _trim_lines_for_llm(lines: list[str], *, max_chars: int) -> tuple[str, bool]:
    """Keep the most recent lines that fit the char budget."""
    if not lines:
        return "", False
    parts: list[str] = []
    used = 0
    truncated = False
    for ln in reversed(lines):
        add = len(ln) + (1 if parts else 0)
        if used + add > max_chars:
            truncated = True
            break
        parts.append(ln)
        used += add
    parts.reverse()
    return "\n".join(parts), truncated


def summarize_customer_slack_for_llm_export(
    customer_label: str,
    slack_payload: dict[str, Any],
    *,
    lookback_days: int,
) -> dict[str, Any]:
    """Return a careful LLM summary of *slack_payload* for one customer."""
    label = (customer_label or "").strip()
    base: dict[str, Any] = {
        "customer": label,
        "lookback_days": lookback_days,
        "status": "skipped",
        "message_count_analyzed": 0,
        "channels_included": [],
    }
    if not llm_export_slack_llm_enabled():
        base["status"] = "skipped"
        base["skipped"] = "CORTEX_LLM_EXPORT_SLACK_LLM disabled"
        return base

    lines, msg_count = _conversation_lines_for_llm(slack_payload)
    summaries = slack_payload.get("conversation_summaries")
    if isinstance(summaries, list):
        base["channels_included"] = [
            str(s.get("channel_name") or s.get("channel_id") or "")
            for s in summaries
            if isinstance(s, dict) and not s.get("error")
        ]
    base["message_count_analyzed"] = msg_count
    if msg_count == 0:
        base["status"] = "ok"
        base["summary_markdown"] = "No human Slack messages matched this customer in the lookback window."
        base["themes"] = []
        base["open_items"] = []
        base["sentiment"] = "unknown"
        return base

    transcript, truncated = _trim_lines_for_llm(lines, max_chars=_max_input_chars())
    if truncated:
        base["input_truncated"] = True
        base["messages_in_transcript"] = len(transcript.split("\n")) if transcript else 0
        base["messages_omitted"] = max(0, msg_count - int(base.get("messages_in_transcript") or 0))
        logger.info(
            "Slack LLM summary for %r: truncating input messages=%d kept=%d omitted=%d",
            label,
            msg_count,
            base["messages_in_transcript"],
            base["messages_omitted"],
        )
    else:
        logger.info(
            "Slack LLM summary for %r: starting messages=%d channels=%s chars=%d",
            label,
            msg_count,
            base.get("channels_included"),
            len(transcript),
        )

    system = (
        "You are a careful customer success analyst summarizing Slack channel history for leadership.\n"
        "Output JSON only. Use ONLY the transcript provided — do not invent facts, people, dates, or outcomes.\n"
        "When evidence is thin, say so explicitly.\n\n"
        "Return one JSON object with:\n"
        '- "summary_markdown": 4–8 bullet markdown lines covering themes, requests, risks, and wins\n'
        '- "themes": array of short topic labels (max 8)\n'
        '- "open_items": array of unresolved asks or follow-ups mentioned in the transcript (max 8)\n'
        '- "sentiment": one of positive|neutral|mixed|concerned|unknown\n'
        '- "notable_quotes": array of up to 3 short verbatim snippets from the transcript (optional)\n'
    )
    user = (
        f"Customer: {label}\n"
        f"Lookback: {lookback_days} days\n"
        f"Messages in transcript: {msg_count}"
        + (" (older messages omitted for size)" if truncated else "")
        + "\n\nTranscript (oldest to newest):\n"
        + transcript
    )

    started = time.monotonic()
    try:
        from .config import llm_client

        resp = _llm_create_with_retry(
            llm_client(),
            model=LLM_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        raw_txt = _strip_json_code_fence((resp.choices[0].message.content or "").strip())
        data = json.loads(raw_txt)
        if not isinstance(data, dict):
            raise ValueError("LLM response was not a JSON object")
        base["status"] = "ok"
        base["summary_markdown"] = str(data.get("summary_markdown") or "").strip()
        base["themes"] = [str(t) for t in (data.get("themes") or []) if str(t).strip()][:8]
        base["open_items"] = [str(t) for t in (data.get("open_items") or []) if str(t).strip()][:8]
        base["sentiment"] = str(data.get("sentiment") or "unknown").strip() or "unknown"
        quotes = data.get("notable_quotes")
        if isinstance(quotes, list) and quotes:
            base["notable_quotes"] = [str(q)[:300] for q in quotes if str(q).strip()][:3]
        base["llm_seconds"] = round(time.monotonic() - started, 3)
        return base
    except Exception as exc:
        err = str(exc)[:400]
        logger.warning("Slack LLM summary failed for %s: %s", label, err)
        if _allow_slack_llm_fallback():
            base["status"] = "fallback"
            base["error"] = err
            base["summary_markdown"] = (
                f"_LLM summary unavailable ({err}). Raw message lines are in "
                "`conversation_summaries` below._"
            )
            base["llm_seconds"] = round(time.monotonic() - started, 3)
            return base
        base["status"] = "error"
        base["error"] = err
        base["llm_seconds"] = round(time.monotonic() - started, 3)
        raise SlackSummaryLlmError(f"Slack LLM summary failed for {label}: {err}") from exc
