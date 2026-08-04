"""LLM designs full engineering-portfolio slides (layout + content).

Deck YAML still defines slide order / ids. Hand-built Python slide builders are
bypassed when ``CORTEX_ENG_PORTFOLIO_LLM_SLIDES`` / ``CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES``
is on. Design-standards docs are intentionally NOT injected — the model invents
structure from the data. Provider is Anthropic Claude via ``eng_portfolio_llm_client``
(default model: ``claude-opus-5``).
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .claude_slide_ir import IR_SCHEMA_FOR_PROMPT, normalize_slide_ir, render_slide_ir
from .config import (
    CORTEX_ENG_PORTFOLIO_CLAUDE_ALLOW_FALLBACK,
    CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL,
    CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES,
    eng_portfolio_llm_client,
    logger,
)
from .deck_builder_utils import _normalize_builder_return
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence
from .slide_utils import (
    slide_object_id_base as _slide_object_id_base,
    unique_slide_object_id_base as _unique_slide_object_id_base,
)

_MAX_DIGEST_CHARS = 12_000
# Claude Opus thinking models: max_tokens is total (reasoning + visible).
_SLIDE_MAX_TOKENS = 16_384
_SLIDE_MAX_ATTEMPTS = 2


class EngPortfolioClaudeError(RuntimeError):
    """LLM did not return a usable slide IR (strict mode — no silent hand-built fallback)."""


EngPortfolioLlmError = EngPortfolioClaudeError


def eng_portfolio_claude_slides_enabled() -> bool:
    return bool(CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES)


def _allow_fallback() -> bool:
    return bool(CORTEX_ENG_PORTFOLIO_CLAUDE_ALLOW_FALLBACK)


def _extract_json_object(raw: str) -> str:
    """Return the first top-level ``{...}`` substring, or stripped raw text."""
    s = _strip_json_code_fence(raw or "")
    start = s.find("{")
    if start < 0:
        return s
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(s[start:], start=start):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]
    return s[start:]


def _parse_slide_ir_json(raw: str) -> dict[str, Any]:
    blob = _extract_json_object(raw)
    return json.loads(blob)


def _trim(obj: Any, *, max_chars: int = _MAX_DIGEST_CHARS) -> Any:
    raw = json.dumps(obj, default=str, ensure_ascii=False)
    if len(raw) <= max_chars:
        return obj
    if not isinstance(obj, dict):
        return obj
    out: dict[str, Any] = {}
    for k, v in obj.items():
        if isinstance(v, list) and len(v) > 8:
            out[k] = v[:8]
        elif isinstance(v, dict):
            nested = json.dumps(v, default=str, ensure_ascii=False)
            if len(nested) > max_chars // 3:
                # Keep only top-level scalars / short lists inside nested dicts
                slim: dict[str, Any] = {}
                for nk, nv in v.items():
                    if isinstance(nv, list):
                        slim[nk] = nv[:5]
                    elif isinstance(nv, (str, int, float, bool)) or nv is None:
                        slim[nk] = nv
                out[k] = slim
            else:
                out[k] = v
        else:
            out[k] = v
    raw2 = json.dumps(out, default=str, ensure_ascii=False)
    if len(raw2) <= max_chars:
        return out
    # Last resort: keep only eng_portfolio + days
    slim_root = {"days": out.get("days"), "eng_portfolio": out.get("eng_portfolio")}
    return slim_root


def build_eng_portfolio_digest(report: dict[str, Any]) -> dict[str, Any]:
    """Compact facts Claude may cite when designing slides."""
    eng = report.get("eng_portfolio") or {}
    digest: dict[str, Any] = {
        "days": report.get("days") or eng.get("days") or 30,
        "eng_portfolio": {
            k: eng.get(k)
            for k in (
                "sprint",
                "in_flight_count",
                "closed_count",
                "by_status",
                "by_type",
                "themes",
                "open_bugs",
                "blocker_critical",
                "by_assignee",
                "by_assignee_active",
                "flow",
                "work_split",
                "support_pressure",
                "bug_flow",
                "epic_progress",
                "team_scorecard",
                "project_snapshots",
                "sprint_velocity",
                "backlog_staleness",
            )
            if eng.get(k) is not None
        },
    }
    # Cap noisy lists
    for key in ("open_bugs", "blocker_critical", "themes"):
        blob = digest["eng_portfolio"].get(key)
        if isinstance(blob, list) and len(blob) > 15:
            digest["eng_portfolio"][key] = blob[:15]
    cu = report.get("cursor_usage")
    if isinstance(cu, dict) and cu.get("configured"):
        digest["cursor_usage"] = {
            k: cu.get(k)
            for k in (
                "window_days",
                "totals",
                "model_mix",
                "top_users",
                "engineer_scope",
                "cost_summary",
            )
            if cu.get(k) is not None
        }
    gp = report.get("github_productivity")
    if isinstance(gp, dict) and gp.get("configured"):
        digest["github_productivity"] = {
            k: gp.get(k)
            for k in ("window_days", "totals", "by_engineer", "delivery", "change_profile")
            if gp.get(k) is not None
        }
    ai = report.get("ai_productivity")
    if isinstance(ai, dict) and ai.get("configured"):
        digest["ai_productivity"] = {
            k: ai.get(k)
            for k in ("window_days", "summary", "matrix", "trend", "coaching")
            if ai.get(k) is not None
        }
    return _trim(digest)


def digest_for_slide(full: dict[str, Any], slide_type: str) -> dict[str, Any]:
    """Shrink the digest to fields relevant to *slide_type* (keeps prompts smaller)."""
    st = (slide_type or "").strip().lower()
    out: dict[str, Any] = {"days": full.get("days")}
    eng = full.get("eng_portfolio") or {}
    if isinstance(eng, dict):
        if st.startswith("cursor_") or st.startswith("ai_") or st in (
            "productivity_summary",
            "productivity_trend",
            "productivity_coaching",
        ):
            # Light eng context only
            out["eng_portfolio"] = {
                k: eng[k]
                for k in ("sprint", "closed_count", "in_flight_count")
                if eng.get(k) is not None
            }
        else:
            out["eng_portfolio"] = eng
    if st.startswith("cursor_") or st.startswith("ai_") or "efficiency" in st or st.startswith(
        "productivity_"
    ):
        if full.get("cursor_usage") is not None:
            out["cursor_usage"] = full["cursor_usage"]
    if st.startswith("github_") or st.startswith("ai_") or st.startswith("productivity_"):
        if full.get("github_productivity") is not None:
            out["github_productivity"] = full["github_productivity"]
        if full.get("ai_productivity") is not None:
            out["ai_productivity"] = full["ai_productivity"]
    if st in ("eng_portfolio_title", "eng_toc", "eng_divider", "data_quality", "eng_exec_summary"):
        # Keep richer context for overview slides
        out = dict(full)
    return _trim(out, max_chars=8_000)


_SYSTEM = (
    "You design one Engineering Portfolio Review slide for a VP of Engineering. "
    "Invent both visual structure and copy. No fixed layout template. "
    "Output MUST be valid compact JSON only — no markdown fences, no commentary. "
    "Prefer fewer, shorter strings over long prose. "
    + IR_SCHEMA_FOR_PROMPT
)


def _slide_brief(entry: dict[str, Any], deck_purpose: str) -> dict[str, Any]:
    return {
        "slide_id": entry.get("id"),
        "slide_type": entry.get("slide_type") or entry.get("id"),
        "title": entry.get("title") or entry.get("name") or entry.get("id"),
        "section_title": entry.get("title") if (entry.get("slide_type") == "eng_divider") else None,
        "prompt": (entry.get("prompt") or "").strip()[:600],
        "deck_purpose": (deck_purpose or "")[:400],
    }


def _llm_slide_completion(client: Any, *, model: str, messages: list[dict[str, str]]) -> str:
    kws: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": _SLIDE_MAX_TOKENS,
    }
    try:
        resp = _llm_create_with_retry(
            client, **{**kws, "response_format": {"type": "json_object"}}
        )
    except Exception as e:
        emsg = str(e).lower()
        if "response_format" in emsg or "json_object" in emsg:
            logger.debug("eng portfolio slides: retrying without response_format: %s", str(e)[:200])
            resp = _llm_create_with_retry(client, **kws)
        else:
            raise
    try:
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        raise EngPortfolioClaudeError(f"Claude response missing content: {e}") from e


def generate_slide_ir_via_claude(
    *,
    entry: dict[str, Any],
    digest: dict[str, Any],
    deck_purpose: str = "",
    client: Any | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Call the eng-portfolio LLM for a single slide; return normalized IR or raise."""
    cl = client or eng_portfolio_llm_client()
    model_name = model or CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL
    brief = _slide_brief(entry, deck_purpose)
    slide_type = str(brief.get("slide_type") or "")
    scoped = digest_for_slide(digest, slide_type)
    user = (
        "Design this slide end-to-end (structure + content). "
        "Return compact valid JSON only.\n\n"
        f"SLIDE BRIEF:\n{json.dumps(brief, ensure_ascii=False)}\n\n"
        f"DATA DIGEST:\n{json.dumps(scoped, default=str, ensure_ascii=False)}\n"
    )
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": user},
    ]
    last_err: Exception | None = None
    text = ""
    for attempt in range(1, _SLIDE_MAX_ATTEMPTS + 1):
        try:
            text = _llm_slide_completion(cl, model=model_name, messages=messages)
        except Exception as e:
            raise EngPortfolioClaudeError(
                f"Claude API failed for slide {brief.get('slide_id')!r}: {e}"
            ) from e
        try:
            parsed = _parse_slide_ir_json(text)
            return normalize_slide_ir(parsed)
        except (json.JSONDecodeError, ValueError, EngPortfolioClaudeError) as e:
            last_err = e
            logger.warning(
                "eng portfolio Claude JSON parse failed for %s (attempt %d/%d): %s",
                brief.get("slide_id"),
                attempt,
                _SLIDE_MAX_ATTEMPTS,
                e,
            )
            if attempt >= _SLIDE_MAX_ATTEMPTS:
                break
            # Repair pass: ask model to rewrite the broken payload as valid compact JSON.
            messages = [
                {"role": "system", "content": _SYSTEM},
                {
                    "role": "user",
                    "content": (
                        "Your previous answer was invalid or truncated JSON. "
                        "Rewrite it as one compact valid JSON object for this slide. "
                        "Keep ≤10 short elements; omit long speaker_notes.\n\n"
                        f"SLIDE BRIEF:\n{json.dumps(brief, ensure_ascii=False)}\n\n"
                        f"BROKEN OUTPUT:\n{(text or '')[:2500]}\n"
                    ),
                },
            ]
    raise EngPortfolioClaudeError(
        f"Claude returned non-JSON for slide {brief.get('slide_id')!r}: {last_err}; "
        f"head={((text or '')[:240])!r}"
    )


def generate_eng_portfolio_slide_irs(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    *,
    deck_purpose: str = "",
    max_workers: int = 4,
) -> list[dict[str, Any]]:
    """Generate IR for every plan entry (parallel). Raises on any failure unless fallback allowed."""
    digest = build_eng_portfolio_digest(report)
    client = eng_portfolio_llm_client()
    model = CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL
    results: list[dict[str, Any] | None] = [None] * len(slide_plan)
    errors: list[str] = []

    def _one(i: int, entry: dict[str, Any]) -> tuple[int, dict[str, Any] | None, str | None]:
        try:
            ir = generate_slide_ir_via_claude(
                entry=entry,
                digest=digest,
                deck_purpose=deck_purpose,
                client=client,
                model=model,
            )
            return i, ir, None
        except EngPortfolioClaudeError as e:
            return i, None, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_one, i, e) for i, e in enumerate(slide_plan)]
        for fut in as_completed(futs):
            i, ir, err = fut.result()
            if err:
                errors.append(err)
                logger.error("eng portfolio Claude slide failed: %s", err)
            else:
                results[i] = ir

    if errors and not _allow_fallback():
        raise EngPortfolioClaudeError(
            f"{len(errors)} Claude slide generation failure(s): " + "; ".join(errors[:5])
        )
    if errors and _allow_fallback():
        logger.warning(
            "CORTEX_ENG_PORTFOLIO_LLM_ALLOW_FALLBACK: %d slide(s) failed; "
            "caller must use hand-built builders for gaps",
            len(errors),
        )
    # Fill any holes with empty marker
    out: list[dict[str, Any]] = []
    for i, ir in enumerate(results):
        if ir is None:
            out.append({"_claude_failed": True, "slide_id": slide_plan[i].get("id")})
        else:
            out.append(ir)
    return out


def render_eng_portfolio_claude_slide_plan(
    report: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    deck_id: str,
    *,
    deck_purpose: str = "",
) -> tuple[list[dict], int, list[tuple[str, dict[str, Any]]], dict[str, Any] | None, list[dict[str, Any]]]:
    """Claude-IR path with the same return shape as ``render_slide_plan``."""
    from .slide_registry import get_slide_builder

    irs = generate_eng_portfolio_slide_irs(
        report, slide_plan, deck_purpose=deck_purpose or str(report.get("deck_purpose") or "")
    )
    report["_claude_slide_irs"] = irs

    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []
    used_slide_sids: set[str] = set()
    plan_work = list(slide_plan)

    for entry, ir in zip(plan_work, irs):
        base_sid = _slide_object_id_base(str(entry["id"]), idx)
        sid = _unique_slide_object_id_base(str(entry["id"]), idx, used_slide_sids)
        used_slide_sids.add(sid)
        report["_current_slide"] = entry

        if ir.get("_claude_failed"):
            if not _allow_fallback():
                raise EngPortfolioClaudeError(f"Missing Claude IR for slide {entry.get('id')!r}")
            # Opt-in fallback to hand-built builder
            slide_type = entry.get("slide_type", entry["id"])
            builder = get_slide_builder(slide_type)
            if not builder:
                raise EngPortfolioClaudeError(
                    f"Claude failed and no hand-built builder for {slide_type!r}"
                )
            logger.warning(
                "Falling back to hand-built builder for %s (Claude IR missing)",
                slide_type,
            )
            ret = builder(reqs, sid, report, idx)
            next_idx, note_ids = _normalize_builder_return(ret, sid)
            for nid in note_ids:
                note_targets.append((nid, dict(entry)))
            idx = next_idx
            continue

        next_idx = render_slide_ir(reqs, sid, ir, idx)
        notes = (ir.get("speaker_notes") or "").strip()
        if notes:
            entry_notes = dict(entry)
            entry_notes["_claude_speaker_notes"] = notes
            note_targets.append((sid, entry_notes))
        else:
            note_targets.append((sid, dict(entry)))
        idx = next_idx

    slides_created = idx - 1
    logger.info(
        "eng portfolio Claude slides: deck=%s model=%s slides=%d elements_ok",
        deck_id,
        CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL,
        slides_created,
    )
    return reqs, slides_created, note_targets, None, plan_work
