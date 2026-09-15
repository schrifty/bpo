"""LLM designs full engineering-portfolio slides (layout + content).

Deck YAML still defines slide order / ids. Hand-built Python slide builders are
bypassed when ``CORTEX_ENG_PORTFOLIO_LLM_SLIDES`` / ``CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES``
is on. A deck-level narrative pass runs first; every slide prompt then includes that
spine plus explicit LeanDNA chrome (see ``docs/PRESENTATION/CLAUDE_DECK_STYLE_GUIDE.md``).
Provider is Anthropic Claude via ``eng_portfolio_llm_client`` (default: ``claude-opus-5``).
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
_MAX_SLIDE_DIGEST_CHARS = 8_000
# The Jira portfolio blob runs ~700 KB, so each section gets its own budget instead of
# the big one crowding the small integration payloads out of the digest entirely.
# Generous: per-slide scoping below picks the sections a slide argues from, so the
# shared digest must still hold them all when it reaches digest_for_slide.
_MAX_ENG_SECTION_CHARS = 60_000
_MAX_INTEGRATION_SECTION_CHARS = 2_500
_PROTECTED_DIGEST_KEYS = (
    "days",
    "eng_portfolio",
    "cursor_usage",
    "github_productivity",
    "ai_productivity",
)
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


def _json_len(obj: Any) -> int:
    return len(json.dumps(obj, default=str, ensure_ascii=False))


def _slim_value(value: Any, *, list_cap: int) -> Any:
    """Cap the collections inside one section so a single blob cannot dominate."""
    if isinstance(value, list):
        return value[:list_cap]
    if not isinstance(value, dict):
        return value
    out: dict[str, Any] = {}
    for k, v in value.items():
        if isinstance(v, list):
            out[k] = v[:list_cap]
        elif isinstance(v, dict):
            out[k] = {
                nk: (nv[:list_cap] if isinstance(nv, list) else nv)
                for nk, nv in v.items()
                if not isinstance(nv, dict)
            }
        else:
            out[k] = v
    return out


def _trim(
    obj: Any, *, max_chars: int = _MAX_DIGEST_CHARS, protect: tuple[str, ...] = ()
) -> Any:
    """Shrink *obj* to fit *max_chars*, thinning the biggest sections first.

    Sections named in *protect* are never dropped: the integration payloads are small
    and are the only source their slides have, so losing them would make a configured
    integration look like missing data.
    """
    if not isinstance(obj, dict) or _json_len(obj) <= max_chars:
        return obj
    out = dict(obj)
    for cap in (8, 5, 3, 1):
        if _json_len(out) <= max_chars:
            break
        for key in sorted(out, key=lambda k: _json_len(out[k]), reverse=True):
            if _json_len(out) <= max_chars:
                break
            out[key] = _slim_value(out[key], list_cap=cap)
    while _json_len(out) > max_chars:
        droppable = [k for k in out if k not in protect]
        if not droppable:
            break
        out.pop(max(droppable, key=lambda k: _json_len(out[k])))
    return out


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
                "customer_reported_bugs",
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
    digest["eng_portfolio"] = _trim(
        digest["eng_portfolio"], max_chars=_MAX_ENG_SECTION_CHARS
    )
    cu = report.get("cursor_usage")
    if isinstance(cu, dict) and cu.get("configured"):
        digest["cursor_usage"] = _trim(
            {
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
            },
            max_chars=_MAX_INTEGRATION_SECTION_CHARS,
        )
    gp = report.get("github_productivity")
    if isinstance(gp, dict) and gp.get("configured"):
        digest["github_productivity"] = _trim(
            {
                k: gp.get(k)
                for k in ("window_days", "totals", "by_engineer", "delivery", "change_profile")
                if gp.get(k) is not None
            },
            max_chars=_MAX_INTEGRATION_SECTION_CHARS,
        )
    ai = report.get("ai_productivity")
    if isinstance(ai, dict) and ai.get("configured"):
        digest["ai_productivity"] = _trim(
            {
                k: ai.get(k)
                for k in ("window_days", "summary", "matrix", "trend", "coaching")
                if ai.get(k) is not None
            },
            max_chars=_MAX_INTEGRATION_SECTION_CHARS,
        )
    return digest


# Jira sections each slide actually argues from. Without this every eng slide sees the
# same blob, trims to the same few scalars, and repeats the same numbers.
_ENG_CORE_KEYS = ("sprint", "in_flight_count", "closed_count")
_ENG_SLIDE_KEYS: dict[str, tuple[str, ...]] = {
    "eng_team_roster": ("by_assignee", "by_assignee_active", "team_scorecard"),
    "eng_team_scorecard": ("team_scorecard", "by_assignee_active"),
    "eng_epic_progress": ("epic_progress", "themes"),
    "eng_velocity": ("sprint_velocity", "by_status"),
    "eng_current_sprint": ("by_status", "by_type", "flow"),
    "eng_flow_bottlenecks": ("flow", "by_status", "backlog_staleness"),
    "eng_capacity": ("by_assignee_active", "work_split", "flow"),
    "eng_work_split": ("work_split", "by_type", "themes"),
    "eng_bug_health": ("open_bugs", "blocker_critical", "bug_flow"),
    "eng_bug_flow": ("bug_flow", "open_bugs"),
    "eng_backlog_health": ("backlog_staleness", "by_status", "themes"),
    "eng_customer_reported_bugs": ("customer_reported_bugs", "open_bugs"),
    "eng_jira_project": ("project_snapshots",),
}
# Cover, agenda, dividers, exec summary and the Takeaways page argue across the whole
# review, so they get headline sections from every area rather than one slide's slice.
_ENG_OVERVIEW_KEYS = (
    "by_status",
    "by_type",
    "themes",
    "flow",
    "work_split",
    "customer_reported_bugs",
    "bug_flow",
    "blocker_critical",
    "team_scorecard",
    "sprint_velocity",
    "epic_progress",
)
_ENG_OVERVIEW_SLIDE_TYPES = (
    "eng_portfolio_title",
    "eng_toc",
    "eng_divider",
    "data_quality",
    "eng_exec_summary",
    "eng_takeaways",
)


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
                k: eng[k] for k in _ENG_CORE_KEYS if eng.get(k) is not None
            }
        elif st in _ENG_SLIDE_KEYS:
            out["eng_portfolio"] = {
                k: eng[k]
                for k in _ENG_CORE_KEYS + _ENG_SLIDE_KEYS[st]
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
    if st in _ENG_OVERVIEW_SLIDE_TYPES:
        # Cross-cutting context: headline sections from every area, plus integrations
        out = dict(full)
        if isinstance(eng, dict):
            out["eng_portfolio"] = {
                k: eng[k]
                for k in _ENG_CORE_KEYS + _ENG_OVERVIEW_KEYS
                if eng.get(k) is not None
            }
    return _trim(
        out, max_chars=_MAX_SLIDE_DIGEST_CHARS, protect=_PROTECTED_DIGEST_KEYS
    )


_THEME = """
THEME (required chrome — do not invent a different brand):
- Canvas 720×405, white page (#FFFFFF). Content column x=48, w=624.
- Content slides: navy #0B1F33 header bar at y=0, h=48; title 20–24pt bold white
  inside it. Title states the insight with a number (not a topic label).
- Cover: full navy field, deck name as hero, one supporting line (audience + as-of).
  No KPI row on the cover.
- Dividers: full navy background, large white section title, plus a takeaway that
  previews the section message with a number. No "SECTION" eyebrow.
- Data slides: optional one-line context under the header → kpi_row (4–6 tiles,
  h≥72, shared #009AFF on #E8F4FC unless a tile is off-target / no-data / the
  single mint callout) and/or a short table → 2–4 bullets of interpretation →
  takeaway band at y=360, h=32, fill #EEF0F3.
- Color is status or series only. Never rainbow tiles. Palette only:
  #0B1F33 navy, #009AFF/#E8F4FC ordinary, #C0392B/#FDECEA attention,
  #6B7280/#EEF0F3 no data, #AEFFF6 one callout, #38C0CE rules/series 2.
""".strip()

_NARRATIVE_RULES = """
NARRATIVE:
- Audience: VP of Engineering. Operational review, not product marketing.
- Deck arc: cover → Takeaways → agenda → exec summary → sections (Team, Outcomes,
  Operational Health, Quality, Engineering Output, AI Tooling, Productivity) → appendix.
- Honor DECK NARRATIVE in the user message. This slide must support that spine;
  do not introduce a competing thesis or wander into another section's topic.
- Takeaways slide: 3–5 numbered bullets from DECK NARRATIVE.takeaways (implication
  + number). A reader of only that page should know what is going well, what is
  at risk, and what decision is asked.
- Dividers use DECK NARRATIVE.sections for this section. Data slides pick evidence
  that proves the section message.
- 'Portfolio' here means the customer book of business, never tickets or bugs —
  call work items the backlog, bug backlog, or queue.
- Banned filler: monitor closely, requires further review, strategic review,
  leverage synergies, demands attention, investigate further.
- Only digest facts. If a number is missing, say so — do not invent it.
- Every data slide and divider needs a takeaway with a supporting number.
  Exempt: cover, agenda, the Takeaways page itself, appendix data-quality.
""".strip()

_SYSTEM = (
    "You design one Engineering Portfolio Review slide for a VP of Engineering. "
    "Stay inside the IR vocabulary. Follow THEME and NARRATIVE exactly. "
    "For every table, calculate bottom = y + 26pt × total rows, reserve 12pt "
    "before the next element, and paginate or omit rows instead of overflowing. "
    "Output MUST be valid compact JSON only — no markdown fences, no commentary. "
    "Prefer fewer, shorter strings over long prose. "
    + _THEME
    + "\n"
    + _NARRATIVE_RULES
    + "\n"
    + IR_SCHEMA_FOR_PROMPT
)

_NARRATIVE_SYSTEM = (
    "You write the narrative spine of an Engineering Portfolio Review for a VP "
    "of Engineering. Return compact JSON only, no markdown. "
    "Use only facts and numbers in the data digest. Each takeaway and section "
    "line is one sentence that includes a supporting number and an implication. "
    "No filler ('monitor closely', 'investigate further'). "
    "'Portfolio' means the customer book of business, never tickets. "
    "Shape:\n"
    "{"
    '"cover_line":"audience + as-of ≤80 chars",'
    '"decision":"the one decision this review asks ≤120 chars",'
    '"takeaways":["implication + number","... 3 to 5 items"],'
    '"sections":{"Team & Org":"one sentence with a number"}'
    "}\n"
    "sections keys MUST match the provided section titles exactly."
)


def _slide_role(slide_type: str) -> str:
    st = (slide_type or "").strip().lower()
    if st == "eng_portfolio_title":
        return "cover"
    if st == "eng_takeaways":
        return "takeaways"
    if st == "eng_toc":
        return "agenda"
    if st == "eng_divider":
        return "divider"
    if st == "eng_exec_summary":
        return "exec_summary"
    if st == "data_quality":
        return "appendix"
    return "data"


def _slide_archetype(role: str) -> str:
    return {
        "cover": "cover",
        "takeaways": "takeaways",
        "agenda": "agenda",
        "divider": "section_divider",
        "exec_summary": "standing_snapshot",
        "appendix": "detail_scorecard",
        "data": "analytical_insight",
    }.get(role, "analytical_insight")


def annotate_eng_portfolio_plan(slide_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach section, role, and position so each slide prompt knows the deck arc."""
    n = len(slide_plan)
    section = ""
    out: list[dict[str, Any]] = []
    for i, entry in enumerate(slide_plan):
        st = str(entry.get("slide_type") or entry.get("id") or "")
        role = _slide_role(st)
        if role == "divider":
            section = str(entry.get("title") or "").strip()
        row = dict(entry)
        row["_role"] = role
        row["_archetype"] = _slide_archetype(role)
        row["_section"] = section
        row["_index"] = i + 1
        row["_of"] = n
        out.append(row)
    return out


def _section_titles(slide_plan: list[dict[str, Any]]) -> list[str]:
    titles: list[str] = []
    for entry in slide_plan:
        st = str(entry.get("slide_type") or entry.get("id") or "")
        if _slide_role(st) == "divider":
            title = str(entry.get("title") or "").strip()
            if title:
                titles.append(title)
    return titles


def generate_eng_portfolio_narrative(
    digest: dict[str, Any],
    slide_plan: list[dict[str, Any]],
    *,
    deck_purpose: str = "",
    client: Any | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """One Claude call that writes the deck spine every slide must honor."""
    cl = client or eng_portfolio_llm_client()
    model_name = model or CORTEX_ENG_PORTFOLIO_CLAUDE_MODEL
    overview = digest_for_slide(digest, "eng_takeaways")
    user = (
        "Write the narrative spine for this Engineering Portfolio Review.\n\n"
        f"DECK PURPOSE:\n{(deck_purpose or '')[:800]}\n\n"
        f"SECTION TITLES:\n{json.dumps(_section_titles(slide_plan), ensure_ascii=False)}\n\n"
        f"DATA DIGEST:\n{json.dumps(overview, default=str, ensure_ascii=False)}\n"
    )
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _NARRATIVE_SYSTEM},
        {"role": "user", "content": user},
    ]
    last_err: Exception | None = None
    text = ""
    for attempt in range(1, _SLIDE_MAX_ATTEMPTS + 1):
        try:
            text = _llm_slide_completion(cl, model=model_name, messages=messages)
        except Exception as e:
            raise EngPortfolioClaudeError(f"Claude API failed for deck narrative: {e}") from e
        try:
            parsed = _parse_slide_ir_json(text)
            if not isinstance(parsed, dict):
                raise EngPortfolioClaudeError("narrative JSON must be an object")
            takeaways = parsed.get("takeaways")
            if not isinstance(takeaways, list) or len(takeaways) < 3:
                raise EngPortfolioClaudeError("narrative needs at least 3 takeaways")
            sections = parsed.get("sections")
            if sections is not None and not isinstance(sections, dict):
                raise EngPortfolioClaudeError("narrative sections must be an object")
            return {
                "cover_line": str(parsed.get("cover_line") or "").strip()[:80],
                "decision": str(parsed.get("decision") or "").strip()[:120],
                "takeaways": [str(t).strip()[:160] for t in takeaways[:5] if str(t).strip()],
                "sections": {
                    str(k).strip(): str(v).strip()[:180]
                    for k, v in (sections or {}).items()
                    if str(k).strip() and str(v).strip()
                },
            }
        except (json.JSONDecodeError, ValueError, EngPortfolioClaudeError) as e:
            last_err = e
            logger.warning(
                "eng portfolio narrative JSON failed (attempt %d/%d): %s",
                attempt,
                _SLIDE_MAX_ATTEMPTS,
                e,
            )
            if attempt >= _SLIDE_MAX_ATTEMPTS:
                break
            messages = [
                {"role": "system", "content": _NARRATIVE_SYSTEM},
                {
                    "role": "user",
                    "content": (
                        "Your previous answer was invalid JSON or missing takeaways. "
                        "Rewrite as one compact valid JSON object with cover_line, "
                        "decision, takeaways (3-5), and sections.\n\n"
                        f"BROKEN OUTPUT:\n{(text or '')[:2500]}\n"
                    ),
                },
            ]
    raise EngPortfolioClaudeError(
        f"Claude returned unusable deck narrative: {last_err}; "
        f"head={((text or '')[:240])!r}"
    )


def _slide_brief(entry: dict[str, Any], deck_purpose: str) -> dict[str, Any]:
    st = str(entry.get("slide_type") or entry.get("id") or "")
    role = str(entry.get("_role") or _slide_role(st))
    section = str(entry.get("_section") or "")
    if role == "divider":
        section = str(entry.get("title") or section)
    return {
        "slide_id": entry.get("id"),
        "slide_type": st,
        "role": role,
        "archetype": str(entry.get("_archetype") or _slide_archetype(role)),
        "section": section or None,
        "position": f"{entry.get('_index') or '?'} of {entry.get('_of') or '?'}",
        "title": entry.get("title") or entry.get("name") or entry.get("id"),
        "section_title": entry.get("title") if role == "divider" else None,
        "prompt": (entry.get("prompt") or "").strip()[:800],
        "deck_purpose": (deck_purpose or "")[:600],
    }


def _llm_slide_completion(client: Any, *, model: str, messages: list[dict[str, str]]) -> str:
    # Claude Opus rejects temperature (deprecated for this model family).
    kws: dict[str, Any] = {
        "model": model,
        "messages": messages,
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
    narrative: dict[str, Any] | None = None,
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
        "Design this one slide (structure + content) using THEME chrome and the "
        "assigned archetype. Honor DECK NARRATIVE. Return compact valid JSON only.\n\n"
        f"DECK NARRATIVE:\n{json.dumps(narrative or {}, ensure_ascii=False)}\n\n"
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
    plan = annotate_eng_portfolio_plan(slide_plan)
    narrative = generate_eng_portfolio_narrative(
        digest,
        plan,
        deck_purpose=deck_purpose,
        client=client,
        model=model,
    )
    report["_claude_deck_narrative"] = narrative
    logger.info(
        "eng portfolio Claude narrative: takeaways=%d sections=%d decision=%s",
        len(narrative.get("takeaways") or []),
        len(narrative.get("sections") or {}),
        (narrative.get("decision") or "")[:80],
    )
    results: list[dict[str, Any] | None] = [None] * len(plan)
    errors: list[str] = []

    def _one(i: int, entry: dict[str, Any]) -> tuple[int, dict[str, Any] | None, str | None]:
        try:
            ir = generate_slide_ir_via_claude(
                entry=entry,
                digest=digest,
                deck_purpose=deck_purpose,
                narrative=narrative,
                client=client,
                model=model,
            )
            return i, ir, None
        except EngPortfolioClaudeError as e:
            return i, None, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_one, i, e) for i, e in enumerate(plan)]
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
    _warn_slides_without_takeaways(slide_plan, out)
    return out


# Slides that carry no data of their own, so a "so what" bar would be filler.
_TAKEAWAY_EXEMPT_SLIDE_TYPES = frozenset(
    {"eng_portfolio_title", "eng_toc", "eng_takeaways", "data_quality"}
)


def _warn_slides_without_takeaways(
    slide_plan: list[dict[str, Any]], irs: list[dict[str, Any]]
) -> None:
    """Surface slides Claude left without a takeaway so the gap is visible in logs."""
    missing = [
        str(entry.get("id") or "")
        for entry, ir in zip(slide_plan, irs)
        if not ir.get("_claude_failed")
        and str(entry.get("slide_type") or entry.get("id") or "")
        not in _TAKEAWAY_EXEMPT_SLIDE_TYPES
        and not any(
            str((el or {}).get("type") or "").lower() == "takeaway"
            for el in (ir.get("elements") or [])
            if isinstance(el, dict)
        )
    ]
    if missing:
        logger.warning(
            "eng portfolio Claude slides: %d slide(s) have no takeaway element: %s",
            len(missing),
            ", ".join(missing[:10]),
        )


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
