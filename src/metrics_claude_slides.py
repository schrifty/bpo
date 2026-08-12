"""Claude designs the metrics scorecard slides (layout + copy) for ``metrics-deck``.

The deck exists to answer one question for a reader who has not been following the
function: *where does this department stand right now?* Claude gets the KPI facts
plus that editorial brief and invents the slide structure; this module only turns
its IR into Google Slides requests via :mod:`claude_slide_ir`.

Strict by default (see ``fail-loud-integrations``): a Claude or parse failure
raises unless ``CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK`` is set, in which case the
caller reverts to the hand-built KPI-card grid.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from .claude_slide_ir import IR_SCHEMA_FOR_PROMPT, normalize_slide_ir, render_slide_ir
from .config import (
    CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK,
    CORTEX_METRICS_CLAUDE_MODEL,
    CORTEX_METRICS_CLAUDE_SLIDES,
    logger,
    metrics_llm_client,
)
from .eng_portfolio_claude_slides import _parse_slide_ir_json
from .llm_utils import _llm_create_with_retry

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .metrics_digest import DigestRow

_SLIDE_MAX_TOKENS = 16_384
_SLIDE_MAX_ATTEMPTS = 2
_ROWS_PER_DETAIL_SLIDE = 7

# Friendly names for the function a tag represents (falls back to Title Case).
_FUNCTION_LABELS = {
    "akkr": "Engineering & AI Program",
    "support": "Customer Support",
    "engineering": "Engineering",
    "mfr": "Manufacturing",
}


class MetricsClaudeError(RuntimeError):
    """Claude did not return usable slide IR for the metrics deck."""


def metrics_claude_slides_enabled() -> bool:
    return bool(CORTEX_METRICS_CLAUDE_SLIDES)


def metrics_claude_allow_fallback() -> bool:
    return bool(CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK)


def function_label(tag: str | None) -> str:
    if not tag:
        return "The Business"
    key = str(tag).strip().lower()
    return _FUNCTION_LABELS.get(key, str(tag).strip().replace("_", " ").title())


def _row_status(row: DigestRow) -> str:
    if row.error:
        return "error"
    if row.value is None:
        return "no_data"
    if row.target is None:
        return "no_target"
    return "off_target" if row.off_target else "on_target"


def _gap_to_target(row: DigestRow) -> tuple[str, float] | None:
    """Signed distance to target, positive when the KPI is doing better.

    Percent KPIs are compared in percentage points; a relative percent-of-a-percent
    gap (e.g. "-989%" for 163% against a 15% target) reads as nonsense on a slide.
    """
    if row.value is None or row.target is None:
        return None
    if row.unit == "percent":
        delta = float(row.value) - float(row.target)
        key = "points_vs_target"
    elif row.target:
        delta = (float(row.value) - float(row.target)) / abs(float(row.target)) * 100.0
        key = "pct_vs_target"
    else:
        return None
    if row.direction == "lower":
        delta = -delta
    return key, round(delta, 1)


def row_fact(row: DigestRow) -> dict[str, Any]:
    """One KPI as facts Claude may cite (display strings keep units/formatting)."""
    fact: dict[str, Any] = {
        "name": row.name,
        "value": row.value_display,
        "target": row.target_display,
        "status": _row_status(row),
    }
    if row.direction:
        fact["better_when"] = row.direction
    gap = _gap_to_target(row)
    if gap is not None:
        fact[gap[0]] = gap[1]
    if row.context:
        fact["how_computed"] = row.context
    if row.description:
        fact["definition"] = " ".join(str(row.description).split())[:220]
    if row.error:
        fact["error"] = str(row.error)[:160]
    return fact


def build_metrics_digest(
    rows: list[DigestRow], *, tag: str | None, as_of: str
) -> dict[str, Any]:
    """Compact scorecard facts: standing, counts, and every KPI."""
    facts = [row_fact(r) for r in rows]
    counted = [f for f in facts if f["status"] in ("on_target", "off_target")]
    off = [f for f in facts if f["status"] == "off_target"]
    unavailable = [f for f in facts if f["status"] in ("error", "no_data")]
    return {
        "function": function_label(tag),
        "as_of": as_of,
        "kpi_count": len(facts),
        "on_target_count": len(counted) - len(off),
        "off_target_count": len(off),
        "measured_count": len(counted),
        "unavailable_count": len(unavailable),
        "kpis": facts,
    }


def build_metrics_deck_plan(digest: dict[str, Any]) -> list[dict[str, Any]]:
    """Slide plan: standing first, then what is off track, then the full table."""
    facts: list[dict[str, Any]] = list(digest.get("kpis") or [])
    off = [f for f in facts if f["status"] == "off_target"]
    function = digest.get("function")
    plan: list[dict[str, Any]] = [
        {
            "id": "standing",
            "slide_type": "standing",
            "title": f"Where {function} Stands",
            "purpose": (
                "The only slide a busy reader may look at. State the overall verdict "
                "in a headline, then show the handful of KPIs that justify it."
            ),
            "must_include": [
                "A headline sentence naming the overall state of the function "
                "(healthy / mixed / under pressure) and the single biggest reason.",
                "A kpi_row of 4-6 tiles for the most decision-relevant KPIs: "
                "value, and target in the label (e.g. 'TTFR (target 48h)').",
                "Color tiles so off-target reads red-ish and on-target reads blue/teal.",
                "One takeaway line: the so-what for a leader.",
            ],
            "kpis": facts,
        }
    ]
    if off:
        plan.append(
            {
                "id": "attention",
                "slide_type": "attention",
                "title": "Off Target — What Needs Attention",
                "purpose": (
                    "Show every KPI that is missing its target, worst gap first, so a "
                    "reader knows where the function is losing ground."
                ),
                "must_include": [
                    "A table with columns KPI, Current, Target, Gap — worst first. "
                    "Write the gap as points (e.g. '-148 pts') when the fact is "
                    "points_vs_target, and as a percent when it is pct_vs_target.",
                    "A short bullet per top-3 miss saying what the number implies "
                    "(use only the supplied definition/how_computed facts).",
                    "A takeaway naming the one thing to fix first.",
                ],
                "kpis": off,
            }
        )
    on_track = [f for f in facts if f["status"] != "off_target"]
    ordered = off + on_track
    total_pages = max(
        1, (len(ordered) + _ROWS_PER_DETAIL_SLIDE - 1) // _ROWS_PER_DETAIL_SLIDE
    )
    # Spread rows evenly so the last page is never a lone straggler.
    per_page = max(1, -(-len(ordered) // total_pages))
    for page, start in enumerate(range(0, len(ordered), per_page), 1):
        chunk = ordered[start : start + per_page]
        suffix = f" ({page} of {total_pages})" if total_pages > 1 else ""
        plan.append(
            {
                "id": f"detail_{page}",
                "slide_type": "detail",
                "title": f"{function} Scorecard{suffix}",
                "purpose": (
                    "The reference view: every KPI with its value, target, status, and "
                    "how it was computed, scannable in one pass."
                ),
                "must_include": [
                    "One table row per KPI with columns KPI, Value, Target, Status.",
                    "Status wording: 'Off target', 'On target', or 'No data'.",
                    "Keep cells short; do not invent KPIs beyond the supplied rows.",
                    "Write as if these are the KPIs for this page — never mention "
                    "other pages, missing rows, or the deck's structure.",
                ],
                "kpis": chunk,
            }
        )
    return plan


def deck_purpose_brief(digest: dict[str, Any]) -> str:
    function = digest.get("function")
    return (
        f"This deck gives a reader a quick, clear view of where {function} stands "
        f"as of {digest.get('as_of')}. Assume the reader has not followed this "
        "function and has 60 seconds: they must leave knowing the overall health, "
        "which KPIs are off target and by how much, and what it means. "
        "Lead with the judgment, then the numbers that support it. "
        f"Scope: {digest.get('kpi_count')} KPIs, "
        f"{digest.get('off_target_count')} off target, "
        f"{digest.get('on_target_count')} on target, "
        f"{digest.get('unavailable_count')} unavailable."
    )


_SYSTEM = (
    "You design one slide of an executive KPI scorecard deck. The deck's only job "
    "is to make where a department stands obvious at a glance: verdict first, "
    "evidence second, no data dumps and no filler. Write like a chief of staff "
    "briefing an executive — plain language, no hype, no invented numbers. "
    "Follow LeanDNA Claude deck style: navy header + white title, brand palette "
    "only, one takeaway per slide, short KPI labels, only facts in the brief. "
    "Use only the KPI facts provided; never estimate, forecast, or add commentary "
    "the data does not support. If a KPI is unavailable, say so plainly. "
    "Output MUST be valid compact JSON only — no markdown fences, no commentary. "
    + IR_SCHEMA_FOR_PROMPT
)

_STATUS_COLOR_GUIDE = (
    "Status colors: off target #C0392B (or #FDECEA fill), on target #009AFF / "
    "#38C0CE, neutral/no data #6B7280 on #EEF0F3. Every slide gets a navy #0B1F33 "
    "header bar with white title text at the top."
)


def _slide_brief(entry: dict[str, Any], digest: dict[str, Any]) -> dict[str, Any]:
    return {
        "slide_id": entry.get("id"),
        "slide_type": entry.get("slide_type"),
        "title": entry.get("title"),
        "purpose": entry.get("purpose"),
        "must_include": entry.get("must_include"),
        "function": digest.get("function"),
        "as_of": digest.get("as_of"),
        "scorecard_counts": {
            "kpis": digest.get("kpi_count"),
            "off_target": digest.get("off_target_count"),
            "on_target": digest.get("on_target_count"),
            "unavailable": digest.get("unavailable_count"),
        },
    }


def _completion(client: Any, *, model: str, messages: list[dict[str, str]]) -> str:
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
            logger.debug("metrics Claude slides: retrying without response_format")
            resp = _llm_create_with_retry(client, **kws)
        else:
            raise
    try:
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        raise MetricsClaudeError(f"Claude response missing content: {e}") from e


def generate_metrics_slide_ir(
    *,
    entry: dict[str, Any],
    digest: dict[str, Any],
    client: Any | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    """Design one scorecard slide with Claude; return normalized IR or raise."""
    cl = client or metrics_llm_client()
    model_name = model or CORTEX_METRICS_CLAUDE_MODEL
    brief = _slide_brief(entry, digest)
    user = (
        "Design this scorecard slide end-to-end (structure + content). "
        "Return compact valid JSON only.\n\n"
        f"DECK PURPOSE:\n{deck_purpose_brief(digest)}\n\n"
        f"{_STATUS_COLOR_GUIDE}\n\n"
        f"SLIDE BRIEF:\n{json.dumps(brief, ensure_ascii=False)}\n\n"
        f"KPI FACTS FOR THIS SLIDE:\n"
        f"{json.dumps(entry.get('kpis') or [], default=str, ensure_ascii=False)}\n"
    )
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": user},
    ]
    last_err: Exception | None = None
    text = ""
    for attempt in range(1, _SLIDE_MAX_ATTEMPTS + 1):
        try:
            text = _completion(cl, model=model_name, messages=messages)
        except Exception as e:
            raise MetricsClaudeError(
                f"Claude API failed for slide {brief.get('slide_id')!r}: {e}"
            ) from e
        try:
            return normalize_slide_ir(_parse_slide_ir_json(text))
        except (json.JSONDecodeError, ValueError) as e:
            last_err = e
            logger.warning(
                "metrics Claude JSON parse failed for %s (attempt %d/%d): %s",
                brief.get("slide_id"),
                attempt,
                _SLIDE_MAX_ATTEMPTS,
                e,
            )
            if attempt >= _SLIDE_MAX_ATTEMPTS:
                break
            messages = [
                {"role": "system", "content": _SYSTEM},
                {
                    "role": "user",
                    "content": (
                        "Your previous answer was invalid or truncated JSON. "
                        "Rewrite it as one compact valid JSON object for this slide. "
                        "Keep <=10 short elements; omit speaker_notes.\n\n"
                        f"SLIDE BRIEF:\n{json.dumps(brief, ensure_ascii=False)}\n\n"
                        f"BROKEN OUTPUT:\n{(text or '')[:2500]}\n"
                    ),
                },
            ]
    raise MetricsClaudeError(
        f"Claude returned non-JSON for slide {brief.get('slide_id')!r}: {last_err}; "
        f"head={((text or '')[:240])!r}"
    )


def generate_metrics_slide_irs(
    plan: list[dict[str, Any]],
    digest: dict[str, Any],
    *,
    max_workers: int = 4,
) -> list[dict[str, Any]]:
    """Design every planned slide in parallel. Raises on the first failure."""
    client = metrics_llm_client()
    results: list[dict[str, Any] | None] = [None] * len(plan)
    errors: list[str] = []

    def _one(i: int, entry: dict[str, Any]) -> tuple[int, dict[str, Any] | None, str | None]:
        try:
            return i, generate_metrics_slide_ir(
                entry=entry, digest=digest, client=client
            ), None
        except MetricsClaudeError as e:
            return i, None, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_one, i, e) for i, e in enumerate(plan)]
        for fut in as_completed(futs):
            i, ir, err = fut.result()
            if err:
                errors.append(err)
                logger.error("metrics Claude slide failed: %s", err)
            else:
                results[i] = ir

    if errors:
        raise MetricsClaudeError(
            f"{len(errors)} Claude slide failure(s): " + "; ".join(errors[:3])
        )
    return [ir for ir in results if ir is not None]


def render_metrics_claude_slides(
    reqs: list[dict[str, Any]],
    rows: list[DigestRow],
    *,
    tag: str | None,
    as_of: str,
    start_index: int,
) -> tuple[int, list[str]]:
    """Append Claude-designed scorecard slides. Returns (next index, slide ids)."""
    digest = build_metrics_digest(rows, tag=tag, as_of=as_of)
    plan = build_metrics_deck_plan(digest)
    irs = generate_metrics_slide_irs(plan, digest)

    idx = start_index
    sids: list[str] = []
    for entry, ir in zip(plan, irs):
        sid = f"metrics_c{idx}_{str(entry.get('id') or 'slide')}"
        idx = render_slide_ir(reqs, sid, ir, idx)
        sids.append(sid)
    logger.info(
        "metrics Claude slides: model=%s tag=%s slides=%d kpis=%d",
        CORTEX_METRICS_CLAUDE_MODEL,
        tag or "all",
        len(sids),
        len(rows),
    )
    return idx, sids
