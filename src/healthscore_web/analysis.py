"""Claude analysis of one entity's Health Score inputs and their history.

The digest is built from the same observations the score uses. Claude is told
the band so a green site's write-up dwells on what is working and a red site's
on what is wrong; unscored inputs are listed as gaps, never guessed at.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Callable

from src.config import LLM_MODEL, anthropic_llm_client, logger
from src.healthscore_web.api import (
    _apply_override_flags,
    _bounded_points,
    _raised_override_names,
    _score_numbers,
    score_band,
    shown_score,
)
from src.healthscore_web.store import latest_by_metric
from src.kpi_web.situation import _message_text, strip_markdown_chrome
from src.llm_utils import _llm_create_with_retry

_HISTORY_POINTS = 8
_MAX_DIGEST_CHARS = 60_000


class HealthscoreAnalysisError(RuntimeError):
    """Claude site analysis failed. No placeholder text is substituted."""


ANALYSIS_SYSTEM_PROMPT = """\
You write the site analysis inside Cortex Healthscore for LeanDNA's customer success \
team. The reader clicked Analysis on one customer entity (a site) in the Healthscore \
Report and wants to know what is going on with that site right now.

Ground rules
- Use only the JSON digest. Every number, period, and input name you cite must appear \
in it. Never invent a reading, a trend, a person, or a cause.
- `health_score` is 0–100 and is computed only from inputs that have a reading. Each \
input's `score_pct` is its points as a share of its maximum; `weight` is how much it \
counts. An input with `latest: null` is unscored: name it as a gap in what we can see, \
do not guess its value.
- `history` on each input is oldest to newest. `daily_scores` is the site's health \
score by day, oldest to newest. Call out a change only when the history shows it.
- `overrides_raised` are hard flags that force the score to 0. If any is raised, lead \
with it.
- Copy input names exactly from `name`.

Devastating readings
- `lead_with` lists inputs that are devastating: Usage level (breadth & depth) at \
30% or below (0 or 1 of 6 points, including no usage data), and ROI multiple under \
3x (0 of 6 points). An unscored ROI multiple is a gap, not a low multiple.
- When `lead_with` is not empty, the opening and the first bullets are those inputs, \
ahead of the band emphasis below, including on a green site. Say plainly that the \
site is in trouble because of them. A weight of zero does not make one less serious.
- A raised override still comes before these.

Emphasis follows the band
- Green: spend most of the analysis on why the site is doing well and which inputs \
carry the score, then briefly, one or two sentences, on what could still improve.
- Red: spend most of the analysis on what is wrong and which inputs are pulling the \
score down, then briefly, one or two sentences, on what is going well.
- Yellow: split the time evenly between strengths and weaknesses.
- Unscored: explain that nothing is scored yet and list what would need a reading.

Format: plain text, no markdown headings, bold, or tables. Open with one or two \
sentences on where the site stands. Then short bullets, each starting with "- ", \
naming the input and its reading. Finish with one line on what the team should do \
next. 120–220 words. Never exceed 260.
"""


def _is_devastating(key: str, latest: dict[str, Any] | None) -> bool:
    """Bottom band of usage level (≤30%) or ROI multiple (<3x)."""
    if not latest or latest.get("points") is None:
        return False
    points = float(latest["points"])
    if key == "usage_level":
        return points <= 1
    if key == "roi_multiple":
        return points <= 0
    return False


def _input_history(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: str(row.get("period_key") or ""))
    out = []
    for row in ordered[-_HISTORY_POINTS:]:
        out.append(
            {
                "period_key": row.get("period_key"),
                "value": row.get("effective_value"),
                "points": row.get("effective_points"),
                "overridden": bool(row.get("overridden")),
                "note": row.get("note"),
            }
        )
    return out


def build_analysis_digest(
    *,
    entity: dict[str, Any],
    framework: dict[str, Any],
    observations: list[dict[str, Any]],
    daily_scores: list[dict[str, Any]],
    as_of: date | None = None,
) -> dict[str, Any]:
    """Facts Claude may use: each input's definition, latest reading, and history."""
    latest = latest_by_metric(observations)
    by_metric: dict[str, list[dict[str, Any]]] = {}
    for row in observations:
        by_metric.setdefault(str(row.get("metric_name")), []).append(row)

    inputs: list[dict[str, Any]] = []
    for definition in framework["inputs"]:
        key = str(definition["key"])
        if definition.get("deactivated"):
            continue
        row = latest.get(key)
        weight = definition.get("weight")
        max_points = definition.get("max_points")
        latest_block = None
        if row and row.get("effective_points") is not None and max_points:
            points = _bounded_points(float(row["effective_points"]), definition)
            latest_block = {
                "period_key": row.get("period_key"),
                "value": row.get("effective_value"),
                "points": points,
                "score_pct": round(points / float(max_points) * 100, 1),
                "source": row.get("source_mode"),
                "note": row.get("note"),
            }
        inputs.append(
            {
                "key": key,
                "name": definition.get("name"),
                "pillar": definition.get("pillar"),
                "description": definition.get("description"),
                "scoring": definition.get("scoring"),
                "grain": definition.get("grain"),
                "weight": weight,
                "max_points": max_points,
                "latest": latest_block,
                "devastating": _is_devastating(key, latest_block),
                "history": _input_history(by_metric.get(key, [])),
            }
        )

    numbers = _apply_override_flags(
        _score_numbers(
            framework,
            {
                str(definition["key"]): (latest.get(str(definition["key"])) or {}).get(
                    "effective_points"
                )
                for definition in framework["inputs"]
            },
        ),
        _raised_override_names(
            framework,
            {
                str(definition["key"]): (latest.get(str(definition["key"])) or {}).get(
                    "effective_value"
                )
                for definition in framework["overrides"]
            },
        ),
    )
    score = numbers["score"]
    shown = shown_score(score)
    return {
        "as_of": (as_of or date.today()).isoformat(),
        "entity": {
            "id": entity.get("id"),
            "name": entity.get("name"),
            "parent": entity.get("parent_name"),
            "contract_status": entity.get("contract_status"),
            "contract_end_date": entity.get("contract_end_date"),
            "arr": entity.get("arr"),
        },
        "health_score": shown,
        "band": score_band(score) or "unscored",
        "weight_scored_pct": numbers.get("coverage_pct"),
        "overrides_raised": numbers.get("zeroed_by") or [],
        "lead_with": [item["name"] for item in inputs if item["devastating"]],
        "scored_inputs": sum(1 for item in inputs if item["latest"]),
        "unscored_inputs": [item["name"] for item in inputs if not item["latest"]],
        "inputs": inputs,
        "daily_scores": [
            {"period_key": row.get("period_key"), "value": row.get("value")}
            for row in daily_scores
        ],
    }


def _prompt(digest: dict[str, Any]) -> tuple[str, str]:
    payload = json.dumps(digest, default=str, separators=(",", ":"))
    if len(payload) > _MAX_DIGEST_CHARS:
        payload = payload[:_MAX_DIGEST_CHARS] + "…"
    band = digest.get("band") or "unscored"
    user = (
        f"Analyze this site. Its band is {band}. Follow the emphasis rule for that band.\n\n"
        f"{payload}"
    )
    return ANALYSIS_SYSTEM_PROMPT, user


def generate_site_analysis(
    digest: dict[str, Any],
    *,
    invoke: Callable[..., str] | None = None,
) -> str:
    """Ask Claude for the analysis. Empty or failed output raises."""
    system, user = _prompt(digest)
    call = invoke if invoke is not None else _claude_complete
    text = strip_markdown_chrome(call(system=system, user=user) or "")
    if not text:
        raise HealthscoreAnalysisError("Claude returned an empty site analysis")
    return text


def _claude_complete(*, system: str, user: str) -> str:
    try:
        client = anthropic_llm_client()
    except RuntimeError as exc:
        raise HealthscoreAnalysisError(str(exc)) from exc
    model = (LLM_MODEL if str(LLM_MODEL).startswith("claude") else "") or "claude-sonnet-4-6"
    try:
        resp = _llm_create_with_retry(
            client,
            model=model,
            max_tokens=1500,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score site analysis Claude call failed")
        raise HealthscoreAnalysisError(f"Claude site analysis call failed: {exc}") from exc
    text = _message_text(resp)
    if not text:
        try:
            finish = resp.choices[0].finish_reason
        except (AttributeError, IndexError, TypeError):
            finish = "unknown"
        raise HealthscoreAnalysisError(
            f"Claude returned an empty site analysis (model={model}, finish_reason={finish})"
        )
    return text
