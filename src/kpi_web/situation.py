"""Claude briefing of stored KPIs vs roughly a week ago and a month ago."""

from __future__ import annotations

import calendar
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from src.config import LLM_MODEL, anthropic_llm_client, logger
from src.kpi_store import (
    GRAIN_DAILY,
    GRAIN_HOURLY,
    GRAIN_MONTH,
    GRAIN_QUARTERLY,
    GRAIN_WEEKLY,
    StoredKPI,
    list_kpis,
)
from src.llm_utils import _llm_create_with_retry
from src.metrics_registry import (
    has_metric_generator,
    iter_all_metrics,
    registry_metric_direction,
    registry_metric_grain,
    registry_metric_target,
)

_MAX_DIGEST_CHARS = 24_000


class KpiSituationError(RuntimeError):
    """Claude situation briefing failed (do not substitute placeholder copy)."""


@dataclass(frozen=True)
class _Point:
    day: date
    value: float
    period_key: str


def period_as_date(grain: str, period_key: str, as_of: str | None = None) -> date | None:
    """Calendar day used to age a stored reading for week/month comparisons."""
    raw_as_of = (as_of or "").strip()
    if raw_as_of:
        try:
            return date.fromisoformat(raw_as_of[:10])
        except ValueError:
            pass
    key = (period_key or "").strip()
    g = (grain or "").strip().lower()
    try:
        if g == GRAIN_HOURLY and "T" in key:
            return date.fromisoformat(key.split("T", 1)[0])
        if g == GRAIN_DAILY:
            return date.fromisoformat(key[:10])
        if g == GRAIN_WEEKLY:
            year_s, week_s = key.split("-W", 1)
            return date.fromisocalendar(int(year_s), int(week_s), 1)
        if g == GRAIN_MONTH:
            year_s, month_s = key.split("-", 1)
            year, month = int(year_s), int(month_s)
            last = calendar.monthrange(year, month)[1]
            return date(year, month, last)
        if g == GRAIN_QUARTERLY:
            year_s, q_s = key.split("-Q", 1)
            year, quarter = int(year_s), int(q_s)
            month = quarter * 3
            last = calendar.monthrange(year, month)[1]
            return date(year, month, last)
    except (ValueError, TypeError):
        return None
    try:
        return date.fromisoformat(key[:10])
    except ValueError:
        return None


def _pct_change(current: float, prior: float | None) -> float | None:
    if prior is None:
        return None
    if prior == 0:
        return None if current == 0 else None
    return round(100.0 * (current - prior) / abs(prior), 2)


def _nearest(points: list[_Point], target: date, *, earliest: date, latest: date) -> _Point | None:
    candidates = [p for p in points if earliest <= p.day <= latest]
    if not candidates:
        return None
    return min(candidates, key=lambda p: (abs((p.day - target).days), -p.day.toordinal()))


def _target_outcome(value: float, target: float | None, direction: str | None) -> str | None:
    if target is None or direction not in ("higher", "lower"):
        return None
    if direction == "higher":
        return "made" if value >= target else "missed"
    return "made" if value <= target else "missed"


def compare_series(
    points: list[_Point],
    *,
    grain: str,
    as_of: date,
) -> dict[str, Any]:
    """Pick current, ~7-day, and ~30-day readings from newest-first or unsorted points."""
    ok = sorted(points, key=lambda p: p.day, reverse=True)
    if not ok:
        return {"current": None, "week_ago": None, "month_ago": None}
    current = ok[0]
    older = [p for p in ok if p.day < current.day]
    g = (grain or "").strip().lower()
    week: _Point | None = None
    month: _Point | None = None
    if g in (GRAIN_MONTH, GRAIN_QUARTERLY):
        month = older[0] if older else None
    elif g == GRAIN_WEEKLY:
        week = _nearest(
            older,
            current.day - timedelta(days=7),
            earliest=current.day - timedelta(days=10),
            latest=current.day - timedelta(days=1),
        )
        month = _nearest(
            older,
            current.day - timedelta(days=28),
            earliest=current.day - timedelta(days=40),
            latest=current.day - timedelta(days=14),
        )
    else:
        week = _nearest(
            older,
            current.day - timedelta(days=7),
            earliest=current.day - timedelta(days=10),
            latest=current.day - timedelta(days=1),
        )
        month = _nearest(
            older,
            current.day - timedelta(days=30),
            earliest=current.day - timedelta(days=45),
            latest=current.day - timedelta(days=14),
        )
    def pack(p: _Point | None) -> dict[str, Any] | None:
        if p is None:
            return None
        return {"period_key": p.period_key, "date": p.day.isoformat(), "value": p.value}

    cur = pack(current)
    wk = pack(week)
    mo = pack(month)
    return {
        "current": cur,
        "week_ago": wk,
        "month_ago": mo,
        "week_change_pct": _pct_change(current.value, week.value if week else None),
        "month_change_pct": _pct_change(current.value, month.value if month else None),
        "as_of": as_of.isoformat(),
        "history_points": len(ok),
        "week_note": (
            None
            if wk
            else (
                "monthly/quarterly grain has no week-ago close"
                if g in (GRAIN_MONTH, GRAIN_QUARTERLY)
                else "no stored reading within ~7 days of the latest point"
            )
        ),
        "month_note": None if mo else "no stored reading within ~30 days of the latest point",
    }


def _points_from_rows(rows: list[StoredKPI]) -> list[_Point]:
    out: list[_Point] = []
    for row in rows:
        obs = row.effective_observation
        if not obs.ok or obs.display_value is None:
            continue
        try:
            value = float(obs.display_value)
        except (TypeError, ValueError):
            continue
        day = period_as_date(row.grain, row.period_key, obs.as_of)
        if day is None:
            continue
        out.append(_Point(day=day, value=value, period_key=row.period_key))
    return out


def build_situation_digest(
    conn: sqlite3.Connection,
    registry: dict[str, Any],
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Compact facts for Claude: instrumented KPIs with stored readings."""
    today = as_of or date.today()
    stored = list_kpis(conn)
    by_name: dict[str, list[StoredKPI]] = {}
    for row in stored:
        by_name.setdefault(row.metric_name, []).append(row)

    kpis: list[dict[str, Any]] = []
    skipped_no_generator = 0
    skipped_no_reading = 0
    for name, entry in iter_all_metrics(registry=registry):
        if not has_metric_generator(entry):
            skipped_no_generator += 1
            continue
        grain = registry_metric_grain(entry)
        points = _points_from_rows(by_name.get(name, []))
        if not points:
            skipped_no_reading += 1
            continue
        try:
            target = registry_metric_target(entry)
        except ValueError:
            target = None
        try:
            direction = registry_metric_direction(entry)
        except ValueError:
            direction = None
        compared = compare_series(points, grain=grain, as_of=today)
        current_val = compared["current"]["value"] if compared.get("current") else None
        kpis.append(
            {
                "name": name,
                "grain": grain,
                "target": target,
                "direction": direction,
                "target_outcome": (
                    _target_outcome(current_val, target, direction)
                    if current_val is not None
                    else None
                ),
                **compared,
            }
        )
    kpis.sort(key=lambda row: str(row["name"]).casefold())
    return {
        "as_of": today.isoformat(),
        "kpi_count": len(kpis),
        "skipped_no_generator": skipped_no_generator,
        "skipped_no_stored_reading": skipped_no_reading,
        "kpis": kpis,
        "coverage_note": (
            "Daily KPIs are often stored at month-end plus today, so week-ago "
            "may be missing even when month-ago exists."
        ),
    }


def generate_situation_analysis(
    digest: dict[str, Any],
    *,
    invoke=None,
) -> str:
    """Ask Claude for a landing-pane briefing. Fail loud on empty or unusable output."""
    if int(digest.get("kpi_count") or 0) < 1:
        raise KpiSituationError(
            "no stored generator KPI readings to compare with a week ago or a month ago"
        )
    payload = json.dumps(digest, default=str, separators=(",", ":"))
    if len(payload) > _MAX_DIGEST_CHARS:
        payload = payload[:_MAX_DIGEST_CHARS] + "…"
    system = (
        "You are briefing LeanDNA engineering and support leads on the Cortex KPI catalog. "
        "Use only the JSON digest. Compare the latest reading to week-ago and month-ago "
        "when those fields are present. If week_ago is null, say the catalog cannot support "
        "a week comparison for that grain or history shape — do not invent a weekly number. "
        "Call out target made/missed when target_outcome is set. Prefer the largest moves "
        "and cluster support vs engineering/AI. Do not speculate on causes. "
        "Write 3–8 tight paragraphs or short bullets (plain text, no markdown headings)."
    )
    user = (
        "Analyze the current KPI situation relative to a week ago and a month ago.\n\n"
        f"{payload}"
    )
    call = invoke if invoke is not None else _claude_complete
    text = call(system=system, user=user)
    cleaned = (text or "").strip()
    if not cleaned:
        raise KpiSituationError("Claude returned an empty KPI situation briefing")
    return cleaned


def _message_text(resp: Any) -> str:
    try:
        choice = resp.choices[0]
        msg = choice.message
    except (AttributeError, IndexError, TypeError):
        return ""
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("text"):
                parts.append(str(block["text"]))
            else:
                text = getattr(block, "text", None)
                if text:
                    parts.append(str(text))
        content = "\n".join(parts)
    if not (content or "").strip():
        content = getattr(msg, "reasoning_content", None) or getattr(msg, "reasoning", None)
    return str(content or "").strip()


def _claude_complete(*, system: str, user: str) -> str:
    try:
        client = anthropic_llm_client()
    except RuntimeError as exc:
        raise KpiSituationError(str(exc)) from exc
    model = (LLM_MODEL if str(LLM_MODEL).startswith("claude") else "") or "claude-sonnet-4-6"
    try:
        resp = _llm_create_with_retry(
            client,
            model=model,
            max_tokens=2500,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation Claude call failed")
        raise KpiSituationError(f"Claude KPI situation call failed: {exc}") from exc
    text = _message_text(resp)
    if not text:
        finish = None
        try:
            finish = resp.choices[0].finish_reason
        except (AttributeError, IndexError, TypeError):
            finish = "unknown"
        raise KpiSituationError(
            f"Claude returned an empty KPI situation briefing (model={model}, finish_reason={finish})"
        )
    return text
