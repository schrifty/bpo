"""Claude briefing of stored KPIs vs roughly a week ago and a month ago."""

from __future__ import annotations

import calendar
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Iterable

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
    registry_metric_description,
    registry_metric_direction,
    registry_metric_grain,
    registry_metric_mgmt_guidance,
    registry_metric_owner,
    registry_metric_tags,
    registry_metric_target,
)

_MAX_DIGEST_CHARS = 40_000
_MAX_GUIDANCE_CHARS = 240
_MAX_DESCRIPTION_CHARS = 160

# Tag → team label used to group KPIs for "your team / peer teams / company".
_TEAM_TAGS: tuple[tuple[str, str], ...] = (
    ("support", "Support"),
    ("care", "Support"),
    ("engineering", "Engineering"),
    ("ai", "Engineering (AI adoption)"),
    ("finance", "Finance / cost"),
    ("akkr", "Portfolio (AKKR)"),
)


class KpiSituationError(RuntimeError):
    """Claude situation briefing failed (do not substitute placeholder copy)."""


SITUATION_SYSTEM_PROMPT = """\
You write the Situation panel of the Cortex KPI catalog for LeanDNA. Readers are \
engineering, support, and AI-adoption leads plus the exec who owns the catalog. They \
open this page to answer four questions, in this order: How is my team doing? How are \
the teams next to me doing? How is the company doing overall? What should I do about it \
this week? Write so each question is answered in the first few lines that address it.

Ground rules
- Use only the JSON digest. Every number you cite must appear in it. Never invent a \
week-ago or month-ago value; when `week_ago` or `month_ago` is null, say the comparison \
is unavailable for that KPI and move on (the `week_note` / `month_note` fields say why).
- Direction matters more than sign. `direction: higher` means up is good; `lower` means \
down is good. `week_move_is_good` / `month_move_is_good` already encode this; trust them.
- Targets are commitments. Lead with anything in `company.targets_missed`, and name the \
gap in the KPI's own unit (e.g. "84.9% vs a 90% target"). Celebrate a made target only \
when it was recently missed or the margin is thin — otherwise one clause is enough.
- A KPI is worth a sentence only if it moved materially (roughly ≥10% on a rate or ratio, \
or a clear break in a count), crossed its target, or has been stuck while missing. Skip \
flat-and-healthy KPIs or summarize them in one line ("the rest of Support is steady").
- Monthly KPIs describe the last closed month, not this week. Do not describe a monthly \
close as "this week's" movement.
- Monthly/quarterly grains have no week-ago reading; say so once, not per KPI.
- Do not speculate about root cause. You may quote or paraphrase `mgmt_guidance` as the \
lever the owner has already agreed to pull; that is the only source of "why" you may use.
- Zero is a real value for Cursor spend and token KPIs. Treat it as a measurement.
- Flag data problems as work items, not as commentary: a KPI with only one stored point, \
a grain that cannot answer the reader's question, or an owner with no instrumented KPIs \
(`pending_kpis_by_team`) is an action for someone. A `current.period_key` older than the \
last closed period means the store is stale for that KPI — say "stale" and skip trend talk.
- Cluster related KPIs into one point instead of listing each (e.g. "the three Cursor \
spend/token KPIs all closed down ~10%"). Readers scan; they do not read a ledger.

Format: plain text only. No markdown of any kind — no **bold**, no # headings, no \
--- rules, no tables. Section labels are a bare line of text ("Your team", "Peer teams", \
"Company", "Watch this week"). Bullets start with "- ".

Structure
1. Your team — for `viewer.teams` (or, for the catalog admin, the team with the most \
missed targets). Three to five bullets: target status first, then the largest moves vs a \
month ago and, where available, a week ago. Mention KPIs the reader owns by name.
2. Peer teams — one compact paragraph per other team in `company.by_team` with at least \
one instrumented KPI. Name only the one or two things a peer lead would want to know \
about you, and where a peer's trend will land on your desk (e.g. escalation rate rising \
into engineering; AI spend rising while PR volume falls).
3. Company — three lines max: how many targets made vs missed, the single biggest \
improvement and the single biggest deterioration across the catalog this month, and \
whether AI investment (spend, tokens, active users) is tracking with output (PRs merged, \
issues shipped, cycle time).
4. Watch this week — three to five actionable bullets. Each pairs a KPI trend with a \
concrete next step drawn from `mgmt_guidance`, a target gap, or a data gap. Phrase as \
"verb + object + why now". No generic advice.

Tone: direct, quantitative, calm. Numbers with units and the comparison period every \
time ("64 open, up 12% vs last week, down 39% vs a month ago"). Target 250–400 words; \
never exceed 480. Stop when the reader can act; do not pad.
"""


@dataclass(frozen=True)
class _Point:
    day: date
    value: float
    period_key: str


def period_as_date(grain: str, period_key: str, as_of: str | None = None) -> date | None:
    """Calendar day used to age a stored reading for week/month comparisons.

    The period key is the store's primary key and names the period the reading
    describes, so it wins. ``as_of`` is only a fallback when the key does not
    parse (it records when a generator ran, which can differ from the period —
    rows captured before the period-key fix carry a stale key and a fresh as_of).
    """
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
        pass
    try:
        return date.fromisoformat(key[:10])
    except ValueError:
        pass
    raw_as_of = (as_of or "").strip()
    if raw_as_of:
        try:
            return date.fromisoformat(raw_as_of[:10])
        except ValueError:
            return None
    return None


_MD_BOLD = re.compile(r"\*\*(.+?)\*\*")
_MD_RULE = re.compile(r"^\s*(?:-{3,}|\*{3,}|_{3,})\s*$")
_MD_HEADING = re.compile(r"^\s*#{1,6}\s+")


def _clean_md_line(line: str) -> str | None:
    """Clean one line, or return None when the whole line is markdown chrome."""
    if _MD_RULE.match(line):
        return None
    line = _MD_HEADING.sub("", line)
    line = _MD_BOLD.sub(r"\1", line)
    return line.rstrip()


def strip_markdown_chrome(text: str) -> str:
    """Drop bold markers, horizontal rules, and heading hashes; keep bullets."""
    out = [
        cleaned
        for cleaned in (_clean_md_line(line) for line in (text or "").splitlines())
        if cleaned is not None
    ]
    cleaned = "\n".join(out)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


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


def team_for_tags(tags: Iterable[str]) -> str:
    have = {str(t).strip().lower() for t in tags}
    for tag, label in _TEAM_TAGS:
        if tag in have:
            return label
    return "Other"


def _trim(text: str | None, limit: int) -> str | None:
    s = " ".join(str(text or "").split())
    if not s:
        return None
    return s if len(s) <= limit else s[: limit - 1].rstrip() + "…"


def _is_good_move(change_pct: float | None, direction: str | None) -> bool | None:
    if change_pct is None or direction not in ("higher", "lower"):
        return None
    if change_pct == 0:
        return None
    return (change_pct > 0) == (direction == "higher")


def _company_summary(kpis: list[dict[str, Any]]) -> dict[str, Any]:
    made = [k["name"] for k in kpis if k.get("target_outcome") == "made"]
    missed = [k["name"] for k in kpis if k.get("target_outcome") == "missed"]
    no_target = [k["name"] for k in kpis if k.get("target_outcome") is None]

    def movers(field: str, *, good: bool | None, limit: int = 6) -> list[dict[str, Any]]:
        out = []
        for k in kpis:
            pct = k.get(field)
            if pct is None:
                continue
            verdict = _is_good_move(pct, k.get("direction"))
            if good is not None and verdict is not good:
                continue
            out.append({"name": k["name"], "team": k["team"], "change_pct": pct})
        out.sort(key=lambda m: -abs(float(m["change_pct"])))
        return out[:limit]

    by_team: dict[str, dict[str, int]] = {}
    for k in kpis:
        t = by_team.setdefault(k["team"], {"kpis": 0, "made": 0, "missed": 0, "improving_month": 0, "worsening_month": 0})
        t["kpis"] += 1
        if k.get("target_outcome") == "made":
            t["made"] += 1
        elif k.get("target_outcome") == "missed":
            t["missed"] += 1
        verdict = _is_good_move(k.get("month_change_pct"), k.get("direction"))
        if verdict is True:
            t["improving_month"] += 1
        elif verdict is False:
            t["worsening_month"] += 1
    return {
        "targets_made": made,
        "targets_missed": missed,
        "no_target_or_direction": no_target,
        "biggest_improvements_month": movers("month_change_pct", good=True),
        "biggest_deteriorations_month": movers("month_change_pct", good=False),
        "biggest_improvements_week": movers("week_change_pct", good=True, limit=4),
        "biggest_deteriorations_week": movers("week_change_pct", good=False, limit=4),
        "by_team": by_team,
    }


def build_situation_digest(
    conn: sqlite3.Connection,
    registry: dict[str, Any],
    *,
    as_of: date | None = None,
    viewer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compact facts for Claude: instrumented KPIs with stored readings.

    *viewer* (optional) describes who is reading: ``email``, ``name``,
    ``is_catalog_admin``, and ``packs`` (topic packs they lead). Their team is
    inferred from the KPIs they own plus those packs so the briefing can lead
    with "your team" before peers and the company.
    """
    today = as_of or date.today()
    stored = list_kpis(conn)
    by_name: dict[str, list[StoredKPI]] = {}
    for row in stored:
        by_name.setdefault(row.metric_name, []).append(row)

    viewer_email = str((viewer or {}).get("email") or "").strip().lower()
    kpis: list[dict[str, Any]] = []
    skipped_no_generator = 0
    skipped_no_reading = 0
    pending_by_team: dict[str, list[str]] = {}
    for name, entry in iter_all_metrics(registry=registry):
        tags = registry_metric_tags(entry)
        team = team_for_tags(tags)
        if not has_metric_generator(entry):
            skipped_no_generator += 1
            pending_by_team.setdefault(team, []).append(name)
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
        owner = (registry_metric_owner(entry) or "").strip().lower() or None
        compared = compare_series(points, grain=grain, as_of=today)
        current_val = compared["current"]["value"] if compared.get("current") else None
        kpis.append(
            {
                "name": name,
                "team": team,
                "tags": list(tags),
                "owner": owner,
                "owned_by_viewer": bool(viewer_email and owner == viewer_email),
                "grain": grain,
                "description": _trim(registry_metric_description(entry), _MAX_DESCRIPTION_CHARS),
                "mgmt_guidance": _trim(registry_metric_mgmt_guidance(entry), _MAX_GUIDANCE_CHARS),
                "target": target,
                "direction": direction,
                "target_outcome": (
                    _target_outcome(current_val, target, direction)
                    if current_val is not None
                    else None
                ),
                "week_move_is_good": _is_good_move(compared.get("week_change_pct"), direction),
                "month_move_is_good": _is_good_move(compared.get("month_change_pct"), direction),
                **compared,
            }
        )
    kpis.sort(key=lambda row: (str(row["team"]), str(row["name"]).casefold()))

    viewer_block: dict[str, Any] | None = None
    if viewer:
        owned_teams = sorted({k["team"] for k in kpis if k["owned_by_viewer"]})
        pack_teams = sorted(
            {
                team_for_tags([p])
                for p in (viewer.get("packs") or [])
            }
            - {"Other"}
        )
        viewer_block = {
            "email": viewer_email or None,
            "name": viewer.get("name"),
            "role": "catalog admin (cross-team view)" if viewer.get("is_catalog_admin") else "team lead",
            "teams": sorted(set(owned_teams) | set(pack_teams)),
            "owned_kpis": [k["name"] for k in kpis if k["owned_by_viewer"]],
        }

    return {
        "as_of": today.isoformat(),
        "kpi_count": len(kpis),
        "skipped_no_generator": skipped_no_generator,
        "skipped_no_stored_reading": skipped_no_reading,
        "pending_kpis_by_team": pending_by_team,
        "viewer": viewer_block,
        "company": _company_summary(kpis),
        "kpis": kpis,
        "coverage_note": (
            "Daily KPIs are often stored at month-end plus today, so week-ago "
            "may be missing even when month-ago exists. Monthly KPIs describe the "
            "last closed calendar month; quarterly the last closed quarter."
        ),
    }


def _situation_prompt(digest: dict[str, Any]) -> tuple[str, str]:
    if int(digest.get("kpi_count") or 0) < 1:
        raise KpiSituationError(
            "no stored generator KPI readings to compare with a week ago or a month ago"
        )
    payload = json.dumps(digest, default=str, separators=(",", ":"))
    if len(payload) > _MAX_DIGEST_CHARS:
        payload = payload[:_MAX_DIGEST_CHARS] + "…"
    user = (
        "Write the Situation briefing for the reader described in `viewer` "
        "(if viewer is null, write for the whole leadership team). Use only this digest.\n\n"
        f"{payload}"
    )
    return SITUATION_SYSTEM_PROMPT, user


def iter_situation_analysis(
    digest: dict[str, Any],
    *,
    stream=None,
) -> Iterable[str]:
    """Yield the briefing a line at a time as Claude writes it.

    Cleaning is line-buffered because every markdown construct we strip (rules,
    heading hashes, bold spans) is contained within a single line, so a line is
    the smallest unit we can clean without risking a half-written ``**`` pair
    reaching the reader.
    """
    system, user = _situation_prompt(digest)
    source = stream if stream is not None else _claude_stream
    buffer = ""
    produced = False
    for delta in source(system=system, user=user):
        buffer += str(delta or "")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            cleaned = _clean_md_line(line)
            if cleaned is None:
                continue
            if cleaned.strip():
                produced = True
            yield cleaned + "\n"
    tail = _clean_md_line(buffer)
    if tail and tail.strip():
        produced = True
        yield tail
    if not produced:
        raise KpiSituationError("Claude returned an empty KPI situation briefing")


def generate_situation_analysis(
    digest: dict[str, Any],
    *,
    invoke=None,
) -> str:
    """Ask Claude for a landing-pane briefing. Fail loud on empty or unusable output."""
    system, user = _situation_prompt(digest)
    call = invoke if invoke is not None else _claude_complete
    text = call(system=system, user=user)
    cleaned = strip_markdown_chrome(text or "")
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


def _claude_client_and_model():
    try:
        client = anthropic_llm_client()
    except RuntimeError as exc:
        raise KpiSituationError(str(exc)) from exc
    model = (LLM_MODEL if str(LLM_MODEL).startswith("claude") else "") or "claude-sonnet-4-6"
    return client, model


def _claude_stream(*, system: str, user: str) -> Iterable[str]:
    client, model = _claude_client_and_model()
    try:
        resp = _llm_create_with_retry(
            client,
            model=model,
            max_tokens=3000,
            stream=True,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation Claude stream failed")
        raise KpiSituationError(f"Claude KPI situation call failed: {exc}") from exc
    try:
        for chunk in resp:
            try:
                delta = chunk.choices[0].delta
            except (AttributeError, IndexError, TypeError):
                continue
            text = getattr(delta, "content", None)
            if text:
                yield str(text)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation Claude stream broke mid-response")
        raise KpiSituationError(f"Claude KPI situation stream failed: {exc}") from exc


def _claude_complete(*, system: str, user: str) -> str:
    client, model = _claude_client_and_model()
    try:
        resp = _llm_create_with_retry(
            client,
            model=model,
            max_tokens=3000,
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
