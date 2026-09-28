"""Engineering portfolio flow, work split, and the SDLC snapshot.

``JiraClient.get_engineering_portfolio`` fetches from Jira and delegates shaping
here. The pure helpers stay importable from ``jira_client`` for existing callers.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Any

import requests

from .config import LLM_MODEL_FAST, llm_client, logger

_ACTIVE_WIP_STATUSES = ("In Progress", "In Review")
# Labels / issue types that mark reactive (unplanned) work rather than roadmap.
_REACTIVE_LABELS = (
    "customer_escalation", "escalation", "escalated", "incident",
    "hotfix", "support", "production", "outage", "sev1", "sev2",
)
_REACTIVE_TYPES = ("Bug", "Incident", "Escalation", "Support", "Problem")


def _eng_parse_day(value: Any) -> "date | None":
    try:
        return date.fromisoformat(str(value or "")[:10])
    except (ValueError, TypeError):
        return None


def _eng_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 1)
    return round((ordered[mid - 1] + ordered[mid]) / 2, 1)


def _is_reactive_ticket(ticket: dict) -> bool:
    """Classify a LEAN ticket as reactive/unplanned (bug, escalation, incident)."""
    if (ticket.get("type") or "") in _REACTIVE_TYPES:
        return True
    labels = {str(label).lower() for label in (ticket.get("labels") or [])}
    return any(reactive in labels for reactive in _REACTIVE_LABELS)


def _parse_jira_dt(value: Any) -> "datetime | None":
    """Parse a Jira timestamp into an aware datetime (date-only strings → UTC midnight)."""
    s = str(value or "").strip()
    if not s:
        return None
    # Normalize "+00:00" → "+0000" so %z parses consistently.
    s = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", s)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    d = _eng_parse_day(s)
    if d:
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return None


def compute_status_timeline(
    histories: list[dict],
    *,
    created: Any,
    current_status: str,
    now: "datetime | None" = None,
) -> dict[str, Any]:
    """Derive time-in-status from a Jira changelog (``expand=changelog`` histories).

    Returns total days spent in each status, days in the *current* status (the
    precise "stalled" measure that replaces the noisy ``updated`` proxy), and a
    reopen count. ``histories`` is the raw ``changelog.histories`` list; each entry
    carries a ``created`` timestamp and ``items`` with ``field == 'status'``
    transitions (``fromString`` / ``toString``).
    """
    now = now or datetime.now(timezone.utc)
    created_dt = _parse_jira_dt(created)

    transitions: list[tuple[datetime, str, str]] = []  # (when, from, to)
    for h in histories or []:
        when = _parse_jira_dt(h.get("created"))
        if not when:
            continue
        for it in h.get("items") or []:
            if (it.get("field") or "").lower() == "status":
                transitions.append((when, it.get("fromString") or "", it.get("toString") or ""))
    transitions.sort(key=lambda t: t[0])

    time_in_status: dict[str, float] = {}

    def _add(status: str, start: "datetime | None", end: "datetime | None") -> None:
        if not status or not start or not end:
            return
        days = (end - start).total_seconds() / 86400.0
        if days > 0:
            time_in_status[status] = round(time_in_status.get(status, 0.0) + days, 2)

    # Initial segment status = first transition's "from", else the current status.
    seg_start = created_dt
    seg_status = transitions[0][1] if transitions else current_status
    reopened = 0
    _DONE_STATES = ("closed", "done", "resolved")
    for when, frm, to in transitions:
        _add(seg_status, seg_start, when)
        if (to or "").lower() == "reopened" or (frm or "").lower() in _DONE_STATES:
            reopened += 1
        seg_start = when
        seg_status = to
    _add(seg_status, seg_start, now)

    last_change = transitions[-1][0] if transitions else created_dt
    days_in_current = (
        round((now - last_change).total_seconds() / 86400.0, 1) if last_change else None
    )
    return {
        "time_in_status": time_in_status,
        "days_in_current_status": days_in_current,
        "reopened": reopened,
        "transitions": len(transitions),
    }


def summarize_status_flow(active_items: list[dict], *, now: "datetime | None" = None) -> dict[str, Any]:
    """Aggregate changelog-derived flow across active items, mutating each in place.

    Each item should carry ``status``, ``created``, ``changelog`` (histories list),
    and optionally ``flagged`` (bool). Sets ``days_in_status`` / ``time_in_status`` /
    ``reopened`` on each item and returns per-status median current-occupancy (so the
    real chokepoint is visible), an overall median, and a blocked (flagged) count.
    """
    now = now or datetime.now(timezone.utc)
    by_status_days: dict[str, list[float]] = {}
    all_days: list[float] = []
    blocked = 0
    enriched = 0
    for it in active_items:
        if it.get("flagged"):
            blocked += 1
        tl = compute_status_timeline(
            it.get("changelog") or [],
            created=it.get("created"),
            current_status=it.get("status") or "",
            now=now,
        )
        d = tl.get("days_in_current_status")
        it["days_in_status"] = d
        it["time_in_status"] = tl.get("time_in_status")
        it["reopened"] = tl.get("reopened")
        if d is not None:
            enriched += 1
            all_days.append(d)
            by_status_days.setdefault(it.get("status") or "—", []).append(d)
    return {
        "source": "changelog",
        "by_status_median_days": {s: _eng_median(v) for s, v in by_status_days.items()},
        "median_days_in_status": _eng_median(all_days),
        "blocked_count": blocked,
        "enriched_count": enriched,
    }


# Backlog staleness thresholds. The LEAN "open" set is dominated by abandoned work
# (~80% untouched in 30d, ~60% in 180d), which inflates WIP, stall, and queue numbers
# across the deck. We separate genuinely-active work from the zombie tail so the slides
# stay actionable and add an explicit hygiene number a VP can act on.
ENG_ABANDONED_DAYS = 180   # no movement in this long → abandoned/zombie, not real WIP
ENG_ACTIVE_WIP_DAYS = 30   # touched within this window → actively being worked


def compute_eng_flow(
    in_flight: list[dict],
    closed: list[dict],
    *,
    today: "date | None" = None,
    stage_age_by_key: "dict[str, float] | None" = None,
    flagged_keys: "set[str] | None" = None,
    abandoned_days: int = ENG_ABANDONED_DAYS,
) -> dict[str, Any]:
    """Derive flow / bottleneck signals from in-flight and recently closed LEAN tickets.

    Surfaces where work is piling up (active WIP by status), where it is stalling,
    how old active work is, and whether cycle time is trending up.

    The stall measure prefers ``stage_age_by_key`` — changelog-derived days in the
    current status — when supplied; otherwise it falls back to a ``updated`` idle
    proxy. ``flagged_keys`` marks items flagged as blocked/impediment in Jira. Both
    feed the stale counts, attention selection, ranking, and the slide headline.
    """
    today = today or date.today()
    stage_age_by_key = stage_age_by_key or {}
    flagged_set = set(flagged_keys or ())
    active = [t for t in in_flight if (t.get("status") or "") in _ACTIVE_WIP_STATUSES]

    def _idle_days(ticket: dict) -> int | None:
        d = _eng_parse_day(ticket.get("updated"))
        return (today - d).days if d else None

    def _age_days(ticket: dict) -> int | None:
        d = _eng_parse_day(ticket.get("created"))
        return (today - d).days if d else None

    active_ages = [a for a in (_age_days(t) for t in active) if a is not None]
    stale_gt5 = 0
    stale_gt10 = 0
    stale_recent = 0          # genuinely stalled but still actionable (10d < stall ≤ abandoned_days)
    abandoned_in_stage = 0    # parked in the same stage longer than abandoned_days (zombie WIP)
    carryover_count = 0
    carryover_points = 0.0
    blocked_count = 0
    # Active stage ages for items that are NOT abandoned — used to report honest stage
    # medians (the all-items median is dragged to years by zombies parked in-stage).
    active_status_eff: dict[str, list[float]] = {}
    # "Needs attention" = active items that are flagged blocked, carried over across a
    # sprint boundary (sprint_count ≥ 2), or stalled past the freshness threshold —
    # but NOT items abandoned in-stage past ``abandoned_days`` (those are a hygiene
    # problem, surfaced separately, and would otherwise drown the actionable list).
    # Stall uses changelog days-in-status when available, else the ``updated`` proxy.
    attention_rows: list[dict[str, Any]] = []
    abandoned_rows: list[dict[str, Any]] = []
    for ticket in active:
        key = ticket.get("key", "")
        idle = _idle_days(ticket)
        stage_age = stage_age_by_key.get(key)
        # Effective stall measure: changelog stage age beats the noisy update proxy.
        eff_stall = stage_age if stage_age is not None else idle
        sprint_count = int(ticket.get("sprint_count") or len(ticket.get("sprints") or []))
        is_carryover = sprint_count >= 2
        is_flagged = key in flagged_set
        is_abandoned = eff_stall is not None and eff_stall > abandoned_days
        sp = ticket.get("story_points")
        if is_flagged:
            blocked_count += 1
        if is_carryover:
            carryover_count += 1
            carryover_points += float(sp) if sp is not None else 0.0
        if eff_stall is not None and eff_stall > 5:
            stale_gt5 += 1
        if eff_stall is not None and eff_stall > 10:
            stale_gt10 += 1
        if eff_stall is not None and 10 < eff_stall <= abandoned_days:
            stale_recent += 1
        if eff_stall is not None and not is_abandoned:
            active_status_eff.setdefault(ticket.get("status") or "—", []).append(float(eff_stall))
        row = {
            "key": key,
            "summary": (ticket.get("summary") or "")[:90],
            "status": ticket.get("status", ""),
            "priority": ticket.get("priority", "") or "",
            "assignee": ticket.get("assignee", "") or "Unassigned",
            "story_points": sp,
            "sprint_count": sprint_count,
            "carryover": is_carryover,
            "flagged": is_flagged,
            "idle_days": idle,
            "days_in_status": stage_age,
            "age_days": _age_days(ticket),
        }
        if is_abandoned and not is_flagged:
            abandoned_in_stage += 1
            abandoned_rows.append(row)
            continue
        if is_flagged or is_carryover or (eff_stall is not None and eff_stall > 5):
            attention_rows.append(row)

    def _eff(row: dict) -> float:
        v = row.get("days_in_status")
        if v is None:
            v = row.get("idle_days")
        return v or 0

    # Rank: flagged first, then carried-over, then longest stalled, then oldest.
    attention_rows.sort(
        key=lambda r: (
            0 if r["flagged"] else 1,
            0 if r["carryover"] else 1,
            -_eff(r),
            -(r["age_days"] or 0),
        )
    )
    # Backward-compatible alias: ``stale_items`` historically meant stalled>5 rows.
    stale_rows = [r for r in attention_rows if _eff(r) > 5]
    stale_rows.sort(key=lambda r: -_eff(r))
    abandoned_rows.sort(key=lambda r: -_eff(r))

    # Cycle-time trend: median (resolved - created) of closed tickets bucketed by
    # the ISO week they closed in, oldest → newest (updated ≈ resolved for closed).
    week_cycles: dict[str, list[float]] = {}
    for ticket in closed:
        created = _eng_parse_day(ticket.get("created"))
        resolved = _eng_parse_day(ticket.get("updated"))
        if not (created and resolved) or resolved < created:
            continue
        iso = resolved.isocalendar()
        wk = f"{iso[0]}-W{iso[1]:02d}"
        week_cycles.setdefault(wk, []).append((resolved - created).days)
    cycle_trend = [
        {"week": wk, "median_cycle_days": _eng_median(vals), "closed": len(vals)}
        for wk, vals in sorted(week_cycles.items())
    ][-6:]
    cycle_delta = None
    medians = [p["median_cycle_days"] for p in cycle_trend if p["median_cycle_days"] is not None]
    if len(medians) >= 2:
        cycle_delta = round(medians[-1] - medians[0], 1)

    return {
        "active_count": len(active),
        "in_progress": sum(1 for t in active if (t.get("status") or "") == "In Progress"),
        "in_review": sum(1 for t in active if (t.get("status") or "") == "In Review"),
        "stale_gt5": stale_gt5,
        "stale_gt10": stale_gt10,
        "stale_recent": stale_recent,
        "abandoned_in_stage": abandoned_in_stage,
        "abandoned_days": abandoned_days,
        "carryover_count": carryover_count,
        "carryover_points": round(carryover_points, 1) if carryover_points else 0.0,
        "blocked_count": blocked_count,
        "median_active_age_days": _eng_median([float(a) for a in active_ages]),
        "oldest_active_age_days": max(active_ages) if active_ages else None,
        "by_status_median_active": {
            s: _eng_median(v) for s, v in active_status_eff.items() if v
        },
        "stale_items": stale_rows[:6],
        "attention_items": attention_rows[:12],
        "abandoned_items": abandoned_rows[:6],
        "cycle_trend": cycle_trend,
        "cycle_delta_days": cycle_delta,
    }


def compute_eng_work_split(
    in_flight: list[dict],
    closed: list[dict],
    *,
    escalated_to_eng: int = 0,
) -> dict[str, Any]:
    """Split engineering work into planned (roadmap) vs unplanned (reactive) load.

    Reactive = bugs, escalations, incidents, or escalation-labeled tickets.  Returns
    current WIP split, trailing closed-period split, and the reactive share of each,
    so a VP can see how much capacity keep-the-lights-on work is consuming.
    """
    def _split(tickets: list[dict]) -> dict[str, int]:
        reactive = sum(1 for t in tickets if _is_reactive_ticket(t))
        return {"planned": len(tickets) - reactive, "unplanned": reactive, "total": len(tickets)}

    wip = _split(in_flight)
    closed_split = _split(closed)
    bugs_wip = sum(1 for t in in_flight if (t.get("type") or "") == "Bug")
    return {
        "wip": wip,
        "closed": closed_split,
        "reactive_wip_pct": round(wip["unplanned"] / wip["total"] * 100) if wip["total"] else 0,
        "reactive_closed_pct": (
            round(closed_split["unplanned"] / closed_split["total"] * 100) if closed_split["total"] else 0
        ),
        "unplanned_breakdown": {
            "Bugs": bugs_wip,
            "Escalations / other": max(0, wip["unplanned"] - bugs_wip),
        },
        "escalated_to_eng": int(escalated_to_eng or 0),
    }


def _generate_eng_insights(eng: dict) -> dict[str, list[str]]:
    """Generate 2-3 LeanDNA-style insight bullets for each engineering portfolio slide.

    Returns a dict keyed by slide name, each value a list of bullet strings.
    Runs all GPT calls in parallel.  Falls back to [] on any error.
    """
    from concurrent.futures import ThreadPoolExecutor

    _oai = llm_client()

    def _call(slide_name: str, prompt: str) -> tuple[str, list[str]]:
        try:
            resp = _oai.chat.completions.create(
                model=LLM_MODEL_FAST,
                messages=[
                    {"role": "system", "content": (
                        "You are a technical analyst writing slide bullets for an engineering review deck. "
                        "Follow these rules strictly:\n"
                        "- Write exactly 2-3 bullets\n"
                        "- Each bullet is one sentence, 8-14 words\n"
                        "- Lead with the insight or implication, not the raw number\n"
                        "- Use plain text, no markdown, no hyphens at the start\n"
                        "- Tone: direct, analytical, not salesy"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=200,
            )
            raw = resp.choices[0].message.content.strip()
            bullets = [
                line.lstrip("•-–— ").strip()
                for line in raw.splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            return slide_name, [b for b in bullets if b][:3]
        except Exception as e:
            logger.warning("Insight generation failed for %s: %s", slide_name, e)
            return slide_name, []

    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "current sprint")
    in_flight = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    active = eng.get("by_status", {}).get("In Progress", 0)
    themes = [t for t in (eng.get("themes") or []) if t.get("theme") != "Untagged"]
    top_theme = themes[0]["theme"] if themes else "unknown"
    by_type = eng.get("by_type") or {}
    bugs_if = by_type.get("Bug", 0)

    open_bugs = eng.get("open_bugs") or []
    blockers = eng.get("blocker_critical") or []
    by_prio_bug: dict[str, int] = {}
    for b in open_bugs:
        p = b.get("priority", "Unknown").split(":")[0]
        by_prio_bug[p] = by_prio_bug.get(p, 0) + 1

    throughput = eng.get("throughput") or []
    recent_tp = throughput[-4:] if throughput else []
    avg_closed = sum(w.get("resolved", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0
    avg_created = sum(w.get("created", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0

    crb = eng.get("customer_reported_bugs") or {}
    crb_total = crb.get("total", 0)
    crb_open = crb.get("open", 0)
    crb_critical = crb.get("open_blocker_critical", 0)
    days = eng.get("days", 30)

    enhancements = eng.get("enhancements") or {}
    er_open = enhancements.get("open_count", 0)
    er_shipped = enhancements.get("shipped_count", 0)

    tasks = [
        ("sprint_snapshot", (
            f"Sprint: {sprint_name}. Total open tickets: {in_flight} (includes backlog and in-progress). "
            f"Actively being worked (In Progress + In Review): {active}. "
            f"Closed this period: {closed}. Top theme by ticket count: {top_theme}. "
            f"Open bugs: {bugs_if}. Type breakdown: {dict(list(by_type.items())[:5])}. "
            "Write 2-3 insight bullets about the sprint's focus, how much is actively in motion vs backlog, and any risks."
        )),
        ("bug_health", (
            f"Open bugs: {len(open_bugs)}. Blockers/Critical: {len(blockers)}. "
            f"Priority breakdown: {by_prio_bug}. "
            f"Blocker examples: {', '.join(b['key'] + ': ' + b['summary'][:50] for b in blockers[:3])}. "
            "Write 2-3 insight bullets about the bug backlog severity and what needs attention."
        )),
        ("velocity", (
            f"4-week avg tickets created/week: {avg_created:.1f}. "
            f"4-week avg tickets closed/week: {avg_closed:.1f}. "
            f"Net flow (closed minus created): {(avg_closed - avg_created):.1f} per week. "
            f"Total in-flight: {in_flight}. Total closed this period: {closed}. "
            "Write 2-3 insight bullets about team throughput, backlog trend, and delivery pace."
        )),
        ("customer_reported_bugs", (
            f"Customer-reported bugs in last {days} days: {crb_total} reported, {crb_open} still open. "
            f"Open blocker/critical: {crb_critical}. "
            f"Still-open rate: {(crb_open / crb_total * 100):.0f}% of reported."
        ) if crb_total else (
            f"No customer-reported bugs for last {days} days. Write a note that escalated-bug inflow was zero or unavailable."
        )),
    ]

    insights: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_call, name, prompt) for name, prompt in tasks]
        for f in futures:
            name, bullets = f.result()
            insights[name] = bullets

    return insights


def _generate_eng_takeaways(eng: dict) -> dict[str, str]:
    """Generate one "what this means" implication sentence per content slide.

    Returns ``{slide_key: sentence}``. Each value is a single, plain-text sentence
    that states the implication / decision the slide drives — rendered in the slide's
    bottom takeaway band (replacing the old per-slide scope/methodology footer). Runs
    all calls in parallel; any failure yields an empty string (band is then skipped).
    """
    from concurrent.futures import ThreadPoolExecutor

    _oai = llm_client()

    def _call(slide_key: str, prompt: str) -> tuple[str, str]:
        try:
            resp = _oai.chat.completions.create(
                model=LLM_MODEL_FAST,
                messages=[
                    {"role": "system", "content": (
                        "You write the single-sentence 'so what' takeaway at the bottom of an "
                        "engineering-review slide for a VP of Engineering. Rules:\n"
                        "- Output EXACTLY one sentence, 12-26 words, plain text only\n"
                        "- State the implication or the decision it forces, not a restatement of the numbers\n"
                        "- Cite one concrete number for weight, and name a specific, actionable next step "
                        "(who/what to do), not a vague gesture\n"
                        "- BANNED vague filler: 'strategic review', 'root causes', 'investigate', 'demands attention', "
                        "'requires immediate action', 'closely monitor', 'reassess' — say the concrete action instead\n"
                        "- 'Portfolio' means the customer book of business, never tickets or bugs: call work items "
                        "the backlog, bug backlog, or queue\n"
                        "- No markdown, no leading bullet/dash, no label, no preamble\n"
                        "- Tone: direct, analytical, board-room; never salesy or hedging"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                # LLM_MODEL_FAST (gemini-2.5-flash) is a thinking model where max_tokens
                # is the TOTAL budget (reasoning + visible); a low cap leaves only a few
                # output tokens and truncates the sentence mid-word. Give ample headroom.
                max_tokens=1024,
            )
            text = " ".join((resp.choices[0].message.content or "").split()).strip()
            text = text.lstrip("•-–—* ").strip()
            return slide_key, text
        except Exception as e:
            logger.warning("Takeaway generation failed for %s: %s", slide_key, e)
            return slide_key, ""

    # ── Pull the same derived structures the slides render from ──
    scard = (eng.get("team_scorecard") or {}).get("summary") or {}
    flow = eng.get("flow") or {}
    sflow = flow.get("status_flow") or {}
    lean = (eng.get("project_snapshots") or {}).get("LEAN") or {}
    split = eng.get("work_split") or {}
    crb = eng.get("customer_reported_bugs") or {}
    bug_flow = eng.get("bug_flow") or {}
    epic_progress = eng.get("epic_progress") or {}

    def _epic_line(r: dict) -> str:
        base = f"{r.get('key')} {r.get('pct')}% complete with {r.get('remaining')} issues remaining"
        if r.get("stalled"):
            return base + " (STALLED, no activity 30d)"
        return base + f", {r.get('active_30d')} updated/30d"

    epic_top = "; ".join(
        _epic_line(r) for r in (epic_progress.get("epics") or [])[:3]
    ) or "none"

    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "current sprint")
    in_flight = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    by_status = eng.get("by_status") or {}
    active = int(by_status.get("In Progress", 0) or 0) + int(by_status.get("In Review", 0) or 0)
    by_type = eng.get("by_type") or {}
    bugs_if = by_type.get("Bug", 0)
    themes = [t for t in (eng.get("themes") or []) if t.get("theme") != "Untagged"]
    top_themes = ", ".join(
        f"{t.get('theme')} ({t.get('total')})" for t in themes[:4] if t.get("theme")
    )

    open_bugs = eng.get("open_bugs") or []
    blockers = eng.get("blocker_critical") or []
    bug_prio: dict[str, int] = {}
    for b in open_bugs:
        p = str(b.get("priority", "Unknown")).split(":")[0]
        bug_prio[p] = bug_prio.get(p, 0) + 1

    by_assignee = eng.get("by_assignee") or {}
    wip_vals = sorted((int(v) for v in by_assignee.values()), reverse=True)
    total_wip = sum(wip_vals)
    active_vals = sorted((int(v) for v in (eng.get("by_assignee_active") or {}).values()), reverse=True)
    total_active = sum(active_vals)
    top3_share = int(round(sum(active_vals[:3]) / total_active * 100)) if total_active else 0
    engineers = sum(1 for v in active_vals if v > 0)
    assigned_stale = sum(int(v) for v in (eng.get("by_assignee_stale") or {}).values())
    staleness = eng.get("backlog_staleness") or {}

    median_by_status = flow.get("by_status_median_active") or sflow.get("by_status_median_days") or {}
    stage_str = ", ".join(f"{k} {v:.0f}d" for k, v in list(median_by_status.items())[:4] if v is not None)

    try:
        from .eng_sprint_velocity import build_sprint_velocity_series
        vseries = build_sprint_velocity_series(eng.get("sprint_velocity"))
        sp_total = vseries.get("sp_total") or []
        tickets_total = vseries.get("tickets_total") or []
        sp_teams = ", ".join(vseries.get("teams") or []) or "scrum boards"
        zero_sp = ", ".join(vseries.get("zero_sp_teams") or [])
    except Exception:
        sp_total, tickets_total, sp_teams, zero_sp = [], [], "scrum boards", ""

    days = eng.get("days", 30)

    tasks = [
        ("team_scorecard", (
            f"Last sprint the engineering teams closed {scard.get('total_throughput')} issues total (throughput) "
            f"at an average lead time of {scard.get('average_median_lead_days')}d (created to resolved). "
            "Six LEAN squads run continuous flow with no fixed sprint commitment. The CUSTOMER 'Active Scrum' "
            "board parks a large standing backlog inside each weekly sprint (~100 issues roll over week to week), "
            "so its commit-vs-complete ratio is a sprint-hygiene artifact, not a true delivery rate. "
            "Implication: compare teams on throughput and lead time; flag the CUSTOMER board's bloated in-sprint "
            "scope as a backlog-hygiene issue to fix. Do NOT frame it as a delivery or predictability failure."
        )),
        ("current_sprint", (
            f"Sprint {sprint_name}: {in_flight} open items, {active} actively in progress/review, "
            f"{bugs_if} bugs in flight, {closed} closed this period. Top themes: {top_themes}. "
            f"{staleness.get('abandoned_open')} open items ({staleness.get('abandoned_pct')}%) untouched "
            f">{staleness.get('abandoned_days')}d. "
            "Implication about focus and how much is truly moving vs sitting in an abandoned backlog?"
        )),
        ("flow_bottlenecks", (
            f"Active WIP {flow.get('active_count')}, in review {flow.get('in_review')}, "
            f"recently stalled (10–{flow.get('abandoned_days')}d in stage) {flow.get('stale_recent')}, "
            f"abandoned in stage >{flow.get('abandoned_days')}d {flow.get('abandoned_in_stage')} (zombie WIP). "
            f"Stage medians on active (non-abandoned) items: {stage_str}. "
            "Implication: separate the actionable recent stalls to unblock from the abandoned backlog to triage/close?"
        )),
        ("backlog_health", (
            f"Open LEAN queue {lean.get('open_count')} tickets, median age {lean.get('median_open_age_days')}d, "
            f"{lean.get('open_over_90_count')} open >90 days, oldest {lean.get('oldest_open_age_days')}d, "
            f"avg resolve cycle {lean.get('avg_resolved_cycle_days')}d. "
            "Implication about queue hygiene and whether the backlog is being worked or just aging?"
        )),
        ("capacity", (
            f"{total_active} actively-worked WIP items (touched ≤{staleness.get('active_days')}d) across {engineers} engineers; "
            f"top 3 hold {top3_share}% of active WIP. Of {total_wip} total assigned open items, "
            f"{assigned_stale} are stale (assigned but untouched >{staleness.get('abandoned_days')}d). "
            "Implication about real load balance and key-person risk — and stale-assignment cleanup?"
        )),
        ("work_split", (
            f"Reactive share of WIP {split.get('reactive_wip_pct')}%, reactive share of closed "
            f"{split.get('reactive_closed_pct')}%; unplanned breakdown {split.get('unplanned_breakdown')}. "
            "Implication about how much roadmap capacity reactive work is consuming?"
        )),
        ("bug_health", (
            f"Open bugs {len(open_bugs)}, blocker/critical {len(blockers)}, priority mix {bug_prio}. "
            f"Top blockers: {', '.join(b.get('key','') for b in blockers[:3])}. "
            "Implication about severity and what demands attention this sprint?"
        )),
        ("bug_flow", (
            f"Bug backlog flow over the last {bug_flow.get('weeks_count')} weeks: "
            f"{bug_flow.get('created_total')} bugs created vs {bug_flow.get('resolved_total')} resolved "
            f"(net {bug_flow.get('net_total')}, trend {bug_flow.get('trend')}); {bug_flow.get('open_now')} open now. "
            "Implication: is the team out-pacing incoming bugs or falling behind — and what does the trend demand?"
        )),
        ("epic_progress", (
            f"{epic_progress.get('epic_count')} in-flight initiatives (epics) ranked by remaining work. "
            "Percentages are % of child issues COMPLETE (so a high % means nearly done, NOT a lot left); "
            "'remaining' is the count of open child issues. "
            f"{epic_progress.get('total_remaining')} child issues still open across all epics, "
            f"{epic_progress.get('early_stage_count')} early-stage (<50% complete), {epic_progress.get('at_risk_count')} at risk "
            f"(stalled = open work but no child activity in 30d). Top by remaining work: {epic_top}. "
            "Implication about whether the big rocks are actually moving and where delivery risk concentrates? "
            "Do not confuse % complete with % remaining."
        )),
        ("velocity", (
            f"Story points delivered per recent sprint (oldest to newest): {sp_total}. "
            f"Tickets delivered per sprint (oldest to newest): {tickets_total}. "
            f"SP-estimating boards: {sp_teams}. Boards without story points: {zero_sp or 'none'}. "
            "Implication about the delivery trend — consider BOTH story points and ticket throughput "
            "(ticket count can fall even when SP looks flat); avoid over-reading a one-sprint move?"
        )),
        ("customer_reported_bugs", (
            f"Customer-reported bugs (LEAN bugs customers escalated) last {days} days: "
            f"{crb.get('total')} reported, {crb.get('open')} still open, "
            f"{crb.get('open_blocker_critical')} open blocker/critical, priority mix {crb.get('by_priority')}. "
            "Implication about externally-reported quality and the capacity it consumes?"
        )) if crb.get("total") else (
            "customer_reported_bugs",
            f"No customer-reported bugs in the last {days} days. State that escalated-bug inflow was zero or unavailable.",
        ),
    ]

    takeaways: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_call, key, prompt) for key, prompt in tasks]
        for f in futures:
            key, sentence = f.result()
            takeaways[key] = sentence

    return takeaways


def _normalize_lean_issue(issue: dict) -> dict:
    from .jira_client import SPRINT_FIELD, STORY_POINTS_FIELD, _extract_adf_text

    f = issue.get("fields", {})
    sp_list = f.get(SPRINT_FIELD) or []
    # Non-future sprints this issue has belonged to. len > 1 ⇒ carried over
    # across sprint boundaries (spillover) — a stronger stall signal than
    # the ``updated`` idle proxy.
    sprint_names = [s.get("name", "") for s in sp_list if s.get("state") != "future"]
    sp_raw = f.get(STORY_POINTS_FIELD)
    try:
        story_points = float(sp_raw) if sp_raw is not None else None
    except (TypeError, ValueError):
        story_points = None
    desc_raw = _extract_adf_text(f.get("description"))
    parent = f.get("parent") or {}
    parent_summary = (parent.get("fields") or {}).get("summary", "") if parent else ""
    return {
        "key": issue["key"],
        "summary": f.get("summary", ""),
        "parent_summary": parent_summary,
        "status": f.get("status", {}).get("name", ""),
        "type": f.get("issuetype", {}).get("name", ""),
        "priority": (f.get("priority") or {}).get("name", ""),
        "assignee": (f.get("assignee") or {}).get("displayName", ""),
        "labels": f.get("labels") or [],
        "created": (f.get("created") or "")[:10],
        "updated": (f.get("updated") or "")[:10],
        "resolution": (f.get("resolution") or {}).get("name", "") if f.get("resolution") else "",
        "sprints": sprint_names,
        "sprint_count": len(sprint_names),
        "story_points": story_points,
        "description_text": (desc_raw or "")[:4000],
    }


def _rollup_in_flight(in_flight: list[dict], *, today: date | None = None) -> dict[str, Any]:
    """Theme, type, status, assignee, and backlog-staleness rollups for open LEAN work."""
    _theme_re = re.compile(r"^\[([^\]]+)\]")

    def _theme(t: dict) -> str:
        m = _theme_re.match(t.get("summary") or "")
        if m:
            return m.group(1).strip()
        parent = (t.get("parent_summary") or "").strip()
        if parent:
            parent = _theme_re.sub("", parent).strip()
            if parent:
                return parent[:28]
        return "Untagged"

    themes: dict[str, list[dict]] = {}
    for t in in_flight:
        themes.setdefault(_theme(t), []).append(t)

    theme_summary = [
        {
            "theme": th,
            "total": len(tix),
            "in_progress": sum(1 for t in tix if t["status"] in ("In Progress", "In Review")),
            "open": sum(1 for t in tix if t["status"] in ("Open", "Reopened")),
            "bugs": sum(1 for t in tix if t["type"] == "Bug"),
            "tickets": [
                {"key": t["key"], "summary": t["summary"][:70], "status": t["status"], "assignee": t["assignee"]}
                for t in tix[:4]
            ],
        }
        for th, tix in sorted(themes.items(), key=lambda x: -len(x[1]))
    ]

    by_type: dict[str, int] = {}
    by_status: dict[str, int] = {}
    by_assignee: dict[str, int] = {}
    for t in in_flight:
        by_type[t["type"]] = by_type.get(t["type"], 0) + 1
        by_status[t["status"]] = by_status.get(t["status"], 0) + 1
        if t["assignee"]:
            by_assignee[t["assignee"]] = by_assignee.get(t["assignee"], 0) + 1

    today = today or date.today()

    def _idle_days(t: dict) -> int | None:
        d = _eng_parse_day(t.get("updated"))
        return (today - d).days if d else None

    by_assignee_active: dict[str, int] = {}
    by_assignee_stale: dict[str, int] = {}
    abandoned_open = 0
    fresh_open = 0
    for t in in_flight:
        idle = _idle_days(t)
        if idle is None:
            continue
        nm = t.get("assignee") or ""
        if idle <= ENG_ACTIVE_WIP_DAYS:
            fresh_open += 1
            if nm:
                by_assignee_active[nm] = by_assignee_active.get(nm, 0) + 1
        if idle > ENG_ABANDONED_DAYS:
            abandoned_open += 1
            if nm:
                by_assignee_stale[nm] = by_assignee_stale.get(nm, 0) + 1

    return {
        "themes": theme_summary,
        "by_type": dict(sorted(by_type.items(), key=lambda x: -x[1])),
        "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1])),
        "by_assignee": dict(sorted(by_assignee.items(), key=lambda x: -x[1])),
        "by_assignee_active": dict(sorted(by_assignee_active.items(), key=lambda x: -x[1])),
        "by_assignee_stale": dict(sorted(by_assignee_stale.items(), key=lambda x: -x[1])),
        "backlog_staleness": {
            "open_total": len(in_flight),
            "abandoned_days": ENG_ABANDONED_DAYS,
            "active_days": ENG_ACTIVE_WIP_DAYS,
            "abandoned_open": abandoned_open,
            "abandoned_pct": round(100 * abandoned_open / len(in_flight)) if in_flight else 0,
            "fresh_open": fresh_open,
        },
        "open_bugs": [t for t in in_flight if t["type"] == "Bug"],
        "blocker_critical": [t for t in in_flight if t["priority"].startswith(("Blocker", "Critical"))],
    }


def _velocity_rows(recent_sprints: list[dict], closed: list[dict]) -> list[dict]:
    velocity: list[dict] = []
    for sp in recent_sprints:
        count = sum(1 for t in closed if sp["name"] in t.get("sprints", []))
        velocity.append({"sprint": sp["name"], "closed": count, "start": sp["start"], "end": sp["end"]})
    return velocity


def _fetch_sprint_context(client: Any) -> tuple[dict, list[dict]]:
    sprint_info: dict = {}
    recent_sprints: list[dict] = []
    live_sprint = client.fetch_active_board_sprint(44)
    if live_sprint:
        sprint_info = live_sprint
    try:
        resp = requests.get(
            f"{client.api_base_url}/rest/agile/1.0/board/44/sprint?state=closed&maxResults=4",
            headers=client._headers,
            timeout=10,
        )
        if resp.ok:
            for s in reversed(resp.json().get("values", [])[-4:]):
                recent_sprints.append({
                    "id": s["id"],
                    "name": s["name"],
                    "start": s.get("startDate", "")[:10],
                    "end": s.get("endDate", "")[:10],
                })
    except Exception as e:
        logger.warning("Sprint fetch failed: %s", e)
    return sprint_info, recent_sprints


def _fetch_lean_core(client: Any, days: int) -> tuple[list[dict], list[dict], list[str]]:
    from .jira_client import SPRINT_FIELD, STORY_POINTS_FIELD

    eng_fields = [
        "summary", "status", "issuetype", "priority", "assignee",
        "labels", "created", "updated", "resolution", "description", "parent",
        SPRINT_FIELD, STORY_POINTS_FIELD,
    ]
    lean_core_errors: list[str] = []
    try:
        in_flight_raw = client._search(
            "project = LEAN AND status in (\"In Progress\", \"In Review\", \"Open\", \"Reopened\") ORDER BY updated DESC",
            max_results=3000,
            fields=eng_fields,
            data_description="LEAN in-flight engineering work (Open / In Progress / In Review / Reopened)",
        )
    except Exception as e:
        logger.warning("LEAN in-flight fetch failed: %s", e)
        lean_core_errors.append(f"LEAN in-flight: {e}")
        in_flight_raw = []
    try:
        closed_raw = client._search(
            f"project = LEAN AND status = Closed AND updated >= -{days}d ORDER BY updated DESC",
            max_results=2000,
            fields=eng_fields,
            data_description=f"LEAN issues closed or updated in last {days} days",
        )
    except Exception as e:
        logger.warning("LEAN closed fetch failed: %s", e)
        lean_core_errors.append(f"LEAN closed: {e}")
        closed_raw = []
    return (
        [_normalize_lean_issue(i) for i in in_flight_raw],
        [_normalize_lean_issue(i) for i in closed_raw],
        lean_core_errors,
    )


def _normalize_er(issue: dict) -> dict:
    from .jira_client import _extract_adf_text, _extract_comments

    f = issue["fields"]
    return {
        "key": issue["key"],
        "summary": f.get("summary", "")[:500],
        "status": f.get("status", {}).get("name", ""),
        "priority": (f.get("priority") or {}).get("name", ""),
        "labels": f.get("labels") or [],
        "updated": (f.get("updated") or "")[:10],
        "description_text": _extract_adf_text(f.get("description")),
        "comment_texts": _extract_comments(f.get("comment")),
    }


def _with_enhancement_narratives(er_open: list[dict], er_shipped: list[dict]) -> tuple[list[dict], list[dict]]:
    from .jira_client import _summarize_ticket

    def _er_narrative(entry: dict) -> str:
        return _summarize_ticket({
            "key": entry["key"],
            "summary": entry["summary"],
            "status": entry["status"],
            "resolution": "",
            "assignee": "",
            "description_text": entry.get("description_text", ""),
            "comment_texts": entry.get("comment_texts", []),
        })

    open_cap = 20
    shipped_cap = 10
    narr_src = er_open[:open_cap] + er_shipped[:shipped_cap]
    with ThreadPoolExecutor(max_workers=12) as pool:
        narratives = list(pool.map(_er_narrative, narr_src))
    n_open = min(open_cap, len(er_open))
    open_out = []
    for i, entry in enumerate(er_open):
        entry = dict(entry)
        if i < n_open:
            entry["narrative"] = narratives[i]
        open_out.append(entry)
    shipped_out = []
    for i, entry in enumerate(er_shipped[:shipped_cap]):
        entry = dict(entry)
        entry["narrative"] = narratives[n_open + i]
        shipped_out.append(entry)
    return open_out, shipped_out


def _fetch_enhancements(client: Any, days: int) -> dict[str, Any]:
    er_fields = [
        "summary", "status", "issuetype", "priority",
        "labels", "created", "updated", "resolution",
        "description", "comment",
    ]
    try:
        er_open_raw = client._search(
            (
                "project = ER AND resolution is EMPTY "
                "AND status not in (Done, Closed, \"Not Taken\") "
                "ORDER BY updated DESC"
            ),
            max_results=500,
            fields=er_fields,
            data_description="ER open enhancement backlog (all open)",
        )
    except Exception as e:
        logger.warning("ER open fetch failed: %s", e)
        er_open_raw = []
    try:
        er_shipped_raw = client._search(
            (
                "project = ER AND resolution in (Fixed, Done) "
                "AND updated >= -365d "
                "ORDER BY updated DESC"
            ),
            max_results=100,
            fields=er_fields,
            data_description="ER shipped or Done enhancements (last year)",
        )
    except Exception as e:
        logger.warning("ER shipped fetch failed: %s", e)
        er_shipped_raw = []
    try:
        er_declined_count = client.jql_match_count(
            "project = ER AND resolution in (\"Won't Do\", \"Won't Fix\", Declined, \"Future Consideration\", \"Not Taken\")",
            data_description="ER declined / won't do / not taken (count query)",
        ) or 0
    except Exception as e:
        logger.warning("ER declined fetch failed: %s", e)
        er_declined_count = 0
    er_open = [_normalize_er(i) for i in er_open_raw]
    er_shipped = [_normalize_er(i) for i in er_shipped_raw]
    open_with_narratives, shipped_with_narratives = _with_enhancement_narratives(er_open, er_shipped)
    return {
        "total": len(er_open) + len(er_shipped) + er_declined_count,
        "open_count": len(er_open),
        "shipped_count": len(er_shipped),
        "declined_count": er_declined_count,
        "open": open_with_narratives,
        "shipped": shipped_with_narratives,
        "days": days,
    }


def _fetch_customer_reported_bugs(client: Any, days: int) -> dict[str, Any]:
    try:
        from .jira_customer_reported_bugs import build_customer_reported_bug_pressure

        return build_customer_reported_bug_pressure(client, days=days)
    except Exception as e:
        logger.warning("Customer-reported bug pressure failed: %s", e)
        return {"error": str(e), "total": 0, "days": days}


def _fetch_project_snapshots(client: Any) -> dict[str, Any]:
    project_snapshots: dict[str, Any] = {}
    project_keys = ("HELP", "CUSTOMER", "LEAN")
    with ThreadPoolExecutor(max_workers=len(project_keys)) as pool:
        future_to_pk = {pool.submit(client.get_project_operational_snapshot, pk): pk for pk in project_keys}
        for fut in as_completed(future_to_pk):
            pk = future_to_pk[fut]
            try:
                project_snapshots[pk] = fut.result()
            except Exception as e:
                logger.warning("Project snapshot %s failed: %s", pk, e)
                project_snapshots[pk] = {"error": str(e), "project_key": pk, "base_url": client.base_url}
    return project_snapshots


def _fetch_eng_enrichment(client: Any, days: int) -> dict[str, Any]:
    try:
        from .eng_team_scorecard import build_eng_team_scorecard

        team_scorecard = build_eng_team_scorecard(client, days=days)
    except Exception as e:
        logger.warning("Team scorecard fetch failed: %s", e)
        team_scorecard = {"error": str(e), "teams": [], "summary": {}}

    try:
        from .eng_team_roster import build_eng_team_roster

        team_roster = build_eng_team_roster(client, timeout=60.0)
    except Exception as e:
        logger.warning("Team roster fetch failed: %s", e)
        team_roster = {"error": str(e), "teams": [], "total_engineers": 0}

    try:
        from .jira_sprint_story_points import get_sprint_story_points_history

        sprint_velocity = get_sprint_story_points_history(client, history_count=6, timeout=60.0)
    except Exception as e:
        logger.warning("Sprint story-point velocity fetch failed: %s", e)
        sprint_velocity = {"error": str(e), "boards": []}

    try:
        from .eng_bug_flow import build_eng_bug_flow

        bug_flow = build_eng_bug_flow(client, window_days=84, timeout=60.0)
    except Exception as e:
        logger.warning("Bug flow fetch failed: %s", e)
        bug_flow = {"error": str(e), "weeks": []}

    try:
        from .eng_epic_progress import build_eng_epic_progress

        epic_progress = build_eng_epic_progress(client, max_epics=8, timeout=60.0)
    except Exception as e:
        logger.warning("Epic progress fetch failed: %s", e)
        epic_progress = {"error": str(e), "epics": []}

    return {
        "team_scorecard": team_scorecard,
        "team_roster": team_roster,
        "sprint_velocity": sprint_velocity,
        "bug_flow": bug_flow,
        "epic_progress": epic_progress,
    }


def build_engineering_portfolio(client: Any, days: int = 30) -> dict[str, Any]:
    """Fetch and shape the engineering SDLC snapshot for ``days`` of closed work."""
    jql_start = client._jql_log_len()
    sprint_info, recent_sprints = _fetch_sprint_context(client)
    in_flight, closed, lean_core_errors = _fetch_lean_core(client, days)
    rollup = _rollup_in_flight(in_flight)
    enhancements = _fetch_enhancements(client, days)
    customer_reported_bugs = _fetch_customer_reported_bugs(client, days)
    throughput = client._bucket_by_week([
        {"created": t["created"], "updated": t["updated"], "resolution": t["resolution"]}
        for t in in_flight + closed
    ])
    project_snapshots = _fetch_project_snapshots(client)
    help_ticket_trends = client._get_help_ticket_volume_trends()
    sides = _fetch_eng_enrichment(client, days)

    stage_age_by_key: dict[str, float] = {}
    flagged_keys: set[str] = set()
    status_flow: dict[str, Any] = {}
    try:
        status_flow, stage_age_by_key, flagged_keys = client._compute_changelog_signals(in_flight)
    except Exception as e:
        logger.warning("Flow changelog enrichment failed: %s", e)
    flow = compute_eng_flow(
        in_flight, closed,
        stage_age_by_key=stage_age_by_key or None,
        flagged_keys=flagged_keys or None,
    )
    if status_flow:
        flow["status_flow"] = status_flow
        flow["blocked_count"] = status_flow.get("blocked_count", flow.get("blocked_count", 0))
    work_split = compute_eng_work_split(
        in_flight, closed, escalated_to_eng=customer_reported_bugs.get("total", 0)
    )

    eng_data = {
        "base_url": client.base_url,
        "days": days,
        "sprint": sprint_info,
        "recent_sprints": recent_sprints,
        "in_flight_count": len(in_flight),
        "closed_count": len(closed),
        "by_type": rollup["by_type"],
        "by_status": rollup["by_status"],
        "by_assignee": rollup["by_assignee"],
        "by_assignee_active": rollup["by_assignee_active"],
        "by_assignee_stale": rollup["by_assignee_stale"],
        "backlog_staleness": rollup["backlog_staleness"],
        "themes": rollup["themes"],
        "open_bugs": rollup["open_bugs"],
        "blocker_critical": rollup["blocker_critical"],
        "velocity": _velocity_rows(recent_sprints, closed),
        "throughput": throughput,
        "enhancements": enhancements,
        "customer_reported_bugs": customer_reported_bugs,
        "project_snapshots": project_snapshots,
        "help_ticket_trends": help_ticket_trends,
        "team_scorecard": sides["team_scorecard"],
        "team_roster": sides["team_roster"],
        "sprint_velocity": sides["sprint_velocity"],
        "bug_flow": sides["bug_flow"],
        "epic_progress": sides["epic_progress"],
        "flow": flow,
        "work_split": work_split,
        "jql_queries": client._jql_since(jql_start),
    }
    if lean_core_errors:
        eng_data["error"] = "; ".join(lean_core_errors)
    eng_data["takeaways"] = _generate_eng_takeaways(eng_data)
    return eng_data
