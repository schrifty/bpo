"""Build a slide-ready Cursor usage report for the engineering portfolio deck.

Aggregates the Cursor Team Admin API into the shape the ``cursor_usage`` slide
renders for a VP of Engineering:

- adoption (active engineers vs. seats),
- spend this billing cycle,
- model usage mix (where the tokens/cost go),
- highest-volume users (power users + idle-seat signal),
- usage by month over time (adoption + volume trend).

Sourcing is hybrid by design:

- **Monthly trend** comes from ``/teams/daily-usage-data`` (cheap: one call per
  ~30-day chunk, aggregated per user-day) so we can show several months.
- **Tokens, model mix, top users** come from ``/teams/filtered-usage-events`` over a
  shorter recent window (heavier, paginated) so token/model detail is available.
- **Spend** comes from ``/teams/spend`` (current billing cycle only).

Each section degrades independently: a failing endpoint records an error note in
``errors`` rather than discarding the whole report (the slide surfaces the gap).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .cursor_client import CursorClient, CursorClientError, cursor_configured

logger = logging.getLogger("bpo")

DEFAULT_EVENTS_WINDOW_DAYS = 30
DEFAULT_TREND_MONTHS = 6
TOP_USERS_LIMIT = 6
MODEL_MIX_LIMIT = 5
# Safety cap so a very active team cannot make the deck build pull unbounded pages.
_MAX_EVENTS = 20000
# Below this token floor a per-engineer efficiency ratio (lines per 1K tokens) is noise —
# a couple of accepted lines divided by a tiny token count produces a meaningless spike — so
# such engineers are excluded from the efficiency ranking (aggregate totals still count them).
_EFFICIENCY_MIN_TOKENS = 1000


def _month_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _month_label(key: str) -> str:
    try:
        return datetime.strptime(key, "%Y-%m").strftime("%b")
    except ValueError:
        return key


def _event_dt(event: dict[str, Any]) -> datetime | None:
    raw = event.get("timestamp")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _event_tokens(event: dict[str, Any]) -> int:
    tu = event.get("tokenUsage")
    if not isinstance(tu, dict):
        return 0
    return int(tu.get("inputTokens") or 0) + int(tu.get("outputTokens") or 0)


def _event_io_tokens(event: dict[str, Any]) -> tuple[int, int]:
    """(input_tokens, output_tokens) for an event."""
    tu = event.get("tokenUsage") if isinstance(event.get("tokenUsage"), dict) else {}
    return int(tu.get("inputTokens") or 0), int(tu.get("outputTokens") or 0)


def _event_cost_cents(event: dict[str, Any]) -> float:
    """Authoritative per-event cost in cents.

    Prefer ``chargedCents`` (model cost + Cursor token fee, reconciles with /teams/spend);
    fall back to ``tokenUsage.totalCents`` (model-only) when the charged field is absent.
    """
    charged = event.get("chargedCents")
    if isinstance(charged, (int, float)):
        return float(charged)
    tu = event.get("tokenUsage")
    if isinstance(tu, dict) and isinstance(tu.get("totalCents"), (int, float)):
        return float(tu["totalCents"])
    return 0.0


# Cursor reports its auto-select pseudo-model as the literal "default"; relabel it so a
# reviewer does not mistake it for an unset/error value.
_MODEL_DISPLAY = {"default": "Auto (default)"}


def _friendly_model(name: Any) -> str:
    n = str(name or "unknown")
    return _MODEL_DISPLAY.get(n, n)


def _day_key(dt: datetime) -> str:
    return dt.date().isoformat()


def _day_label(key: str) -> str:
    try:
        return datetime.strptime(key, "%Y-%m-%d").strftime("%-m/%-d")
    except ValueError:
        return key


def _build_monthly_trend(client: CursorClient, *, months: int, end: datetime) -> list[dict[str, Any]]:
    """Per-month active users + total requests from daily-usage-data."""
    start = end - timedelta(days=max(1, months) * 31)
    rows = client.get_daily_usage(start, end)

    req_by_month: dict[str, int] = defaultdict(int)
    users_by_month: dict[str, set] = defaultdict(set)
    for r in rows:
        ts = r.get("date")
        try:
            dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        key = _month_key(dt)
        requests_total = (
            int(r.get("composerRequests") or 0)
            + int(r.get("chatRequests") or 0)
            + int(r.get("agentRequests") or 0)
        )
        req_by_month[key] += requests_total
        if r.get("isActive", True) and r.get("userId") is not None:
            users_by_month[key].add(r.get("userId"))

    keys = sorted(req_by_month.keys() | users_by_month.keys())
    # Keep only the most recent *months* buckets.
    keys = keys[-max(1, months):]
    return [
        {
            "month": k,
            "label": _month_label(k),
            "requests": int(req_by_month.get(k, 0)),
            "active_users": len(users_by_month.get(k, set())),
        }
        for k in keys
    ]


def _build_events_rollup(
    client: CursorClient, *, window_days: int, end: datetime
) -> dict[str, Any]:
    """Token totals, cost, model mix, daily trend, and per-user behavior from events.

    Usage events are the only source carrying token-level and authoritative cost detail
    (``chargedCents``), so all three Cursor slides — cost, usage, and user behavior —
    are driven from this one paginated pull.
    """
    start = end - timedelta(days=max(1, window_days))
    # Large page size keeps the request count (and thus rate-limit pressure) low.
    events = client.get_usage_events(start, end, page_size=500, max_events=_MAX_EVENTS)

    input_tokens = output_tokens = 0
    charged_cents = 0.0
    by_model_events: dict[str, int] = defaultdict(int)
    by_model_tokens: dict[str, int] = defaultdict(int)
    by_model_cents: dict[str, float] = defaultdict(float)

    user_tokens: dict[str, int] = defaultdict(int)
    user_input: dict[str, int] = defaultdict(int)
    user_output: dict[str, int] = defaultdict(int)
    user_events: dict[str, int] = defaultdict(int)
    user_cents: dict[str, float] = defaultdict(float)
    user_model_tokens: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    day_input: dict[str, int] = defaultdict(int)
    day_output: dict[str, int] = defaultdict(int)
    day_cents: dict[str, float] = defaultdict(float)
    day_events: dict[str, int] = defaultdict(int)
    day_users: dict[str, set] = defaultdict(set)

    # Cursor drops ``userEmail`` on events for since-removed members (and the numeric
    # userId filter rejects the string member IDs), so those tokens cannot be attributed
    # to a user. Track the unattributed slice so the report can warn instead of silently
    # understating per-user/top-user totals.
    unattributed_events = 0
    unattributed_tokens = 0
    unattributed_cents = 0.0

    for e in events:
        in_t, out_t = _event_io_tokens(e)
        input_tokens += in_t
        output_tokens += out_t
        toks = in_t + out_t
        cents = _event_cost_cents(e)
        charged_cents += cents
        model = _friendly_model(e.get("model"))
        by_model_events[model] += 1
        by_model_tokens[model] += toks
        by_model_cents[model] += cents

        email = e.get("userEmail")
        if email:
            user_tokens[email] += toks
            user_input[email] += in_t
            user_output[email] += out_t
            user_events[email] += 1
            user_cents[email] += cents
            user_model_tokens[email][model] += toks
        else:
            unattributed_events += 1
            unattributed_tokens += toks
            unattributed_cents += cents

        dt = _event_dt(e)
        if dt is not None:
            key = _day_key(dt)
            day_input[key] += in_t
            day_output[key] += out_t
            day_cents[key] += cents
            day_events[key] += 1
            if email:
                day_users[key].add(email)

    total_tokens = input_tokens + output_tokens
    model_mix = [
        {
            "model": m,
            "events": by_model_events[m],
            "tokens": by_model_tokens[m],
            "cents": round(by_model_cents[m], 2),
            "share": round(by_model_tokens[m] / total_tokens, 4) if total_tokens else 0.0,
        }
        for m in sorted(by_model_tokens, key=lambda k: by_model_tokens[k], reverse=True)
    ][:MODEL_MIX_LIMIT]

    def _user_models(email: str, limit: int = 3) -> list[dict[str, Any]]:
        mt = user_model_tokens.get(email, {})
        total = sum(mt.values()) or 1
        ranked = sorted(mt.items(), key=lambda kv: kv[1], reverse=True)[:limit]
        return [{"model": m, "tokens": t, "share": round(t / total, 4)} for m, t in ranked]

    top_users = [
        {
            "email": email,
            "tokens": user_tokens[email],
            "input_tokens": user_input[email],
            "output_tokens": user_output[email],
            "events": user_events[email],
            "cents": round(user_cents[email], 2),
            "models": _user_models(email),
        }
        for email in sorted(user_tokens, key=lambda k: user_tokens[k], reverse=True)
    ]

    # Daily time series (chronological) for cost-/tokens-over-time charts.
    day_keys = sorted(day_input.keys() | day_output.keys() | day_cents.keys() | day_users.keys())
    daily = [
        {
            "date": k,
            "label": _day_label(k),
            "input_tokens": int(day_input.get(k, 0)),
            "output_tokens": int(day_output.get(k, 0)),
            "total_tokens": int(day_input.get(k, 0) + day_output.get(k, 0)),
            "cents": round(day_cents.get(k, 0.0), 2),
            "events": int(day_events.get(k, 0)),
            "active_users": len(day_users.get(k, set())),
        }
        for k in day_keys
    ]

    # Model-usage-by-user matrix for the behavior slide: top users × top models,
    # with everything outside the top models folded into "Other".
    matrix = _build_user_model_matrix(top_users, user_model_tokens)

    return {
        "event_count": len(events),
        "truncated": len(events) >= _MAX_EVENTS,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "charged_cents": round(charged_cents, 2),
        "model_mix": model_mix,
        "daily": daily,
        "user_model_matrix": matrix,
        "unattributed_events": unattributed_events,
        "unattributed_tokens": unattributed_tokens,
        "unattributed_cents": round(unattributed_cents, 2),
        "_top_users": top_users,
    }


def _build_user_model_matrix(
    top_users: list[dict[str, Any]],
    user_model_tokens: dict[str, dict[str, int]],
    *,
    user_limit: int = 6,
    model_limit: int = 3,
) -> dict[str, Any]:
    """Stacked-bar-ready matrix: top users (x) × top models (series) by tokens."""
    users = [u["email"] for u in top_users[:user_limit]]
    if not users:
        return {"users": [], "models": [], "series": {}}

    model_totals: dict[str, int] = defaultdict(int)
    for email in users:
        for model, toks in user_model_tokens.get(email, {}).items():
            model_totals[model] += toks
    top_models = [m for m, _ in sorted(model_totals.items(), key=lambda kv: kv[1], reverse=True)[:model_limit]]
    has_other = len(model_totals) > len(top_models)

    series: dict[str, list[int]] = {m: [] for m in top_models}
    if has_other:
        series["Other"] = []
    for email in users:
        mt = user_model_tokens.get(email, {})
        for m in top_models:
            series[m].append(int(mt.get(m, 0)))
        if has_other:
            other = sum(t for mm, t in mt.items() if mm not in top_models)
            series["Other"].append(int(other))

    models = top_models + (["Other"] if has_other else [])
    return {"users": users, "models": models, "series": series}


def _build_accepted_lines_rollup(
    client: CursorClient, *, window_days: int, end: datetime
) -> dict[str, Any]:
    """Accepted/total AI-written lines per day and per user from daily-usage-data.

    This is the "what did we keep" signal: ``acceptedLinesAdded`` and ``totalLinesAdded``
    are reported per user-day by ``/teams/daily-usage-data``. Pulled over the *same* window
    as the usage events (not the longer monthly-trend window) so it can be joined by email
    against token cost to produce efficiency ratios. ``all_members=True`` keeps idle and
    low-volume engineers in the denominator rather than only active ones.
    """
    start = end - timedelta(days=max(1, window_days))
    rows = client.get_daily_usage(start, end, all_members=True)

    accepted_total = total_total = accepts_total = rejects_total = 0
    user_accepted: dict[str, int] = defaultdict(int)
    user_total: dict[str, int] = defaultdict(int)
    day_accepted: dict[str, int] = defaultdict(int)
    day_total: dict[str, int] = defaultdict(int)
    # Daily-usage rows for since-removed members can arrive without an email; their lines
    # still count in the aggregate but cannot be attributed to a per-user efficiency row.
    unattributed_accepted = 0

    for r in rows:
        acc = int(r.get("acceptedLinesAdded") or 0)
        tot = int(r.get("totalLinesAdded") or 0)
        accepted_total += acc
        total_total += tot
        accepts_total += int(r.get("totalAccepts") or 0)
        rejects_total += int(r.get("totalRejects") or 0)
        email = r.get("email")
        if email:
            user_accepted[email] += acc
            user_total[email] += tot
        elif acc:
            unattributed_accepted += acc
        ts = r.get("date")
        try:
            dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        key = _day_key(dt)
        day_accepted[key] += acc
        day_total[key] += tot

    day_keys = sorted(day_accepted.keys() | day_total.keys())
    daily = [
        {
            "date": k,
            "label": _day_label(k),
            "accepted_lines": int(day_accepted.get(k, 0)),
            "total_lines": int(day_total.get(k, 0)),
        }
        for k in day_keys
    ]
    return {
        "accepted_lines": accepted_total,
        "total_lines": total_total,
        "total_accepts": accepts_total,
        "total_rejects": rejects_total,
        "unattributed_accepted_lines": unattributed_accepted,
        "user_accepted": dict(user_accepted),
        "user_total": dict(user_total),
        "daily": daily,
    }


def _build_efficiency(
    lines: dict[str, Any], events_rollup: dict[str, Any], *, window_days: int
) -> dict[str, Any]:
    """Join accepted lines (daily-usage) with token cost (events) into efficiency ratios.

    Team-level: lines-kept ratio, accepted lines per 1K tokens, and cost per accepted line.
    Per-engineer: the same throughput ratio, joined by email and filtered to a token floor.

    Caveat baked into the labels (see slide): ``acceptedLinesAdded`` spans all surfaces
    including Tab/autocomplete, while token cost is model-API (agent/chat/composer) usage, so
    "cost per accepted line" is a *blended* efficiency proxy, not a clean unit cost.
    """
    accepted = int(lines.get("accepted_lines") or 0)
    total_lines = int(lines.get("total_lines") or 0)
    total_tokens = int(events_rollup.get("total_tokens") or 0)
    charged_cents = float(events_rollup.get("charged_cents") or 0.0)

    lines_kept = round(accepted / total_lines, 4) if total_lines else None
    lines_per_1k = round(accepted / (total_tokens / 1000), 2) if total_tokens else None
    cents_per_line = round(charged_cents / accepted, 4) if accepted else None

    user_accepted = lines.get("user_accepted") or {}
    top_users_src = events_rollup.get("_top_users") or []
    tokens_by_email = {u["email"]: int(u.get("tokens") or 0) for u in top_users_src}
    cents_by_email = {u["email"]: float(u.get("cents") or 0.0) for u in top_users_src}

    per_user: list[dict[str, Any]] = []
    for email, acc in user_accepted.items():
        toks = tokens_by_email.get(email, 0)
        if acc <= 0 or toks < _EFFICIENCY_MIN_TOKENS:
            continue
        cents = cents_by_email.get(email, 0.0)
        per_user.append(
            {
                "email": email,
                "accepted_lines": int(acc),
                "tokens": toks,
                "cents": round(cents, 2),
                "lines_per_1k_tokens": round(acc / (toks / 1000), 2),
                "cents_per_line": round(cents / acc, 4) if acc else None,
            }
        )
    per_user.sort(key=lambda u: u["lines_per_1k_tokens"], reverse=True)

    # Merge per-day cost (events) onto the per-day accepted-lines series so the slide can draw
    # accepted-lines bars against a cost line without re-joining at render time.
    events_daily_cents = {
        d.get("date"): float(d.get("cents") or 0.0) for d in (events_rollup.get("daily") or [])
    }
    daily = [
        {**d, "cents": round(events_daily_cents.get(d.get("date"), 0.0), 2)}
        for d in (lines.get("daily") or [])
    ]

    return {
        "accepted_lines": accepted,
        "total_lines": total_lines,
        "lines_kept": lines_kept,
        "total_tokens": total_tokens,
        "charged_cents_window": round(charged_cents, 2),
        "accepted_lines_per_1k_tokens": lines_per_1k,
        "cost_per_accepted_line_cents": cents_per_line,
        "daily": daily,
        "top_efficiency": per_user[:TOP_USERS_LIMIT],
    }


def _cents_to_dollars(cents: Any) -> str:
    return f"${float(cents) / 100:,.0f}" if isinstance(cents, (int, float)) else "unknown"


def _focus_prompt(report: dict[str, Any], focus: str) -> str:
    """Build the focus-specific user prompt for a Cursor slide takeaway."""
    members = report.get("members") or {}
    totals = report.get("totals") or {}
    daily = report.get("daily") or []
    top_users = report.get("top_users") or []
    model_mix = report.get("model_mix") or []
    window_days = int(report.get("window_days") or 30)

    seats = int(members.get("total") or 0)
    active = int(members.get("active_window") or 0)
    cycle_spend = _cents_to_dollars(totals.get("spend_cents_cycle"))
    window_cost = _cents_to_dollars(totals.get("charged_cents_window"))

    if focus == "cost":
        cost_per_active = (
            _cents_to_dollars((totals.get("charged_cents_window") or 0) / active) if active else "unknown"
        )
        # Half-over-half average daily cost is far more robust than first->last day,
        # whose endpoints are noisy and often partial.
        trend_note = "trend: insufficient data"
        if len(daily) >= 4:
            mid = len(daily) // 2
            first_half = [float(d.get("cents") or 0) for d in daily[:mid]]
            second_half = [float(d.get("cents") or 0) for d in daily[mid:]]
            avg1 = sum(first_half) / len(first_half) if first_half else 0.0
            avg2 = sum(second_half) / len(second_half) if second_half else 0.0
            if avg1 > 0:
                pct = int(round((avg2 - avg1) / avg1 * 100))
                direction = "up" if pct > 0 else ("down" if pct < 0 else "flat")
                trend_note = (
                    f"avg daily cost {direction} {abs(pct)}% in the second half of the window "
                    f"({_cents_to_dollars(avg1)}/day -> {_cents_to_dollars(avg2)}/day)"
                )
            elif avg2 > 0:
                trend_note = f"avg daily cost ramped to {_cents_to_dollars(avg2)}/day in the second half"
        idle = max(0, seats - active)
        return (
            f"Cursor AI coding-assistant COST for the engineering org over the last {window_days} days. "
            f"Usage-based cost in window: {window_cost}; billing-cycle overage: {cycle_spend}; "
            f"active engineers: {active} of {seats} seats ({idle} idle); cost per active engineer: {cost_per_active}. "
            f"Cost trend: {trend_note}. "
            "Implication for a VP of Engineering about cost trajectory, ROI per active engineer, "
            "or idle paid seats — and the concrete next step. Do not invent a spend cap or a number "
            "not given above."
        )
    if focus == "efficiency":
        eff = report.get("efficiency") or {}
        accepted = int(eff.get("accepted_lines") or 0)
        kept = eff.get("lines_kept")
        per1k = eff.get("accepted_lines_per_1k_tokens")
        cpl = eff.get("cost_per_accepted_line_cents")
        kept_str = (
            f"{int(round(float(kept) * 100))}% of AI-written lines kept"
            if kept is not None else "lines-kept unknown"
        )
        per1k_str = (
            f"{per1k} accepted lines per 1K tokens" if per1k is not None else "throughput unknown"
        )
        cpl_str = f"{float(cpl):.2f}\u00a2 per accepted line" if cpl is not None else "cost-per-line unknown"
        return (
            f"Cursor AI coding-assistant EFFICIENCY (output kept per token and per dollar) for the "
            f"engineering org over the last {window_days} days. {accepted} AI-written lines accepted; "
            f"{kept_str}; {per1k_str}; {cpl_str}; usage cost {window_cost} across {active} active engineers. "
            "This is an efficiency/ROI correlation, NOT a per-engineer productivity ranking — do not imply "
            "a low-token engineer is less productive. Implication for a VP of Engineering about ROI per "
            "token/dollar or where efficiency is trending — and the concrete next step. Do not invent "
            "numbers not given above."
        )
    if focus == "users":
        top_str = ", ".join(
            f"{(u.get('email') or '').split('@')[0]} ({int(u.get('tokens') or 0)} tok, "
            f"{(u.get('models') or [{}])[0].get('model', '?')})"
            for u in top_users[:4]
        ) or "none"
        return (
            f"Cursor AI coding-assistant USER BEHAVIOR for the engineering org over the last {window_days} days. "
            f"Active engineers: {active} of {seats} seats. Highest-volume users (tokens, top model): {top_str}. "
            "Implication for a VP of Engineering about usage concentration among a few power users, "
            "idle seats, or uneven adoption — and the concrete next step?"
        )
    # default: usage (tokens + models)
    in_t = int(totals.get("input_tokens") or 0)
    out_t = int(totals.get("output_tokens") or 0)
    model_str = ", ".join(
        f"{m.get('model')} {int(round(float(m.get('share') or 0) * 100))}%" for m in model_mix[:3]
    ) or "none"
    return (
        f"Cursor AI coding-assistant USAGE for the engineering org over the last {window_days} days. "
        f"Total tokens: {in_t + out_t} ({in_t} input / {out_t} output); "
        f"active engineers: {active} of {seats}. Model mix by tokens: {model_str}. "
        "Implication for a VP of Engineering about workload mix (input vs output), model concentration, "
        "or cost-efficiency of model choice — and the concrete next step?"
    )


def generate_cursor_usage_takeaway(report: dict[str, Any], focus: str = "usage") -> str:
    """One-sentence VP 'what this means' implication for a Cursor slide.

    *focus* selects the angle: ``"cost"``, ``"usage"``, or ``"users"``.
    Mirrors the engineering portfolio takeaway pattern: LLM-written, single sentence.
    Returns ``""`` on any failure so the slide simply omits the band (no placeholder,
    consistent with the other engineering slides).
    """
    if not report or not report.get("configured"):
        return ""

    prompt = _focus_prompt(report, focus)

    try:
        from .config import LLM_MODEL_FAST, llm_client

        client = llm_client()
        resp = client.chat.completions.create(
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
                    "- No markdown, no leading bullet/dash, no label, no preamble\n"
                    "- Tone: direct, analytical, board-room; never salesy or hedging"
                )},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=1024,
        )
        text = " ".join((resp.choices[0].message.content or "").split()).strip()
        return text.lstrip("•-–—* ").strip()
    except Exception as e:
        logger.warning("Cursor usage takeaway generation failed: %s", e)
        return ""


def generate_cursor_usage_takeaways(report: dict[str, Any]) -> dict[str, str]:
    """Per-slide takeaways for the four Cursor slides (cost, usage, efficiency, users).

    Returns a dict keyed by focus; each value is ``""`` on failure so the slide
    omits the band rather than rendering a placeholder.
    """
    if not report or not report.get("configured"):
        return {"cost": "", "usage": "", "users": "", "efficiency": ""}
    return {
        focus: generate_cursor_usage_takeaway(report, focus)
        for focus in ("cost", "usage", "users", "efficiency")
    }


def build_cursor_usage_report(
    *,
    window_days: int = DEFAULT_EVENTS_WINDOW_DAYS,
    trend_months: int = DEFAULT_TREND_MONTHS,
    client: CursorClient | None = None,
) -> dict[str, Any]:
    """Assemble the ``cursor_usage`` report blob. Always returns a dict.

    When Cursor is not configured, returns ``{"configured": False}`` so the deck can
    filter the slide out cleanly. Per-section API failures are collected in ``errors``.
    """
    if client is None and not cursor_configured():
        return {"configured": False, "errors": ["CURSOR_ADMIN_API_KEY not set"]}

    try:
        client = client or CursorClient()
    except CursorClientError as e:
        return {"configured": False, "errors": [str(e)]}

    end = datetime.now(timezone.utc)
    errors: list[str] = []

    # Members (seats + roster for active rate).
    members: list[dict[str, Any]] = []
    try:
        members = client.get_team_members()
    except CursorClientError as e:
        errors.append(f"members: {e}")

    # Monthly adoption / volume trend.
    monthly: list[dict[str, Any]] = []
    try:
        monthly = _build_monthly_trend(client, months=trend_months, end=end)
    except CursorClientError as e:
        errors.append(f"daily-usage: {e}")

    # Token / model / per-user rollup from events.
    events_rollup: dict[str, Any] = {}
    try:
        events_rollup = _build_events_rollup(client, window_days=window_days, end=end)
    except CursorClientError as e:
        errors.append(f"usage-events: {e}")

    # Accepted/total AI-written lines (efficiency numerator), same window as events.
    lines_rollup: dict[str, Any] = {}
    try:
        lines_rollup = _build_accepted_lines_rollup(client, window_days=window_days, end=end)
    except CursorClientError as e:
        errors.append(f"daily-usage-lines: {e}")

    # Efficiency = accepted lines joined to token cost (degrades to empty ratios if either
    # source failed; the slide is omitted/blank rather than showing misleading numbers).
    efficiency = _build_efficiency(lines_rollup, events_rollup, window_days=window_days)

    # Spend (current billing cycle).
    spend_rows: list[dict[str, Any]] = []
    spend_by_email: dict[str, float] = {}
    try:
        spend_rows = client.get_spend()
        for r in spend_rows:
            email = r.get("email")
            if email:
                spend_by_email[email] = float(r.get("overallSpendCents") or 0)
    except CursorClientError as e:
        errors.append(f"spend: {e}")

    # Active-this-window users: distinct users seen in events, else latest trend month.
    top_users_src = events_rollup.get("_top_users") or []
    active_window = len(top_users_src)
    if not active_window and monthly:
        active_window = int(monthly[-1].get("active_users") or 0)

    # Join top users (by tokens) with spend.
    top_users = [
        {
            "email": u["email"],
            "tokens": u["tokens"],
            "input_tokens": u.get("input_tokens", 0),
            "output_tokens": u.get("output_tokens", 0),
            "events": u["events"],
            "window_cents": u.get("cents"),
            "models": u.get("models", []),
            "spend_cents": spend_by_email.get(u["email"]),
        }
        for u in top_users_src[:TOP_USERS_LIMIT]
    ]

    total_spend_cents = sum(spend_by_email.values()) if spend_by_email else None

    # Surface (don't swallow) data-attribution gaps. These are warnings, not errors:
    # the totals are still correct in aggregate; only the per-user breakdown is affected.
    warnings: list[str] = []
    unattributed_events = int(events_rollup.get("unattributed_events") or 0)
    unattributed_tokens = int(events_rollup.get("unattributed_tokens") or 0)
    if unattributed_events:
        total_events = int(events_rollup.get("event_count") or 0)
        total_tokens_all = int(events_rollup.get("total_tokens") or 0)
        evt_pct = round(unattributed_events / total_events * 100, 1) if total_events else 0.0
        tok_pct = round(unattributed_tokens / total_tokens_all * 100, 1) if total_tokens_all else 0.0
        warnings.append(
            f"{unattributed_events} usage event(s) ({evt_pct}%, {unattributed_tokens} tokens, "
            f"{tok_pct}% of tokens) had no userEmail — likely removed accounts; per-user and "
            "top-user totals understate their usage (aggregate totals are unaffected)."
        )
    # Efficiency join coverage: engineers with token spend but no accepted-line attribution
    # (or vice versa) understate the per-engineer efficiency ranking — warn, do not drop.
    user_accepted = lines_rollup.get("user_accepted") or {}
    if user_accepted and top_users_src:
        token_emails = {u.get("email") for u in top_users_src if u.get("email")}
        line_emails = {e for e, acc in user_accepted.items() if acc}
        tokens_no_lines = token_emails - line_emails
        if tokens_no_lines:
            warnings.append(
                f"{len(tokens_no_lines)} engineer(s) had token usage but no accepted-line data "
                f"({', '.join(sorted(e.split('@')[0] for e in tokens_no_lines)[:5])}"
                f"{'…' if len(tokens_no_lines) > 5 else ''}); excluded from the efficiency ranking."
            )
    unattributed_lines = int(lines_rollup.get("unattributed_accepted_lines") or 0)
    if unattributed_lines:
        warnings.append(
            f"{unattributed_lines} accepted line(s) had no engineer email (likely removed "
            "accounts); counted in team totals but not per-engineer efficiency."
        )

    # Top users that carry no current-cycle spend row (removed user, or no spend yet).
    if spend_by_email:
        spend_misses = [u["email"] for u in top_users if u.get("spend_cents") is None]
        if spend_misses:
            warnings.append(
                f"{len(spend_misses)} top user(s) had no current-cycle spend row "
                f"({', '.join(e.split('@')[0] for e in spend_misses[:5])}"
                f"{'…' if len(spend_misses) > 5 else ''}); spend column shows blank for them."
            )
    for w in warnings:
        logger.warning("Cursor usage report: %s", w)

    report = {
        "configured": True,
        "generated_at": end.isoformat(),
        "window_days": window_days,
        "trend_months": trend_months,
        "members": {
            "total": len(members),
            "active_window": active_window,
        },
        "totals": {
            "total_tokens": events_rollup.get("total_tokens", 0),
            "input_tokens": events_rollup.get("input_tokens", 0),
            "output_tokens": events_rollup.get("output_tokens", 0),
            "event_count": events_rollup.get("event_count", 0),
            "charged_cents_window": events_rollup.get("charged_cents"),
            "spend_cents_cycle": total_spend_cents,
        },
        "monthly": monthly,
        "daily": events_rollup.get("daily", []),
        "efficiency": efficiency,
        "top_users": top_users,
        "model_mix": events_rollup.get("model_mix", []),
        "user_model_matrix": events_rollup.get("user_model_matrix", {"users": [], "models": [], "series": {}}),
        "errors": errors,
        "warnings": warnings,
    }
    logger.info(
        "Cursor usage report: %s seats, %s active, %s tokens, %s model(s), %s error(s), %s warning(s)",
        report["members"]["total"], active_window,
        report["totals"]["total_tokens"], len(report["model_mix"]), len(errors), len(warnings),
    )
    return report
