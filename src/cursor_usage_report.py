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

    for e in events:
        in_t, out_t = _event_io_tokens(e)
        input_tokens += in_t
        output_tokens += out_t
        toks = in_t + out_t
        cents = _event_cost_cents(e)
        charged_cents += cents
        model = str(e.get("model") or "unknown")
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
        "_top_users": top_users,
    }


def _build_user_model_matrix(
    top_users: list[dict[str, Any]],
    user_model_tokens: dict[str, dict[str, int]],
    *,
    user_limit: int = 6,
    model_limit: int = 4,
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
        first = daily[0].get("cents", 0) if daily else 0
        last = daily[-1].get("cents", 0) if daily else 0
        cost_per_active = (
            _cents_to_dollars((totals.get("charged_cents_window") or 0) / active) if active else "unknown"
        )
        return (
            f"Cursor AI coding-assistant COST for the engineering org over the last {window_days} days. "
            f"Spend this billing cycle: {cycle_spend}; usage-based cost in window: {window_cost}; "
            f"active engineers in window: {active} of {seats} seats; cost per active engineer: {cost_per_active}. "
            f"Daily cost start->end (cents): {first}->{last}. "
            "Implication for a VP of Engineering about cost trajectory, ROI per active engineer, "
            "or idle paid seats — and the concrete next step?"
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
    """Per-slide takeaways for the three Cursor slides (cost, usage, users).

    Returns a dict keyed by focus; each value is ``""`` on failure so the slide
    omits the band rather than rendering a placeholder.
    """
    if not report or not report.get("configured"):
        return {"cost": "", "usage": "", "users": ""}
    return {focus: generate_cursor_usage_takeaway(report, focus) for focus in ("cost", "usage", "users")}


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
        "top_users": top_users,
        "model_mix": events_rollup.get("model_mix", []),
        "user_model_matrix": events_rollup.get("user_model_matrix", {"users": [], "models": [], "series": {}}),
        "errors": errors,
    }
    logger.info(
        "Cursor usage report: %s seats, %s active, %s tokens, %s model(s), %s error(s)",
        report["members"]["total"], active_window,
        report["totals"]["total_tokens"], len(report["model_mix"]), len(errors),
    )
    return report
