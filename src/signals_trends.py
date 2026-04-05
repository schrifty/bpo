"""Pendo visitor-window comparisons for Notable Signals (trend banner + LLM context).

Uses a small number of extra ``visitors`` aggregation calls (prior period, optional WoW/MoM/YoY).
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .config import (
    BPO_SIGNALS_TRENDS,
    BPO_SIGNALS_TRENDS_MOM,
    BPO_SIGNALS_TRENDS_PRIOR_PERIOD,
    BPO_SIGNALS_TRENDS_TIMEOUT,
    BPO_SIGNALS_TRENDS_WOW,
    BPO_SIGNALS_TRENDS_YOY,
    logger,
)


def _is_internal_visitor(v: dict[str, Any]) -> bool:
    agent = (v.get("metadata") or {}).get("agent") or {}
    return bool(agent.get("isinternaluser")) or agent.get("role") == "LeanDNAStaff"


def _filter_customer_visitors_from_list(
    customer_name: str,
    visitors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    from .pendo_client import _name_matches

    out: list[dict[str, Any]] = []
    for v in visitors:
        if _is_internal_visitor(v):
            continue
        agent = (v.get("metadata") or {}).get("agent") or {}
        sitenames = agent.get("sitenames") or []
        if any(_name_matches(customer_name, str(sn)) for sn in sitenames):
            out.append(v)
    return out


def _engagement_bucket_counts(customer_visitors: list[dict], window_end_ms: int) -> dict[str, int]:
    """Same buckets as ``get_customer_health`` but with an arbitrary window end (epoch ms)."""
    active_7d = active_30d = dormant = 0
    for v in customer_visitors:
        auto = (v.get("metadata") or {}).get("auto") or {}
        lv = auto.get("lastvisit", 0)
        if lv:
            days_ago = (window_end_ms - lv) / (86400 * 1000)
            if days_ago <= 7:
                active_7d += 1
            elif days_ago <= 30:
                active_30d += 1
            else:
                dormant += 1
        else:
            dormant += 1
    return {"active_7d": active_7d, "active_30d": active_30d, "dormant": dormant}


def _snapshot_metrics(
    pc: Any,
    customer_name: str,
    start_ms: int,
    end_ms: int,
    *,
    timeout: tuple[int, float] | None = None,
) -> dict[str, Any] | None:
    to = timeout if timeout is not None else (10, float(BPO_SIGNALS_TRENDS_TIMEOUT))
    try:
        visitors = pc.get_visitors_range(start_ms, end_ms, _timeout=to)
    except Exception as e:
        logger.warning("signals_trends: visitor range fetch failed (%s–%s): %s", start_ms, end_ms, e)
        return None
    cv = _filter_customer_visitors_from_list(customer_name, visitors)
    total = len(cv)
    if total == 0:
        return {"total_users": 0, "active_7d": 0, "weekly_active_rate_pct": 0.0}
    eng = _engagement_bucket_counts(cv, end_ms)
    rate = eng["active_7d"] / total
    return {
        "total_users": total,
        "active_7d": eng["active_7d"],
        "weekly_active_rate_pct": round(rate * 100, 1),
    }


def _delta_pp(cur_pct: float, prev_pct: float) -> float:
    return round(cur_pct - prev_pct, 1)


def _fetch_trend_windows_parallel(
    pc: Any,
    customer_name: str,
    windows: list[tuple[str, int, int]],
) -> dict[str, dict[str, Any] | None]:
    """Run independent visitor-range snapshots in parallel (thread-safe Pendo posts)."""
    if not windows:
        return {}
    timeout = (10, float(BPO_SIGNALS_TRENDS_TIMEOUT))
    results: dict[str, dict[str, Any] | None] = {}

    def _job(key: str, s: int, e: int) -> tuple[str, dict[str, Any] | None]:
        return key, _snapshot_metrics(pc, customer_name, s, e, timeout=timeout)

    n = len(windows)
    with ThreadPoolExecutor(max_workers=min(8, n)) as pool:
        future_map = {pool.submit(_job, k, s, e): k for k, s, e in windows}
        for fut in as_completed(future_map):
            key = future_map[fut]
            try:
                k, snap = fut.result()
                results[k] = snap
            except Exception as e:
                logger.warning("signals_trends: window %r worker failed: %s", key, e)
                results[key] = None
    return results


def build_signals_trend_context(
    pc: Any,
    customer_name: str,
    days: int,
    report: dict[str, Any],
) -> dict[str, Any] | None:
    """Build compact trend + cohort context for the signals LLM and slide banner."""
    if not BPO_SIGNALS_TRENDS:
        return None

    eng = report.get("engagement") or {}
    bench = report.get("benchmarks") or {}
    acct = report.get("account") or {}

    current = {
        "window_days": int(days),
        "active_7d": eng.get("active_7d"),
        "total_users": acct.get("total_visitors"),
        "weekly_active_rate_pct": eng.get("active_rate_7d"),
    }

    cohort_block: dict[str, Any] = {
        "cohort_name": bench.get("cohort_name") or bench.get("cohort") or "",
        "cohort_median_weekly_active_rate_pct": bench.get("cohort_median_rate"),
        "portfolio_median_weekly_active_rate_pct": bench.get("peer_median_rate"),
    }

    out: dict[str, Any] = {
        "primary_comparison_note": (
            f"Current period = last {days} days from today. "
            "Use the comparison whose label best matches the business question (WoW for weekly rhythm, "
            "QoQ when primary window matches a quarter, YoY for seasonality only when present)."
        ),
        "current_period": current,
        "cohort": cohort_block,
    }

    end_ms = int(time.time() * 1000)
    ms_day = 86400 * 1000

    windows: list[tuple[str, int, int]] = []
    if BPO_SIGNALS_TRENDS_PRIOR_PERIOD and days >= 7:
        start_current = end_ms - days * ms_day
        start_prior = start_current - days * ms_day
        windows.append(("prior", start_prior, start_current))

    if BPO_SIGNALS_TRENDS_WOW:
        w = 7
        c_end = end_ms
        c_start = c_end - w * ms_day
        p_end = c_start
        p_start = p_end - w * ms_day
        windows.append(("wow_cur", c_start, c_end))
        windows.append(("wow_prev", p_start, p_end))

    if BPO_SIGNALS_TRENDS_MOM and days >= 14:
        w = 30
        c_end = end_ms
        c_start = c_end - w * ms_day
        p_end = c_start
        p_start = p_end - w * ms_day
        windows.append(("mom_cur", c_start, c_end))
        windows.append(("mom_prev", p_start, p_end))

    if BPO_SIGNALS_TRENDS_YOY and days >= 60:
        w = 365
        c_end = end_ms
        c_start = c_end - w * ms_day
        p_end = c_start
        p_start = p_end - w * ms_day
        windows.append(("yoy_cur", c_start, c_end))
        windows.append(("yoy_prev", p_start, p_end))

    fetched = _fetch_trend_windows_parallel(pc, customer_name, windows)

    prev = fetched.get("prior")
    if (
        prev
        and current.get("weekly_active_rate_pct") is not None
        and prev.get("total_users", 0) > 0
    ):
        cur_pct = float(current["weekly_active_rate_pct"])
        pr_pct = float(prev["weekly_active_rate_pct"])
        out["prior_same_length"] = {
            "label": "QoQ" if days >= 80 else "prior_period",
            "description": f"Prior {days}-day window immediately before current",
            "window_days": days,
            **prev,
            "weekly_active_rate_pct_delta_vs_current_pp": _delta_pp(cur_pct, pr_pct),
        }

    cur_w = fetched.get("wow_cur")
    prev_w = fetched.get("wow_prev")
    if (
        cur_w
        and prev_w
        and cur_w.get("total_users", 0) > 0
        and prev_w.get("total_users", 0) > 0
    ):
        out["wow"] = {
            "label": "WoW",
            "description": "Last 7 days vs the 7 days immediately before",
            "current_window": cur_w,
            "prior_window": prev_w,
            "weekly_active_rate_pct_delta_pp": _delta_pp(
                float(cur_w["weekly_active_rate_pct"]),
                float(prev_w["weekly_active_rate_pct"]),
            ),
            "active_7d_delta": int(cur_w["active_7d"]) - int(prev_w["active_7d"]),
        }

    cur_m = fetched.get("mom_cur")
    prev_m = fetched.get("mom_prev")
    if (
        cur_m
        and prev_m
        and cur_m.get("total_users", 0) > 0
        and prev_m.get("total_users", 0) > 0
    ):
        out["mom"] = {
            "label": "MoM",
            "description": "Last 30 days vs the 30 days immediately before",
            "current_window": cur_m,
            "prior_window": prev_m,
            "weekly_active_rate_pct_delta_pp": _delta_pp(
                float(cur_m["weekly_active_rate_pct"]),
                float(prev_m["weekly_active_rate_pct"]),
            ),
        }

    cur_y = fetched.get("yoy_cur")
    prev_y = fetched.get("yoy_prev")
    if (
        cur_y
        and prev_y
        and cur_y.get("total_users", 0) > 0
        and prev_y.get("total_users", 0) > 0
    ):
        out["yoy"] = {
            "label": "YoY",
            "description": "Last 365 days vs the 365 days immediately before (expensive)",
            "current_window": cur_y,
            "prior_window": prev_y,
            "weekly_active_rate_pct_delta_pp": _delta_pp(
                float(cur_y["weekly_active_rate_pct"]),
                float(prev_y["weekly_active_rate_pct"]),
            ),
        }

    return out


def finalize_signals_trends_banner(report: dict[str, Any]) -> None:
    """Set ``signals_trends_display`` if missing, using trend context (heuristic)."""
    if (report.get("signals_trends_display") or "").strip():
        return
    ctx = report.get("signals_trend_context")
    if not isinstance(ctx, dict):
        return

    cur_pct = ctx.get("current_period", {}).get("weekly_active_rate_pct")
    cohort = ctx.get("cohort") or {}
    cmed = cohort.get("cohort_median_weekly_active_rate_pct")
    parts: list[str] = []

    def _fmt_delta(label: str, dpp: float | None) -> str | None:
        if dpp is None:
            return None
        if abs(dpp) < 0.05:
            return None
        direction = "up" if dpp > 0 else "down"
        return f"{label}: weekly active rate {direction} {abs(dpp):.1f} pp vs comparison window"

    # Prefer WoW then prior (QoQ-style) then MoM by absolute delta
    candidates: list[tuple[float, str]] = []
    wow = ctx.get("wow")
    if isinstance(wow, dict) and wow.get("weekly_active_rate_pct_delta_pp") is not None:
        d = float(wow["weekly_active_rate_pct_delta_pp"])
        candidates.append((abs(d), _fmt_delta("WoW", d) or ""))
    ps = ctx.get("prior_same_length")
    if isinstance(ps, dict) and ps.get("weekly_active_rate_pct_delta_vs_current_pp") is not None:
        d = float(ps["weekly_active_rate_pct_delta_vs_current_pp"])
        lbl = "QoQ" if ps.get("label") == "QoQ" else "Prior period"
        candidates.append((abs(d), _fmt_delta(lbl, d) or ""))
    mom = ctx.get("mom")
    if isinstance(mom, dict) and mom.get("weekly_active_rate_pct_delta_pp") is not None:
        d = float(mom["weekly_active_rate_pct_delta_pp"])
        candidates.append((abs(d), _fmt_delta("MoM", d) or ""))
    yoy = ctx.get("yoy")
    if isinstance(yoy, dict) and yoy.get("weekly_active_rate_pct_delta_pp") is not None:
        d = float(yoy["weekly_active_rate_pct_delta_pp"])
        candidates.append((abs(d), _fmt_delta("YoY", d) or ""))

    candidates = [(a, t) for a, t in candidates if t]
    if candidates:
        candidates.sort(key=lambda x: -x[0])
        parts.append(candidates[0][1])

    if cur_pct is not None and cmed is not None:
        try:
            parts.append(
                f"Cohort ({cohort.get('cohort_name') or 'peer'}): median weekly active {float(cmed):.1f}% "
                f"(this account {float(cur_pct):.1f}%)"
            )
        except (TypeError, ValueError):
            pass

    if parts:
        report["signals_trends_display"] = " · ".join(parts)[:280]
