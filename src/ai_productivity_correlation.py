"""Join GitHub engineering output with Cursor token spend (company + individual)."""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

logger = logging.getLogger("bpo")

_MIN_TOKENS_FOR_RANK = 1000
_QUADRANT_LABELS = (
    "high_tokens_high_output",
    "high_tokens_low_output",
    "low_tokens_high_output",
    "low_tokens_low_output",
)


def _safe_ratio(num: float | int | None, den: float | int | None) -> float | None:
    if num is None or den is None:
        return None
    d = float(den)
    if d <= 0:
        return None
    return round(float(num) / d, 4)


def _company_kpis(
    *,
    tokens: int,
    charged_cents: float,
    commits: int,
    merged_prs: int,
    lines_added: int,
    active_engineers: int,
) -> dict[str, Any]:
    return {
        "total_tokens": tokens,
        "charged_cents_window": round(charged_cents, 2),
        "commits": commits,
        "merged_prs": merged_prs,
        "lines_added": lines_added,
        "active_engineers": active_engineers,
        "tokens_per_commit": _safe_ratio(tokens, commits),
        "cents_per_merged_pr": _safe_ratio(charged_cents, merged_prs),
        "commits_per_1k_tokens": _safe_ratio(commits, tokens / 1000 if tokens else None),
        "cents_per_1k_lines_added": _safe_ratio(charged_cents, lines_added / 1000 if lines_added else None),
        "commits_per_active_engineer": _safe_ratio(commits, active_engineers),
    }


def _cursor_engineer_usage(cursor_usage: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Return engineer-scope totals and per-email usage from ``cursor_usage``."""
    usage = cursor_usage.get("usage_engineers") or {}
    by_email = dict(cursor_usage.get("engineer_usage_by_email") or {})
    totals = usage.get("totals") or {}
    return totals, by_email


def _quadrant(tokens: int, commits: int, *, med_tokens: float, med_commits: float) -> str:
    high_tokens = tokens >= med_tokens
    high_output = commits >= med_commits
    if high_tokens and high_output:
        return _QUADRANT_LABELS[0]
    if high_tokens and not high_output:
        return _QUADRANT_LABELS[1]
    if not high_tokens and high_output:
        return _QUADRANT_LABELS[2]
    return _QUADRANT_LABELS[3]


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    if len(set(xs)) <= 1 or len(set(ys)) <= 1:
        return None
    try:
        corr_fn = getattr(statistics, "correlation", None)
        if corr_fn is not None:
            return round(corr_fn(xs, ys), 4)
    except (statistics.StatisticsError, ValueError):
        return None
    # Python < 3.10 has no statistics.correlation — compute Pearson r directly.
    n = len(xs)
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sum((x - mean_x) ** 2 for x in xs)
    den_y = sum((y - mean_y) ** 2 for y in ys)
    if den_x <= 0 or den_y <= 0:
        return None
    return round(num / (den_x * den_y) ** 0.5, 4)


def _iso_week_key_from_day_key(day_key: str) -> str | None:
    raw = (day_key or "").strip()
    if not raw:
        return None
    try:
        if len(raw) == 10 and raw[4] == "-":
            dt = datetime.strptime(raw, "%Y-%m-%d")
        else:
            dt = datetime.strptime(raw, "%m/%d/%y")
        iso = dt.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    except ValueError:
        return None


def _build_weekly_trend(
    cursor_usage: dict[str, Any],
    github_productivity: dict[str, Any],
) -> list[dict[str, Any]]:
    """Merge engineer-scoped Cursor tokens and GitHub commits by ISO week."""
    tokens_by_week: dict[str, int] = defaultdict(int)
    daily = (cursor_usage.get("usage_engineers") or {}).get("daily") or []
    for row in daily:
        wk = _iso_week_key_from_day_key(str(row.get("date") or row.get("label") or ""))
        if not wk:
            continue
        tokens_by_week[wk] += int(row.get("total_tokens") or row.get("tokens") or 0)

    gh_weekly = {
        str(row.get("week") or ""): int(row.get("engineer_commits") or row.get("commits") or 0)
        for row in (github_productivity.get("weekly") or [])
        if isinstance(row, dict) and row.get("week")
    }
    gh_merges = {
        str(row.get("week") or ""): int(row.get("engineer_merged_prs") or row.get("merged_prs") or 0)
        for row in (github_productivity.get("weekly") or [])
        if isinstance(row, dict) and row.get("week")
    }
    weeks = sorted(set(tokens_by_week.keys()) | set(gh_weekly.keys()) | set(gh_merges.keys()))
    out: list[dict[str, Any]] = []
    for wk in weeks:
        label = wk.split("-W")[-1] if "-W" in wk else wk
        out.append(
            {
                "week": wk,
                "label": f"W{int(label)}" if label.isdigit() else label,
                "tokens": tokens_by_week.get(wk, 0),
                "commits": gh_weekly.get(wk, 0),
                "merged_prs": gh_merges.get(wk, 0),
            }
        )
    return out


def build_ai_productivity_correlation(
    cursor_usage: dict[str, Any] | None,
    github_productivity: dict[str, Any] | None,
    identity: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Correlate Cursor spend with GitHub output for engineers."""
    if not cursor_usage or not cursor_usage.get("configured"):
        return None
    if not github_productivity or not github_productivity.get("configured"):
        return None

    warnings: list[str] = list(github_productivity.get("warnings") or [])
    window_days = int(
        github_productivity.get("window_days") or cursor_usage.get("window_days") or 30
    )

    eng_totals, cursor_by_email = _cursor_engineer_usage(cursor_usage)
    gh_company = github_productivity.get("company_engineers") or {}
    gh_by_email = github_productivity.get("by_email") or {}

    if not (cursor_usage.get("usage_engineers") or {}).get("configured"):
        warnings.append("Cursor engineer scope unavailable — company KPIs use org-wide Cursor totals")
        eng_totals = cursor_usage.get("totals") or {}

    canonical_emails = sorted(set(gh_by_email.keys()) | set(cursor_by_email.keys()))
    if identity and identity.get("configured"):
        canonical_emails = sorted(set(identity.get("canonical_emails") or []) | set(canonical_emails))

    individuals: list[dict[str, Any]] = []
    xs: list[float] = []
    ys: list[float] = []

    for email in canonical_emails:
        gh = gh_by_email.get(email) or {}
        cu = cursor_by_email.get(email) or cursor_by_email.get(email.casefold()) or {}
        tokens = int(cu.get("tokens") or 0)
        commits = int(gh.get("commits") or 0)
        merged_prs = int(gh.get("merged_prs") or 0)
        lines_added = int(gh.get("lines_added") or 0)
        cents = float(cu.get("cents") or 0.0)
        if tokens <= 0 and commits <= 0 and merged_prs <= 0:
            continue
        row = {
            "email": email,
            "tokens": tokens,
            "charged_cents_window": round(cents, 2),
            "commits": commits,
            "merged_prs": merged_prs,
            "lines_added": lines_added,
            "lines_deleted": int(gh.get("lines_deleted") or 0),
            "repos_touched": gh.get("repos_touched") or [],
            "tokens_per_commit": _safe_ratio(tokens, commits),
            "cents_per_merged_pr": _safe_ratio(cents, merged_prs),
            "commits_per_1k_tokens": _safe_ratio(commits, tokens / 1000 if tokens else None),
        }
        individuals.append(row)
        if tokens >= _MIN_TOKENS_FOR_RANK:
            xs.append(float(tokens))
            ys.append(float(commits))

    med_tokens = statistics.median(xs) if xs else 0.0
    med_commits = statistics.median(ys) if ys else 0.0
    quadrants: dict[str, list[str]] = {label: [] for label in _QUADRANT_LABELS}
    for row in individuals:
        if row["tokens"] < _MIN_TOKENS_FOR_RANK:
            continue
        label = _quadrant(row["tokens"], row["commits"], med_tokens=med_tokens, med_commits=med_commits)
        quadrants[label].append(row["email"])

    unmatched_cursor = sorted(
        email
        for email, cu in cursor_by_email.items()
        if int(cu.get("tokens") or 0) >= _MIN_TOKENS_FOR_RANK
        and email not in gh_by_email
    )
    unmatched_github = sorted(
        email
        for email, gh in gh_by_email.items()
        if (int(gh.get("commits") or 0) + int(gh.get("merged_prs") or 0)) > 0
        and email not in cursor_by_email
    )
    if unmatched_cursor:
        warnings.append(
            f"{len(unmatched_cursor)} engineer(s) had Cursor tokens but no GitHub commits in window"
        )
    if unmatched_github:
        warnings.append(
            f"{len(unmatched_github)} engineer(s) had GitHub activity but no Cursor usage in window"
        )

    active = int((cursor_usage.get("usage_engineers") or {}).get("active_window") or 0)
    company = _company_kpis(
        tokens=int(eng_totals.get("total_tokens") or 0),
        charged_cents=float(eng_totals.get("charged_cents_window") or 0.0),
        commits=int(gh_company.get("commits") or 0),
        merged_prs=int(gh_company.get("merged_prs") or 0),
        lines_added=int(gh_company.get("lines_added") or 0),
        active_engineers=active,
    )
    company["token_commit_correlation"] = _pearson(xs, ys)

    individuals.sort(
        key=lambda r: (r.get("commits_per_1k_tokens") or 0, r.get("commits") or 0),
        reverse=True,
    )

    ranked = [r for r in individuals if int(r.get("tokens") or 0) >= _MIN_TOKENS_FOR_RANK]
    top_yield = ranked[:6]
    review = sorted(
        [r for r in ranked if r["email"] in quadrants[_QUADRANT_LABELS[1]]],
        key=lambda r: int(r.get("tokens") or 0),
        reverse=True,
    )[:6]
    quadrant_counts = {label: len(quadrants.get(label) or []) for label in _QUADRANT_LABELS}
    weekly_trend = _build_weekly_trend(cursor_usage, github_productivity)

    return {
        "configured": True,
        "generated_at": github_productivity.get("generated_at"),
        "window_days": window_days,
        "since": github_productivity.get("since"),
        "company": company,
        "by_email": {row["email"]: row for row in individuals},
        "individuals": individuals,
        "top_yield": top_yield,
        "review": review,
        "quadrants": quadrants,
        "quadrant_counts": quadrant_counts,
        "weekly_trend": weekly_trend,
        "medians": {"tokens": med_tokens, "commits": med_commits},
        "identity": {
            "engineer_count": len(canonical_emails),
            "matched_individuals": len(individuals),
            "unmatched_cursor": unmatched_cursor[:20],
            "unmatched_github": unmatched_github[:20],
            "unmatched_cursor_count": len(unmatched_cursor),
            "unmatched_github_count": len(unmatched_github),
        },
        "warnings": warnings,
        "caveats": [
            "Commits and PR counts are a proxy for output, not business value.",
            "Cursor tokens meter model API usage; Tab/autocomplete lines are not fully represented.",
            "Correlation does not imply causation.",
        ],
    }


def compute_ai_correlation_insights(ai: dict[str, Any] | None) -> str:
    """Actionable takeaway for the AI Spend vs. GitHub Output slide."""
    if not ai or not ai.get("configured"):
        return ""
    company = ai.get("company") or {}
    days = int(ai.get("window_days") or 30)
    commits = int(company.get("commits") or 0)
    tokens = int(company.get("total_tokens") or 0)
    corr = company.get("token_commit_correlation")
    weekly = ai.get("weekly_trend") or []

    signals: list[str] = []
    if isinstance(corr, (int, float)):
        r = float(corr)
        if tokens >= 50_000 and commits >= 10 and abs(r) < 0.15:
            signals.append(
                f"Token spend and commits barely move together (r={r:.2f}) in {days}d—"
                "AI usage is not yet translating into a consistent git-output lift; inspect per-engineer yield next."
            )
        elif r >= 0.45 and commits >= 10:
            signals.append(
                f"Tokens and commits rise together (r={r:.2f})—"
                "high-AI weeks coincide with shipping; protect the toolchain rather than cutting spend blindly."
            )
        elif r <= -0.25:
            signals.append(
                f"Tokens and commits diverge (r={r:.2f})—"
                "more model usage without matching git output may signal experimentation, blocked work, or bad prompts."
            )

    if weekly and len(weekly) >= 3:
        recent = weekly[-3:]
        tok_slope = int(recent[-1].get("tokens") or 0) - int(recent[0].get("tokens") or 0)
        com_slope = int(recent[-1].get("commits") or 0) - int(recent[0].get("commits") or 0)
        if tok_slope > 20_000 and com_slope <= 0:
            signals.append(
                "Recent weeks show rising token spend without more commits—"
                "check whether teams are stuck in review, reworking, or using AI off the critical path."
            )

    if not signals:
        signals.append(
            f"{commits} commits vs {_fmt_ai_tokens(tokens)} tokens in {days}d—"
            "treat correlation as diagnostic: compare weekly chart shape before changing AI budget or headcount."
        )
    return signals[0][:320].rstrip()


def compute_productivity_summary_insights(ai: dict[str, Any] | None) -> str:
    """Takeaway for the productivity summary landing slide."""
    if not ai or not ai.get("configured"):
        return ""
    company = ai.get("company") or {}
    days = int(ai.get("window_days") or 30)
    commits = int(company.get("commits") or 0)
    merged = int(company.get("merged_prs") or 0)
    cpt = company.get("commits_per_1k_tokens")
    cpt_txt = f"{cpt:g}" if isinstance(cpt, (int, float)) else "—"
    return (
        f"Engineer-scoped: {commits} commits, {merged} merged PRs, "
        f"{cpt_txt} commits per 1K tokens ({days}d)—use the following slides for trend, yield, and coaching."
    )[:320].rstrip()


def compute_productivity_coaching_insights(ai: dict[str, Any] | None) -> str:
    """Takeaway for the under-yield coaching list slide."""
    if not ai or not ai.get("configured"):
        return ""
    review = ai.get("review") or []
    days = int(ai.get("window_days") or 30)
    if not review:
        return (
            f"No high-token / low-output engineers flagged in {days}d—"
            "revisit after the next sprint if token spend rises without merge cadence."
        )
    names = ", ".join(_short_email_local(r.get("email")) for r in review[:3])
    extra = f" (+{len(review) - 3} more)" if len(review) > 3 else ""
    return (
        f"{len(review)} engineer(s) show high Cursor spend with low git output ({names}{extra})—"
        "focus coaching on PR throughput and targeted prompts, not blanket seat cuts."
    )[:320].rstrip()


def compute_productivity_trend_insights(ai: dict[str, Any] | None) -> str:
    """Takeaway for the weekly productivity trend slide."""
    if not ai or not ai.get("configured"):
        return ""
    weekly = ai.get("weekly_trend") or []
    days = int(ai.get("window_days") or 30)
    if len(weekly) < 2:
        return f"Weekly productivity trend unavailable for {days}d window."
    last = weekly[-1]
    prev = weekly[-2]
    dc = int(last.get("commits") or 0) - int(prev.get("commits") or 0)
    dm = int(last.get("merged_prs") or 0) - int(prev.get("merged_prs") or 0)
    dt = int(last.get("tokens") or 0) - int(prev.get("tokens") or 0)
    if dt > 10_000 and dc <= 0 and dm <= 0:
        return (
            "Latest week: token spend up while commits and merges flat—"
            "investigate review bottlenecks before treating AI adoption as productive."
        )
    if dc > 0 and dm > 0:
        return (
            "Latest week: commits and merges moved up with token spend—"
            "productivity is flowing through git, not just the model API."
        )
    return (
        f"Weekly commits, merges, and tokens over {days}d—"
        "look for weeks where all three rise together before scaling AI seats."
    )[:320].rstrip()


def _short_email_local(email: Any) -> str:
    e = str(email or "").strip()
    return e.split("@", 1)[0] if "@" in e else e


def compute_ai_matrix_insights(ai: dict[str, Any] | None) -> str:
    """Actionable takeaway for the AI Productivity Matrix slide."""
    if not ai or not ai.get("configured"):
        return ""
    days = int(ai.get("window_days") or 30)
    counts = ai.get("quadrant_counts") or {}
    high_low = int(counts.get("high_tokens_low_output") or 0)
    high_high = int(counts.get("high_tokens_high_output") or 0)
    low_high = int(counts.get("low_tokens_high_output") or 0)
    matched = int((ai.get("identity") or {}).get("matched_individuals") or 0)
    review = ai.get("review") or []

    signals: list[str] = []
    if high_low >= 3:
        signals.append(
            f"{high_low} engineers sit in high-token / low-output in {days}d—"
            "prioritize prompt coaching and PR throughput before expanding seat count."
        )
    elif low_high >= 3:
        signals.append(
            f"{low_high} engineers deliver strong output with lighter AI spend—"
            "study their workflow for team playbooks rather than mandating more token usage."
        )
    elif high_high >= matched // 2 and matched >= 4:
        signals.append(
            "Most matched engineers are high-token and high-output—"
            "AI spend is concentrated among people who are also shipping; watch for review bottlenecks next."
        )

    if review:
        names = ", ".join(str(r.get("email") or "").split("@", 1)[0] for r in review[:2])
        signals.append(
            f"Review queue includes {names}—"
            "pair high spend with merge cadence before labeling AI adoption a success."
        )

    if not signals:
        signals.append(
            f"Quadrants split on median tokens and commits ({days}d)—"
            "use high-token/low-output as the coaching short-list, not the headline commit count."
        )
    return signals[0][:320].rstrip()


def _fmt_ai_tokens(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}K"
    return str(value)
