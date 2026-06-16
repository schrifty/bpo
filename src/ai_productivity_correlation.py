"""Join GitHub engineering output with Cursor token spend (company + individual)."""

from __future__ import annotations

import logging
import statistics
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
        return round(statistics.correlation(xs, ys), 4)
    except (statistics.StatisticsError, ValueError):
        return None


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

    return {
        "configured": True,
        "generated_at": github_productivity.get("generated_at"),
        "window_days": window_days,
        "since": github_productivity.get("since"),
        "company": company,
        "by_email": {row["email"]: row for row in individuals},
        "individuals": individuals,
        "quadrants": quadrants,
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
