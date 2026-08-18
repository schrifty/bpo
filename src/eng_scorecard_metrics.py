"""LeanDNA metric generators for the Engineering MFR scorecard KPIs.

Board-level AI adoption / impact metrics from Cursor, GitHub, and Jira, scoped to the
Engineering Department (Atlassian ``Dev - *`` teams) where applicable. Generators
return ``{"value": …}`` or ``{"numerator": …, "denominator": …}`` for percentages,
and ``{"error": …}`` on failure so ``metrics-upsert`` fails loud.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger("cortex")

DEFAULT_WINDOW_DAYS = 30
WAU_WINDOW_DAYS = 7
# Omit from eng AI-adoption headcount KPIs (WAU, Tokens/Token Cost per Dev).
# Either historical ("Dev - …") or current ("Dev-…") Atlassian naming.
ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS = frozenset(
    {"dev - data implementation", "dev-data implementation"}
)
WAU_EXCLUDED_DEV_TEAMS = ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS  # backward-compatible alias

ISSUES_SHIPPED_JQL_TEMPLATE = (
    "project = LEAN AND statusCategory = Done AND resolved >= -{days}d"
)
ISSUES_SHIPPED_PREVIOUS_MONTH_JQL = (
    "project = LEAN AND statusCategory = Done "
    'AND resolved >= startOfMonth("-1") AND resolved < startOfMonth()'
)

# New LEAN bugs filed in the window (proxy for release defects until fixVersion/link exists).
BUGS_CREATED_JQL_TEMPLATE = (
    "project = LEAN AND issuetype = Bug AND created >= -{days}d"
)
BUGS_CREATED_PREVIOUS_MONTH_JQL = (
    "project = LEAN AND issuetype = Bug "
    'AND created >= startOfMonth("-1") AND created < startOfMonth()'
)

# % Growth Allocation excludes these from both numerator and denominator.
_GROWTH_EXCLUDED_ISSUE_TYPES = frozenset({"Bug"})
_TECH_DEBT_LABELS = frozenset(
    {"tech-debt", "tech_debt", "technical_debt", "techdebt"}
)


def _engineer_scope(
    jira: Any,
    *,
    timeout: float,
    exclude_teams: frozenset[str] | set[str] | None = None,
) -> dict[str, Any]:
    from .eng_team_roster import build_engineer_audience_scope

    scope = build_engineer_audience_scope(
        jira, timeout=timeout, exclude_teams=exclude_teams
    )
    if scope.get("error"):
        return {"error": f"Engineering Department roster unavailable: {scope['error']}"}
    headcount = int(scope.get("headcount") or 0)
    if headcount <= 0:
        return {"error": "Engineering Department headcount is 0 (no Dev - * Atlassian teams)"}
    emails = {str(e).strip().casefold() for e in (scope.get("emails") or []) if e}
    return {
        "headcount": headcount,
        "emails": emails,
        "excluded_teams": list(scope.get("excluded_teams") or []),
    }


def _previous_calendar_month_bounds(
    as_of: datetime | None = None,
) -> tuple[datetime, datetime, str]:
    """Return the previous completed month as a UTC half-open interval."""
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    end = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    previous = end - timedelta(days=1)
    start = datetime(previous.year, previous.month, 1, tzinfo=timezone.utc)
    return start, end, f"{previous.year:04d}-{previous.month:02d}"


def _jql_half_open_day_range(
    field: str,
    start: datetime,
    end: datetime,
) -> str:
    """Jira date clause for ``field >= start AND field < end`` (UTC calendar days)."""
    return (
        f'{field} >= "{start.strftime("%Y-%m-%d")}" '
        f'AND {field} < "{end.strftime("%Y-%m-%d")}"'
    )


def _scorecard_period(
    *, days: int | None, as_of: datetime | None = None
) -> tuple[datetime, datetime, dict[str, Any]]:
    """Resolve an explicit trailing window or the default previous calendar month."""
    if days is None:
        start, end, month = _previous_calendar_month_bounds(as_of)
        return start, end, {
            "month": month,
            "method": "actual_previous_month",
        }
    window = max(1, int(days))
    end = as_of or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    else:
        end = end.astimezone(timezone.utc)
    return end - timedelta(days=window), end, {"window_days": window}


def _cursor_events(
    client: Any,
    *,
    days: int | None,
    as_of: datetime | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    start, end, _ = _scorecard_period(days=days, as_of=as_of)
    # Cursor's API treats the end as inclusive; remain inside the half-open month.
    request_end = end - timedelta(milliseconds=1) if days is None else end
    try:
        return client.get_usage_events(start, request_end)
    except Exception as e:  # noqa: BLE001 — surface as metric error
        return {"error": f"Cursor usage events unavailable: {e}"}


def _engineer_event_stats(
    events: list[dict[str, Any]],
    *,
    engineer_emails: set[str],
) -> dict[str, Any]:
    from .cursor_usage_report import _event_cost_cents, _event_io_tokens

    active: set[str] = set()
    tokens = 0
    charged_cents = 0.0
    for event in events:
        email = str(event.get("userEmail") or "").strip().casefold()
        if not email or email not in engineer_emails:
            continue
        active.add(email)
        in_t, out_t = _event_io_tokens(event)
        tokens += in_t + out_t
        charged_cents += _event_cost_cents(event)
    inactive = engineer_emails - active
    return {
        "active_users": len(active),
        "active_emails": sorted(active),
        "inactive_emails": sorted(inactive),
        "tokens": tokens,
        "charged_cents": round(charged_cents, 2),
    }


def get_weekly_active_ai_users(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = WAU_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Weekly Active AI Users: active Cursor users ÷ Engineering Department headcount.

    Scope is the Engineering Department — members of Atlassian ``Dev - *`` teams
    (see :func:`eng_team_roster.build_engineer_audience_scope`), excluding
    ``Dev - Data Implementation``. Non-engineering Cursor users are excluded from
    both the numerator and the denominator.
    """
    scope = _engineer_scope(
        jira_client, timeout=timeout, exclude_teams=ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS
    )
    if scope.get("error"):
        return scope
    window = max(1, int(days) or WAU_WINDOW_DAYS)
    events = _cursor_events(cursor_client, days=window)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    active = int(stats["active_users"])
    pct = round(100.0 * active / headcount, 2) if headcount else 0.0
    excluded = scope.get("excluded_teams") or []
    inactive_emails = stats.get("inactive_emails") or []
    logger.info(
        "Weekly Active AI Users: %s / %s Engineering Department (window=%sd%s) = %s%%",
        active,
        headcount,
        window,
        f", excl={excluded}" if excluded else "",
        pct,
    )
    if inactive_emails:
        logger.info("  Inactive (%sd): %s", window, ", ".join(inactive_emails[:10]))
    return {
        "numerator": float(active),
        "denominator": float(headcount),
        "value": pct,
        "active_users": active,
        "headcount": headcount,
        "window_days": window,
        "scope": "engineering_department",
        "excluded_teams": excluded,
        "inactive_emails": inactive_emails,
    }


def get_wau_pct(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int = WAU_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Deprecated alias for :func:`get_weekly_active_ai_users`."""
    return get_weekly_active_ai_users(
        cursor_client, jira_client, days=days, timeout=timeout
    )


def get_tokens_per_dev(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Tokens per Dev: total model tokens ÷ Engineering Department headcount.

    Same engineer scope as Weekly Active AI Users (``Dev - *``, excluding
    ``Dev - Data Implementation``).
    """
    scope = _engineer_scope(
        jira_client, timeout=timeout, exclude_teams=ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS
    )
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days, as_of=as_of)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    tokens = int(stats["tokens"])
    per_dev = round(tokens / headcount, 1) if headcount else 0.0
    excluded = scope.get("excluded_teams") or []
    logger.info(
        "Tokens per Dev: %s tokens / %s Engineering Department = %s%s",
        tokens,
        headcount,
        per_dev,
        f" (excl={excluded})" if excluded else "",
    )
    _, _, period = _scorecard_period(days=days, as_of=as_of)
    return {
        "numerator": float(tokens),
        "denominator": float(headcount),
        "value": per_dev,
        "tokens": tokens,
        "headcount": headcount,
        "scope": "engineering_department",
        "excluded_teams": excluded,
        **period,
    }


def get_token_cost_per_dev(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Token Cost per Dev: engineer Cursor spend (USD) ÷ Engineering Department headcount.

    Same scope and window as :func:`get_tokens_per_dev` (``Dev - *``, excluding
    ``Dev - Data Implementation``); numerator is charged Cursor usage
    (``chargedCents`` / 100) instead of token count.
    """
    scope = _engineer_scope(
        jira_client, timeout=timeout, exclude_teams=ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS
    )
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days, as_of=as_of)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    spend_usd = round(float(stats["charged_cents"]) / 100.0, 4)
    per_dev = round(spend_usd / headcount, 4) if headcount else 0.0
    excluded = scope.get("excluded_teams") or []
    _, _, period = _scorecard_period(days=days, as_of=as_of)
    period_label = (
        f"month={period['month']}"
        if period.get("month")
        else f"window={period['window_days']}d"
    )
    logger.info(
        "Token Cost per Dev: $%s / %s Engineering Department = $%s (%s%s)",
        spend_usd,
        headcount,
        per_dev,
        period_label,
        f", excl={excluded}" if excluded else "",
    )
    return {
        "numerator": spend_usd,
        "denominator": float(headcount),
        "value": per_dev,
        "spend_usd": spend_usd,
        "headcount": headcount,
        "scope": "engineering_department",
        "excluded_teams": excluded,
        **period,
    }


def _load_merged_prs_for_scorecard(
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
    label: str = "merged PRs",
) -> dict[str, Any]:
    """Load merged PRs for scorecard KPIs (Search API, engineer-scoped when mapped).

    Shares :meth:`GitHubClient.list_merged_pulls_since` cache with sibling generators so a
    digest run does not triple Search quota.
    """
    from .engineer_identity_map import load_github_email_aliases
    from .github_client import (
        GitHubClient,
        GitHubError,
        _github_org,
        _github_repos_env,
        _resolve_repo_specs,
        github_configured,
    )
    from .github_productivity_report import _resolve_contributor_login

    if not github_configured():
        return {"error": "GitHub not configured (GITHUB_TOKEN / org)"}

    since, until, period = _scorecard_period(days=days, as_of=as_of)

    engineer_emails: set[str] = set()
    login_to_email: dict[str, str] = {}
    try:
        from .engineer_identity_map import build_engineer_identity_map
        from .jira_client import get_shared_jira_client

        identity = build_engineer_identity_map(jira_client=get_shared_jira_client())
        if identity.get("configured"):
            engineer_emails = {
                str(e).strip().casefold()
                for e in (identity.get("canonical_emails") or [])
                if e
            }
            login_to_email = {
                str(k).strip().lower(): str(v).strip().casefold()
                for k, v in (identity.get("login_to_email") or {}).items()
                if k and v
            }
    except Exception as e:  # noqa: BLE001
        logger.warning("%s: identity map unavailable (%s); using org totals", label, e)

    email_aliases, _ = load_github_email_aliases()

    try:
        gh = GitHubClient()
        repo_specs = _resolve_repo_specs(
            org=_github_org(),
            repos_env=_github_repos_env(),
            client=gh,
        )
    except GitHubError as e:
        return {"error": str(e)}

    pulls: list[dict[str, Any]] = []
    for owner, repo in repo_specs:
        try:
            if days is None:
                repo_pulls = gh.list_merged_pulls_since(
                    owner, repo, since=since, until=until
                )
            else:
                repo_pulls = gh.list_merged_pulls_since(owner, repo, since=since)
        except GitHubError as e:
            return {"error": f"GitHub merged PR list failed for {owner}/{repo}: {e}"}
        for pull in repo_pulls:
            if engineer_emails:
                user = pull.get("user") if isinstance(pull.get("user"), dict) else {}
                login = str(user.get("login") or "").strip().lower()
                canonical = _resolve_contributor_login(
                    login,
                    login_to_email=login_to_email,
                    email_aliases=email_aliases,
                    engineer_emails=engineer_emails,
                )
                if not canonical:
                    continue
            # Annotate without mutating the shared Search cache entry.
            annotated = dict(pull)
            annotated["_owner"] = owner
            annotated["_repo"] = repo
            pulls.append(annotated)

    return {
        "pulls": pulls,
        "scope": "engineers" if engineer_emails else "org",
        "client": gh,
        **period,
    }


def get_prs_merged(
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """PRs Merged: engineer-scoped merged PRs in the window (falls back to org total)."""
    inv = _load_merged_prs_for_scorecard(
        days=days, as_of=as_of, timeout=timeout, label="PRs Merged"
    )
    if inv.get("error"):
        return inv
    merged = len(inv.get("pulls") or [])
    scope = str(inv.get("scope") or "org")
    period_label = (
        f"month={inv['month']}"
        if inv.get("month")
        else f"window={inv.get('window_days')}d"
    )
    logger.info("PRs Merged: %s (%s, %s)", merged, scope, period_label)
    result = {
        "value": merged,
        "scope": scope,
    }
    if inv.get("month"):
        result.update(month=inv["month"], method=inv.get("method"))
    else:
        result["window_days"] = inv.get("window_days")
    return result


# Cursor attribution (assisted) vs agent-authored (automated) PR signals.
_AI_ASSISTED_PR_MARKERS = (
    "made with cursor",
    "made-with: cursor",
    "co-authored-by: cursor",
    "cursoragent@",
    "generated with cursor",
    "cursor agent",
)
_AI_ASSISTED_COMMIT_MARKERS = (
    "made with cursor",
    "made-with: cursor",
    "co-authored-by: cursor",
    "cursoragent@",
    "generated with cursor",
)
_AI_AUTOMATED_PR_MARKERS = (
    "generated with cursor",
    "created by cursor",
    "cursoragent@",
)
_AI_AUTOMATED_COMMIT_MARKERS = (
    "generated with cursor",
    "created by cursor",
)
_AI_ASSISTED_PR_LOGINS = frozenset({"cursoragent", "cursor", "cursor[bot]"})
_AI_ASSISTED_PR_LABEL_NEEDLES = ("ai-assisted", "cursor-agent", "cursor")
_AI_AUTOMATED_PR_LABEL_NEEDLES = ("ai-automated", "ai-generated", "cursor-agent")


def _commit_messages_blob(commits: list[dict[str, Any]] | None) -> str:
    if not commits:
        return ""
    parts: list[str] = []
    for commit in commits:
        inner = commit.get("commit") if isinstance(commit.get("commit"), dict) else {}
        msg = inner.get("message") if isinstance(inner, dict) else None
        if msg:
            parts.append(str(msg))
    return "\n".join(parts).lower()


def commits_have_cursor_trailer(commits: list[dict[str, Any]] | None) -> bool:
    """True when any commit message carries a Cursor attribution trailer."""
    blob = _commit_messages_blob(commits)
    return bool(blob) and any(marker in blob for marker in _AI_ASSISTED_COMMIT_MARKERS)


def commits_have_ai_automated_signal(commits: list[dict[str, Any]] | None) -> bool:
    """True when any commit is agent-authored or has an automated Cursor marker.

    Stricter than :func:`commits_have_cursor_trailer`: ``Made-with: Cursor`` /
    ``Co-authored-by: Cursor`` alone do not count (those are assisted). Agent
    ``author.login`` or ``Generated with Cursor`` / ``Created by Cursor`` in the
    commit message do.
    """
    if not commits:
        return False
    for commit in commits:
        author = commit.get("author") if isinstance(commit.get("author"), dict) else {}
        login = str(author.get("login") or "").strip().lower()
        if login in _AI_ASSISTED_PR_LOGINS:
            return True
    blob = _commit_messages_blob(commits)
    return bool(blob) and any(marker in blob for marker in _AI_AUTOMATED_COMMIT_MARKERS)


def pr_is_ai_assisted(
    pull: dict[str, Any],
    *,
    commits: list[dict[str, Any]] | None = None,
) -> bool:
    """True when a PR shows Cursor attribution (PR body/label/author or commit trailer).

    When *commits* is provided, also treat the PR as assisted if any commit message
    contains a Cursor trailer (``Co-authored-by: Cursor``, ``Made-with: Cursor``, etc.).
    """
    if pr_is_ai_automated(pull, commits=commits):
        return True
    for lab in pull.get("labels") or []:
        name = str(lab.get("name") if isinstance(lab, dict) else lab or "").strip().lower()
        if name and any(needle in name for needle in _AI_ASSISTED_PR_LABEL_NEEDLES):
            return True
    blob = f"{pull.get('title') or ''}\n{pull.get('body') or ''}".lower()
    if any(marker in blob for marker in _AI_ASSISTED_PR_MARKERS):
        return True
    return commits_have_cursor_trailer(commits)


def pr_is_ai_automated(
    pull: dict[str, Any],
    *,
    commits: list[dict[str, Any]] | None = None,
) -> bool:
    """True when a PR was generated by an AI agent (PR signals or commit-level agent markers).

    When *commits* is provided, also treat the PR as automated if a commit is authored
    by ``cursoragent`` / Cursor bot or the message has an automated marker
    (``Generated with Cursor``, ``cursoragent@``, etc.). Plain ``Made-with: Cursor``
    alone is assisted, not automated.
    """
    user = pull.get("user") if isinstance(pull.get("user"), dict) else {}
    login = str(user.get("login") or "").strip().lower()
    if login in _AI_ASSISTED_PR_LOGINS:
        return True
    for lab in pull.get("labels") or []:
        name = str(lab.get("name") if isinstance(lab, dict) else lab or "").strip().lower()
        if name and any(needle in name for needle in _AI_AUTOMATED_PR_LABEL_NEEDLES):
            return True
    blob = f"{pull.get('title') or ''}\n{pull.get('body') or ''}".lower()
    if any(marker in blob for marker in _AI_AUTOMATED_PR_MARKERS):
        return True
    return commits_have_ai_automated_signal(commits)


def _fetch_pull_commits(
    client: Any,
    pull: dict[str, Any],
) -> list[dict[str, Any]] | dict[str, Any]:
    """Load PR commits for AI PR classification; return ``{"error": ...}`` on failure."""
    from .github_client import GitHubError

    owner = str(pull.get("_owner") or "").strip()
    repo = str(pull.get("_repo") or "").strip()
    number = pull.get("number")
    if not owner or not repo or number is None:
        return []
    try:
        return client.list_pull_commits(owner, repo, int(number))
    except GitHubError as e:
        return {
            "error": (
                f"GitHub PR commits failed for {owner}/{repo}#{number}: {e}"
            )
        }
    except Exception as e:  # noqa: BLE001
        return {
            "error": (
                f"GitHub PR commits failed for {owner}/{repo}#{number}: {e}"
            )
        }


def _merged_prs_ai_pct(
    *,
    mode: str,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Shared counter: AI-classified merged PRs ÷ total merged PRs.

    *mode* is ``assisted`` (Cursor attribution, including commit trailers) or
    ``automated`` (AI-generated PR/commit agent signals).
    """
    label = "% AI-Assisted PRs" if mode == "assisted" else "% AI-Automated PRs"
    classify = pr_is_ai_assisted if mode == "assisted" else pr_is_ai_automated

    inv = _load_merged_prs_for_scorecard(days=days, timeout=timeout, label=label)
    if inv.get("error"):
        return inv

    pulls = inv.get("pulls") or []
    total = len(pulls)
    window = int(inv.get("window_days") or days)
    scope = str(inv.get("scope") or "org")
    client = inv.get("client")

    if total <= 0:
        return {
            "error": (
                f"no merged PRs in last {window}d "
                f"({scope}) for {label}"
            )
        }

    matched = 0
    commit_checks = 0
    for pull in pulls:
        # PR-level signals first; only then fetch commits (rate-limit friendly).
        if classify(pull):
            matched += 1
            continue
        if client is None:
            continue
        commits_or_err = _fetch_pull_commits(client, pull)
        if isinstance(commits_or_err, dict) and commits_or_err.get("error"):
            return commits_or_err
        commit_checks += 1
        commits = commits_or_err if isinstance(commits_or_err, list) else None
        if classify(pull, commits=commits):
            matched += 1

    pct = round(100.0 * matched / total, 2)
    logger.info(
        "%s: %s / %s (%s, window=%sd, commit_checks=%s) = %s%%",
        label,
        matched,
        total,
        scope,
        window,
        commit_checks,
        pct,
    )
    return {
        "numerator": float(matched),
        "denominator": float(total),
        "value": pct,
        "matched_prs": matched,
        "total_prs": total,
        "scope": scope,
        "mode": mode,
        "window_days": window,
        "commit_trailer_checks": commit_checks,
    }


def get_ai_assisted_prs_pct(
    cursor_client: Any | None = None,
    jira_client: Any | None = None,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """% AI-Assisted PRs: AI Code Share % applied to merged PR volume.

    Commit-message / trailer attribution under-counts assisted work, so this KPI
    uses Cursor's AI share of committed code as a proxy: the reported percentage
    is AI Code Share, and ``matched_prs`` is ``round(total_prs × share / 100)``.
    """
    from .cursor_client import get_shared_cursor_client
    from .jira_client import get_shared_jira_client

    cursor = cursor_client or get_shared_cursor_client()
    jira = jira_client or get_shared_jira_client()

    share = get_ai_code_share(
        cursor, jira, days=days, as_of=as_of, timeout=timeout
    )
    if share.get("error"):
        return {
            "error": f"% AI-Assisted PRs unavailable (AI Code Share failed): {share['error']}"
        }

    inv = _load_merged_prs_for_scorecard(
        days=days, as_of=as_of, timeout=timeout, label="% AI-Assisted PRs"
    )
    if inv.get("error"):
        return inv

    pulls = inv.get("pulls") or []
    total = len(pulls)
    scope = str(inv.get("scope") or "org")
    if total <= 0:
        period_label = (
            f"previous calendar month {inv['month']}"
            if inv.get("month")
            else f"last {inv.get('window_days')}d"
        )
        return {
            "error": (
                f"no merged PRs in {period_label} ({scope}) for % AI-Assisted PRs"
            )
        }

    share_pct = float(share["value"])
    estimated = int(round(total * share_pct / 100.0))
    logger.info(
        "%% AI-Assisted PRs: ~%s / %s PRs via AI Code Share %s%% "
        "(%s lines AI / %s total, %s, period=%s)",
        estimated,
        total,
        share_pct,
        share.get("ai_lines"),
        share.get("total_lines"),
        scope,
        inv.get("month") or f"{inv.get('window_days')}d",
    )
    result = {
        "numerator": float(estimated),
        "denominator": float(total),
        "value": share_pct,
        "matched_prs": estimated,
        "total_prs": total,
        "scope": scope,
        "mode": "ai_code_share_proxy",
        "ai_code_share_pct": share_pct,
        "ai_lines": share.get("ai_lines"),
        "total_lines": share.get("total_lines"),
    }
    if inv.get("month"):
        result.update(month=inv["month"], method=inv.get("method"))
    else:
        result["window_days"] = inv.get("window_days")
    return result


def get_ai_automated_prs_pct(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Deprecated: prefer :func:`get_ai_code_share`. Agent-authored PR share (trailer-based)."""
    return _merged_prs_ai_pct(mode="automated", days=days, timeout=timeout)


def get_ai_assisted_automated_prs_pct(
    cursor_client: Any | None = None,
    jira_client: Any | None = None,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Deprecated alias for :func:`get_ai_assisted_prs_pct`."""
    return get_ai_assisted_prs_pct(
        cursor_client, jira_client, days=days, as_of=as_of, timeout=timeout
    )


def get_ai_code_share(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Code Share: Cursor-attributed lines ÷ total changed lines in commits.

    Uses Cursor ``/teams/daily-usage-data`` git line attribution for Engineering
    Department members (``Dev - *``, excluding Data Implementation):

    ``(acceptedLinesAdded + acceptedLinesDeleted)
      ÷ (totalLinesAdded + totalLinesDeleted)``

    This matches Cursor's "AI Share of Committed Code" signal (Tab + Agent lines
    that survived into commits ÷ all lines changed in those commits). True
    merged-PR SHA joining needs the Enterprise AI Code Tracking API, which this
    team key cannot access yet.
    """
    scope = _engineer_scope(
        jira_client, timeout=timeout, exclude_teams=ENG_AI_ADOPTION_EXCLUDED_DEV_TEAMS
    )
    if scope.get("error"):
        return scope

    start, end, period = _scorecard_period(days=days, as_of=as_of)
    request_end = end - timedelta(milliseconds=1) if days is None else end
    try:
        rows = cursor_client.get_daily_usage(start, request_end, all_members=True)
    except Exception as e:  # noqa: BLE001
        return {"error": f"Cursor daily-usage unavailable for AI Code Share: {e}"}

    emails = scope["emails"]
    ai_lines = 0
    total_lines = 0
    matched_rows = 0
    for row in rows or []:
        email = str(row.get("email") or "").strip().casefold()
        if not email or email not in emails:
            continue
        matched_rows += 1
        ai_lines += int(row.get("acceptedLinesAdded") or 0) + int(
            row.get("acceptedLinesDeleted") or 0
        )
        total_lines += int(row.get("totalLinesAdded") or 0) + int(
            row.get("totalLinesDeleted") or 0
        )

    if total_lines <= 0:
        return {
            "error": (
                "AI Code Share denominator is 0 — no Cursor totalLinesAdded/"
                "Deleted for Engineering Department in "
                f"{period.get('month') or str(period.get('window_days')) + 'd'}"
            )
        }

    pct = round(100.0 * ai_lines / total_lines, 2)
    excluded = scope.get("excluded_teams") or []
    logger.info(
        "AI Code Share: %s / %s lines = %s%% "
        "(eng daily-usage, period=%s%s, rows=%s)",
        ai_lines,
        total_lines,
        pct,
        period.get("month") or f"{period.get('window_days')}d",
        f", excl={excluded}" if excluded else "",
        matched_rows,
    )
    return {
        "numerator": float(ai_lines),
        "denominator": float(total_lines),
        "value": pct,
        "ai_lines": ai_lines,
        "total_lines": total_lines,
        "scope": "engineering_department",
        "source": "cursor_daily_usage",
        "excluded_teams": excluded,
        **period,
    }


def get_issues_shipped(
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """Issues Shipped: actual LEAN Done issues for the previous calendar month.

    Default (digest/upsert): count issues resolved from the start of the previous
    month up to, but not including, the start of the current month. The result is
    always an actual count; it is never extrapolated.

    Pass ``days`` only for an explicit backward-compatible trailing-window count.
    """
    if days is not None:
        window = max(1, int(days))
        jql = ISSUES_SHIPPED_JQL_TEMPLATE.format(days=window)
        count = jira_client.jql_match_count(
            jql,
            data_description=f"Issues Shipped (LEAN Done, resolved last {window}d)",
        )
        if count is None:
            return {
                "error": (
                    "Jira count unavailable for Issues Shipped "
                    "(POST /rest/api/3/search/approximate-count returned no count)"
                )
            }
        raw = int(count)
        logger.info("Issues Shipped: %s (window=%sd)", raw, window)
        return {"value": raw, "jql": jql, "window_days": window}

    start, end, month_key = _previous_calendar_month_bounds(as_of)
    jql = (
        "project = LEAN AND statusCategory = Done AND "
        + _jql_half_open_day_range("resolved", start, end)
    )
    count = jira_client.jql_match_count(
        jql,
        data_description=(
            f"Issues Shipped (LEAN Done, resolved previous calendar month {month_key})"
        ),
    )
    if count is None:
        return {
            "error": (
                "Jira count unavailable for Issues Shipped "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            )
        }
    value = int(count)

    logger.info(
        "Issues Shipped: %s actual for previous calendar month %s",
        value,
        month_key,
    )
    return {
        "value": value,
        "jql": jql,
        "month": month_key,
        "method": "actual_previous_month",
    }


def get_defects_per_100_issues(
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Defects per 100 Issues: LEAN bugs created per 100 issues shipped.

    Computed as ``(bugs_created ÷ issues_shipped) × 100`` so the scorecard value is
    **bugs per 100 shipped issues** (a percentage-scale rate), not a 0–1 ratio.
    Example: 57 bugs and 281 shipped → 20.28 means ~20 bugs were filed for every
    100 issues completed.

    Proxy for release-defect rate until a defect→release link (fixVersion / post-deploy
    window) is available. Numerator is bugs *created* in the window; denominator is the
    same Issues Shipped count as :func:`get_issues_shipped`. Lower is better.
    """
    if days is None:
        start, end, month_key = _previous_calendar_month_bounds(as_of)
        bugs_jql = (
            "project = LEAN AND issuetype = Bug AND "
            + _jql_half_open_day_range("created", start, end)
        )
        period = {"month": month_key, "method": "actual_previous_month"}
        description = f"LEAN Bugs created previous calendar month {month_key}"
    else:
        window = max(1, int(days))
        bugs_jql = BUGS_CREATED_JQL_TEMPLATE.format(days=window)
        period = {"window_days": window}
        description = f"LEAN Bugs created last {window}d"
    bugs_count = jira_client.jql_match_count(
        bugs_jql,
        data_description=f"{description} (Defects per 100 Issues)",
    )
    if bugs_count is None:
        return {
            "error": (
                "Jira count unavailable for Defects per 100 Issues numerator "
                "(LEAN Bugs created; approximate-count returned no count)"
            )
        }

    shipped = get_issues_shipped(
        jira_client, days=days, as_of=as_of, timeout=timeout
    )
    if shipped.get("error"):
        return {
            "error": (
                "Defects per 100 Issues denominator failed: "
                f"{shipped['error']}"
            )
        }
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {
            "error": (
                "Issues Shipped is 0 — cannot compute Defects per 100 Issues"
            )
        }

    bugs = int(bugs_count)
    rate = round(100.0 * bugs / issues, 2)
    logger.info(
        "Defects per 100 Issues: %s bugs / %s issues shipped = %s (period=%s)",
        bugs,
        issues,
        rate,
        period.get("month") or f"{period.get('window_days')}d",
    )
    return {
        "numerator": float(bugs),
        "denominator": float(issues),
        "value": rate,
        "bugs_created": bugs,
        "issues_shipped": issues,
        "bugs_jql": bugs_jql,
        "shipped_jql": shipped.get("jql"),
        **period,
    }


# Backward-compatible alias.
get_defect_introduction_rate = get_defects_per_100_issues


def get_growth_allocation_pct(
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """% Growth Allocation: planned/roadmap closed work ÷ eligible closed work.

    Bugs and tech-debt-labeled issues are excluded from both the numerator and
    denominator so growth share is measured against non-bug, non-debt delivery.
    """
    from .jira_client import compute_eng_work_split

    if days is None:
        start, end, month_key = _previous_calendar_month_bounds(as_of)
        period = {"month": month_key, "method": "actual_previous_month"}
        date_clause = _jql_half_open_day_range("resolved", start, end)
        period_label = f"previous calendar month {month_key}"
    else:
        window = max(1, int(days))
        period = {"window_days": window}
        date_clause = f"resolved >= -{window}d"
        period_label = f"last {window}d"
    fields = ["summary", "status", "issuetype", "labels", "resolved"]
    try:
        closed_raw = jira_client._search(
            f"project = LEAN AND statusCategory = Done AND {date_clause} "
            "ORDER BY resolved DESC",
            max_results=2000,
            fields=fields,
            data_description=f"LEAN Done issues for growth allocation ({period_label})",
        )
    except Exception as e:  # noqa: BLE001
        return {"error": f"Jira search unavailable for % Growth Allocation: {e}"}

    closed = []
    excluded_bugs = 0
    excluded_tech_debt = 0
    for issue in closed_raw or []:
        f = issue.get("fields") or {}
        ticket = {
            "type": (f.get("issuetype") or {}).get("name", ""),
            "labels": f.get("labels") or [],
        }
        if (ticket.get("type") or "") in _GROWTH_EXCLUDED_ISSUE_TYPES:
            excluded_bugs += 1
            continue
        labels = {str(label).lower() for label in (ticket.get("labels") or [])}
        if labels & _TECH_DEBT_LABELS:
            excluded_tech_debt += 1
            continue
        closed.append(ticket)

    split = compute_eng_work_split([], closed)
    closed_split = split.get("closed") or {}
    planned = int(closed_split.get("planned") or 0)
    total = int(closed_split.get("total") or 0)
    if total <= 0:
        return {
            "error": (
                f"no eligible LEAN Done issues in {period_label} for growth "
                "allocation (after excluding bugs and tech debt)"
            )
        }
    logger.info(
        "%% Growth Allocation: planned=%s / total=%s "
        "(excluded bugs=%s tech_debt=%s, period=%s)",
        planned,
        total,
        excluded_bugs,
        excluded_tech_debt,
        period.get("month") or f"{period.get('window_days')}d",
    )
    return {
        "numerator": float(planned),
        "denominator": float(total),
        "planned": planned,
        "unplanned": int(closed_split.get("unplanned") or 0),
        "excluded_bugs": excluded_bugs,
        "excluded_tech_debt": excluded_tech_debt,
        **period,
    }


def get_ai_spend_pct(
    cursor_client: Any,
    *,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Spend %: monthly AI spend (USD) ÷ monthly engineering spend (USD).

    Numerator is projected calendar-month Cursor spend (:func:`get_monthly_ai_spend`)
    for the month containing *as_of* (default: now). Denominator is
    ``CORTEX_ENGINEERING_MONTHLY_SPEND_USD`` (finance headcount/opex, excluding AI
    tooling). Fails loud when unset/invalid or Cursor spend fails.
    """
    import os

    from .config import CORTEX_ENGINEERING_MONTHLY_SPEND_USD
    from .cursor_ai_usage_metrics import get_monthly_ai_spend

    eng_spend = CORTEX_ENGINEERING_MONTHLY_SPEND_USD
    if eng_spend is None:
        raw = (os.environ.get("CORTEX_ENGINEERING_MONTHLY_SPEND_USD") or "").strip()
        if raw:
            return {
                "error": (
                    "CORTEX_ENGINEERING_MONTHLY_SPEND_USD is not a valid number "
                    f"(got {raw!r})"
                )
            }
        return {
            "error": (
                "CORTEX_ENGINEERING_MONTHLY_SPEND_USD is not set — required for AI Spend % "
                "(total monthly engineering spend in USD)"
            )
        }
    if float(eng_spend) <= 0:
        return {
            "error": (
                f"CORTEX_ENGINEERING_MONTHLY_SPEND_USD must be > 0 (got {eng_spend})"
            )
        }

    ai = get_monthly_ai_spend(cursor_client, as_of=as_of, timeout=timeout)
    if ai.get("error"):
        return ai
    ai_usd = float(ai.get("value") if ai.get("value") is not None else ai.get("spend_usd") or 0)
    eng_usd = float(eng_spend)
    pct = round(100.0 * ai_usd / eng_usd, 2)
    logger.info(
        "AI Spend %%: $%s AI / $%s engineering = %s%%",
        ai_usd,
        eng_usd,
        pct,
    )
    return {
        "numerator": ai_usd,
        "denominator": eng_usd,
        "value": pct,
        "ai_spend_usd": ai_usd,
        "engineering_spend_usd": eng_usd,
        "scope": "monthly",
    }


def get_ai_spend_per_issue(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Spend / Issue: engineer Cursor spend (USD) ÷ issues shipped."""
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days, as_of=as_of)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    shipped = get_issues_shipped(
        jira_client, days=days, as_of=as_of, timeout=timeout
    )
    if shipped.get("error"):
        return shipped
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {"error": "Issues Shipped is 0 — cannot compute AI Spend / Issue"}
    spend_usd = round(float(stats["charged_cents"]) / 100.0, 4)
    per_issue = round(spend_usd / issues, 4)
    _, _, period = _scorecard_period(days=days, as_of=as_of)
    logger.info(
        "AI Spend / Issue: $%s / %s issues = $%s (period=%s)",
        spend_usd,
        issues,
        per_issue,
        period.get("month") or f"{period.get('window_days')}d",
    )
    return {
        "numerator": spend_usd,
        "denominator": float(issues),
        "value": per_issue,
        "spend_usd": spend_usd,
        "issues_shipped": issues,
        **period,
    }


def _engineering_headcount_monthly_usd() -> dict[str, Any]:
    """Finance-configured monthly engineering headcount/opex (USD). Fail loud if unset."""
    import os

    from .config import CORTEX_ENGINEERING_MONTHLY_SPEND_USD

    eng_spend = CORTEX_ENGINEERING_MONTHLY_SPEND_USD
    if eng_spend is None:
        raw = (os.environ.get("CORTEX_ENGINEERING_MONTHLY_SPEND_USD") or "").strip()
        if raw:
            return {
                "error": (
                    "CORTEX_ENGINEERING_MONTHLY_SPEND_USD is not a valid number "
                    f"(got {raw!r})"
                )
            }
        return {
            "error": (
                "CORTEX_ENGINEERING_MONTHLY_SPEND_USD is not set — required for "
                "Headcount + AI Spend / Issue (monthly engineering headcount/opex USD, "
                "excluding AI tooling)"
            )
        }
    if float(eng_spend) <= 0:
        return {
            "error": (
                f"CORTEX_ENGINEERING_MONTHLY_SPEND_USD must be > 0 (got {eng_spend})"
            )
        }
    return {"value": float(eng_spend)}


def get_headcount_plus_ai_spend_per_issue(
    cursor_client: Any,
    jira_client: Any,
    *,
    days: int | None = None,
    as_of: datetime | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Headcount + AI Spend / Issue: (prorated headcount cost + AI spend) ÷ issues shipped.

    Headcount cost is ``CORTEX_ENGINEERING_MONTHLY_SPEND_USD`` (monthly engineering
    headcount/opex, excluding AI tooling). The default previous-month calculation
    uses the full monthly amount; explicit trailing-day windows remain prorated.
    AI spend is engineer-scoped Cursor charged USD over the same period.
    """
    hc = _engineering_headcount_monthly_usd()
    if hc.get("error"):
        return hc
    monthly_spend = float(hc["value"])
    _, _, period = _scorecard_period(days=days, as_of=as_of)
    if days is None:
        headcount_usd = round(monthly_spend, 4)
    else:
        window_days = max(1, int(days))
        headcount_usd = round(monthly_spend * (window_days / 30.0), 4)

    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days, as_of=as_of)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    ai_usd = round(float(stats["charged_cents"]) / 100.0, 4)

    shipped = get_issues_shipped(
        jira_client, days=days, as_of=as_of, timeout=timeout
    )
    if shipped.get("error"):
        return shipped
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {
            "error": (
                "Issues Shipped is 0 — cannot compute Headcount + AI Spend / Issue"
            )
        }

    total_usd = round(headcount_usd + ai_usd, 4)
    per_issue = round(total_usd / issues, 4)
    logger.info(
        "Headcount + AI Spend / Issue: ($%s HC + $%s Cursor) / %s issues = $%s "
        "(period=%s, monthly HC=$%s)",
        headcount_usd,
        ai_usd,
        issues,
        per_issue,
        period.get("month") or f"{period.get('window_days')}d",
        monthly_spend,
    )
    return {
        "numerator": total_usd,
        "denominator": float(issues),
        "value": per_issue,
        "headcount_usd": headcount_usd,
        "ai_spend_usd": ai_usd,
        "total_usd": total_usd,
        "issues_shipped": issues,
        "engineering_monthly_spend_usd": monthly_spend,
        **period,
    }
