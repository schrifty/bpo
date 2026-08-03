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
ISSUES_SHIPPED_WINDOW_DAYS = 84  # 12 weeks

ISSUES_SHIPPED_JQL_TEMPLATE = (
    "project = LEAN AND statusCategory = Done AND resolved >= -{days}d"
)

# New LEAN bugs filed in the window (proxy for release defects until fixVersion/link exists).
BUGS_CREATED_JQL_TEMPLATE = (
    "project = LEAN AND issuetype = Bug AND created >= -{days}d"
)


def _engineer_scope(jira: Any, *, timeout: float) -> dict[str, Any]:
    from .eng_team_roster import build_engineer_audience_scope

    scope = build_engineer_audience_scope(jira, timeout=timeout)
    if scope.get("error"):
        return {"error": f"Engineering Department roster unavailable: {scope['error']}"}
    headcount = int(scope.get("headcount") or 0)
    if headcount <= 0:
        return {"error": "Engineering Department headcount is 0 (no Dev - * Atlassian teams)"}
    emails = {str(e).strip().casefold() for e in (scope.get("emails") or []) if e}
    return {"headcount": headcount, "emails": emails}


def _cursor_events(client: Any, *, days: int) -> list[dict[str, Any]] | dict[str, Any]:
    window = max(1, int(days))
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=window)
    try:
        return client.get_usage_events(start, end)
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
    return {
        "active_users": len(active),
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

    Scope is the Engineering Department only — members of Atlassian ``Dev - *`` teams
    (see :func:`eng_team_roster.build_engineer_audience_scope`). Non-engineering Cursor
    users are excluded from both the numerator and the denominator.
    """
    scope = _engineer_scope(jira_client, timeout=timeout)
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
    logger.info(
        "Weekly Active AI Users: %s / %s Engineering Department (window=%sd) = %s%%",
        active,
        headcount,
        window,
        pct,
    )
    return {
        "numerator": float(active),
        "denominator": float(headcount),
        "value": pct,
        "active_users": active,
        "headcount": headcount,
        "window_days": window,
        "scope": "engineering_department",
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
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Tokens per Dev: total model tokens ÷ Engineering Department headcount."""
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    headcount = int(scope["headcount"])
    tokens = int(stats["tokens"])
    per_dev = round(tokens / headcount, 1) if headcount else 0.0
    logger.info(
        "Tokens per Dev: %s tokens / %s Engineering Department = %s",
        tokens,
        headcount,
        per_dev,
    )
    return {
        "numerator": float(tokens),
        "denominator": float(headcount),
        "value": per_dev,
        "tokens": tokens,
        "headcount": headcount,
        "window_days": max(1, int(days)),
        "scope": "engineering_department",
    }


def _load_merged_prs_for_scorecard(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
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

    window = max(1, int(days))
    since = datetime.now(timezone.utc) - timedelta(days=window)

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
        "window_days": window,
        "client": gh,
    }


def get_prs_merged(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """PRs Merged: engineer-scoped merged PRs in the window (falls back to org total)."""
    inv = _load_merged_prs_for_scorecard(days=days, timeout=timeout, label="PRs Merged")
    if inv.get("error"):
        return inv
    merged = len(inv.get("pulls") or [])
    scope = str(inv.get("scope") or "org")
    window = int(inv.get("window_days") or days)
    logger.info("PRs Merged: %s (%s, window=%sd)", merged, scope, window)
    return {
        "value": merged,
        "scope": scope,
        "window_days": window,
    }


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
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """% AI-Assisted PRs: Cursor attribution on PR body/labels/author or commit trailers ÷ total."""
    return _merged_prs_ai_pct(mode="assisted", days=days, timeout=timeout)


def get_ai_automated_prs_pct(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """% AI-Automated PRs: agent-authored PR/commit signals ÷ total merged PRs."""
    return _merged_prs_ai_pct(mode="automated", days=days, timeout=timeout)


def get_ai_assisted_automated_prs_pct(
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Deprecated alias for :func:`get_ai_assisted_prs_pct`."""
    return get_ai_assisted_prs_pct(days=days, timeout=timeout)


def get_issues_shipped(
    jira_client: Any,
    *,
    days: int = ISSUES_SHIPPED_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """Issues Shipped: LEAN issues completed (Done) with resolved date in the window.

    Default window is 12 weeks (:data:`ISSUES_SHIPPED_WINDOW_DAYS`). The digest/upsert
    invoker always uses that period regardless of ``--days``.
    """
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
    logger.info("Issues Shipped: %s (window=%sd)", count, window)
    return {"value": int(count), "jql": jql, "window_days": window}


def get_defect_introduction_rate(
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Defect Introduction Rate: LEAN bugs created ÷ Issues Shipped × 100.

    Proxy for release-defect rate until a defect→release link (fixVersion / post-deploy
    window) is available. Numerator is bugs *created* in the window; denominator is the
    same Issues Shipped count as :func:`get_issues_shipped`. Lower is better.
    """
    window = max(1, int(days))
    bugs_jql = BUGS_CREATED_JQL_TEMPLATE.format(days=window)
    bugs_count = jira_client.jql_match_count(
        bugs_jql,
        data_description=f"LEAN Bugs created last {window}d (Defect Introduction Rate)",
    )
    if bugs_count is None:
        return {
            "error": (
                "Jira count unavailable for Defect Introduction Rate numerator "
                "(LEAN Bugs created; approximate-count returned no count)"
            )
        }

    shipped = get_issues_shipped(jira_client, days=window, timeout=timeout)
    if shipped.get("error"):
        return {
            "error": (
                "Defect Introduction Rate denominator failed: "
                f"{shipped['error']}"
            )
        }
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {
            "error": (
                "Issues Shipped is 0 — cannot compute Defect Introduction Rate"
            )
        }

    bugs = int(bugs_count)
    rate = round(100.0 * bugs / issues, 2)
    logger.info(
        "Defect Introduction Rate: %s bugs / %s issues shipped = %s%% (window=%sd)",
        bugs,
        issues,
        rate,
        window,
    )
    return {
        "numerator": float(bugs),
        "denominator": float(issues),
        "value": rate,
        "bugs_created": bugs,
        "issues_shipped": issues,
        "bugs_jql": bugs_jql,
        "shipped_jql": shipped.get("jql"),
        "window_days": window,
    }


def get_growth_allocation_pct(
    jira_client: Any,
    *,
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """% Growth Allocation: planned/roadmap closed work ÷ total closed engineering work."""
    from .jira_client import compute_eng_work_split

    window = max(1, int(days))
    fields = ["summary", "status", "issuetype", "labels", "resolved"]
    try:
        closed_raw = jira_client._search(
            f"project = LEAN AND statusCategory = Done AND resolved >= -{window}d "
            "ORDER BY resolved DESC",
            max_results=2000,
            fields=fields,
            data_description=f"LEAN Done issues for growth allocation ({window}d)",
        )
    except Exception as e:  # noqa: BLE001
        return {"error": f"Jira search unavailable for % Growth Allocation: {e}"}

    closed = []
    for issue in closed_raw or []:
        f = issue.get("fields") or {}
        closed.append(
            {
                "type": (f.get("issuetype") or {}).get("name", ""),
                "labels": f.get("labels") or [],
            }
        )
    split = compute_eng_work_split([], closed)
    closed_split = split.get("closed") or {}
    planned = int(closed_split.get("planned") or 0)
    total = int(closed_split.get("total") or 0)
    if total <= 0:
        return {"error": f"no LEAN Done issues in last {window}d for growth allocation"}
    logger.info(
        "%% Growth Allocation: planned=%s / total=%s (window=%sd)",
        planned,
        total,
        window,
    )
    return {
        "numerator": float(planned),
        "denominator": float(total),
        "planned": planned,
        "unplanned": int(closed_split.get("unplanned") or 0),
        "window_days": window,
    }


def get_ai_spend_pct(
    cursor_client: Any,
    *,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Spend %: monthly AI spend (USD) ÷ monthly engineering spend (USD).

    Numerator is projected calendar-month Cursor spend (:func:`get_monthly_ai_spend`).
    Denominator is ``CORTEX_ENGINEERING_MONTHLY_SPEND_USD`` (finance headcount/opex,
    excluding AI tooling). Fails loud when unset/invalid or Cursor spend fails.
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

    ai = get_monthly_ai_spend(cursor_client, timeout=timeout)
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
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """AI Spend / Issue: engineer Cursor spend (USD) ÷ issues shipped."""
    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=days)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    shipped = get_issues_shipped(jira_client, days=days, timeout=timeout)
    if shipped.get("error"):
        return shipped
    issues = int(shipped.get("value") or 0)
    if issues <= 0:
        return {"error": "Issues Shipped is 0 — cannot compute AI Spend / Issue"}
    spend_usd = round(float(stats["charged_cents"]) / 100.0, 4)
    per_issue = round(spend_usd / issues, 4)
    logger.info(
        "AI Spend / Issue: $%s / %s issues = $%s (window=%sd)",
        spend_usd,
        issues,
        per_issue,
        days,
    )
    return {
        "numerator": spend_usd,
        "denominator": float(issues),
        "value": per_issue,
        "spend_usd": spend_usd,
        "issues_shipped": issues,
        "window_days": max(1, int(days)),
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
    days: int = DEFAULT_WINDOW_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Headcount + AI Spend / Issue: (prorated headcount cost + AI spend) ÷ issues shipped.

    Headcount cost is ``CORTEX_ENGINEERING_MONTHLY_SPEND_USD`` prorated to the window
    (``monthly × days / 30``). Treat that env as engineering headcount/opex **excluding**
    AI tooling so AI is not double-counted. AI spend is engineer-scoped Cursor charged
    USD over the same window (same numerator as :func:`get_ai_spend_per_issue`).
    """
    hc = _engineering_headcount_monthly_usd()
    if hc.get("error"):
        return hc
    window_days = max(1, int(days))
    headcount_usd = round(float(hc["value"]) * (window_days / 30.0), 4)

    scope = _engineer_scope(jira_client, timeout=timeout)
    if scope.get("error"):
        return scope
    events = _cursor_events(cursor_client, days=window_days)
    if isinstance(events, dict) and events.get("error"):
        return events
    stats = _engineer_event_stats(events, engineer_emails=scope["emails"])
    ai_usd = round(float(stats["charged_cents"]) / 100.0, 4)

    shipped = get_issues_shipped(jira_client, days=window_days, timeout=timeout)
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
        "Headcount + AI Spend / Issue: ($%s headcount + $%s AI) / %s issues = $%s "
        "(window=%sd)",
        headcount_usd,
        ai_usd,
        issues,
        per_issue,
        window_days,
    )
    return {
        "numerator": total_usd,
        "denominator": float(issues),
        "value": per_issue,
        "headcount_usd": headcount_usd,
        "ai_spend_usd": ai_usd,
        "total_usd": total_usd,
        "issues_shipped": issues,
        "window_days": window_days,
        "engineering_monthly_spend_usd": float(hc["value"]),
    }
