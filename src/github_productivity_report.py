"""GitHub engineering output metrics for productivity correlation."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .engineer_identity_map import canonicalize_email, load_github_email_aliases
from .github_cache import cache_get, cache_key, cache_set
from .github_client import (
    GitHubClient,
    GitHubError,
    _commit_author_email,
    _commit_dt,
    _github_org,
    _github_repos_env,
    _parse_iso_dt,
    _resolve_repo_specs,
    github_configured,
    parse_github_noreply_login,
)

logger = logging.getLogger("bpo")

_EMPTY_PERSON = {
    "commits": 0,
    "merged_prs": 0,
    "lines_added": 0,
    "lines_deleted": 0,
    "repos_touched": [],
    "avg_pr_cycle_hours": None,
}


def _iso_week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _week_label(key: str) -> str:
    if "-W" in key:
        year, week = key.split("-W", 1)
        return f"W{int(week)}"
    return key


def _blank_person() -> dict[str, Any]:
    return {**_EMPTY_PERSON, "repos_touched": []}


def _sum_person(target: dict[str, Any], source: dict[str, Any]) -> None:
    target["commits"] += int(source.get("commits") or 0)
    target["merged_prs"] += int(source.get("merged_prs") or 0)
    target["lines_added"] += int(source.get("lines_added") or 0)
    target["lines_deleted"] += int(source.get("lines_deleted") or 0)
    repos = set(target.get("repos_touched") or [])
    repos.update(source.get("repos_touched") or [])
    target["repos_touched"] = sorted(repos)
    src_cycle = source.get("avg_pr_cycle_hours")
    if src_cycle is not None:
        cycles = target.get("_cycle_hours") or []
        cycles.append(float(src_cycle))
        target["_cycle_hours"] = cycles


def _finalize_person(row: dict[str, Any]) -> dict[str, Any]:
    cycles = row.pop("_cycle_hours", None)
    if cycles:
        row["avg_pr_cycle_hours"] = round(sum(cycles) / len(cycles), 2)
    else:
        row["avg_pr_cycle_hours"] = row.get("avg_pr_cycle_hours")
    return row


def _contributor_lines_in_window(
    stats: list[dict[str, Any]], *, since: datetime, login_to_email: dict[str, str], engineer_emails: set[str]
) -> dict[str, dict[str, int]]:
    """Sum weekly contributor buckets whose week start is within the lookback window."""
    email_aliases, _ = load_github_email_aliases()
    since_ts = since.timestamp()
    out: dict[str, dict[str, int]] = {}
    for row in stats:
        author = row.get("author") if isinstance(row.get("author"), dict) else {}
        login = str(author.get("login") or "").strip().lower()
        canonical = login_to_email.get(login)
        if not canonical:
            continue
        if engineer_emails and canonical not in engineer_emails:
            continue
        adds = dels = 0
        for week in row.get("weeks") or []:
            if not isinstance(week, dict):
                continue
            if float(week.get("w") or 0) >= since_ts - 7 * 86400:
                adds += int(week.get("a") or 0)
                dels += int(week.get("d") or 0)
        if adds or dels:
            bucket = out.setdefault(canonical, {"lines_added": 0, "lines_deleted": 0})
            bucket["lines_added"] += adds
            bucket["lines_deleted"] += dels
    return out


def _attribute_pull(
    pull: dict[str, Any],
    *,
    since: datetime,
    login_to_email: dict[str, str],
    email_aliases: dict[str, str],
    engineer_emails: set[str],
) -> tuple[str | None, float | None, str]:
    """Return (canonical_email, cycle_hours, state) where state is open|merged|skip."""
    merged_at = _parse_iso_dt(pull.get("merged_at"))
    updated_at = _parse_iso_dt(pull.get("updated_at"))
    if pull.get("state") == "open":
        return None, None, "open"
    if merged_at and merged_at >= since:
        user_obj = pull.get("user") if isinstance(pull.get("user"), dict) else {}
        login = str(user_obj.get("login") or "").strip().lower()
        canonical = login_to_email.get(login)
        if not canonical and login:
            canonical = email_aliases.get(f"{login}@users.noreply.github.com")
        if engineer_emails and canonical and canonical not in engineer_emails:
            canonical = None
        created = _parse_iso_dt(pull.get("created_at"))
        cycle = None
        if created and merged_at:
            cycle = round((merged_at - created).total_seconds() / 3600, 2)
        return canonical, cycle, "merged"
    if updated_at and updated_at < since:
        return None, None, "skip"
    return None, None, "skip"


def build_github_productivity_report(
    *,
    org: str | None = None,
    repos_env: str | None = None,
    window_days: int | None = None,
    client: GitHubClient | None = None,
    identity: dict[str, Any] | None = None,
    use_cache: bool = True,
) -> dict[str, Any] | None:
    """Aggregate GitHub output metrics keyed by canonical engineer email."""
    if not github_configured():
        return None

    org_name = (org if org is not None else _github_org()) or None
    repos_raw = repos_env if repos_env is not None else _github_repos_env()
    days = window_days if window_days is not None else 30
    days = max(1, min(int(days), 365))

    cache_storage_key: str | None = None
    if use_cache:
        cache_storage_key = cache_key(
            "productivity_report",
            {"org": org_name, "repos": repos_raw, "days": days, "identity": bool(identity)},
        )
        cached = cache_get(cache_storage_key)
        if cached is not None:
            logger.debug("GitHub cache hit: productivity report")
            return cached

    since = datetime.now(timezone.utc) - timedelta(days=days)

    gh = client or GitHubClient()
    user = gh.get_authenticated_user()
    repo_specs = _resolve_repo_specs(org=org_name, repos_env=repos_raw, client=gh)

    engineer_emails: set[str] = set()
    login_to_email: dict[str, str] = {}
    if identity and identity.get("configured"):
        engineer_emails = set(identity.get("canonical_emails") or [])
        login_to_email = dict(identity.get("login_to_email") or {})
    email_aliases, _ = load_github_email_aliases()

    all_by_email: dict[str, dict[str, Any]] = {}
    engineer_by_email: dict[str, dict[str, Any]] = {}
    company_all = _blank_person()
    company_engineers = _blank_person()
    company_open_prs = 0
    company_releases = 0
    weekly_commits: dict[str, int] = defaultdict(int)
    weekly_engineer_commits: dict[str, int] = defaultdict(int)
    repos_summary: list[dict[str, Any]] = []
    warnings: list[str] = list(identity.get("warnings") or []) if identity else []

    for owner, repo in repo_specs:
        full_name = f"{owner}/{repo}"
        repo_all = _blank_person()
        repo_engineers = _blank_person()
        repo_open_prs = 0
        repo_releases = 0
        meta = gh.get_repo(owner, repo)

        for commit in gh.list_commits(owner, repo, since=since):
            when = _commit_dt(commit)
            if when and when < since:
                continue
            raw_email = _commit_author_email(commit)
            login = parse_github_noreply_login(raw_email) if not raw_email else None
            if not raw_email and login and login in login_to_email:
                canonical = login_to_email[login]
            else:
                canonical = canonicalize_email(
                    raw_email,
                    email_aliases=email_aliases,
                    login_to_email=login_to_email,
                    engineer_emails=engineer_emails,
                )
            repo_all["commits"] += 1
            company_all["commits"] += 1
            if when:
                wk = _iso_week_key(when)
                weekly_commits[wk] += 1
            if canonical:
                if login and login not in login_to_email:
                    login_to_email[login] = canonical
                person = all_by_email.setdefault(canonical, _blank_person())
                person["commits"] += 1
                if full_name not in person["repos_touched"]:
                    person["repos_touched"].append(full_name)
                if engineer_emails and canonical in engineer_emails:
                    eng = engineer_by_email.setdefault(canonical, _blank_person())
                    eng["commits"] += 1
                    if full_name not in eng["repos_touched"]:
                        eng["repos_touched"].append(full_name)
                    repo_engineers["commits"] += 1
                    company_engineers["commits"] += 1
                    if when:
                        weekly_engineer_commits[_iso_week_key(when)] += 1

        for pull in gh.list_pull_requests(owner, repo, state="all"):
            canonical, cycle, state = _attribute_pull(
                pull,
                since=since,
                login_to_email=login_to_email,
                email_aliases=email_aliases,
                engineer_emails=engineer_emails,
            )
            if state == "open":
                repo_open_prs += 1
                company_open_prs += 1
                continue
            if state != "merged" or not canonical:
                continue
            repo_all["merged_prs"] += 1
            company_all["merged_prs"] += 1
            person = all_by_email.setdefault(canonical, _blank_person())
            person["merged_prs"] += 1
            if cycle is not None:
                person.setdefault("_cycle_hours", []).append(cycle)
            if engineer_emails and canonical in engineer_emails:
                eng = engineer_by_email.setdefault(canonical, _blank_person())
                eng["merged_prs"] += 1
                if cycle is not None:
                    eng.setdefault("_cycle_hours", []).append(cycle)
                repo_engineers["merged_prs"] += 1
                company_engineers["merged_prs"] += 1

        for rel in gh.list_releases(owner, repo, limit=30):
            published = _parse_iso_dt(rel.get("published_at") or rel.get("created_at"))
            if published and published >= since:
                repo_releases += 1
                company_releases += 1

        try:
            stats = gh.get_contributor_stats(owner, repo)
            for canonical, lines in _contributor_lines_in_window(
                stats, since=since, login_to_email=login_to_email, engineer_emails=set()
            ).items():
                person = all_by_email.setdefault(canonical, _blank_person())
                person["lines_added"] += lines["lines_added"]
                person["lines_deleted"] += lines["lines_deleted"]
                repo_all["lines_added"] += lines["lines_added"]
                repo_all["lines_deleted"] += lines["lines_deleted"]
                company_all["lines_added"] += lines["lines_added"]
                company_all["lines_deleted"] += lines["lines_deleted"]
                if engineer_emails and canonical in engineer_emails:
                    eng = engineer_by_email.setdefault(canonical, _blank_person())
                    eng["lines_added"] += lines["lines_added"]
                    eng["lines_deleted"] += lines["lines_deleted"]
                    repo_engineers["lines_added"] += lines["lines_added"]
                    repo_engineers["lines_deleted"] += lines["lines_deleted"]
                    company_engineers["lines_added"] += lines["lines_added"]
                    company_engineers["lines_deleted"] += lines["lines_deleted"]
        except GitHubError as e:
            warnings.append(f"contributor stats {full_name}: {e}")

        repos_summary.append(
            {
                "full_name": meta.get("full_name") or full_name,
                "default_branch": meta.get("default_branch"),
                "pushed_at": meta.get("pushed_at"),
                "commits": repo_all["commits"],
                "merged_prs": repo_all["merged_prs"],
                "open_prs": repo_open_prs,
                "releases": repo_releases,
                "lines_added": repo_all["lines_added"],
                "lines_deleted": repo_all["lines_deleted"],
            }
        )

    for bucket in (all_by_email, engineer_by_email):
        for email in list(bucket.keys()):
            bucket[email] = _finalize_person(bucket[email])

    company_all = _finalize_person(company_all)
    company_engineers = _finalize_person(company_engineers)
    company_all["open_prs"] = company_open_prs
    company_all["releases"] = company_releases
    company_engineers["open_prs"] = 0
    company_engineers["releases"] = 0

    unmatched_github = sorted(set(all_by_email.keys()) - engineer_emails) if engineer_emails else []

    weekly = [
        {
            "week": wk,
            "label": _week_label(wk),
            "commits": weekly_commits[wk],
            "engineer_commits": weekly_engineer_commits.get(wk, 0),
        }
        for wk in sorted(weekly_commits.keys())
    ]

    result = {
        "configured": True,
        "api": "rest",
        "user_login": user.get("login"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "org": org_name,
        "repos": [f"{o}/{r}" for o, r in repo_specs],
        "window_days": days,
        "since": since.isoformat(),
        "company_all": company_all,
        "company_engineers": company_engineers,
        "by_email": engineer_by_email if engineer_emails else all_by_email,
        "by_email_all": all_by_email,
        "weekly": weekly,
        "repos_summary": repos_summary,
        "identity": {
            "matched_engineers": len(engineer_by_email),
            "unmatched_github_emails": unmatched_github[:20],
            "unmatched_github_count": len(unmatched_github),
        },
        "warnings": warnings,
    }
    if cache_storage_key:
        cache_set(cache_storage_key, result)
    return result


def github_qa_blob(productivity: dict[str, Any] | None) -> dict[str, Any]:
    """Minimal ``report[\"github\"]`` block for existing QA source pills."""
    if not productivity or not productivity.get("configured"):
        return {}
    if productivity.get("error"):
        return {"error": productivity["error"]}
    return {
        "configured": True,
        "api": productivity.get("api", "rest"),
        "user_login": productivity.get("user_login"),
        "generated_at": productivity.get("generated_at"),
    }
