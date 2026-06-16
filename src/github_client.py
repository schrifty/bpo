"""GitHub REST API client — auth preflight, repo activity, and engineer productivity signals.

Uses a classic PAT or fine-grained token (``GITHUB_TOKEN``) against the GitHub REST API.
Designed to join with Cursor usage by **author email** on commits (the most reliable
identity key GitHub exposes without elevated scopes).

Public surface:
- :func:`github_configured` / :func:`check_github_api` — deck preflight (non-throwing)
- :class:`GitHubClient` — fail-loud REST wrapper with pagination and rate-limit retries
- :func:`build_github_activity_report` — org/repo activity rollup for ``report["github"]``

Required token scopes (classic PAT): ``repo`` (private repos) or ``public_repo``; for org
repo listing add ``read:org``. Fine-grained tokens need equivalent repository/org read access.
"""

from __future__ import annotations

import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from .config import (
    BPO_GITHUB_LOOKBACK_DAYS,
    BPO_GITHUB_MAX_COMMITS_PER_REPO,
    BPO_GITHUB_MAX_REPOS,
    GITHUB_API_BASE_URL,
    GITHUB_ORG,
    GITHUB_REPOS,
    GITHUB_TOKEN,
    logger,
)
from .config_paths import GITHUB_REPO_DENYLIST_FILE

_NOREPLY_LOGIN_RE = re.compile(
    r"^(?:\d+\+)?([a-z0-9-]+)@users\.noreply\.github\.com$", re.IGNORECASE
)

_DEFAULT_TIMEOUT_S = 30.0
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGES = 100
_RATE_LIMIT_MAX_RETRIES = 5
_RATE_LIMIT_BACKOFF_BASE_S = 5.0
_RATE_LIMIT_BACKOFF_CAP_S = 120.0
_GITHUB_API_VERSION = "2022-11-28"
_LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class GitHubError(RuntimeError):
    """Raised for GitHub configuration, HTTP, or response-parsing failures."""


def github_configured() -> bool:
    return bool(GITHUB_TOKEN and str(GITHUB_TOKEN).strip())


def _github_org() -> str | None:
    return GITHUB_ORG


def _github_repos_env() -> str | None:
    return GITHUB_REPOS


def _github_lookback_days() -> int:
    return BPO_GITHUB_LOOKBACK_DAYS


def _github_max_repos() -> int:
    return BPO_GITHUB_MAX_REPOS


def _github_max_commits_per_repo() -> int:
    return BPO_GITHUB_MAX_COMMITS_PER_REPO


def check_github_api() -> tuple[bool, str | None]:
    """Return (True, None) if GitHub is not configured or ``GET /user`` succeeds."""
    if not github_configured():
        return True, None
    try:
        client = GitHubClient()
        client.get_authenticated_user()
        return True, None
    except GitHubError as e:
        return False, str(e)[:120]
    except Exception as e:
        logger.warning("GitHub preflight failed: %s", e)
        return False, f"GitHub: {str(e)[:120]}"


def _parse_iso_dt(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _parse_link_next(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        m = _LINK_NEXT_RE.search(part.strip())
        if m:
            return m.group(1)
    return None


def parse_github_noreply_login(email: str | None) -> str | None:
    """Extract GitHub login from ``{id}+login@users.noreply.github.com`` addresses."""
    raw = str(email or "").strip().lower()
    if not raw:
        return None
    m = _NOREPLY_LOGIN_RE.match(raw)
    return m.group(1) if m else None


def _load_repo_denylist(org: str | None) -> set[str]:
    """Return lowercased repo slugs or full ``owner/repo`` names to exclude."""
    denied: set[str] = set()
    if GITHUB_REPO_DENYLIST_FILE.is_file():
        try:
            import yaml

            raw = yaml.safe_load(GITHUB_REPO_DENYLIST_FILE.read_text(encoding="utf-8")) or {}
            items = raw.get("repos") if isinstance(raw, dict) else raw
            if isinstance(items, list):
                for item in items:
                    s = str(item or "").strip().lower()
                    if s:
                        denied.add(s)
        except Exception as e:
            logger.warning("GitHub repo denylist unreadable: %s", e)
    org_slug = (org or "").strip().lower()
    out: set[str] = set()
    for item in denied:
        if "/" in item:
            out.add(item)
        elif org_slug:
            out.add(f"{org_slug}/{item}")
            out.add(item)
        else:
            out.add(item)
    return out


def _repo_is_denied(owner: str, repo: str, denied: set[str]) -> bool:
    full = f"{owner}/{repo}".lower()
    return full in denied or repo.lower() in denied


def _filter_repo_specs(
    specs: list[tuple[str, str]], *, org: str | None, denied: set[str]
) -> list[tuple[str, str]]:
    if not denied:
        return specs
    kept = [(o, r) for o, r in specs if not _repo_is_denied(o, r, denied)]
    if not kept and specs:
        raise GitHubError("All configured GitHub repos are on the denylist")
    return kept


def _resolve_repo_specs(
    *,
    org: str | None,
    repos_env: str | None,
    client: GitHubClient | None = None,
    max_repos: int | None = None,
) -> list[tuple[str, str]]:
    """Return ``(owner, repo)`` pairs from env and/or org listing."""
    cap = _github_max_repos() if max_repos is None else max(1, int(max_repos))
    specs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _add(owner: str, repo: str) -> None:
        key = (owner.strip(), repo.strip())
        if not key[0] or not key[1] or key in seen:
            return
        seen.add(key)
        specs.append(key)

    if repos_env:
        for part in repos_env.split(","):
            token = part.strip()
            if not token:
                continue
            if "/" in token:
                owner, repo = token.split("/", 1)
                _add(owner, repo)
            elif org:
                _add(org, token)
            else:
                raise GitHubError(
                    f"GITHUB_REPOS entry {token!r} must be owner/repo when GITHUB_ORG is unset"
                )

    if specs:
        denied = _load_repo_denylist(org)
        return _filter_repo_specs(specs, org=org, denied=denied)[:cap]

    if not org:
        raise GitHubError(
            "GitHub activity requires GITHUB_ORG and/or GITHUB_REPOS "
            "(comma-separated owner/repo or repo names under the org)"
        )
    if client is None:
        raise GitHubError("internal: client required to list org repos")
    for row in client.list_org_repos(org):
        full = str(row.get("full_name") or "").strip()
        if "/" in full:
            owner, repo = full.split("/", 1)
            _add(owner, repo)
        if len(specs) >= cap:
            break
    if not specs:
        raise GitHubError(f"No repositories found for org {org!r}")
    denied = _load_repo_denylist(org)
    return _filter_repo_specs(specs, org=org, denied=denied)


class GitHubClient:
    """Thin, fail-loud wrapper over the GitHub REST API."""

    def __init__(
        self,
        token: str | None = None,
        *,
        base_url: str | None = None,
        session: Any | None = None,
        timeout: float = _DEFAULT_TIMEOUT_S,
        max_retries: int | None = None,
    ) -> None:
        key = (token if token is not None else GITHUB_TOKEN) or ""
        self.token = str(key).strip()
        if not self.token:
            raise GitHubError(
                "GitHub token is not configured. Set GITHUB_TOKEN (classic PAT or fine-grained token)."
            )
        self.base_url = (base_url or GITHUB_API_BASE_URL or "https://api.github.com").rstrip("/")
        self.timeout = float(timeout)
        self._max_retries = _RATE_LIMIT_MAX_RETRIES if max_retries is None else max(0, int(max_retries))
        self._session = session or requests.Session()

    def _headers(self, *, accept: str | None = None) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": accept or "application/vnd.github+json",
            "X-GitHub-Api-Version": _GITHUB_API_VERSION,
        }

    def _backoff_seconds(self, attempt: int, *, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), _RATE_LIMIT_BACKOFF_CAP_S)
            except (TypeError, ValueError):
                pass
        base = _RATE_LIMIT_BACKOFF_BASE_S * (2 ** attempt)
        return min(base, _RATE_LIMIT_BACKOFF_CAP_S) + random.uniform(0.0, 1.0)

    def _raise_for_status(self, resp: requests.Response, url: str) -> None:
        if getattr(resp, "ok", False):
            return
        status = getattr(resp, "status_code", "?")
        snippet = (getattr(resp, "text", "") or "").strip().replace("\n", " ")[:300]
        if status == 401:
            hint = "invalid or expired GITHUB_TOKEN"
        elif status == 403:
            hint = "forbidden — check token scopes (repo/read:org) and org membership"
        elif status == 404:
            hint = "not found — verify org/repo names and token access"
        elif status == 429:
            hint = "rate limited — back off and retry"
        else:
            hint = "unexpected GitHub API error"
        raise GitHubError(f"GitHub API HTTP {status} for {url} ({hint}): {snippet}")

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
    ) -> requests.Response:
        for attempt in range(self._max_retries + 1):
            last = attempt >= self._max_retries
            try:
                resp = self._session.request(
                    method,
                    url,
                    headers=self._headers(accept=accept),
                    params=params,
                    timeout=self.timeout,
                )
            except requests.RequestException as e:
                if last:
                    raise GitHubError(f"GitHub API request to {url} failed: {e}") from e
                wait = self._backoff_seconds(attempt)
                logger.warning(
                    "GitHub API %s network error (%s); retry %d/%d in %.0fs",
                    url,
                    e,
                    attempt + 1,
                    self._max_retries,
                    wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code == 429 and not last:
                wait = self._backoff_seconds(attempt, retry_after=resp.headers.get("Retry-After"))
                logger.warning(
                    "GitHub API rate limited (429); retry %d/%d in %.0fs",
                    attempt + 1,
                    self._max_retries,
                    wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 500 and not last:
                wait = self._backoff_seconds(attempt)
                logger.warning(
                    "GitHub API HTTP %d; retry %d/%d in %.0fs",
                    resp.status_code,
                    attempt + 1,
                    self._max_retries,
                    wait,
                )
                time.sleep(wait)
                continue
            self._raise_for_status(resp, url)
            return resp
        raise GitHubError(f"GitHub API request to {url} failed after {self._max_retries} retries")

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None, accept: str | None = None) -> Any:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        resp = self._request("GET", url, params=params, accept=accept)
        if not (resp.text or "").strip():
            return {}
        try:
            return resp.json()
        except ValueError as e:
            raise GitHubError(f"GitHub API returned non-JSON from {url}: {e}") from e

    def _paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        accept: str | None = None,
        max_pages: int = _MAX_PAGES,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        base_params = dict(params or {})
        base_params.setdefault("per_page", _DEFAULT_PAGE_SIZE)
        url = f"{self.base_url}/{path.lstrip('/')}"
        results: list[dict[str, Any]] = []
        next_from_link = False
        for _ in range(max_pages):
            resp = self._request(
                "GET",
                url,
                params=base_params if not next_from_link else None,
                accept=accept,
            )
            try:
                batch = resp.json()
            except ValueError as e:
                raise GitHubError(f"GitHub API returned non-JSON from {url}: {e}") from e
            if not isinstance(batch, list):
                raise GitHubError(f"GitHub API expected a list from {url}, got {type(batch).__name__}")
            results.extend(x for x in batch if isinstance(x, dict))
            if max_items is not None and len(results) >= max_items:
                return results[:max_items]
            next_url = _parse_link_next(resp.headers.get("Link"))
            if not next_url:
                break
            url = next_url
            next_from_link = True
            base_params = {}
        else:
            raise GitHubError(f"GitHub pagination for {path} exceeded {max_pages} pages")
        return results

    # ── Auth / org ───────────────────────────────────────────────────────────
    def get_authenticated_user(self) -> dict[str, Any]:
        data = self._get_json("/user")
        return data if isinstance(data, dict) else {}

    def list_org_repos(self, org: str, *, repo_type: str = "all") -> list[dict[str, Any]]:
        org_name = (org or "").strip()
        if not org_name:
            raise GitHubError("org name is required")
        return self._paginate(
            f"/orgs/{org_name}/repos",
            params={"type": repo_type, "sort": "updated", "direction": "desc"},
            max_items=_github_max_repos(),
        )

    def get_repo(self, owner: str, repo: str) -> dict[str, Any]:
        data = self._get_json(f"/repos/{owner}/{repo}")
        return data if isinstance(data, dict) else {}

    # ── Activity ─────────────────────────────────────────────────────────────
    def list_releases(self, owner: str, repo: str, *, limit: int = 30) -> list[dict[str, Any]]:
        return self._paginate(
            f"/repos/{owner}/{repo}/releases",
            params={"per_page": min(limit, _DEFAULT_PAGE_SIZE)},
            max_items=max(1, limit),
        )

    def list_commits(
        self,
        owner: str,
        repo: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        author: str | None = None,
        max_commits: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if since is not None:
            params["since"] = since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if until is not None:
            params["until"] = until.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if author:
            params["author"] = author.strip()
        cap = _github_max_commits_per_repo() if max_commits is None else max(1, int(max_commits))
        return self._paginate(
            f"/repos/{owner}/{repo}/commits",
            params=params,
            max_items=cap,
        )

    def list_pull_requests(
        self,
        owner: str,
        repo: str,
        *,
        state: str = "all",
        max_pulls: int | None = None,
    ) -> list[dict[str, Any]]:
        cap = 500 if max_pulls is None else max(1, int(max_pulls))
        return self._paginate(
            f"/repos/{owner}/{repo}/pulls",
            params={"state": state, "sort": "updated", "direction": "desc"},
            max_items=cap,
        )

    def list_org_members(self, org: str, *, max_members: int = 500) -> list[dict[str, Any]]:
        org_name = (org or "").strip()
        if not org_name:
            raise GitHubError("org name is required")
        return self._paginate(
            f"/orgs/{org_name}/members",
            params={"per_page": _DEFAULT_PAGE_SIZE},
            max_items=max(1, int(max_members)),
        )

    def get_contributor_stats(
        self,
        owner: str,
        repo: str,
        *,
        max_wait_s: float = 45.0,
        poll_s: float = 2.0,
    ) -> list[dict[str, Any]]:
        """Weekly contributor stats (additions/deletions/commits). GitHub may return 202 while computing."""
        url = f"{self.base_url}/repos/{owner}/{repo}/stats/contributors"
        deadline = time.time() + float(max_wait_s)
        while time.time() < deadline:
            resp = self._session.request(
                "GET",
                url,
                headers=self._headers(),
                timeout=self.timeout,
            )
            if resp.status_code == 202:
                time.sleep(poll_s)
                continue
            self._raise_for_status(resp, url)
            try:
                data = resp.json()
            except ValueError as e:
                raise GitHubError(f"GitHub API returned non-JSON from {url}: {e}") from e
            if not isinstance(data, list):
                raise GitHubError(f"GitHub contributor stats expected list from {url}")
            return [x for x in data if isinstance(x, dict)]
        raise GitHubError(
            f"GitHub contributor stats for {owner}/{repo} still computing after {max_wait_s:.0f}s"
        )


def _commit_author_email(commit: dict[str, Any]) -> str | None:
    inner = commit.get("commit") if isinstance(commit.get("commit"), dict) else {}
    author = inner.get("author") if isinstance(inner.get("author"), dict) else {}
    email = str(author.get("email") or "").strip().lower()
    if not email or email.endswith("@users.noreply.github.com"):
        return None
    return email


def _commit_dt(commit: dict[str, Any]) -> datetime | None:
    inner = commit.get("commit") if isinstance(commit.get("commit"), dict) else {}
    author = inner.get("author") if isinstance(inner.get("author"), dict) else {}
    return _parse_iso_dt(author.get("date"))


def build_github_activity_report(
    *,
    org: str | None = None,
    repos_env: str | None = None,
    window_days: int | None = None,
    client: GitHubClient | None = None,
) -> dict[str, Any] | None:
    """Aggregate repo activity for ``report["github"]``. Returns ``None`` when not configured."""
    if not github_configured():
        return None

    org_name = (org if org is not None else _github_org()) or None
    repos_raw = repos_env if repos_env is not None else _github_repos_env()
    days = _github_lookback_days() if window_days is None else max(1, min(int(window_days), 365))
    since = datetime.now(timezone.utc) - timedelta(days=days)

    gh = client or GitHubClient()
    user = gh.get_authenticated_user()
    repo_specs = _resolve_repo_specs(org=org_name, repos_env=repos_raw, client=gh)

    totals = {
        "commits": 0,
        "prs_merged": 0,
        "prs_open": 0,
        "releases": 0,
    }
    by_email: dict[str, dict[str, int]] = {}
    repos_summary: list[dict[str, Any]] = []

    for owner, repo in repo_specs:
        repo_totals = {"commits": 0, "prs_merged": 0, "prs_open": 0, "releases": 0}
        meta = gh.get_repo(owner, repo)
        commits = gh.list_commits(owner, repo, since=since)
        for commit in commits:
            when = _commit_dt(commit)
            if when and when < since:
                continue
            email = _commit_author_email(commit)
            repo_totals["commits"] += 1
            totals["commits"] += 1
            if email:
                bucket = by_email.setdefault(email, {"commits": 0, "prs_merged": 0})
                bucket["commits"] += 1

        pulls = gh.list_pull_requests(owner, repo, state="all")
        for pull in pulls:
            if not isinstance(pull, dict):
                continue
            merged_at = _parse_iso_dt(pull.get("merged_at"))
            updated_at = _parse_iso_dt(pull.get("updated_at"))
            if pull.get("state") == "open":
                repo_totals["prs_open"] += 1
                totals["prs_open"] += 1
            elif merged_at and merged_at >= since:
                repo_totals["prs_merged"] += 1
                totals["prs_merged"] += 1
                user_obj = pull.get("user") if isinstance(pull.get("user"), dict) else {}
                login = str(user_obj.get("login") or "").strip()
                if login:
                    for email, bucket in by_email.items():
                        if login.lower() in email:
                            bucket["prs_merged"] += 1
                            break
            elif updated_at and updated_at < since:
                continue

        releases = gh.list_releases(owner, repo, limit=30)
        for rel in releases:
            published = _parse_iso_dt(rel.get("published_at") or rel.get("created_at"))
            if published and published >= since:
                repo_totals["releases"] += 1
                totals["releases"] += 1

        repos_summary.append(
            {
                "full_name": meta.get("full_name") or f"{owner}/{repo}",
                "default_branch": meta.get("default_branch"),
                "open_issues_count": meta.get("open_issues_count"),
                "pushed_at": meta.get("pushed_at"),
                **repo_totals,
            }
        )

    return {
        "configured": True,
        "api": "rest",
        "user_login": user.get("login"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "org": org_name,
        "repos": [f"{o}/{r}" for o, r in repo_specs],
        "window_days": days,
        "since": since.isoformat(),
        "totals": totals,
        "by_email": by_email,
        "repos_summary": repos_summary,
    }
