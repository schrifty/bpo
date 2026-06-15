"""Cursor Team Admin API client — read-only AI coding usage and spend.

Pulls team-level Cursor usage data for AI adoption / token / spend metrics:

- ``get_team_members``           → GET  /teams/members
- ``get_daily_usage``            → POST /teams/daily-usage-data (auto-chunked, ≤30d/call)
- ``get_spend``                  → POST /teams/spend (current billing cycle, paginated)
- ``get_usage_events``           → POST /teams/filtered-usage-events (token usage, paginated)
- ``get_usage_summary``          → aggregated daily-usage totals over a trailing window

Authentication is HTTP Basic with ``CURSOR_ADMIN_API_KEY`` as the username and an
empty password (per Cursor docs). Only read endpoints are implemented; member,
spend-limit, and billing-group mutations are intentionally omitted.

Fails loud: any non-2xx response raises :class:`CursorClientError` rather than
returning placeholder data, so callers (and metric generators) surface the issue.
"""

from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from .config import CURSOR_ADMIN_API_KEY, CURSOR_API_BASE_URL, logger

# Cursor caps daily-usage / audit-log ranges at 30 days per request.
_MAX_RANGE_DAYS = 30
_DEFAULT_TIMEOUT_S = 60.0
_DEFAULT_PAGE_SIZE = 1000

# Rate limiting: the Admin API allows 20 requests/min/team. We (a) pace requests
# client-side so a multi-call deck build stays under that ceiling, and (b) retry
# 429/5xx with exponential backoff that honors any Retry-After header.
_DEFAULT_MIN_REQUEST_INTERVAL_S = 3.1  # 60s / 20 req ≈ 3s, plus a small margin
_RATE_LIMIT_MAX_RETRIES = 5
_RATE_LIMIT_BACKOFF_BASE_S = 5.0
_RATE_LIMIT_BACKOFF_CAP_S = 60.0


def _default_min_request_interval() -> float:
    """Min seconds between Cursor API requests (override via env for tuning/tests)."""
    raw = (os.environ.get("BPO_CURSOR_MIN_REQUEST_INTERVAL_S") or "").strip()
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return _DEFAULT_MIN_REQUEST_INTERVAL_S


class CursorClientError(Exception):
    """A Cursor Admin API request failed or the client is misconfigured."""


def cursor_configured() -> bool:
    """True when a Cursor Admin API key is present in the environment."""
    return bool(CURSOR_ADMIN_API_KEY and str(CURSOR_ADMIN_API_KEY).strip())


def _to_epoch_ms(value: Any) -> int:
    """Coerce a date / datetime / epoch (s or ms) into epoch milliseconds (UTC)."""
    if value is None:
        raise CursorClientError("date value is required")
    if isinstance(value, bool):  # guard: bool is an int subclass
        raise CursorClientError(f"invalid date value: {value!r}")
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(value, (int, float)):
        n = int(value)
        # Heuristic: seconds-precision epochs are ~1e9–1e10; ms are ~1e12+.
        return n * 1000 if n < 1_000_000_000_000 else n
    raise CursorClientError(f"unsupported date value type: {type(value).__name__}")


def _chunk_ranges(start_ms: int, end_ms: int, *, max_days: int = _MAX_RANGE_DAYS) -> list[tuple[int, int]]:
    """Split ``[start_ms, end_ms]`` into sub-ranges no longer than *max_days*."""
    if end_ms < start_ms:
        raise CursorClientError("endDate must be >= startDate")
    span = max_days * 24 * 60 * 60 * 1000
    out: list[tuple[int, int]] = []
    cur = start_ms
    while cur <= end_ms:
        out.append((cur, min(cur + span - 1, end_ms)))
        cur += span
    return out


@dataclass(frozen=True)
class CursorUsageSummary:
    """Aggregated daily-usage totals over a trailing window."""

    start_date: str
    end_date: str
    active_users: int
    total_lines_added: int
    accepted_lines_added: int
    total_accepts: int
    total_rejects: int
    total_tabs_shown: int
    total_tabs_accepted: int
    composer_requests: int
    chat_requests: int
    agent_requests: int
    cmdk_usages: int
    bugbot_usages: int
    days: int

    @property
    def acceptance_rate(self) -> float | None:
        """Accepted / (accepted + rejected) AI suggestions, or ``None`` when no data."""
        denom = self.total_accepts + self.total_rejects
        return round(self.total_accepts / denom, 4) if denom else None


class CursorClient:
    """Thin read-only client for the Cursor Team Admin API."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = _DEFAULT_TIMEOUT_S,
        min_request_interval: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        key = (api_key or CURSOR_ADMIN_API_KEY or "").strip()
        if not key:
            raise CursorClientError(
                "Cursor Admin API key not set. Add CURSOR_ADMIN_API_KEY to .env "
                "(Cursor dashboard → Settings → Cursor Admin API key)."
            )
        self._api_key = key
        self.base_url = (base_url or CURSOR_API_BASE_URL or "https://api.cursor.com").rstrip("/")
        self.timeout = timeout
        self._min_request_interval = (
            _default_min_request_interval() if min_request_interval is None
            else max(0.0, float(min_request_interval))
        )
        self._max_retries = _RATE_LIMIT_MAX_RETRIES if max_retries is None else max(0, int(max_retries))
        self._last_request_ts: float | None = None
        self._throttle_lock = threading.Lock()
        self._session = requests.Session()
        # Basic auth: API key as username, empty password.
        self._session.auth = (self._api_key, "")
        self._session.headers.update({"Accept": "application/json"})

    # ── transport ──────────────────────────────────────────────────────────
    def _throttle(self) -> None:
        """Pace requests so a multi-call deck build stays under 20 req/min/team."""
        if self._min_request_interval <= 0:
            return
        with self._throttle_lock:
            if self._last_request_ts is not None:
                wait = self._min_request_interval - (time.monotonic() - self._last_request_ts)
                if wait > 0:
                    time.sleep(wait)
            self._last_request_ts = time.monotonic()

    def _backoff_seconds(self, attempt: int, *, retry_after: str | None = None) -> float:
        """Backoff delay for *attempt*: honor Retry-After, else exponential with jitter."""
        if retry_after:
            try:
                return min(float(retry_after), _RATE_LIMIT_BACKOFF_CAP_S)
            except (TypeError, ValueError):
                pass
        base = _RATE_LIMIT_BACKOFF_BASE_S * (2 ** attempt)
        return min(base, _RATE_LIMIT_BACKOFF_CAP_S) + random.uniform(0.0, 1.0)

    def _request(self, method: str, path: str, *, json_body: dict | None = None,
                 params: dict | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(self._max_retries + 1):
            last = attempt >= self._max_retries
            self._throttle()
            try:
                resp = self._session.request(
                    method, url, json=json_body, params=params, timeout=self.timeout,
                )
            except requests.RequestException as e:
                if last:
                    raise CursorClientError(f"Cursor API {method} {path} failed: {e}") from e
                wait = self._backoff_seconds(attempt)
                logger.warning(
                    "Cursor API %s %s network error (%s); retry %d/%d in %.0fs",
                    method, path, e, attempt + 1, self._max_retries, wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                if last:
                    raise CursorClientError(
                        f"Cursor API {method} {path} rate limited (429) after {self._max_retries} retries"
                    )
                wait = self._backoff_seconds(attempt, retry_after=resp.headers.get("Retry-After"))
                logger.warning(
                    "Cursor API rate limited (429); retry %d/%d in %.0fs",
                    attempt + 1, self._max_retries, wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code == 401:
                raise CursorClientError("Cursor API 401 Unauthorized — check CURSOR_ADMIN_API_KEY")
            if resp.status_code >= 500 and not last:
                wait = self._backoff_seconds(attempt)
                logger.warning(
                    "Cursor API %s %s HTTP %d; retry %d/%d in %.0fs",
                    method, path, resp.status_code, attempt + 1, self._max_retries, wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                detail = (resp.text or "")[:200].replace("\n", " ")
                raise CursorClientError(f"Cursor API {method} {path} HTTP {resp.status_code}: {detail}")
            if not (resp.text or "").strip():
                return {}
            try:
                return resp.json()
            except ValueError as e:
                raise CursorClientError(f"Cursor API {method} {path} returned non-JSON: {e}") from e
        raise CursorClientError(f"Cursor API {method} {path} failed after {self._max_retries} retries")

    # ── members ────────────────────────────────────────────────────────────
    def get_team_members(self, *, include_removed: bool = False) -> list[dict[str, Any]]:
        """All team members (GET /teams/members). Removed users excluded by default."""
        data = self._request("GET", "/teams/members")
        members = data.get("teamMembers") or []
        if include_removed:
            return members
        return [m for m in members if not m.get("isRemoved")]

    # ── daily usage ──────────────────────────────────────────────────────────
    def get_daily_usage(
        self,
        start_date: Any,
        end_date: Any,
        *,
        all_members: bool = False,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        """Daily usage records (POST /teams/daily-usage-data), auto-chunked to ≤30d.

        With ``all_members=True`` the endpoint is paginated to include inactive
        members (each row carries ``isActive``); otherwise only active users return.
        """
        start_ms = _to_epoch_ms(start_date)
        end_ms = _to_epoch_ms(end_date)
        rows: list[dict[str, Any]] = []
        for lo, hi in _chunk_ranges(start_ms, end_ms):
            if all_members:
                rows.extend(self._paginated_daily_usage(lo, hi, page_size=page_size))
            else:
                data = self._request(
                    "POST", "/teams/daily-usage-data",
                    json_body={"startDate": lo, "endDate": hi},
                )
                rows.extend(data.get("data") or [])
        return rows

    def _paginated_daily_usage(self, lo: int, hi: int, *, page_size: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            data = self._request(
                "POST", "/teams/daily-usage-data",
                json_body={"startDate": lo, "endDate": hi, "page": page, "pageSize": page_size},
            )
            out.extend(data.get("data") or [])
            pagination = data.get("pagination") or {}
            if not pagination.get("hasNextPage"):
                break
            page += 1
        return out

    # ── spend ────────────────────────────────────────────────────────────────
    def get_spend(
        self,
        *,
        search_term: str | None = None,
        sort_by: str = "date",
        sort_direction: str = "desc",
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Per-member spend for the current billing cycle (POST /teams/spend), all pages."""
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            body: dict[str, Any] = {
                "sortBy": sort_by,
                "sortDirection": sort_direction,
                "page": page,
                "pageSize": page_size,
            }
            if search_term:
                body["searchTerm"] = search_term
            data = self._request("POST", "/teams/spend", json_body=body)
            out.extend(data.get("teamMemberSpend") or [])
            total_pages = int(data.get("totalPages") or 1)
            if page >= total_pages:
                break
            page += 1
        return out

    # ── usage events (token-level) ───────────────────────────────────────────
    def get_usage_events(
        self,
        start_date: Any,
        end_date: Any,
        *,
        email: str | None = None,
        user_id: int | None = None,
        page_size: int = 100,
        max_events: int | None = None,
    ) -> list[dict[str, Any]]:
        """Granular usage events incl. token usage (POST /teams/filtered-usage-events).

        Paginates until exhausted or *max_events* is reached. Ranges are inclusive on
        both bounds per the Cursor docs.
        """
        start_ms = _to_epoch_ms(start_date)
        end_ms = _to_epoch_ms(end_date)
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            body: dict[str, Any] = {
                "startDate": start_ms,
                "endDate": end_ms,
                "page": page,
                "pageSize": page_size,
            }
            if email:
                body["email"] = email
            if user_id is not None:
                body["userId"] = user_id
            data = self._request("POST", "/teams/filtered-usage-events", json_body=body)
            events = data.get("usageEvents") or []
            out.extend(events)
            if max_events is not None and len(out) >= max_events:
                return out[:max_events]
            pagination = data.get("pagination") or {}
            if not pagination.get("hasNextPage"):
                break
            page += 1
        return out

    # ── convenience aggregate ────────────────────────────────────────────────
    def get_usage_summary(self, *, days: int = 30, end_date: Any | None = None) -> CursorUsageSummary:
        """Aggregate daily-usage totals over a trailing window (default 30 days)."""
        n = max(1, int(days))
        end_dt = end_date if end_date is not None else datetime.now(timezone.utc)
        end_ms = _to_epoch_ms(end_dt)
        start_ms = _to_epoch_ms(
            datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc) - timedelta(days=n - 1)
        )
        rows = self.get_daily_usage(start_ms, end_ms)

        def _sum(field: str) -> int:
            return int(sum(int(r.get(field) or 0) for r in rows))

        active_users = len({r.get("userId") for r in rows if r.get("isActive", True)})
        return CursorUsageSummary(
            start_date=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date().isoformat(),
            end_date=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date().isoformat(),
            active_users=active_users,
            total_lines_added=_sum("totalLinesAdded"),
            accepted_lines_added=_sum("acceptedLinesAdded"),
            total_accepts=_sum("totalAccepts"),
            total_rejects=_sum("totalRejects"),
            total_tabs_shown=_sum("totalTabsShown"),
            total_tabs_accepted=_sum("totalTabsAccepted"),
            composer_requests=_sum("composerRequests"),
            chat_requests=_sum("chatRequests"),
            agent_requests=_sum("agentRequests"),
            cmdk_usages=_sum("cmdkUsages"),
            bugbot_usages=_sum("bugbotUsages"),
            days=n,
        )


_shared_cursor_client: CursorClient | None = None


def get_shared_cursor_client() -> CursorClient:
    """Process-wide singleton ``CursorClient`` (raises if unconfigured)."""
    global _shared_cursor_client
    if _shared_cursor_client is None:
        _shared_cursor_client = CursorClient()
    return _shared_cursor_client


def reset_for_tests() -> None:
    """Drop the shared client singleton (for test isolation)."""
    global _shared_cursor_client
    _shared_cursor_client = None
