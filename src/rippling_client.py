"""Rippling Platform API client — HR org, employees, teams, departments.

Built primarily to source/enrich the engineering team roster & headcount from the
HR system of record. Uses the Rippling **Platform API** (``https://api.rippling.com/
platform/api``) with **Bearer** API-key auth.

Auth & config (see :mod:`src.config`):
- ``RIPPLING_API_KEY``   — required Bearer token (developer portal); scopes:
  ``employee:read``, ``company:teams:read``, ``company:departments:read``.
- ``RIPPLING_API_BASE_URL`` — optional override (default Platform API host).
- ``RIPPLING_API_VERSION``  — optional ``Rippling-Api-Version`` header (date string).

Design notes:
- **Fail loud** (see ``.cursor/rules/fail-loud-integrations.mdc``): a missing key or any
  HTTP/parse error raises :class:`RipplingError` with actionable context (401 → bad/expired
  key, 403 → missing scopes). We never silently return empty/placeholder data.
- Pagination is tolerant of both Platform-API ``limit``/``offset`` list responses and
  cursor-style ``{results, next_link}`` bodies (the newer REST host), so callers don't care.
- Employee field shapes vary by tenant/scope, so normalization checks several key spellings.

This module is integration-only; it does not wire into any deck yet.
"""

from __future__ import annotations

import re
from typing import Any

import requests

from .config import (
    RIPPLING_API_BASE_URL,
    RIPPLING_API_KEY,
    RIPPLING_API_VERSION,
    logger,
)

# Titles that mark a likely team lead/manager, highest priority first.
_LEAD_TITLE_PRIORITY: tuple[str, ...] = (
    "chief", "vp", "vice president", "director", "head", "manager", "lead", "principal",
)
_LEAD_TITLE_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in _LEAD_TITLE_PRIORITY) + r")\b", re.IGNORECASE
)

_DEFAULT_PAGE_SIZE = 100
_MAX_PAGES = 1000  # hard stop so a misbehaving cursor can never spin forever


class RipplingError(RuntimeError):
    """Raised for any Rippling configuration, HTTP, or response-parsing failure."""


def rippling_configured() -> bool:
    """True when a Rippling API key is present in the environment/config."""
    return bool(RIPPLING_API_KEY and str(RIPPLING_API_KEY).strip())


def _truthy_active(value: Any) -> bool | None:
    """Interpret a status-ish value as active(True)/inactive(False)/unknown(None)."""
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"active", "accepted", "hired", "true", "employed", "current"}:
        return True
    if s in {"terminated", "inactive", "false", "offboarded", "departed", "former"}:
        return False
    return None


class RipplingClient:
    """Thin, fail-loud wrapper over the Rippling Platform API.

    Pass ``session`` to inject a custom/mock ``requests``-style session (used in tests).
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        api_version: str | None = None,
        session: Any | None = None,
        timeout: float = 30.0,
    ) -> None:
        key = (api_key if api_key is not None else RIPPLING_API_KEY) or ""
        self.api_key = str(key).strip()
        if not self.api_key:
            raise RipplingError(
                "Rippling API key is not configured. Set RIPPLING_API_KEY (Bearer token "
                "from the Rippling developer portal) before constructing RipplingClient."
            )
        self.base_url = (base_url or RIPPLING_API_BASE_URL or "https://api.rippling.com/platform/api").rstrip("/")
        self.api_version = api_version if api_version is not None else RIPPLING_API_VERSION
        self.timeout = float(timeout)
        self._session = session or requests.Session()

    # ── HTTP layer ──────────────────────────────────────────────────────────
    def _headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if self.api_version:
            headers["Rippling-Api-Version"] = str(self.api_version)
        return headers

    def _raise_for_status(self, resp: requests.Response, url: str) -> None:
        if getattr(resp, "ok", False):
            return
        status = getattr(resp, "status_code", "?")
        snippet = (getattr(resp, "text", "") or "").strip().replace("\n", " ")[:300]
        if status == 401:
            hint = "invalid or expired RIPPLING_API_KEY (tokens expire after ~30d of inactivity)"
        elif status == 403:
            hint = (
                "token is missing required scopes — enable employee:read, company:teams:read, "
                "and company:departments:read"
            )
        elif status == 429:
            hint = "rate limited — back off and retry"
        else:
            hint = "unexpected Rippling API error"
        raise RipplingError(f"Rippling API HTTP {status} for {url} ({hint}): {snippet}")

    def _request(self, url: str, params: dict[str, Any] | None) -> Any:
        try:
            resp = self._session.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        except requests.RequestException as e:
            raise RipplingError(f"Rippling API request to {url} failed: {e}") from e
        self._raise_for_status(resp, url)
        try:
            return resp.json()
        except ValueError as e:
            raise RipplingError(f"Rippling API returned non-JSON from {url}: {e}") from e

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request(f"{self.base_url}/{path.lstrip('/')}", params)

    def _paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        """Collect all rows across pages.

        Handles both Platform-API ``limit``/``offset`` list bodies and cursor-style
        ``{"results": [...], "next_link": "<url>"}`` bodies. Caps page size at 100
        (the API maximum) and guards against runaway cursors.
        """
        page_size = max(1, min(int(page_size), _DEFAULT_PAGE_SIZE))
        base_params = dict(params or {})
        results: list[dict[str, Any]] = []
        offset = 0
        next_url: str | None = None
        for _ in range(_MAX_PAGES):
            if next_url:
                data = self._request(next_url, None)
            else:
                q = dict(base_params, limit=page_size, offset=offset)
                data = self._get(path, q)

            if isinstance(data, dict):
                batch = data.get("results") or data.get("data") or data.get("items") or []
                results.extend(x for x in batch if isinstance(x, dict))
                next_url = data.get("next_link") or data.get("next") or None
                if next_url:
                    continue
                break  # dict without a cursor → single page
            if isinstance(data, list):
                rows = [x for x in data if isinstance(x, dict)]
                results.extend(rows)
                if len(data) < page_size:
                    break
                offset += page_size
                next_url = None
                continue
            break
        else:
            raise RipplingError(
                f"Rippling pagination for {path} exceeded {_MAX_PAGES} pages — aborting to avoid a loop."
            )
        return results

    # ── Preflight ───────────────────────────────────────────────────────────
    def check(self) -> tuple[bool, str | None]:
        """Light auth check: fetch one employee. Returns (ok, error_message)."""
        try:
            self._get("employees", {"limit": 1, "offset": 0})
            return True, None
        except RipplingError as e:
            return False, str(e)[:200]

    # ── Raw resource listers ─────────────────────────────────────────────────
    def list_employees(self, *, include_terminated: bool = False) -> list[dict[str, Any]]:
        """All employees (Platform API ``/employees``). Set ``include_terminated`` for the
        separate include-terminated endpoint."""
        path = "employees/include_terminated" if include_terminated else "employees"
        return self._paginate(path)

    def list_teams(self) -> list[dict[str, Any]]:
        """All company teams (``/teams`` → ``[{name, id, parent}]``)."""
        return self._paginate("teams")

    def list_departments(self) -> list[dict[str, Any]]:
        """All company departments (``/departments``)."""
        return self._paginate("departments")

    # ── Normalization ────────────────────────────────────────────────────────
    @staticmethod
    def _named(value: Any) -> str:
        """Pull a display name out of a string or ``{name|value|label|...}`` object."""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for k in ("name", "value", "label", "title", "displayName"):
                v = value.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    @classmethod
    def normalize_employee(cls, raw: dict[str, Any]) -> dict[str, Any]:
        """Normalize a raw employee record into a stable, tolerant shape."""
        first = (raw.get("firstName") or raw.get("preferredFirstName") or raw.get("first_name") or "").strip()
        last = (raw.get("lastName") or raw.get("last_name") or "").strip()
        full = (
            raw.get("fullName")
            or raw.get("displayName")
            or raw.get("name")
            or " ".join(p for p in (first, last) if p)
        )
        full = str(full).strip()
        email = (
            raw.get("workEmail")
            or raw.get("work_email")
            or raw.get("email")
            or cls._named(raw.get("workEmailObject"))
            or ""
        )
        title = cls._named(raw.get("title") or raw.get("jobTitle") or raw.get("role"))
        department = cls._named(raw.get("department") or raw.get("departmentObject"))
        manager_id = raw.get("manager") or raw.get("managerId") or raw.get("manager_id")
        if isinstance(manager_id, dict):
            manager_id = manager_id.get("id")
        teams = raw.get("teams") or raw.get("teamIds") or raw.get("team_ids") or []
        team_ids = [t.get("id") if isinstance(t, dict) else t for t in teams] if isinstance(teams, list) else []
        active = _truthy_active(
            raw.get("roleState")
            or raw.get("employmentStatus")
            or raw.get("status")
            or raw.get("activeStatus")
        )
        return {
            "id": raw.get("id") or raw.get("employeeId") or raw.get("workerId"),
            "name": full,
            "first_name": first,
            "last_name": last,
            "email": str(email).strip(),
            "title": title,
            "department": department,
            "manager_id": manager_id,
            "team_ids": [t for t in team_ids if t],
            "active": active,
        }

    # ── Roster ────────────────────────────────────────────────────────────────
    @classmethod
    def _pick_lead(cls, members: list[dict[str, Any]]) -> str:
        """Pick the most senior-titled member as the likely lead (best-effort)."""
        best_name = ""
        best_rank = len(_LEAD_TITLE_PRIORITY)  # lower = more senior
        for m in members:
            title = (m.get("title") or "").lower()
            match = _LEAD_TITLE_RE.search(title)
            if not match:
                continue
            rank = _LEAD_TITLE_PRIORITY.index(match.group(1).lower())
            if rank < best_rank:
                best_rank = rank
                best_name = m.get("name") or ""
        return best_name

    def get_org_roster(
        self,
        *,
        group_by: str = "department",
        active_only: bool = True,
        include_terminated: bool = False,
    ) -> dict[str, Any]:
        """Normalized org roster grouped by department (default) or team.

        Returns ``{group_by, total_employees, groups: [{name, headcount, members, lead}]}``
        where ``members`` is a list of display names and ``lead`` is a best-effort guess from
        member titles (blank when none is title-identifiable). ``group_by='team'`` requires
        team membership on employee records and uses ``/teams`` for id→name resolution.
        """
        if group_by not in {"department", "team"}:
            raise RipplingError(f"group_by must be 'department' or 'team', got {group_by!r}")

        employees = [self.normalize_employee(e) for e in self.list_employees(include_terminated=include_terminated)]
        if active_only:
            employees = [e for e in employees if e.get("active") is not False]

        team_name_by_id: dict[Any, str] = {}
        if group_by == "team":
            for t in self.list_teams():
                tid = t.get("id")
                nm = self._named(t)
                if tid is not None and nm:
                    team_name_by_id[tid] = nm

        grouped: dict[str, list[dict[str, Any]]] = {}
        for emp in employees:
            if group_by == "department":
                keys = [emp.get("department") or "Unassigned"]
            else:
                ids = emp.get("team_ids") or []
                keys = [team_name_by_id.get(tid, str(tid)) for tid in ids] or ["Unassigned"]
            for key in keys:
                grouped.setdefault(str(key), []).append(emp)

        groups = [
            {
                "name": name,
                "headcount": len(members),
                "members": [m.get("name") for m in members if m.get("name")],
                "lead": self._pick_lead(members),
            }
            for name, members in grouped.items()
        ]
        groups.sort(key=lambda g: -g["headcount"])
        return {
            "group_by": group_by,
            "total_employees": len(employees),
            "groups": groups,
        }


def get_rippling_client(**kwargs: Any) -> RipplingClient:
    """Construct a configured :class:`RipplingClient` (raises if no API key)."""
    return RipplingClient(**kwargs)


def check_rippling_api() -> tuple[bool, str | None]:
    """Deck-preflight helper: (True, None) when unconfigured or reachable, else (False, msg)."""
    if not rippling_configured():
        return True, None
    try:
        return RipplingClient().check()
    except RipplingError as e:
        logger.warning("Rippling preflight failed: %s", e)
        return False, str(e)[:200]
