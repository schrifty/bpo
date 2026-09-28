"""Chorus REST API client.

Covers the documented Chorus API (``src.chorus_routes.CHORUS_ROUTES``).
Calls go to ``https://chorus.ai`` with ``CHORUS_API_KEY`` as the raw
``Authorization`` value (no ``Bearer`` prefix).

``/api/v1`` responses are JSON:API (``Accept: application/vnd.api+json``).
``/v3`` responses are plain JSON. Do not send ``x-al-version``: the value
documented elsewhere (``2``) is rejected by the live API.

``request`` / ``get`` / ``list_all`` accept any documented route. Named helpers
cover the reads Cortex is most likely to use (current user, users, engagements,
conversations, emails, scorecards, playlists, teams, moments). Recording upload
is ``upload_conversation``. Other writes use ``request``.

``list_all`` follows ``continuation_key`` (engagements), ``meta.page.cursor``
via ``page[after]`` (emails), ``meta.total_pages`` (playlists), and
``page[number]`` when ``page[size]`` is set and a page comes back full
(scorecards). A repeated page of the same ids stops the walk. ``429``,
``500``, and ``504`` are retried. Any other non-2xx response raises
:class:`ChorusClientError`.
"""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import quote

import requests

from .chorus_routes import CHORUS_ROUTES
from .config import CHORUS_API_BASE_URL, CHORUS_API_KEY, logger

_DEFAULT_TIMEOUT_S = 60.0
_DEFAULT_PAGE_SIZE = 100
_MAX_PAGE_SIZE = 100
_MAX_PAGES = 200
_RATE_LIMIT_MAX_RETRIES = 3
_RATE_LIMIT_BACKOFF_BASE_S = 2.0
_RATE_LIMIT_BACKOFF_CAP_S = 60.0
_USER_AGENT = "Cortex chorus-client (https://chorus.ai)"
_V1_ACCEPT = "application/vnd.api+json"
_V3_ACCEPT = "application/json"


class ChorusClientError(Exception):
    """A Chorus API request failed or the client is misconfigured."""


def chorus_configured() -> bool:
    """True when ``CHORUS_API_KEY`` is set."""
    return bool(CHORUS_API_KEY and str(CHORUS_API_KEY).strip())


def chorus_route_exists(method: str, path: str) -> bool:
    """True when ``method`` + ``path`` matches a documented Chorus route."""
    method_u = (method or "").strip().upper()
    parts = [seg for seg in (path or "").split("?")[0].split("/") if seg]
    for template in CHORUS_ROUTES.get(method_u, ()):
        tparts = [seg for seg in template.split("/") if seg]
        if len(tparts) != len(parts):
            continue
        if all(
            (tp.startswith("{") and tp.endswith("}") and pp) or tp == pp
            for tp, pp in zip(tparts, parts)
        ):
            return True
    return False


def check_chorus_api() -> tuple[bool, str | None]:
    """Return ``(True, None)`` when Chorus is unset or ``GET /api/v1/users/me`` succeeds."""
    if not chorus_configured():
        return True, None
    try:
        ChorusClient().me()
        return True, None
    except ChorusClientError as exc:
        return False, f"Chorus: {exc}"[:200]


def _page_records(body: Any) -> tuple[list[Any], dict[str, Any]]:
    """Return ``(records, envelope)`` from a Chorus list payload."""
    if isinstance(body, list):
        return body, {}
    if not isinstance(body, dict):
        raise ChorusClientError(
            f"Chorus list response was {type(body).__name__}, expected an object or array"
        )
    if isinstance(body.get("data"), list):
        return body["data"], body
    if isinstance(body.get("engagements"), list):
        return body["engagements"], body
    for key, val in body.items():
        if key in ("meta", "links", "pagination", "errors"):
            continue
        if isinstance(val, list):
            return val, body
    raise ChorusClientError("Chorus list response did not include a record collection")


def _row_identity(row: Any) -> str | None:
    if not isinstance(row, dict):
        return None
    for key in ("id", "engagement_id"):
        val = row.get(key)
        if val is not None and str(val).strip():
            return f"{key}:{val}"
    return None


def _next_query(body: dict[str, Any], query: dict[str, Any], batch: list[Any]) -> dict[str, Any] | None:
    """Return the next page's query, or ``None`` when this page is the last."""
    if not batch:
        return None
    continuation = body.get("continuation_key")
    if continuation:
        token = str(continuation).strip()
        if not token or token == str(query.get("continuation_key") or ""):
            return None
        nxt = dict(query)
        nxt["continuation_key"] = token
        return nxt
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    if isinstance(meta.get("page"), dict):
        token = str(meta["page"].get("cursor") or "").strip()
        if not token or token == str(query.get("page[after]") or ""):
            return None
        nxt = dict(query)
        nxt["page[after]"] = token
        return nxt
    if meta.get("total_pages") is not None:
        try:
            total_pages = int(meta["total_pages"])
            current = int(meta.get("page_number") or query.get("page[number]") or 1)
        except (TypeError, ValueError) as exc:
            raise ChorusClientError(
                f"Chorus pagination is not an integer: {meta!r}"
            ) from exc
        if current >= total_pages:
            return None
        nxt = dict(query)
        nxt["page[number]"] = current + 1
        return nxt
    if "page[size]" in query and isinstance(body.get("data"), list):
        try:
            size = int(query["page[size]"])
        except (TypeError, ValueError) as exc:
            raise ChorusClientError(
                f"page[size] must be an integer, got {query.get('page[size]')!r}"
            ) from exc
        if len(batch) < size:
            return None
        nxt = dict(query)
        nxt["page[number]"] = int(query.get("page[number]") or 1) + 1
        return nxt
    return None


class ChorusClient:
    """Fail-loud wrapper over the Chorus REST API."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        session: Any | None = None,
        timeout: float = _DEFAULT_TIMEOUT_S,
        max_retries: int | None = None,
        sleeper: Any | None = None,
    ) -> None:
        key = api_key if api_key is not None else CHORUS_API_KEY
        self.api_key = str(key or "").strip()
        if self.api_key.lower().startswith("bearer "):
            self.api_key = self.api_key[7:].strip()
        if not self.api_key:
            raise ChorusClientError(
                "Chorus API key is not configured. Set CHORUS_API_KEY "
                "(create one in Chorus Settings → Personal Settings → API Access)."
            )
        raw_base = (base_url if base_url is not None else CHORUS_API_BASE_URL) or ""
        self.base_url = str(raw_base).strip().rstrip("/")
        if not self.base_url.startswith("https://"):
            raise ChorusClientError(
                f"CHORUS_API_BASE_URL must be an https URL, got {raw_base!r}"
            )
        self.timeout = float(timeout)
        self._max_retries = (
            _RATE_LIMIT_MAX_RETRIES if max_retries is None else max(0, int(max_retries))
        )
        self._session = session or requests.Session()
        self._sleep = sleeper or time.sleep

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        form: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
    ) -> Any:
        """Call one documented route and return the parsed JSON body."""
        method_u = (method or "").strip().upper()
        rel = "/" + str(path or "").lstrip("/")
        rel_path = rel.split("?", 1)[0]
        if not chorus_route_exists(method_u, rel_path):
            raise ChorusClientError(f"Chorus API has no {method_u} {rel_path}")
        if json_body is not None and (form is not None or files is not None):
            raise ChorusClientError("Chorus request cannot send JSON and form data together")
        url = self._url(rel_path)
        resp = self._send(
            method_u,
            url,
            params=params,
            json_body=json_body,
            form=form,
            files=files,
        )
        return self._parse_json(resp, url)

    def get(self, path: str, **params: Any) -> Any:
        """``GET`` a documented path. Query values of ``None`` are omitted."""
        query = {key: val for key, val in params.items() if val is not None}
        return self.request("GET", path, params=query or None)

    def list_all(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        max_pages: int = _MAX_PAGES,
    ) -> list[Any]:
        """Follow Chorus pagination until the collection is exhausted."""
        query = {key: val for key, val in (params or {}).items() if val is not None}
        self._check_page_size(query.get("page[size]"))
        records: list[Any] = []
        seen: set[str] = set()
        pages_left = max(1, int(max_pages))
        while pages_left > 0:
            body = self.get(path, **query)
            batch, envelope = _page_records(body)
            fresh: list[Any] = []
            novel = False
            for row in batch:
                ident = _row_identity(row)
                if ident is not None:
                    if ident in seen:
                        continue
                    seen.add(ident)
                novel = True
                fresh.append(row)
            if batch and not novel:
                return records
            records.extend(fresh)
            pages_left -= 1
            if not isinstance(envelope, dict):
                return records
            nxt = _next_query(envelope, query, batch)
            if nxt is None:
                return records
            query = nxt
        raise ChorusClientError(
            f"Chorus list {path} stopped after {max_pages} pages "
            f"({len(records)} records); pass a narrower filter or a higher max_pages"
        )

    def me(self) -> Any:
        """Authenticated user (``GET /api/v1/users/me``)."""
        return self.get("/api/v1/users/me")

    def list_users(self, **params: Any) -> list[Any]:
        """Every user visible to the token (``GET /v3/users``)."""
        return self.list_all("/v3/users", params=params)

    def list_engagements(self, **params: Any) -> list[Any]:
        """Calls and meetings (``GET /v3/engagements``).

        ``min_date`` and ``max_date`` are Unix epoch seconds. ``continuation_key``
        is filled in by :meth:`list_all`.
        """
        return self.list_all("/v3/engagements", params=params)

    def get_conversation(self, conversation_id: str, **params: Any) -> Any:
        return self.get(
            f"/api/v1/conversations/{_required_id(conversation_id, 'conversation_id')}",
            **params,
        )

    def list_emails(self, **params: Any) -> list[Any]:
        return self.list_all("/api/v1/emails", params=_paged(params))

    def get_email(self, email_id: str, **params: Any) -> Any:
        return self.get(f"/api/v1/emails/{_required_id(email_id, 'email_id')}", **params)

    def get_email_thread(self, thread_id: str, **params: Any) -> Any:
        return self.get(
            f"/api/v1/email_threads/{_required_id(thread_id, 'thread_id')}",
            **params,
        )

    def list_scorecards(self, **params: Any) -> list[Any]:
        return self.list_all("/api/v1/scorecards", params=_paged(params))

    def list_playlists(self, **params: Any) -> list[Any]:
        return self.list_all("/api/v1/playlists", params=_paged(params))

    def get_playlist(self, playlist_id: str, **params: Any) -> Any:
        return self.get(
            f"/api/v1/playlists/{_required_id(playlist_id, 'playlist_id')}",
            **params,
        )

    def list_teams(self, **params: Any) -> list[Any]:
        return self.list_all("/api/v1/teams", params=params)

    def get_team(self, team_id: str, **params: Any) -> Any:
        return self.get(f"/api/v1/teams/{_required_id(team_id, 'team_id')}", **params)

    def list_moments(self, **params: Any) -> list[Any]:
        """Shared moments. ``filter[shared_on]`` is required by Chorus."""
        if not str(params.get("filter[shared_on]") or "").strip():
            raise ChorusClientError(
                "GET /api/v1/moments requires filter[shared_on] (an ISO-8601 range)"
            )
        return self.list_all("/api/v1/moments", params=_paged(params))

    def list_filters(self, **params: Any) -> list[Any]:
        return self.list_all("/api/v1/filters", params=params)

    def upload_conversation(
        self,
        recording: Any,
        *,
        name: str,
        owner_email: str,
        filename: str = "recording",
        content_type: str = "application/octet-stream",
        crm_account_id: str | None = None,
        crm_opportunity_id: str | None = None,
        meeting_id: str | None = None,
        enforce_unique_meeting_id: bool | None = None,
    ) -> Any:
        """Upload a recording (``POST /v3/upload``, multipart)."""
        title = str(name or "").strip()
        owner = str(owner_email or "").strip()
        if not title:
            raise ChorusClientError("name is required to upload a Chorus recording")
        if not owner:
            raise ChorusClientError("owner_email is required to upload a Chorus recording")
        if recording is None or recording == b"":
            raise ChorusClientError("recording bytes are required to upload a Chorus recording")
        form: dict[str, Any] = {"name": title, "user": owner}
        if crm_account_id:
            form["crm_account_id"] = crm_account_id
        if crm_opportunity_id:
            form["crm_opportunity_id"] = crm_opportunity_id
        if meeting_id:
            form["meeting_id"] = meeting_id
        if enforce_unique_meeting_id is not None:
            form["enforce_unique_meeting_id"] = "true" if enforce_unique_meeting_id else "false"
        files = {"data": (filename or "recording", recording, content_type)}
        return self.request("POST", "/v3/upload", form=form, files=files)

    def _url(self, path: str) -> str:
        segments = [quote(seg, safe=":") for seg in path.split("/") if seg]
        return f"{self.base_url}/" + "/".join(segments)

    def _headers(self, *, json_body: Any | None, path: str) -> dict[str, str]:
        accept = _V3_ACCEPT if path.startswith("/v3/") else _V1_ACCEPT
        headers = {
            "Authorization": self.api_key,
            "Accept": accept,
            "User-Agent": _USER_AGENT,
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        return headers

    def _send(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None,
        json_body: Any | None,
        form: dict[str, Any] | None,
        files: dict[str, Any] | None,
    ) -> requests.Response:
        path = url[len(self.base_url) :] if url.startswith(self.base_url) else url
        for attempt in range(self._max_retries + 1):
            last = attempt >= self._max_retries
            try:
                resp = self._session.request(
                    method,
                    url,
                    headers=self._headers(json_body=json_body, path=path),
                    params=params,
                    json=json_body,
                    data=form,
                    files=files,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if last:
                    raise ChorusClientError(f"Chorus API request to {url} failed: {exc}") from exc
                self._pause(attempt)
                continue
            status = getattr(resp, "status_code", 0)
            if status == 429 and not last:
                self._pause(attempt)
                continue
            if status in (500, 504) and not last:
                logger.warning("Chorus API HTTP %s for %s; retrying", status, url)
                self._pause(attempt)
                continue
            self._raise_for_status(resp, url)
            return resp
        raise ChorusClientError(
            f"Chorus API request to {url} failed after {self._max_retries} retries"
        )

    def _pause(self, attempt: int) -> None:
        wait = min(_RATE_LIMIT_BACKOFF_BASE_S * (2 ** attempt), _RATE_LIMIT_BACKOFF_CAP_S)
        wait += random.uniform(0.0, 0.25)
        logger.warning("Chorus API backing off %.1fs before retry", wait)
        self._sleep(wait)

    @staticmethod
    def _parse_json(resp: requests.Response, url: str) -> Any:
        text = (getattr(resp, "text", "") or "").strip()
        if not text:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            raise ChorusClientError(f"Chorus API returned non-JSON from {url}") from exc

    def _raise_for_status(self, resp: requests.Response, url: str) -> None:
        if getattr(resp, "ok", False):
            return
        status = getattr(resp, "status_code", "?")
        detail = _error_detail(resp)
        if status in (401, 403):
            hint = "authentication failed — check CHORUS_API_KEY" if status == 401 else "the token cannot call this route"
        elif status == 404:
            hint = "record not found"
        elif status == 429:
            hint = "rate limited"
        elif status == 400:
            hint = "the request was rejected"
        elif status == 504:
            hint = "timed out — request a smaller page"
        else:
            hint = "unexpected Chorus API error"
        extra = f": {detail}" if detail else ""
        raise ChorusClientError(f"Chorus API HTTP {status} for {url} ({hint}){extra}")

    @staticmethod
    def _check_page_size(value: Any) -> None:
        if value is None:
            return
        try:
            size = int(value)
        except (TypeError, ValueError) as exc:
            raise ChorusClientError(f"page[size] must be an integer, got {value!r}") from exc
        if size < 1 or size > _MAX_PAGE_SIZE:
            raise ChorusClientError(
                f"page[size] must be from 1 to {_MAX_PAGE_SIZE}, got {size}"
            )


def _paged(params: dict[str, Any]) -> dict[str, Any]:
    query = {key: val for key, val in params.items() if val is not None}
    query.setdefault("page[size]", _DEFAULT_PAGE_SIZE)
    return query


def _required_id(value: str, name: str) -> str:
    text = str(value or "").strip()
    if not text or "/" in text:
        raise ChorusClientError(f"{name} must be a single path segment, got {value!r}")
    return text


def _error_detail(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        errors = body.get("errors")
        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
            detail = str(errors[0].get("detail") or errors[0].get("title") or "").strip()
            if detail:
                return detail[:300]
    return (getattr(resp, "text", "") or "").strip().replace("\n", " ")[:300]
