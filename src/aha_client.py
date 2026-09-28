"""Aha! REST API client.

Covers the documented Aha! API (``src.aha_routes.AHA_ROUTES``, from
https://www.aha.io/openapi.json). Calls go to
``https://{domain}.aha.io/api/v1`` with ``CORTEX_AHA_API_KEY`` as a Bearer token.

``request`` / ``get`` / ``list_all`` accept any documented route. Named helpers
cover the account collections Cortex is most likely to read (products, features,
ideas, epics, goals, initiatives, releases, users, idea portals). Writes use
``request`` with ``POST``, ``PUT``, or ``DELETE`` on a documented path.

Pagination follows Aha's ``page`` / ``per_page`` (max 200) and the ``pagination``
object on list responses. ``429``, ``500``, and ``504`` are retried. Any other
non-2xx response raises :class:`AhaClientError`.
"""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import quote

import requests

from .aha_routes import AHA_ROUTES
from .config import CORTEX_AHA_API_KEY, CORTEX_AHA_DOMAIN, logger

_DEFAULT_TIMEOUT_S = 60.0
_DEFAULT_PER_PAGE = 200
_MAX_PER_PAGE = 200
_MAX_PAGES = 100
_RATE_LIMIT_MAX_RETRIES = 3
_RATE_LIMIT_BACKOFF_BASE_S = 2.0
_RATE_LIMIT_BACKOFF_CAP_S = 60.0
_USER_AGENT = "Cortex aha-client (https://www.aha.io/api)"


class AhaClientError(Exception):
    """An Aha! API request failed or the client is misconfigured."""


def aha_configured() -> bool:
    """True when ``CORTEX_AHA_API_KEY`` is set."""
    return bool(CORTEX_AHA_API_KEY and str(CORTEX_AHA_API_KEY).strip())


def normalize_aha_domain(raw: str | None) -> str:
    """Return the account subdomain from a host, URL, or bare name."""
    text = (raw or "").strip().lower()
    if not text:
        raise AhaClientError(
            "Aha account domain is not configured. Set CORTEX_AHA_DOMAIN "
            "to the subdomain in https://<domain>.aha.io."
        )
    for prefix in ("https://", "http://"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
    text = text.split("/", 1)[0]
    if text.endswith(".aha.io"):
        text = text[: -len(".aha.io")]
    if not text or any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789-" for ch in text):
        raise AhaClientError(
            f"CORTEX_AHA_DOMAIN {raw!r} is not a valid Aha account subdomain."
        )
    if text.startswith("-") or text.endswith("-"):
        raise AhaClientError(
            f"CORTEX_AHA_DOMAIN {raw!r} is not a valid Aha account subdomain."
        )
    return text


def aha_route_exists(method: str, path: str) -> bool:
    """True when ``method`` + ``path`` matches a documented Aha! route."""
    method_u = (method or "").strip().upper()
    parts = [seg for seg in (path or "").split("?")[0].split("/") if seg]
    for template in AHA_ROUTES.get(method_u, ()):
        tparts = [seg for seg in template.split("/") if seg]
        if len(tparts) != len(parts):
            continue
        if all(
            (tp.startswith("{") and tp.endswith("}") and pp) or tp == pp
            for tp, pp in zip(tparts, parts)
        ):
            return True
    return False


def check_aha_api() -> tuple[bool, str | None]:
    """Return ``(True, None)`` when Aha is unset or ``GET /me`` succeeds."""
    if not aha_configured():
        return True, None
    try:
        AhaClient().me()
        return True, None
    except AhaClientError as exc:
        return False, f"Aha: {exc}"[:200]


def _collection_page(body: Any) -> tuple[list[Any], int | None]:
    """Return ``(records, total_pages)`` from an Aha list payload."""
    if not isinstance(body, dict):
        raise AhaClientError(
            f"Aha list response was {type(body).__name__}, expected an object"
        )
    pagination = body.get("pagination")
    total_pages: int | None = None
    page_meta = None
    if isinstance(pagination, list) and pagination:
        page_meta = pagination[0]
    elif isinstance(pagination, dict):
        page_meta = pagination
    if isinstance(page_meta, dict) and page_meta.get("total_pages") is not None:
        try:
            total_pages = int(page_meta["total_pages"])
        except (TypeError, ValueError) as exc:
            raise AhaClientError(
                f"Aha pagination total_pages is not an integer: {page_meta.get('total_pages')!r}"
            ) from exc
    for key, val in body.items():
        if key == "pagination":
            continue
        if isinstance(val, list):
            return val, total_pages
    raise AhaClientError("Aha list response did not include a record collection")


class AhaClient:
    """Fail-loud wrapper over the Aha! REST API."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        domain: str | None = None,
        session: Any | None = None,
        timeout: float = _DEFAULT_TIMEOUT_S,
        max_retries: int | None = None,
        sleeper: Any | None = None,
    ) -> None:
        key = api_key if api_key is not None else CORTEX_AHA_API_KEY
        self.api_key = str(key or "").strip()
        if not self.api_key:
            raise AhaClientError(
                "Aha API key is not configured. Set CORTEX_AHA_API_KEY "
                "(create one at https://secure.aha.io/settings/api_keys)."
            )
        self.domain = normalize_aha_domain(domain if domain is not None else CORTEX_AHA_DOMAIN)
        self.base_url = f"https://{self.domain}.aha.io/api/v1"
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
    ) -> Any:
        """Call one documented route and return the parsed JSON body."""
        method_u = (method or "").strip().upper()
        rel = "/" + str(path or "").lstrip("/")
        rel_path = rel.split("?", 1)[0]
        if not aha_route_exists(method_u, rel_path):
            raise AhaClientError(f"Aha API has no {method_u} {rel_path}")
        url = self._url(rel_path)
        resp = self._send(method_u, url, params=params, json_body=json_body)
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
        """Follow ``page`` until the collection is exhausted."""
        query = {key: val for key, val in (params or {}).items() if val is not None}
        try:
            per_page = int(query.get("per_page") or _DEFAULT_PER_PAGE)
        except (TypeError, ValueError) as exc:
            raise AhaClientError(f"per_page must be an integer, got {query.get('per_page')!r}") from exc
        if per_page < 1 or per_page > _MAX_PER_PAGE:
            raise AhaClientError(f"per_page must be from 1 to {_MAX_PER_PAGE}, got {per_page}")
        query["per_page"] = per_page
        page = int(query.pop("page", None) or 1)
        if page < 1:
            raise AhaClientError(f"page must be >= 1, got {page}")
        records: list[Any] = []
        pages_left = max(1, int(max_pages))
        while pages_left > 0:
            body = self.get(path, page=page, **query)
            batch, total_pages = _collection_page(body)
            records.extend(batch)
            pages_left -= 1
            if total_pages is not None and page >= total_pages:
                break
            if total_pages is None and len(batch) < per_page:
                break
            if not batch:
                break
            page += 1
        return records

    def me(self) -> Any:
        """Authenticated user and account (``GET /me``)."""
        return self.get("/me")

    def list_products(self, **params: Any) -> list[Any]:
        return self.list_all("/products", params=params)

    def get_product(self, product_id: str, **params: Any) -> Any:
        return self.get(f"/products/{_required_id(product_id, 'product_id')}", **params)

    def list_features(self, **params: Any) -> list[Any]:
        return self.list_all("/features", params=params)

    def get_feature(self, feature_id: str, **params: Any) -> Any:
        return self.get(f"/features/{_required_id(feature_id, 'feature_id')}", **params)

    def list_epics(self, **params: Any) -> list[Any]:
        return self.list_all("/epics", params=params)

    def get_epic(self, epic_id: str, **params: Any) -> Any:
        return self.get(f"/epics/{_required_id(epic_id, 'epic_id')}", **params)

    def list_ideas(self, **params: Any) -> list[Any]:
        return self.list_all("/ideas", params=params)

    def get_idea(self, idea_id: str, **params: Any) -> Any:
        return self.get(f"/ideas/{_required_id(idea_id, 'idea_id')}", **params)

    def list_goals(self, **params: Any) -> list[Any]:
        return self.list_all("/goals", params=params)

    def get_goal(self, goal_id: str, **params: Any) -> Any:
        return self.get(f"/goals/{_required_id(goal_id, 'goal_id')}", **params)

    def list_initiatives(self, **params: Any) -> list[Any]:
        return self.list_all("/initiatives", params=params)

    def get_initiative(self, initiative_id: str, **params: Any) -> Any:
        return self.get(f"/initiatives/{_required_id(initiative_id, 'initiative_id')}", **params)

    def get_release(self, release_id: str, **params: Any) -> Any:
        return self.get(f"/releases/{_required_id(release_id, 'release_id')}", **params)

    def list_users(self, **params: Any) -> list[Any]:
        return self.list_all("/users", params=params)

    def get_user(self, user_id: str, **params: Any) -> Any:
        return self.get(f"/users/{_required_id(user_id, 'user_id')}", **params)

    def list_idea_portals(self, **params: Any) -> list[Any]:
        return self.list_all("/idea_portals", params=params)

    def list_product_features(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/features", params=params
        )

    def list_product_ideas(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/ideas", params=params
        )

    def list_product_epics(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/epics", params=params
        )

    def list_product_releases(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/releases", params=params
        )

    def list_product_goals(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/goals", params=params
        )

    def list_product_initiatives(self, product_id: str, **params: Any) -> list[Any]:
        return self.list_all(
            f"/products/{_required_id(product_id, 'product_id')}/initiatives", params=params
        )

    def _url(self, path: str) -> str:
        segments = [quote(seg, safe="") for seg in path.split("/") if seg]
        return f"{self.base_url}/" + "/".join(segments)

    def _headers(self, *, json_body: Any | None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
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
    ) -> requests.Response:
        for attempt in range(self._max_retries + 1):
            last = attempt >= self._max_retries
            try:
                resp = self._session.request(
                    method,
                    url,
                    headers=self._headers(json_body=json_body),
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if last:
                    raise AhaClientError(f"Aha API request to {url} failed: {exc}") from exc
                self._pause(attempt, None)
                continue
            status = getattr(resp, "status_code", 0)
            if status == 429 and not last:
                self._pause(attempt, (resp.headers or {}).get("X-Ratelimit-Reset"))
                continue
            if status in (500, 504) and not last:
                logger.warning("Aha API HTTP %s for %s; retrying", status, url)
                self._pause(attempt, None)
                continue
            self._raise_for_status(resp, url)
            return resp
        raise AhaClientError(f"Aha API request to {url} failed after {self._max_retries} retries")

    def _pause(self, attempt: int, reset_at: str | None) -> None:
        wait = _RATE_LIMIT_BACKOFF_BASE_S * (2 ** attempt)
        if reset_at:
            try:
                wait = max(0.0, float(reset_at) - time.time())
            except (TypeError, ValueError):
                pass
        wait = min(wait, _RATE_LIMIT_BACKOFF_CAP_S) + random.uniform(0.0, 0.25)
        logger.warning("Aha API backing off %.1fs before retry", wait)
        self._sleep(wait)

    @staticmethod
    def _parse_json(resp: requests.Response, url: str) -> Any:
        text = (getattr(resp, "text", "") or "").strip()
        if not text:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            raise AhaClientError(f"Aha API returned non-JSON from {url}") from exc

    def _raise_for_status(self, resp: requests.Response, url: str) -> None:
        if getattr(resp, "ok", False):
            return
        status = getattr(resp, "status_code", "?")
        header_msg = ""
        headers = getattr(resp, "headers", None) or {}
        if hasattr(headers, "get"):
            header_msg = (headers.get("X-Error-Message") or "").strip()
        snippet = header_msg or (getattr(resp, "text", "") or "").strip().replace("\n", " ")[:300]
        if status in (401, 403):
            hint = (
                f"authentication failed for {self.domain}.aha.io — check CORTEX_AHA_API_KEY "
                "and CORTEX_AHA_DOMAIN"
            )
        elif status == 404:
            hint = "record not found, or this API key cannot see it"
        elif status == 429:
            hint = "rate limited (300 requests/minute, 20/second)"
        elif status == 400:
            hint = "the request was rejected"
        elif status == 504:
            hint = "timed out — request fewer fields or a smaller page"
        else:
            hint = "unexpected Aha API error"
        detail = f": {snippet}" if snippet else ""
        raise AhaClientError(f"Aha API HTTP {status} for {url} ({hint}){detail}")


def _required_id(value: str, name: str) -> str:
    text = str(value or "").strip()
    if not text or "/" in text:
        raise AhaClientError(f"{name} must be a single path segment, got {value!r}")
    return text
