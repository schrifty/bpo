"""Authenticated HTTP for LeanDNA Data API paths under ``/data/``.

``data_api_get_json`` performs validated ``GET`` requests.
``data_api_mutate_json`` performs validated ``POST``, ``PUT``, or ``DELETE`` (mutations).

Paths are normalized before concatenation with the resolved Data API base URL
(``resolve_leandna_data_api_base_url()`` / ``EXECUTION_ENV`` — see ``src.config``) — no arbitrary hosts.

See ``docs/DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`` for resource inventory;
per-field contracts follow tenant OpenAPI (``scripts/fetch_leandna_swagger.py``).
"""

from __future__ import annotations

import json
import re
from typing import Any

import requests

from .config import leandna_http_mutation_blocked_envelope, logger, resolve_leandna_data_api_base_url
from .leandna_data_api_http import build_leandna_data_api_headers

# Path under /data/ — letters, digits, slashes, hyphens, underscores, dots,
# commas (e.g. LeanProject id lists), braces (OpenAPI path templates — caller passes literal segment).
_DATA_PATH_RE = re.compile(r"^[A-Za-z0-9_./,\-{}]+$")


def normalize_data_api_relative_path(path: str) -> str:
    """Return path segment(s) after ``/data/`` with no leading slash.

    Accepts ``ItemMasterData``, ``/data/Metric``, or ``data/MaterialShortages/...``.
    Raises:
        ValueError: empty, traversal, or disallowed characters.
    """
    raw = (path or "").strip()
    if not raw:
        raise ValueError("path is empty")
    p = raw.lstrip("/")
    lower = p.lower()
    if lower.startswith("data/"):
        p = p[5:].lstrip("/")
    if not p or ".." in p or p.startswith("//"):
        raise ValueError(f"invalid Data API path: {path!r}")
    if not _DATA_PATH_RE.fullmatch(p):
        raise ValueError(f"path contains disallowed characters: {path!r}")
    return p


def data_api_base_url() -> str:
    return resolve_leandna_data_api_base_url()


def _detail_from_error_body(text: str) -> str:
    """Best-effort message from a failed Data API response body."""
    snippet = (text or "").strip().replace("\n", " ")[:800]
    if not snippet:
        return ""
    if snippet.startswith("{"):
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                for key in ("reason", "message", "error", "detail"):
                    val = parsed.get(key)
                    if val is not None and str(val).strip():
                        return str(val).strip()
        except json.JSONDecodeError:
            pass
    return snippet[:200]


def format_data_api_error_envelope(
    env: dict[str, Any],
    *,
    cred_prefix: str | None = None,
) -> str:
    """Human-readable error from ``data_api_get_json`` / ``env_get_json`` failure envelopes."""
    if env.get("ok"):
        return ""
    status = env.get("status")
    preview = (env.get("body_preview") or "").strip()
    detail = _detail_from_error_body(preview) or (env.get("error") or "").strip() or "request failed"
    if status is not None:
        line = f"LeanDNA Data API {status}: {detail}"
    else:
        line = f"LeanDNA Data API: {detail}"
    if status == 401 and cred_prefix:
        line += (
            f" — refresh {cred_prefix}LEANDNA_DATA_API_BEARER_TOKEN or set "
            f"{cred_prefix}LEANDNA_DATA_API_COOKIE from browser DevTools while logged into app.leandna.com"
        )
    return line


def _response_envelope(
    r: requests.Response,
    *,
    url: str,
    max_response_chars: int,
) -> dict[str, Any]:
    """Map a finished ``requests.Response`` to a JSON-serializable tool/client envelope."""
    text = r.text or ""
    if not r.ok:
        snippet = text.strip().replace("\n", " ")[:800]
        detail = _detail_from_error_body(text) or (r.reason or "").strip() or "HTTP error"
        return {
            "ok": False,
            "status": r.status_code,
            "error": detail,
            "body_preview": snippet,
            "url": url,
        }

    truncated = False
    if len(text) > max_response_chars:
        truncated = True
        text = text[:max_response_chars]

    if not text.strip():
        return {"ok": True, "status": r.status_code, "truncated": truncated, "url": url, "body": None}

    try:
        body: Any = json.loads(text)
    except json.JSONDecodeError:
        return {
            "ok": True,
            "status": r.status_code,
            "truncated": truncated,
            "non_json": True,
            "text_preview": text[:2000],
            "url": url,
        }

    return {"ok": True, "status": r.status_code, "truncated": truncated, "url": url, "body": body}


def data_api_get_json(
    path: str,
    *,
    query: dict[str, Any] | None = None,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
    max_response_chars: int = 500_000,
    user_agent_suffix: str = "leandna-data-api-request/1.0",
) -> dict[str, Any]:
    """Perform ``GET {base}/data/{path}`` with LeanDNA auth headers.

    Returns a dict suitable for JSON serialization — either parsed body or an error envelope.
    Large responses are truncated with ``truncated: true`` (never silent empty success).
    """
    try:
        rel = normalize_data_api_relative_path(path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    try:
        base = data_api_base_url()
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    url = f"{base}/data/{rel}"
    params = {k: v for k, v in (query or {}).items() if v is not None and v != ""}

    try:
        headers = build_leandna_data_api_headers(
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e), "hint": "Set LEANDNA_DATA_API_BEARER_TOKEN and/or LEANDNA_DATA_API_COOKIE"}

    logger.info("LeanDNA Data API GET %s params=%s", url, list(params.keys()) if params else "none")
    try:
        r = requests.get(url, headers=headers, params=params or None, timeout=timeout_seconds)
    except requests.RequestException as e:
        return {"ok": False, "error": f"request failed: {e}", "url": url}

    return _response_envelope(r, url=url, max_response_chars=max_response_chars)


def data_api_mutate_json(
    method: str,
    path: str,
    *,
    query: dict[str, Any] | None = None,
    json_body: Any | None = None,
    requested_sites: str | None = None,
    timeout_seconds: float = 120.0,
    max_response_chars: int = 500_000,
    user_agent_suffix: str = "leandna-data-api-request/1.0",
) -> dict[str, Any]:
    """Perform ``POST`` / ``PUT`` / ``DELETE`` on ``{base}/data/{path}`` with LeanDNA auth.

    ``json_body``: sent as JSON for ``POST`` and ``PUT`` when not ``None``. Omitted when ``None``
    (empty body). Ignored for ``DELETE``.

    Returns the same envelope shape as :func:`data_api_get_json` (``ok``, ``body`` or error fields).
    """
    m = (method or "").strip().upper()
    if m not in ("POST", "PUT", "DELETE"):
        return {"ok": False, "error": f"method must be POST, PUT, or DELETE, not {method!r}"}

    blocked = leandna_http_mutation_blocked_envelope(method=m, path=path)
    if blocked is not None:
        return blocked

    try:
        rel = normalize_data_api_relative_path(path)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    try:
        base = data_api_base_url()
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    url = f"{base}/data/{rel}"
    params = {k: v for k, v in (query or {}).items() if v is not None and v != ""}

    try:
        headers = build_leandna_data_api_headers(
            requested_sites=requested_sites,
            user_agent_suffix=user_agent_suffix,
            content_type_json=m in ("POST", "PUT") and json_body is not None,
        )
    except ValueError as e:
        return {"ok": False, "error": str(e), "hint": "Set LEANDNA_DATA_API_BEARER_TOKEN and/or LEANDNA_DATA_API_COOKIE"}

    kw: dict[str, Any] = {"headers": headers, "timeout": timeout_seconds}
    if params:
        kw["params"] = params
    if m in ("POST", "PUT") and json_body is not None:
        kw["json"] = json_body

    logger.info("LeanDNA Data API %s %s params=%s has_json_body=%s", m, url, list(params.keys()) if params else "none", json_body is not None)
    try:
        r = requests.request(m, url, **kw)
    except requests.RequestException as e:
        return {"ok": False, "error": f"request failed: {e}", "url": url}

    return _response_envelope(r, url=url, max_response_chars=max_response_chars)
