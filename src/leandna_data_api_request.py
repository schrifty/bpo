"""Low-level authenticated GET for LeanDNA Data API paths under ``/data/``.

Used by LangChain tools and ad-hoc callers. Paths are normalized and validated
before concatenation with ``LEANDNA_DATA_API_BASE_URL`` — no arbitrary hosts.

See ``docs/DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`` for resource inventory;
per-field contracts follow tenant OpenAPI (``scripts/fetch_leandna_swagger.py``).
"""

from __future__ import annotations

import json
import re
from typing import Any

import requests

from .config import LEANDNA_DATA_API_BASE_URL, logger
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
    return (LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").rstrip("/")


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

    url = f"{data_api_base_url()}/data/{rel}"
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

    text = r.text or ""
    if not r.ok:
        snippet = text.strip().replace("\n", " ")[:800]
        return {
            "ok": False,
            "status": r.status_code,
            "error": r.reason or "HTTP error",
            "body_preview": snippet,
            "url": url,
        }

    truncated = False
    if len(text) > max_response_chars:
        truncated = True
        text = text[:max_response_chars]

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

    return {
        "ok": True,
        "status": r.status_code,
        "truncated": truncated,
        "url": url,
        "body": body,
    }
