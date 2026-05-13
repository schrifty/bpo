"""Opt-in live checks against LeanDNA Data API (read-only).

**This file logs secrets in full** (Bearer token, Cookie, curl replay) when the integration
test runs — intended for disposable sandbox tokens only.

By default CI and normal ``pytest`` runs **skip** these tests. To exercise real HTTP:

1. Set ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` (see ``docs/SETUP/LEANDNA_SETUP.md``).
2. Run with integration flag::

     BPO_LEANDNA_DATA_API_INTEGRATION=1 pytest tests/test_integration_leandna_data_api.py -v

   This test loads ``.env`` with ``override=True`` so values there replace any ``LEANDNA_*``
   already set in the process environment (stale exports otherwise win with ``override=False``).

Uses the OpenAPI **Metrics** catalog list — ``GET {LEANDNA_DATA_API_BASE_URL}/data/Metric`` — same
path as ``src.leandna_metrics_client.list_metric_definitions``, with the same auth headers as
item master / shortages / lean projects.
"""

from __future__ import annotations

import importlib
import logging
import os
import shlex
import time
from pathlib import Path
from urllib.parse import urlencode

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]

_LOG = logging.getLogger("integration_leandna_data_api")

# ``build_leandna_data_api_headers`` defaults to ``bpo-…`` User-Agent; Swagger runs in a browser.
# If staging returns 401 only from Python, try matching a normal browser UA (see LEANDNA_SETUP).
_SWAGGER_LIKE_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _ensure_verbose_logging() -> None:
    """Emit DEBUG to stderr for this test and LeanDNA / urllib3."""
    fmt = "%(asctime)s %(levelname)-5s [%(name)s] %(message)s"
    root = logging.getLogger()
    if not any(type(h) is logging.StreamHandler for h in root.handlers):
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(fmt))
        root.addHandler(h)
    root.setLevel(logging.DEBUG)
    for name in (
        "integration_leandna_data_api",
        "bpo",
        "src.leandna_data_api_http",
        "src.leandna_data_api_request",
        "urllib3.connectionpool",
    ):
        logging.getLogger(name).setLevel(logging.DEBUG)


def _credential_summary(_config: object) -> None:
    bt = (getattr(_config, "LEANDNA_DATA_API_BEARER_TOKEN", None) or "").strip()
    ck = (getattr(_config, "LEANDNA_DATA_API_COOKIE", None) or "").strip()
    _LOG.info("LEANDNA_DATA_API_BEARER_TOKEN=%r", bt)
    _LOG.info("LEANDNA_DATA_API_COOKIE=%r", ck)
    _LOG.info(
        "origin=%r referer=%r",
        (getattr(_config, "LEANDNA_DATA_API_ORIGIN", None) or "").strip() or "(default from base URL)",
        (getattr(_config, "LEANDNA_DATA_API_REFERER", None) or "").strip() or "(default)",
    )


def _log_headers(headers: dict[str, str]) -> None:
    _LOG.info("Request headers: %s", dict(headers))


def _integration_enabled() -> bool:
    raw = (os.environ.get("BPO_LEANDNA_DATA_API_INTEGRATION") or "").strip().lower()
    return raw in ("1", "true", "yes")


def _curl_equivalent(url: str, params: dict[str, str], headers: dict[str, str]) -> str:
    """Single-line curl for logs (verbatim Authorization / Cookie)."""
    q = urlencode(sorted(params.items())) if params else ""
    full_url = f"{url}?{q}" if q else url
    bits: list[str] = ["curl", "-sS", "-X", "GET", shlex.quote(full_url)]
    for name, val in sorted(headers.items()):
        bits.extend(["-H", shlex.quote(f"{name}: {val}")])
    return " ".join(bits)


@pytest.mark.leandna_data_api
@pytest.mark.skipif(
    not _integration_enabled(),
    reason="Set BPO_LEANDNA_DATA_API_INTEGRATION=1 to run live LeanDNA Data API tests",
)
def test_leandna_metrics_list_endpoint_live() -> None:
    """GET /data/Metric (metrics definitions list) returns 200 and JSON the client can parse."""
    _ensure_verbose_logging()
    _LOG.info("=== LeanDNA metrics integration test: begin ===")
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")
    # ``override=True``: if the shell or IDE already exported LEANDNA_* (e.g. an old token),
    # we still want this test to follow the repo ``.env`` you just edited.
    load_dotenv(_ROOT / ".env", override=True)
    _LOG.info("load_dotenv(%s, override=True) completed", _ROOT / ".env")

    # conftest (and plugins) may import ``src.config`` before ``load_dotenv`` runs; config
    # snapshots env at import time. Reload so LEANDNA_* from .env are visible to HTTP clients.
    import src.config as _config

    importlib.reload(_config)
    import src.leandna_data_api_http as _ld_http

    importlib.reload(_ld_http)
    import src.leandna_metrics_client as _ld_metrics

    importlib.reload(_ld_metrics)
    _LOG.info("Reloaded src.config, src.leandna_data_api_http, src.leandna_metrics_client")
    leandna_data_api_credentials_configured = _ld_http.leandna_data_api_credentials_configured
    unwrap_rows = _ld_metrics._unwrap_metric_definition_rows  # noqa: SLF001 — keep parse logic in sync

    _credential_summary(_config)

    if not leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA Data API credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN "
            "and/or LEANDNA_DATA_API_COOKIE"
        )

    base = (_config.LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").rstrip("/")
    metrics_list_url = f"{base}/data/Metric"
    headers = _ld_http.build_leandna_data_api_headers(
        requested_sites=None,
        user_agent_suffix="leandna-integration-test/1.0",
    )
    headers["User-Agent"] = _SWAGGER_LIKE_USER_AGENT
    _log_headers(headers)
    # Match tenant Swagger default try-out: ``GET …/data/Metric?metricTypes=Manual``.
    metric_params = {"metricTypes": "Manual"}
    _LOG.info("Curl equivalent: %s", _curl_equivalent(metrics_list_url, metric_params, headers))

    t0 = time.perf_counter()
    resp = requests.get(metrics_list_url, headers=headers, params=metric_params, timeout=90.0)
    _LOG.info("requests actually used URL: %s", resp.request.url)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    _LOG.info(
        "Response received: status=%s elapsed_ms=%.1f content_length_header=%r encoding=%r",
        resp.status_code,
        elapsed_ms,
        resp.headers.get("Content-Length"),
        resp.encoding,
    )
    _LOG.debug("Response headers: %s", dict(resp.headers))
    body_preview = (resp.text or "")[:1200]
    _LOG.info("Response body length=%s prefix (up to 1200 chars): %s", len(resp.text or ""), body_preview)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        snippet = (resp.text or "").strip().replace("\n", " ")[:800]
        _LOG.error("GET failed: %s body_prefix=%r", e, snippet)
        hint = ""
        if resp.status_code == 401 and "Session not found" in (resp.text or ""):
            hint = (
                " Hint: 401 Session not found usually means LEANDNA_DATA_API_BEARER_TOKEN in .env "
                "is not the same token Swagger/curl uses for this LEANDNA_DATA_API_BASE_URL."
            )
        pytest.fail(f"GET {resp.request.url} failed: {e!s} body_prefix={snippet!r}{hint}")

    rows = unwrap_rows(resp.json())
    _LOG.info("Parsed metric definition rows: count=%s (showing types of first 3)", len(rows))
    for i, row in enumerate(rows[:3]):
        _LOG.debug("Row %s type=%s keys_sample=%s", i, type(row).__name__, list(row.keys())[:12] if isinstance(row, dict) else None)
    assert isinstance(rows, list)
    # Tenants may return an empty catalog; HTTP 200 + parseable body is the contract we assert.
    for row in rows[:5]:
        assert isinstance(row, dict)
    _LOG.info("=== LeanDNA metrics integration test: success ===")
